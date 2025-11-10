use async_graphql::http::{GraphQLPlaygroundConfig, playground_source};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    Extension,
    body::Body,
    extract::State,
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{self, SET_COOKIE},
    },
    response::{IntoResponse, Response},
};
use std::time::Instant;
use tracing::Instrument;
use tracing::{info, warn};

use crate::{
    AppState,
    cookies::{
        build_session_cookie, build_user_cookie, clear_session_cookie, clear_user_cookie,
        extract_session_token,
    },
    observability::{self, RequestContext},
};

use super::{BarffineSchema, RequestUser, context::ClientVersion, metrics::GRAPHQL_METRICS};

pub async fn graphql_handler(
    Extension(schema): Extension<BarffineSchema>,
    State(state): State<AppState>,
    Extension(request_context): Extension<RequestContext>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> Response {
    let mut request = req.into_inner();
    let mut set_cookies: Vec<String> = Vec::new();
    let mut active_user: Option<String> = None;
    let mut active_session: Option<String> = None;
    let mut refreshed_session = false;
    let mut cleared_cookies = false;

    if let Some(session_token) = extract_session_token(&headers) {
        match state.user_store.refresh_session(&session_token).await {
            Ok(Some(session)) => {
                active_user = Some(session.user_id.clone());
                active_session = Some(session.id.clone());
                request = request.data(RequestUser::with_session(
                    session.user_id.clone(),
                    session.id.clone(),
                ));

                set_cookies.push(build_session_cookie(&session.id, session.expires_at));
                set_cookies.push(build_user_cookie(&session.user_id, session.expires_at));
                refreshed_session = true;
            }
            Ok(None) => {
                set_cookies.push(clear_session_cookie());
                set_cookies.push(clear_user_cookie());
                cleared_cookies = true;
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "failed to refresh session cookie"
                );
                set_cookies.push(clear_session_cookie());
                set_cookies.push(clear_user_cookie());
                cleared_cookies = true;
            }
        }
    }

    observability::record_authenticated_identity(active_user.as_deref(), active_session.as_deref());

    let client_version = request_context
        .client_version()
        .and_then(|raw| semver::Version::parse(raw).ok());
    request = request.data(ClientVersion::new(client_version));

    let client_version_label = request_context.client_version().unwrap_or("unknown");

    let operation_name = request
        .operation_name
        .clone()
        .unwrap_or_else(|| "anonymous".to_string());

    let operation_label = operation_name.as_str();

    let graph_span = logfire::span!(
        "graphql {operation}",
        operation = operation_label,
        user_id = active_user.as_deref().unwrap_or("anonymous"),
        session_id = active_session.as_deref(),
        has_session = active_session.is_some(),
        request_id = request_context.request_id(),
        client_version = client_version_label,
        refreshed_session = refreshed_session,
        cleared_cookies = cleared_cookies,
        set_cookie_count = set_cookies.len() as i64
    );

    info!(
        operation = %operation_label,
        user_id = active_user.as_deref().unwrap_or("anonymous"),
        session_id = active_session.as_deref().unwrap_or(""),
        has_session = active_session.is_some(),
        "graphql request started"
    );

    let start = Instant::now();
    let execution = schema.execute(request).instrument(graph_span).await;

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    let has_errors = !execution.errors.is_empty();

    if has_errors {
        for error in &execution.errors {
            warn!(
                operation = %operation_label,
                user_id = active_user.as_deref().unwrap_or("anonymous"),
                message = %error.message,
                "graphql execution returned error"
            );
        }
    }

    info!(
        operation = %operation_label,
        duration_ms = elapsed_ms,
        errors = has_errors,
        user_id = active_user.as_deref().unwrap_or("anonymous"),
        session_id = active_session.as_deref().unwrap_or(""),
        "graphql completed"
    );

    GRAPHQL_METRICS.observe(
        operation_label,
        elapsed_ms,
        &request_context,
        request_context.client_version(),
        active_session.is_some(),
        has_errors,
    );

    let mut response: Response = GraphQLResponse::from(execution).into_response();
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/json; charset=utf-8"),
    );

    for cookie in set_cookies {
        if let Ok(value) = HeaderValue::from_str(&cookie) {
            response.headers_mut().append(SET_COOKIE, value);
        }
    }

    response
}

pub async fn graphql_playground(State(state): State<AppState>) -> impl IntoResponse {
    let endpoint = state
        .server_path
        .as_deref()
        .map(|prefix| format!("{prefix}/graphql"))
        .unwrap_or_else(|| "/graphql".to_string());

    Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .body(playground_source(GraphQLPlaygroundConfig::new(&endpoint)))
        .unwrap()
}

pub async fn graphql_options_handler(headers: HeaderMap) -> Response {
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());

    let allow_headers = headers
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
        .unwrap_or_else(|| "content-type, authorization".to_string());

    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;

    let headers_mut = response.headers_mut();

    if let Some(origin_value) = origin {
        if let Ok(value) = HeaderValue::from_str(&origin_value) {
            headers_mut.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
        }
        headers_mut.insert(
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        );
    } else {
        headers_mut.insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        );
    }

    headers_mut.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );

    if let Ok(value) = HeaderValue::from_str(&allow_headers) {
        headers_mut.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, value);
    }

    headers_mut.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("600"),
    );

    headers_mut.insert(header::VARY, HeaderValue::from_static("Origin"));
    headers_mut.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cookies::{SESSION_COOKIE_NAME, USER_COOKIE_NAME},
        graphql, observability,
        test_support::{seed_workspace, setup_state},
    };
    use axum::{
        Extension, Router,
        body::{Body, to_bytes},
        http::{
            Request, StatusCode,
            header::{
                self, ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS,
                ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_MAX_AGE,
                ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, CONTENT_LENGTH,
                CONTENT_TYPE, ORIGIN, SET_COOKIE, VARY,
            },
        },
        routing::{get, post},
    };
    use serde_json::json;
    use tower::ServiceExt;

    #[tokio::test]
    async fn graphql_workspace_role_uses_session_cookie() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, user_id) = seed_workspace(&state).await;

        let session = state
            .user_store
            .create_session(&user_id)
            .await
            .expect("create session");

        let schema = graphql::build_schema(state.clone());
        let query = format!(
            r#"{{ workspace(id: "{workspace_id}") {{ role permissions {{ Workspace_CreateDoc }} }} }}"#
        );

        let app = Router::new()
            .route("/graphql", post(graphql_handler))
            .layer(Extension(schema))
            .layer(observability::request_context_layer())
            .with_state(state.clone());

        let body = json!({ "query": query }).to_string();
        let request = Request::builder()
            .method("POST")
            .uri("/graphql")
            .header(CONTENT_TYPE, "application/json")
            .header(
                header::COOKIE,
                format!(
                    "{}={}; {}={}",
                    SESSION_COOKIE_NAME, session.id, USER_COOKIE_NAME, user_id
                ),
            )
            .body(Body::from(body))
            .expect("build request");

        let response = app.oneshot(request).await.expect("graphql response");

        let header_map = response.headers().clone();
        let set_cookies: Vec<String> = header_map
            .get_all(SET_COOKIE)
            .iter()
            .map(|value| value.to_str().expect("valid cookie").to_string())
            .collect();
        assert!(
            set_cookies
                .iter()
                .any(|value| value.starts_with(SESSION_COOKIE_NAME)),
            "session cookie should be reissued"
        );
        assert!(
            set_cookies
                .iter()
                .any(|value| value.starts_with(USER_COOKIE_NAME)),
            "user cookie should be reissued"
        );

        let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["data"]["workspace"]["role"], "Owner");
        assert_eq!(
            json["data"]["workspace"]["permissions"]["Workspace_CreateDoc"],
            true
        );
    }

    #[tokio::test]
    async fn graphql_options_handler_provides_cors_headers() {
        let (_temp_dir, _database, state) = setup_state().await;
        let schema = graphql::build_schema(state.clone());

        let app = Router::new()
            .route(
                "/graphql",
                get(graphql_playground)
                    .post(graphql_handler)
                    .options(graphql_options_handler),
            )
            .layer(Extension(schema))
            .layer(observability::request_context_layer())
            .with_state(state);

        let request = Request::builder()
            .method("OPTIONS")
            .uri("/graphql")
            .header(ORIGIN, "https://app.affine.pro")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(
                ACCESS_CONTROL_REQUEST_HEADERS,
                "content-type, x-affine-token",
            )
            .body(Body::empty())
            .expect("options request");

        let response = app
            .oneshot(request)
            .await
            .expect("graphql options response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let headers = response.headers();

        assert_eq!(
            headers
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("https://app.affine.pro")
        );
        assert_eq!(
            headers
                .get(ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        assert_eq!(
            headers
                .get(ACCESS_CONTROL_ALLOW_METHODS)
                .and_then(|value| value.to_str().ok()),
            Some("GET, POST, OPTIONS")
        );
        assert_eq!(
            headers
                .get(ACCESS_CONTROL_ALLOW_HEADERS)
                .and_then(|value| value.to_str().ok()),
            Some("content-type, x-affine-token")
        );
        assert_eq!(
            headers
                .get(ACCESS_CONTROL_MAX_AGE)
                .and_then(|value| value.to_str().ok()),
            Some("600")
        );
        assert_eq!(
            headers.get(VARY).and_then(|value| value.to_str().ok()),
            Some("Origin")
        );
        assert_eq!(
            headers
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some("0")
        );
    }
}
