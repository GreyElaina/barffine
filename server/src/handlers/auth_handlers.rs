// Authentication and session management handlers

use std::sync::OnceLock;

use anyhow::Error as AnyError;
use argon2::password_hash::Error as PasswordHashError;
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header::SET_COOKIE},
    response::{IntoResponse, Response},
};
use barffine_core::feature::FeatureNamespace;
use serde_json::json;
use tokio::sync::Mutex;
use tracing::error;

use crate::{
    auth::{authenticate_with_password, generate_password_hash, pad_session_response},
    cookies::{
        USER_COOKIE_NAME, build_session_cookie, build_user_cookie, clear_session_cookie,
        clear_user_cookie, extract_cookie, extract_session_token,
    },
    error::AppError,
    http::append_set_cookie_headers,
    state::{AppState, FeatureSnapshotPayload},
    types::{
        CreateAdminUserRequest, CreateSessionRequest, CreateSessionResponse, CreateUserResponse,
        DeleteSessionRequest, PreflightRequest, PreflightResponse, RefreshSessionRequest,
        SessionUser, SessionUserPayload, SessionUsersPayload, SignInRequest,
    },
    user::helpers::is_valid_email,
};

static CREATE_ADMIN_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

pub(crate) async fn create_admin_user_handler(
    State(state): State<AppState>,
    Json(payload): Json<CreateAdminUserRequest>,
) -> Result<Response, AppError> {
    let mutex = CREATE_ADMIN_MUTEX.get_or_init(|| Mutex::new(()));
    let guard = mutex.lock().await;
    let result = create_admin_user_locked(&state, payload).await;
    drop(guard);
    result
}

async fn create_admin_user_locked(
    state: &AppState,
    payload: CreateAdminUserRequest,
) -> Result<Response, AppError> {
    let existing = state
        .user_store
        .count(None)
        .await
        .map_err(AppError::from_anyhow)?;
    if existing > 0 {
        return Err(AppError::forbidden("First user already created"));
    }

    let email = payload.email.trim();
    if !is_valid_email(email) {
        return Err(AppError::bad_request("invalid email address"));
    }

    if payload.password.is_empty() {
        return Err(AppError::bad_request("password is required"));
    }

    let password_hash = generate_password_hash(&payload.password)
        .map_err(|err: PasswordHashError| AppError::internal(err.into()))?;

    let user = state
        .user_store
        .create(email, &password_hash, None)
        .await
        .map_err(AppError::from_anyhow)?;

    state
        .user_store
        .add_admin(&user.id)
        .await
        .map_err(AppError::from_anyhow)?;

    let session = state
        .user_store
        .create_session(&user.id)
        .await
        .map_err(AppError::from_anyhow)?;

    let cookies = vec![
        build_session_cookie(&session.id, session.expires_at),
        build_user_cookie(&user.id, session.expires_at),
    ];

    let mut response = Json(CreateUserResponse {
        id: user.id.clone(),
        email: user.email.clone(),
        name: user.name.clone(),
    })
    .into_response();

    append_set_cookie_headers(&mut response, &cookies)?;
    Ok(response)
}

pub(crate) async fn preflight_handler(
    State(state): State<AppState>,
    Json(payload): Json<PreflightRequest>,
) -> Result<Response, AppError> {
    let email = payload.email.trim();
    if !is_valid_email(email) {
        return Err(AppError::bad_request("invalid email address"));
    }

    let preflight = match state
        .user_store
        .find_by_email(email)
        .await
        .map_err(AppError::from_anyhow)?
    {
        Some(user) => PreflightResponse {
            registered: !user.disabled,
            has_password: !user.password_hash.is_empty(),
        },
        None => PreflightResponse {
            registered: false,
            has_password: false,
        },
    };

    Ok(Json(preflight).into_response())
}

pub(crate) async fn get_session_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let session_lookup = pad_session_response(&state, &headers).await?;
    let payload = SessionUserPayload {
        user: session_lookup.user,
    };
    let mut response = Json(payload).into_response();
    append_set_cookie_headers(&mut response, &session_lookup.cookies)?;
    Ok(response)
}

pub(crate) async fn current_user_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let session_lookup = pad_session_response(&state, &headers).await?;
    let mut response = if let Some(user) = session_lookup.user {
        Json(user).into_response()
    } else {
        StatusCode::NO_CONTENT.into_response()
    };
    append_set_cookie_headers(&mut response, &session_lookup.cookies)?;
    Ok(response)
}

pub(crate) async fn list_sessions_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let session_lookup = pad_session_response(&state, &headers).await?;
    let users: Vec<SessionUser> = session_lookup.user.into_iter().collect();
    let payload = SessionUsersPayload { users };
    let mut response = Json(payload).into_response();
    append_set_cookie_headers(&mut response, &session_lookup.cookies)?;
    Ok(response)
}

pub(crate) async fn sign_out_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    if let Some(session_id) = extract_session_token(&headers) {
        state
            .user_store
            .delete_session(&session_id)
            .await
            .map_err(AppError::from_anyhow)?;
    }

    let mut response = Json(json!({})).into_response();
    append_set_cookie_headers(
        &mut response,
        &[clear_session_cookie(), clear_user_cookie()],
    )?;
    Ok(response)
}

pub(crate) async fn feature_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state
        .feature_service
        .snapshot(&FeatureNamespace::Global)
        .await
    {
        Ok(snapshot) => Json(FeatureSnapshotPayload::from(snapshot)).into_response(),
        Err(error) => {
            error!(?error, "failed to fetch feature snapshot");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub(crate) async fn create_session_handler(
    State(state): State<AppState>,
    Json(payload): Json<CreateSessionRequest>,
) -> Result<Response, AppError> {
    let CreateSessionRequest { email, password } = payload;
    let email = email.trim();
    let (user, session) = authenticate_with_password(&state, email, &password).await?;

    let session_cookie = build_session_cookie(&session.id, session.expires_at);
    let user_cookie = build_user_cookie(&user.id, session.expires_at);

    let response = CreateSessionResponse {
        session_id: session.id,
        user_id: user.id,
    };

    let mut resp = Json(response).into_response();
    *resp.status_mut() = StatusCode::CREATED;

    let session_header = HeaderValue::from_str(&session_cookie)
        .map_err(|err| AppError::internal(AnyError::new(err)))?;
    resp.headers_mut().append(SET_COOKIE, session_header);

    let user_header = HeaderValue::from_str(&user_cookie)
        .map_err(|err| AppError::internal(AnyError::new(err)))?;
    resp.headers_mut().append(SET_COOKIE, user_header);

    Ok(resp)
}

pub(crate) async fn sign_in_handler(
    State(state): State<AppState>,
    Json(payload): Json<SignInRequest>,
) -> Result<Response, AppError> {
    let SignInRequest {
        email, password, ..
    } = payload;
    let email = email.trim().to_owned();
    if !is_valid_email(&email) {
        return Err(AppError::bad_request("invalid email address"));
    }

    let password = password.ok_or_else(|| AppError::bad_request("password is required"))?;
    if password.is_empty() {
        return Err(AppError::bad_request("password is required"));
    }

    let (user, session) = authenticate_with_password(&state, &email, &password).await?;

    let cookies = vec![
        build_session_cookie(&session.id, session.expires_at),
        build_user_cookie(&user.id, session.expires_at),
    ];

    let mut response = Json(SessionUser::from(&user)).into_response();
    append_set_cookie_headers(&mut response, &cookies)?;
    Ok(response)
}

pub(crate) async fn delete_session_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Option<Json<DeleteSessionRequest>>,
) -> Result<Response, AppError> {
    let request = payload.map(|wrapper| wrapper.0).unwrap_or_default();

    let session_id = request
        .session_id
        .clone()
        .or_else(|| extract_session_token(&headers));

    let Some(session_id) = session_id else {
        let mut response = ().into_response();
        response.headers_mut().append(
            SET_COOKIE,
            HeaderValue::from_str(&clear_session_cookie())
                .map_err(|err| AppError::internal(AnyError::new(err)))?,
        );
        response.headers_mut().append(
            SET_COOKIE,
            HeaderValue::from_str(&clear_user_cookie())
                .map_err(|err| AppError::internal(AnyError::new(err)))?,
        );
        return Ok(response);
    };

    if let Some(session) = state
        .user_store
        .find_session(&session_id)
        .await
        .map_err(AppError::from_anyhow)?
    {
        let requester = request
            .user_id
            .clone()
            .or_else(|| extract_cookie(&headers, USER_COOKIE_NAME));

        if let Some(user_id) = requester {
            if session.user_id != user_id {
                return Err(AppError::unauthorized(
                    "session does not belong to requesting user",
                ));
            }
        }
    }

    state
        .user_store
        .delete_session(&session_id)
        .await
        .map_err(AppError::from_anyhow)?;

    let mut response = ().into_response();
    response.headers_mut().append(
        SET_COOKIE,
        HeaderValue::from_str(&clear_session_cookie())
            .map_err(|err| AppError::internal(AnyError::new(err)))?,
    );
    response.headers_mut().append(
        SET_COOKIE,
        HeaderValue::from_str(&clear_user_cookie())
            .map_err(|err| AppError::internal(AnyError::new(err)))?,
    );

    Ok(response)
}

pub(crate) async fn refresh_session_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Option<Json<RefreshSessionRequest>>,
) -> Result<Response, AppError> {
    let request = payload.map(|wrapper| wrapper.0).unwrap_or_default();

    let session_id = request
        .session_id
        .clone()
        .or_else(|| extract_session_token(&headers))
        .ok_or_else(|| AppError::unauthorized("session id is required"))?;

    let Some(session) = state
        .user_store
        .refresh_session(&session_id)
        .await
        .map_err(AppError::from_anyhow)?
    else {
        let mut response = ().into_response();
        response.headers_mut().append(
            SET_COOKIE,
            HeaderValue::from_str(&clear_session_cookie())
                .map_err(|err| AppError::internal(AnyError::new(err)))?,
        );
        response.headers_mut().append(
            SET_COOKIE,
            HeaderValue::from_str(&clear_user_cookie())
                .map_err(|err| AppError::internal(AnyError::new(err)))?,
        );
        *response.status_mut() = StatusCode::UNAUTHORIZED;
        return Ok(response);
    };

    let requester = request
        .user_id
        .clone()
        .or_else(|| extract_cookie(&headers, USER_COOKIE_NAME));
    if let Some(user_id) = requester {
        if session.user_id != user_id {
            return Err(AppError::unauthorized(
                "session does not belong to requesting user",
            ));
        }
    }

    let session_cookie =
        HeaderValue::from_str(&build_session_cookie(&session.id, session.expires_at))
            .map_err(|err| AppError::internal(AnyError::new(err)))?;
    let user_cookie =
        HeaderValue::from_str(&build_user_cookie(&session.user_id, session.expires_at))
            .map_err(|err| AppError::internal(AnyError::new(err)))?;

    let mut response = Json(CreateSessionResponse {
        session_id: session.id.clone(),
        user_id: session.user_id.clone(),
    })
    .into_response();
    response.headers_mut().append(SET_COOKIE, session_cookie);
    response.headers_mut().append(SET_COOKIE, user_cookie);

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json,
        body::to_bytes,
        extract::State,
        http::{
            HeaderMap, HeaderValue, StatusCode,
            header::{COOKIE, SET_COOKIE},
        },
    };
    use serde_json::Value as JsonValue;
    use sqlx::query;

    use crate::{
        auth::generate_password_hash,
        cookies::{SESSION_COOKIE_NAME, USER_COOKIE_NAME},
        test_support::setup_state,
        types::{CreateAdminUserRequest, CreateSessionRequest, PreflightRequest, SignInRequest},
    };

    #[tokio::test]
    async fn create_session_sets_cookies() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("cookie@example.com", &password_hash, None)
            .await
            .expect("create user");

        let response = create_session_handler(
            State(state.clone()),
            Json(CreateSessionRequest {
                email: user.email.clone(),
                password: "secret".into(),
            }),
        )
        .await
        .expect("create session");
        assert_eq!(response.status(), StatusCode::CREATED);

        let header_map = response.headers().clone();
        let cookies: Vec<String> = header_map
            .get_all(SET_COOKIE)
            .iter()
            .map(|value| value.to_str().expect("valid cookie").to_string())
            .collect();

        assert_eq!(cookies.len(), 2);

        let session_cookie = cookies
            .iter()
            .find(|value| value.starts_with(&format!("{SESSION_COOKIE_NAME}=")))
            .expect("session cookie present");
        assert!(session_cookie.contains("HttpOnly"));
        assert!(session_cookie.contains("SameSite=Lax"));

        let user_cookie = cookies
            .iter()
            .find(|value| value.starts_with(&format!("{USER_COOKIE_NAME}=")))
            .expect("user cookie present");
        assert!(
            !user_cookie.contains("HttpOnly"),
            "user cookie should be client-readable"
        );
    }

    #[tokio::test]
    async fn preflight_handler_returns_false_for_unknown_user() {
        let (_temp_dir, _database, state) = setup_state().await;

        let response = preflight_handler(
            State(state.clone()),
            Json(PreflightRequest {
                email: "nobody@example.com".into(),
            }),
        )
        .await
        .expect("preflight response");

        assert_eq!(response.status(), StatusCode::OK);
        let (_parts, body) = response.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["registered"], false);
        assert_eq!(json["hasPassword"], false);
    }

    #[tokio::test]
    async fn preflight_handler_returns_true_for_existing_user() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        state
            .user_store
            .create("known@example.com", &password_hash, None)
            .await
            .expect("create user");

        let response = preflight_handler(
            State(state.clone()),
            Json(PreflightRequest {
                email: "known@example.com".into(),
            }),
        )
        .await
        .expect("preflight response");

        assert_eq!(response.status(), StatusCode::OK);
        let (_parts, body) = response.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["registered"], true);
        assert_eq!(json["hasPassword"], true);
    }

    #[tokio::test]
    async fn create_admin_user_handler_creates_first_user() {
        let (_temp_dir, _database, state) = setup_state().await;

        let response = create_admin_user_handler(
            State(state.clone()),
            Json(CreateAdminUserRequest {
                email: "admin@example.com".into(),
                password: "super-secret".into(),
            }),
        )
        .await
        .expect("create admin user");

        assert_eq!(response.status(), StatusCode::OK);
        let (parts, _body) = response.into_parts();
        let cookies: Vec<_> = parts.headers.get_all(SET_COOKIE).iter().collect();
        assert!(cookies.iter().any(|value| {
            value
                .to_str()
                .unwrap()
                .starts_with(&format!("{}=", SESSION_COOKIE_NAME))
        }));
        assert!(cookies.iter().any(|value| {
            value
                .to_str()
                .unwrap()
                .starts_with(&format!("{}=", USER_COOKIE_NAME))
        }));
    }

    #[tokio::test]
    async fn create_admin_user_handler_rejects_when_initialized() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        state
            .user_store
            .create("existing@example.com", &password_hash, None)
            .await
            .expect("create user");

        let result = create_admin_user_handler(
            State(state.clone()),
            Json(CreateAdminUserRequest {
                email: "admin@example.com".into(),
                password: "super-secret".into(),
            }),
        )
        .await;

        let err = result.expect_err("should reject when user exists");
        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(payload.message, "First user already created");
    }

    #[tokio::test]
    async fn sign_in_handler_requires_password_payload() {
        let (_temp_dir, _database, state) = setup_state().await;

        let err = sign_in_handler(
            State(state.clone()),
            Json(SignInRequest {
                email: "user@example.com".into(),
                password: None,
                callback_url: None,
                client_nonce: None,
                redirect_uri: None,
                provider: None,
            }),
        )
        .await
        .expect_err("missing password should error");

        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(payload.message, "password is required");
    }

    #[tokio::test]
    async fn sign_in_handler_returns_session_user() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("signin@example.com", &password_hash, Some("Sign In"))
            .await
            .expect("create user");

        let response = sign_in_handler(
            State(state.clone()),
            Json(SignInRequest {
                email: user.email.clone(),
                password: Some("secret".into()),
                callback_url: None,
                client_nonce: None,
                redirect_uri: None,
                provider: None,
            }),
        )
        .await
        .expect("sign-in response");

        assert_eq!(response.status(), StatusCode::OK);
        let (parts, body) = response.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["id"], user.id);
        assert_eq!(json["email"], user.email);
        assert_eq!(json["name"], "Sign In");

        let cookies: Vec<_> = parts.headers.get_all(SET_COOKIE).iter().collect();
        assert!(cookies.iter().any(|value| {
            value
                .to_str()
                .unwrap()
                .starts_with(&format!("{}=", SESSION_COOKIE_NAME))
        }));
        assert!(cookies.iter().any(|value| {
            value
                .to_str()
                .unwrap()
                .starts_with(&format!("{}=", USER_COOKIE_NAME))
        }));
    }

    #[tokio::test]
    async fn current_user_handler_returns_no_content_without_session() {
        let (_temp_dir, _database, state) = setup_state().await;
        let headers = HeaderMap::new();
        let response = current_user_handler(State(state.clone()), headers)
            .await
            .expect("current user response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn current_user_handler_returns_user_with_session() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("current@example.com", &password_hash, Some("Current"))
            .await
            .expect("create user");
        let session = state
            .user_store
            .create_session(&user.id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!(
                "{}={}; {}={}",
                SESSION_COOKIE_NAME, session.id, USER_COOKIE_NAME, user.id
            ))
            .expect("cookie header"),
        );

        let response = current_user_handler(State(state.clone()), headers)
            .await
            .expect("current user response");

        assert_eq!(response.status(), StatusCode::OK);
        let (parts, body) = response.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["id"], user.id);
        assert_eq!(json["email"], user.email);
        assert_eq!(json["name"], "Current");

        let cookies: Vec<_> = parts.headers.get_all(SET_COOKIE).iter().collect();
        assert!(cookies.iter().any(|value| {
            value
                .to_str()
                .unwrap()
                .starts_with(&format!("{}=", SESSION_COOKIE_NAME))
        }));
        assert!(cookies.iter().any(|value| {
            value
                .to_str()
                .unwrap()
                .starts_with(&format!("{}=", USER_COOKIE_NAME))
        }));
    }

    #[tokio::test]
    async fn delete_session_handler_clears_cookies() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("deleter@example.com", &password_hash, None)
            .await
            .expect("create user");
        let session = state
            .user_store
            .create_session(&user.id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!(
                "{}={}; {}={}",
                SESSION_COOKIE_NAME, session.id, USER_COOKIE_NAME, user.id
            ))
            .expect("cookie header"),
        );

        let response = delete_session_handler(State(state.clone()), headers, None)
            .await
            .expect("delete session response");

        assert_eq!(response.status(), StatusCode::OK);
        let stored = state
            .user_store
            .find_session(&session.id)
            .await
            .expect("query session");
        assert!(stored.is_none(), "session should be removed");

        let set_cookies: Vec<_> = response.headers().get_all(SET_COOKIE).iter().collect();
        assert!(set_cookies.iter().any(|value| {
            value
                .to_str()
                .unwrap()
                .starts_with(&format!("{}=", SESSION_COOKIE_NAME))
        }));
    }

    #[tokio::test]
    async fn refresh_session_handler_extends_expiry() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("refresher@example.com", &password_hash, None)
            .await
            .expect("create user");
        let session = state
            .user_store
            .create_session(&user.id)
            .await
            .expect("create session");
        let original_expiry = session.expires_at;

        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!(
                "{}={}; {}={}",
                SESSION_COOKIE_NAME, session.id, USER_COOKIE_NAME, user.id
            ))
            .expect("cookie header"),
        );

        let response = refresh_session_handler(State(state.clone()), headers, None)
            .await
            .expect("refresh session response");

        assert_eq!(response.status(), StatusCode::OK);
        let (parts, body) = response.into_parts();
        let refreshed = state
            .user_store
            .find_session(&session.id)
            .await
            .expect("query session")
            .expect("session present");
        assert!(refreshed.expires_at >= original_expiry);

        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["session_id"], session.id);
        assert_eq!(json["user_id"], user.id);

        let set_cookies: Vec<_> = parts.headers.get_all(SET_COOKIE).iter().collect();
        assert!(set_cookies.iter().any(|value| {
            value
                .to_str()
                .unwrap()
                .starts_with(&format!("{}=", SESSION_COOKIE_NAME))
        }));
    }

    #[tokio::test]
    async fn get_session_handler_returns_current_user() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create(
                "rest-session@example.com",
                &password_hash,
                Some("Rest User"),
            )
            .await
            .expect("create user");
        let session = state
            .user_store
            .create_session(&user.id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!(
                "{}={}; {}={}",
                SESSION_COOKIE_NAME, session.id, USER_COOKIE_NAME, user.id
            ))
            .expect("cookie header"),
        );

        let response = get_session_handler(State(state.clone()), headers)
            .await
            .expect("session response");
        assert_eq!(response.status(), StatusCode::OK);

        let (parts, body) = response.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        let user_json = json.get("user").expect("user present");
        assert_eq!(user_json["id"], user.id);
        assert_eq!(user_json["email"], user.email);
        assert_eq!(user_json["hasPassword"], true);
        assert_eq!(user_json["emailVerified"], false);

        let set_cookies: Vec<_> = parts.headers.get_all(SET_COOKIE).iter().collect();
        assert!(set_cookies.iter().any(|value| {
            value
                .to_str()
                .unwrap()
                .starts_with(&format!("{}=", SESSION_COOKIE_NAME))
        }));
        assert!(set_cookies.iter().any(|value| {
            value
                .to_str()
                .unwrap()
                .starts_with(&format!("{}=", USER_COOKIE_NAME))
        }));
    }

    #[tokio::test]
    async fn get_session_handler_clears_invalid_session() {
        let (_temp_dir, _database, state) = setup_state().await;
        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!("{}=invalid", SESSION_COOKIE_NAME))
                .expect("cookie header"),
        );

        let response = get_session_handler(State(state.clone()), headers)
            .await
            .expect("session response");
        assert_eq!(response.status(), StatusCode::OK);

        let (parts, body) = response.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert!(json.get("user").is_none(), "user should be absent");

        let set_cookies: Vec<_> = parts.headers.get_all(SET_COOKIE).iter().collect();
        assert!(set_cookies.iter().any(|value| {
            let value = value.to_str().unwrap();
            value.starts_with(&format!("{}=", SESSION_COOKIE_NAME)) && value.contains("Max-Age=0")
        }));
    }

    #[tokio::test]
    async fn list_sessions_handler_returns_single_user() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("rest-sessions@example.com", &password_hash, None)
            .await
            .expect("create user");
        let session = state
            .user_store
            .create_session(&user.id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!(
                "{}={}; {}={}",
                SESSION_COOKIE_NAME, session.id, USER_COOKIE_NAME, user.id
            ))
            .expect("cookie header"),
        );

        let response = list_sessions_handler(State(state.clone()), headers)
            .await
            .expect("sessions response");
        assert_eq!(response.status(), StatusCode::OK);

        let (parts, body) = response.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        let users = json
            .get("users")
            .and_then(|value| value.as_array())
            .expect("users array");
        assert_eq!(users.len(), 1);
        assert_eq!(users[0]["id"], user.id);

        let set_cookies: Vec<_> = parts.headers.get_all(SET_COOKIE).iter().collect();
        assert!(set_cookies.iter().any(|value| {
            value
                .to_str()
                .unwrap()
                .starts_with(&format!("{}=", SESSION_COOKIE_NAME))
        }));
    }

    #[tokio::test]
    async fn sign_out_handler_removes_session_and_clears_cookies() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("signout@example.com", &password_hash, None)
            .await
            .expect("create user");
        let session = state
            .user_store
            .create_session(&user.id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!(
                "{}={}; {}={}",
                SESSION_COOKIE_NAME, session.id, USER_COOKIE_NAME, user.id
            ))
            .expect("cookie header"),
        );

        let response = sign_out_handler(State(state.clone()), headers)
            .await
            .expect("sign-out response");
        assert_eq!(response.status(), StatusCode::OK);

        let stored = state
            .user_store
            .find_session(&session.id)
            .await
            .expect("query session");
        assert!(stored.is_none(), "session should be deleted");

        let set_cookies: Vec<_> = response.headers().get_all(SET_COOKIE).iter().collect();
        assert!(set_cookies.iter().any(|value| {
            let value = value.to_str().unwrap();
            value.starts_with(&format!("{}=", SESSION_COOKIE_NAME)) && value.contains("Max-Age=0")
        }));
        assert!(set_cookies.iter().any(|value| {
            let value = value.to_str().unwrap();
            value.starts_with(&format!("{}=", USER_COOKIE_NAME)) && value.contains("Max-Age=0")
        }));
    }

    #[tokio::test]
    async fn create_session_rejects_disabled_user() {
        let (_temp_dir, database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("disabled@example.com", &password_hash, None)
            .await
            .expect("create user");

        query("UPDATE users SET disabled = 1 WHERE id = ?")
            .bind(&user.id)
            .execute(database.pool())
            .await
            .expect("disable user");

        let result = create_session_handler(
            State(state.clone()),
            Json(CreateSessionRequest {
                email: user.email.clone(),
                password: "secret".into(),
            }),
        )
        .await;

        let err = result.expect_err("disabled user should not authenticate");
        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(payload.message, "invalid credentials");
    }
}
