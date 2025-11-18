// Router configuration

use axum::{
    Extension, Router,
    http::Method,
    routing::{get, post},
};
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_tracing_opentelemetry::middleware::{OtelAxumLayer, OtelInResponseLayer};
use tower_http::{
    cors::{AllowHeaders, AllowOrigin, CorsLayer},
    trace::TraceLayer,
};

use crate::oauth::oauth_router;
use crate::{
    graphql,
    handlers::{
        auth_handlers::*, blob_handlers::*, doc_handlers::*, health_handlers::*, user_handlers::*,
        workspace_handlers::*,
    },
    observability,
    state::AppState,
};

pub fn build_router(state: AppState) -> Router {
    let prefix = state.server_path.clone();
    let router = build_base_router(state);
    if let Some(path) = prefix {
        Router::new().nest(&path, router)
    } else {
        router
    }
}

fn build_base_router(state: AppState) -> Router {
    let schema = graphql::build_schema(state.clone());
    let (socket_layer, socket_io) = crate::socket::build_socket_layer(state.clone());
    let _ = state.socket_io.set(socket_io);

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::mirror_request())
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers(AllowHeaders::mirror_request())
        .allow_credentials(true);

    let api_scope = if state
        .server_path
        .as_deref()
        .is_some_and(|prefix| prefix.ends_with("/api"))
    {
        api_router()
    } else {
        with_api_prefix(api_router())
    };

    let router = Router::new()
        .route("/", get(index_handler))
        .merge(api_scope)
        .route(
            "/graphql",
            get(graphql::graphql_playground)
                .post(graphql::graphql_handler)
                .options(graphql::graphql_options_handler),
        );

    let router = add_legacy_doc_service_routes(router);

    let router = router
        .layer(socket_layer)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(observability::http_make_span())
                .on_response(observability::response_logger()),
        )
        .layer(cors)
        .layer(HttpMetricsLayerBuilder::new().build());

    let router = if observability::otel_layers_enabled() {
        router
            .layer(OtelInResponseLayer::default())
            .layer(OtelAxumLayer::default().filter(observability::should_sample_path))
    } else {
        router
    };

    router
        .layer(Extension(schema))
        .layer(observability::request_context_layer())
        .with_state(state)
}

fn add_legacy_doc_service_routes(router: Router<AppState>) -> Router<AppState> {
    router
}

fn api_router() -> Router<AppState> {
    Router::new()
        .merge(status_router())
        .nest("/setup", setup_router())
        .nest("/auth", auth_router())
        .merge(user_router())
        .merge(workspace_router())
        .merge(oauth_router())
}

fn status_router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health_handler))
        .route("/feature", get(feature_handler))
        .route("/info", get(info_handler))
}

fn setup_router() -> Router<AppState> {
    Router::new().route("/create-admin-user", post(create_admin_user_handler))
}

fn auth_router() -> Router<AppState> {
    Router::new()
        .route("/preflight", post(preflight_handler))
        .route("/sign-in", post(sign_in_handler))
        .route("/session", get(get_session_handler))
        .route("/user", get(current_user_handler))
        .route("/sign-out", get(sign_out_handler))
        .route(
            "/sessions",
            get(list_sessions_handler)
                .post(create_session_handler)
                .delete(delete_session_handler),
        )
        .route("/sessions/refresh", post(refresh_session_handler))
}

fn user_router() -> Router<AppState> {
    Router::new()
        .route("/users", post(create_user_handler).get(get_users_handler))
        .route("/users/{id}", get(get_user_handler))
        .route("/avatars/{key}", get(get_avatar_handler))
}

fn workspace_router() -> Router<AppState> {
    Router::new()
        .route("/workspaces", post(create_workspace_handler))
        .route(
            "/workspaces/{workspace_id}/content",
            get(get_workspace_content_handler),
        )
        .route(
            "/workspaces/{workspace_id}/blobs/{name}",
            get(get_workspace_blob_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}",
            get(get_doc_binary_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/content",
            get(get_doc_content_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/markdown",
            get(get_doc_markdown_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/diff",
            post(get_doc_diff_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/public",
            post(publish_doc_handler).delete(unpublish_doc_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/history",
            get(get_doc_history_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/histories/{timestamp}",
            get(get_doc_history_snapshot_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/comment-attachments/{key}",
            get(get_comment_attachment_handler),
        )
}

fn with_api_prefix(router: Router<AppState>) -> Router<AppState> {
    Router::new().nest("/api", router)
}
