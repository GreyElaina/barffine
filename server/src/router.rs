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

    let router = Router::new()
        // Health & Info
        .route("/", get(index_handler))
        .route("/health", get(health_handler))
        .route("/feature", get(feature_handler))
        .route("/info", get(info_handler))
        // Admin Setup
        .route(
            "/api/setup/create-admin-user",
            post(create_admin_user_handler),
        )
        .route("/create-admin-user", post(create_admin_user_handler))
        // Authentication
        .route("/preflight", post(preflight_handler))
        .route("/sign-in", post(sign_in_handler))
        .route("/session", get(get_session_handler))
        .route("/user", get(current_user_handler))
        .route("/sign-out", get(sign_out_handler))
        .route("/api/auth/preflight", post(preflight_handler))
        .route("/api/auth/sign-in", post(sign_in_handler))
        .route("/api/auth/session", get(get_session_handler))
        .route("/api/auth/user", get(current_user_handler))
        .route("/api/auth/sign-out", get(sign_out_handler))
        .merge(oauth_router())
        // Sessions
        .route(
            "/sessions",
            get(list_sessions_handler)
                .post(create_session_handler)
                .delete(delete_session_handler),
        )
        .route(
            "/api/auth/sessions",
            get(list_sessions_handler)
                .post(create_session_handler)
                .delete(delete_session_handler),
        )
        .route("/sessions/refresh", post(refresh_session_handler))
        .route("/api/auth/sessions/refresh", post(refresh_session_handler))
        // Users
        .route("/users", post(create_user_handler).get(get_users_handler))
        .route("/users/{id}", get(get_user_handler))
        .route("/api/avatars/{key}", get(get_avatar_handler))
        // Workspaces
        .route("/workspaces", post(create_workspace_handler))
        .route("/api/workspaces", post(create_workspace_handler))
        .route(
            "/workspaces/{workspace_id}/content",
            get(get_workspace_content_handler),
        )
        .route(
            "/api/workspaces/{workspace_id}/content",
            get(get_workspace_content_handler),
        )
        .route(
            "/rpc/workspaces/{workspace_id}/content",
            get(get_rpc_workspace_content_handler),
        )
        .route(
            "/workspaces/{workspace_id}/blobs/{name}",
            get(get_workspace_blob_handler),
        )
        .route(
            "/api/workspaces/{workspace_id}/blobs/{name}",
            get(get_workspace_blob_handler),
        )
        // Documents
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}",
            get(get_doc_binary_handler),
        )
        .route(
            "/api/workspaces/{workspace_id}/docs/{doc_id}",
            get(get_doc_binary_handler),
        )
        .route(
            "/rpc/workspaces/{workspace_id}/docs/{doc_id}",
            get(get_rpc_doc_binary_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/content",
            get(get_doc_content_handler),
        )
        .route(
            "/api/workspaces/{workspace_id}/docs/{doc_id}/content",
            get(get_doc_content_handler),
        )
        .route(
            "/rpc/workspaces/{workspace_id}/docs/{doc_id}/content",
            get(get_rpc_doc_content_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/markdown",
            get(get_doc_markdown_handler),
        )
        .route(
            "/api/workspaces/{workspace_id}/docs/{doc_id}/markdown",
            get(get_doc_markdown_handler),
        )
        .route(
            "/rpc/workspaces/{workspace_id}/docs/{doc_id}/markdown",
            get(get_rpc_doc_markdown_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/diff",
            post(get_doc_diff_handler),
        )
        .route(
            "/api/workspaces/{workspace_id}/docs/{doc_id}/diff",
            post(get_doc_diff_handler),
        )
        .route(
            "/rpc/workspaces/{workspace_id}/docs/{doc_id}/diff",
            post(get_rpc_doc_diff_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/public",
            post(publish_doc_handler).delete(unpublish_doc_handler),
        )
        .route(
            "/api/workspaces/{workspace_id}/docs/{doc_id}/public",
            post(publish_doc_handler).delete(unpublish_doc_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/history",
            get(get_doc_history_handler),
        )
        .route(
            "/api/workspaces/{workspace_id}/docs/{doc_id}/history",
            get(get_doc_history_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/histories/{timestamp}",
            get(get_doc_history_snapshot_handler),
        )
        .route(
            "/api/workspaces/{workspace_id}/docs/{doc_id}/histories/{timestamp}",
            get(get_doc_history_snapshot_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/ws",
            get(doc_ws_handler),
        )
        .route(
            "/api/workspaces/{workspace_id}/docs/{doc_id}/ws",
            get(doc_ws_handler),
        )
        .route(
            "/workspaces/{workspace_id}/docs/{doc_id}/comment-attachments/{key}",
            get(get_comment_attachment_handler),
        )
        .route(
            "/api/workspaces/{workspace_id}/docs/{doc_id}/comment-attachments/{key}",
            get(get_comment_attachment_handler),
        )
        // GraphQL
        .route(
            "/graphql",
            get(graphql::graphql_playground)
                .post(graphql::graphql_handler)
                .options(graphql::graphql_options_handler),
        );

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
