// Health check and info endpoints

use axum::{Json, extract::State, response::IntoResponse};

use crate::{state::AppState, types::HealthResponse};

pub(crate) async fn health_handler() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

pub(crate) async fn info_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.metadata.clone())
}

pub(crate) async fn index_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.metadata.clone())
}
