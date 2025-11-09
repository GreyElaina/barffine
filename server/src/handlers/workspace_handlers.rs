// Workspace management handlers

use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};

use crate::{
    error::AppError,
    http::append_set_cookie_headers,
    state::AppState,
    types::{CreateWorkspaceRequest, CreateWorkspaceResponse, WorkspaceContentResponse},
    workspace::service::{
        AccessTokenContext, AccessTokenVerification, WorkspaceCreationParams, WorkspaceService,
    },
};

pub(crate) async fn create_workspace_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CreateWorkspaceRequest>,
) -> Result<Response, AppError> {
    if payload.name.trim().is_empty() {
        return Err(AppError::bad_request("workspace name must not be empty"));
    }

    let auth = state
        .user_service
        .authenticate_rest_request(&headers)
        .await?;

    if let Some(requested_owner) = payload.owner_id.as_deref() {
        if requested_owner != auth.user.id {
            return Err(AppError::forbidden("workspace owner mismatch"));
        }
    }

    let workspace = state
        .workspace_service
        .create_workspace_with_defaults(WorkspaceCreationParams {
            owner_id: &auth.user.id,
            name: Some(payload.name.as_str()),
            public: None,
            enable_ai: None,
            enable_doc_embedding: None,
            enable_url_preview: None,
        })
        .await?;

    let response = CreateWorkspaceResponse {
        id: workspace.id,
        name: workspace.name,
    };

    let mut http_response = (StatusCode::CREATED, Json(response)).into_response();
    append_set_cookie_headers(&mut http_response, &auth.set_cookies)?;
    Ok(http_response)
}

pub(crate) async fn get_workspace_content_handler(
    Path(workspace_id): Path<String>,
    State(workspaces): State<Arc<WorkspaceService>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let response = workspace_content_response(&workspaces, &workspace_id, &headers, false).await?;

    Ok(Json(response))
}

pub(crate) async fn get_rpc_workspace_content_handler(
    Path(workspace_id): Path<String>,
    State(workspaces): State<Arc<WorkspaceService>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let response = workspace_content_response(&workspaces, &workspace_id, &headers, true).await?;

    Ok(Json(response))
}

pub(crate) async fn ensure_workspace_exists(
    state: &AppState,
    workspace_id: &str,
) -> Result<(), AppError> {
    state
        .workspace_service
        .ensure_workspace_exists(workspace_id)
        .await
}

async fn workspace_content_response(
    service: &WorkspaceService,
    workspace_id: &str,
    headers: &HeaderMap,
    require_token: bool,
) -> Result<WorkspaceContentResponse, AppError> {
    let token_status = service.verify_rpc_access_token(
        headers,
        workspace_id,
        &AccessTokenContext {
            workspace_id,
            doc_id: None,
            target: "workspace content",
        },
    )?;

    if require_token && matches!(token_status, AccessTokenVerification::NotPresent) {
        return Err(AppError::invalid_internal_request());
    }

    let workspace = service.fetch_workspace(workspace_id).await?;

    Ok(WorkspaceContentResponse {
        name: workspace.name,
        avatar_key: workspace.avatar_key,
        avatar_url: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::to_bytes,
        extract::{Path, State},
        http::{HeaderMap, StatusCode},
    };

    use crate::test_support::{seed_workspace, setup_state};

    #[tokio::test]
    async fn get_workspace_content_handler_returns_workspace() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, _owner_id) = seed_workspace(&state).await;

        let response = get_workspace_content_handler(
            Path(workspace_id.clone()),
            State(state.workspace_service.clone()),
            HeaderMap::new(),
        )
        .await
        .expect("workspace content response")
        .into_response();

        let (_parts, body) = response.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["name"], "Test Workspace");
    }

    #[tokio::test]
    async fn get_workspace_content_handler_returns_not_found() {
        let (_temp_dir, _database, state) = setup_state().await;
        let err = match get_workspace_content_handler(
            Path("missing-workspace".into()),
            State(state.workspace_service.clone()),
            HeaderMap::new(),
        )
        .await
        {
            Ok(_) => panic!("expected missing workspace to return error"),
            Err(err) => err,
        };
        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(payload.name, "SPACE_NOT_FOUND");
    }
}
