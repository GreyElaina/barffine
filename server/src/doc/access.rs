use barffine_core::{doc_store::DocumentMetadata, workspace::WorkspaceRecord};

use crate::{AppError, AppState};

pub use crate::doc::service::DocAuthorization;

pub async fn document_exists(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
) -> Result<bool, AppError> {
    state
        .doc_access_service
        .document_exists(workspace_id, doc_id)
        .await
}

pub async fn ensure_document_exists(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
) -> Result<(), AppError> {
    state
        .doc_access_service
        .ensure_document_exists(workspace_id, doc_id)
        .await
}

pub async fn resolve_doc_authorization(
    state: &AppState,
    workspace: &WorkspaceRecord,
    metadata: &DocumentMetadata,
    user_id: Option<&str>,
) -> Result<DocAuthorization, AppError> {
    state
        .doc_access_service
        .resolve_doc_authorization(workspace, metadata, user_id)
        .await
}
