use barffine_core::doc_store::DocumentMetadata;

use crate::{AppError, AppState, utils::paths::public_doc_share_path};

use super::{access, mode::DocPublishMode};

pub fn public_share_url(metadata: &DocumentMetadata) -> Option<String> {
    if metadata.public {
        Some(public_doc_share_path(&metadata.workspace_id, &metadata.id))
    } else {
        None
    }
}

pub async fn fetch_required(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
) -> Result<DocumentMetadata, AppError> {
    state
        .document_store
        .find_metadata(workspace_id, doc_id)
        .await
        .map_err(AppError::from_anyhow)?
        .ok_or_else(|| AppError::doc_not_found(workspace_id, doc_id))
}

pub async fn publish_doc(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
    mode: DocPublishMode,
) -> Result<DocumentMetadata, AppError> {
    ensure_workspace_present(state, workspace_id).await?;
    access::ensure_document_exists(state, workspace_id, doc_id).await?;

    state
        .document_store
        .publish_doc(workspace_id, doc_id, mode.as_str())
        .await
        .map_err(AppError::from_anyhow)?
        .ok_or_else(|| AppError::doc_not_found(workspace_id, doc_id))
}

pub async fn unpublish_doc(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
) -> Result<DocumentMetadata, AppError> {
    ensure_workspace_present(state, workspace_id).await?;
    access::ensure_document_exists(state, workspace_id, doc_id).await?;

    let is_public = state
        .document_store
        .is_public(workspace_id, doc_id)
        .await
        .map_err(AppError::from_anyhow)?;

    if !is_public {
        return Err(AppError::bad_request("doc is not public"));
    }

    state
        .document_store
        .unpublish_doc(workspace_id, doc_id)
        .await
        .map_err(AppError::from_anyhow)?
        .ok_or_else(|| AppError::doc_not_found(workspace_id, doc_id))
}

async fn ensure_workspace_present(state: &AppState, workspace_id: &str) -> Result<(), AppError> {
    let exists = state
        .workspace_service
        .workspace_exists(workspace_id)
        .await?;
    if exists {
        Ok(())
    } else {
        Err(AppError::workspace_not_found(workspace_id))
    }
}
