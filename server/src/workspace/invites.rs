use barffine_core::workspace::WorkspaceInviteLinkRecord;
use chrono::Utc;

use crate::{AppError, AppState};

fn invite_link_expired(record: &WorkspaceInviteLinkRecord) -> bool {
    record.expires_at <= Utc::now().timestamp()
}

pub async fn find_active_invite_link(
    state: &AppState,
    workspace_id: &str,
) -> Result<Option<WorkspaceInviteLinkRecord>, AppError> {
    let link = state
        .workspace_store
        .find_invite_link(workspace_id)
        .await
        .map_err(AppError::from_anyhow)?;

    let Some(link) = link else {
        return Ok(None);
    };

    if invite_link_expired(&link) {
        state
            .workspace_store
            .delete_invite_link(workspace_id)
            .await
            .map_err(AppError::from_anyhow)?;
        return Ok(None);
    }

    Ok(Some(link))
}

pub enum InviteLinkLookup {
    Active(WorkspaceInviteLinkRecord),
    Missing,
    Expired,
}

pub async fn lookup_invite_link_by_token(
    state: &AppState,
    token: &str,
) -> Result<InviteLinkLookup, AppError> {
    let link = state
        .workspace_store
        .find_invite_link_by_token(token)
        .await
        .map_err(AppError::from_anyhow)?;

    let Some(link) = link else {
        return Ok(InviteLinkLookup::Missing);
    };

    if invite_link_expired(&link) {
        state
            .workspace_store
            .delete_invite_link(&link.workspace_id)
            .await
            .map_err(AppError::from_anyhow)?;
        return Ok(InviteLinkLookup::Expired);
    }

    Ok(InviteLinkLookup::Active(link))
}
