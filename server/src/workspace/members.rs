use barffine_core::workspace::WorkspaceRecord;

use crate::{AppError, AppState};

pub enum MemberAcceptance {
    Missing,
    AlreadyAccepted,
    Updated,
}

pub async fn leave_workspace(
    state: &AppState,
    workspace: &WorkspaceRecord,
    user_id: &str,
) -> Result<(), AppError> {
    ensure_not_owner(workspace, user_id, "workspace owner cannot leave")?;
    ensure_member_exists(
        state,
        &workspace.id,
        user_id,
        "member not found in workspace",
    )
    .await?;

    state
        .workspace_store
        .remove_member(&workspace.id, user_id)
        .await
        .map_err(AppError::from_anyhow)
}

pub async fn revoke_member(
    state: &AppState,
    workspace: &WorkspaceRecord,
    target_user_id: &str,
) -> Result<(), AppError> {
    ensure_not_owner(
        workspace,
        target_user_id,
        "cannot revoke workspace owner membership",
    )?;

    let existing_role = state
        .workspace_store
        .find_member_role(&workspace.id, target_user_id)
        .await
        .map_err(AppError::from_anyhow)?;

    if existing_role.is_none() {
        return Ok(());
    }

    state
        .workspace_store
        .remove_member(&workspace.id, target_user_id)
        .await
        .map_err(AppError::from_anyhow)
}

fn ensure_not_owner(
    workspace: &WorkspaceRecord,
    user_id: &str,
    error_message: &'static str,
) -> Result<(), AppError> {
    if workspace.owner_id == user_id {
        Err(AppError::bad_request(error_message))
    } else {
        Ok(())
    }
}

async fn ensure_member_exists(
    state: &AppState,
    workspace_id: &str,
    user_id: &str,
    error_message: &'static str,
) -> Result<(), AppError> {
    let member = state
        .workspace_store
        .get_member(workspace_id, user_id)
        .await
        .map_err(AppError::from_anyhow)?;

    if member.is_some() {
        Ok(())
    } else {
        Err(AppError::not_found(error_message))
    }
}

fn is_accepted_status(status: &str) -> bool {
    status.eq_ignore_ascii_case("accepted")
}

pub async fn accept_member_if_pending(
    state: &AppState,
    workspace_id: &str,
    user_id: &str,
) -> Result<MemberAcceptance, AppError> {
    let member = state
        .workspace_store
        .get_member(workspace_id, user_id)
        .await
        .map_err(AppError::from_anyhow)?;

    let Some(member) = member else {
        return Ok(MemberAcceptance::Missing);
    };

    if is_accepted_status(&member.status) {
        return Ok(MemberAcceptance::AlreadyAccepted);
    }

    state
        .workspace_store
        .set_member_status(workspace_id, user_id, "accepted")
        .await
        .map_err(AppError::from_anyhow)?;

    Ok(MemberAcceptance::Updated)
}
