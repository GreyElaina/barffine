use anyhow::Error as AnyError;
use argon2::password_hash;
use async_graphql::{
    Context, Error as GraphQLError, ErrorExtensions, ID, Result as GraphQLResult, Upload, Value,
};
use barffine_core::workspace as core_workspace;
use chrono::{DateTime, Utc};
use std::io::Read;

use crate::{
    AppError, AppState,
    doc::{access as doc_access, metadata as doc_metadata},
    error::UserFriendlyPayload,
};

use super::{
    RequestUser,
    doc::DocPermissions,
    workspace::{Permission, permission_rank, workspace_role_from_str},
};

#[derive(Clone, Copy)]
pub(crate) struct ResolvedWorkspacePermission {
    pub permission: Permission,
    pub from_membership: bool,
}

pub(crate) fn require_request_user(ctx: &Context<'_>) -> GraphQLResult<RequestUser> {
    ctx.data_opt::<RequestUser>()
        .cloned()
        .ok_or_else(|| map_app_error(AppError::unauthorized("authentication required")))
}

pub(crate) async fn require_admin_user(ctx: &Context<'_>) -> GraphQLResult<RequestUser> {
    let request_user = require_request_user(ctx)?;
    let state = ctx.data::<AppState>()?;
    state
        .user_service
        .ensure_admin_user(&request_user.user_id, "administrator privileges required")
        .await
        .map_err(map_app_error)?;

    Ok(request_user)
}

pub(crate) async fn ensure_workspace_exists(
    state: &AppState,
    workspace_id: &str,
) -> GraphQLResult<()> {
    state
        .workspace_service
        .ensure_workspace_exists(workspace_id)
        .await
        .map_err(map_app_error)
}

pub(crate) async fn require_workspace_permission(
    ctx: &Context<'_>,
    workspace_id: &str,
    minimum: Permission,
) -> GraphQLResult<RequestUser> {
    let request_user = require_request_user(ctx)?;
    let state = ctx.data::<AppState>()?;

    let workspace = state
        .workspace_service
        .fetch_workspace(workspace_id)
        .await
        .map_err(map_app_error)?;

    if let Some(resolved) = workspace_permission_for_user(state, &workspace, &request_user.user_id)
        .await
        .map_err(map_app_error)?
    {
        if permission_rank(resolved.permission) >= permission_rank(minimum) {
            return Ok(request_user);
        }
    }

    Err(map_app_error(AppError::forbidden(
        "insufficient workspace permissions",
    )))
}

pub(crate) async fn ensure_document_exists(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
) -> GraphQLResult<()> {
    doc_access::ensure_document_exists(state, workspace_id, doc_id)
        .await
        .map_err(map_app_error)
}

pub(crate) fn ensure_doc_not_workspace(workspace_id: &str, doc_id: &str) -> GraphQLResult<()> {
    if workspace_id == doc_id {
        Err(map_app_error(AppError::bad_request(
            "expect doc not to be workspace",
        )))
    } else {
        Ok(())
    }
}

pub(crate) const SESSION_OWNERSHIP_ERROR: &str = "session does not belong to requesting user";

pub(crate) fn ensure_session_owner<F>(
    session_user_id: &str,
    request_user_id: &str,
    error_builder: F,
) -> GraphQLResult<()>
where
    F: FnOnce() -> AppError,
{
    if session_user_id != request_user_id {
        Err(map_app_error(error_builder()))
    } else {
        Ok(())
    }
}

pub(crate) fn resolve_session_id(
    session_id: Option<ID>,
    request_user: &RequestUser,
) -> GraphQLResult<String> {
    session_id
        .map(|id| id.to_string())
        .or_else(|| request_user.session_id().map(str::to_owned))
        .ok_or_else(|| map_app_error(AppError::unauthorized("session id is required")))
}

pub(crate) async fn workspace_permission_for_user(
    state: &AppState,
    workspace: &core_workspace::WorkspaceRecord,
    user_id: &str,
) -> Result<Option<ResolvedWorkspacePermission>, AppError> {
    if user_id == workspace.owner_id {
        return Ok(Some(ResolvedWorkspacePermission {
            permission: Permission::Owner,
            from_membership: false,
        }));
    }

    if let Some(role) = state
        .workspace_service
        .find_active_member_role(&workspace.id, user_id)
        .await?
    {
        return Ok(Some(ResolvedWorkspacePermission {
            permission: workspace_role_from_str(&role),
            from_membership: true,
        }));
    }

    if state.user_service.is_admin(user_id).await? {
        return Ok(Some(ResolvedWorkspacePermission {
            permission: Permission::Admin,
            from_membership: false,
        }));
    }

    Ok(None)
}

pub(crate) fn normalize_non_empty_input(
    field_name: &'static str,
    value: String,
) -> GraphQLResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(map_app_error(AppError::bad_request(format!(
            "{field_name} must not be empty"
        ))))
    } else {
        Ok(trimmed.to_string())
    }
}

pub(crate) fn normalize_optional_input(
    field_name: &'static str,
    value: Option<String>,
) -> GraphQLResult<Option<String>> {
    value
        .map(|inner| normalize_non_empty_input(field_name, inner))
        .transpose()
}

pub(crate) async fn ensure_workspace_and_doc(
    state: &AppState,
    workspace_id: &str,
    doc_id: Option<&str>,
) -> GraphQLResult<()> {
    ensure_workspace_exists(state, workspace_id).await?;

    if let Some(doc_id) = doc_id {
        ensure_doc_not_workspace(workspace_id, doc_id)?;
        ensure_document_exists(state, workspace_id, doc_id).await?;
    }

    Ok(())
}

pub(crate) async fn doc_permissions_for_user_internal(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
    user_id: &str,
) -> Result<Option<DocPermissions>, AppError> {
    let workspace = state
        .workspace_service
        .fetch_workspace(workspace_id)
        .await?;

    let metadata = doc_metadata::fetch_required(state, workspace_id, doc_id).await?;

    let authorization =
        doc_access::resolve_doc_authorization(state, &workspace, &metadata, Some(user_id)).await?;

    Ok(authorization.permissions)
}

pub(crate) async fn fetch_workspace_record(
    state: &AppState,
    workspace_id: &str,
) -> GraphQLResult<core_workspace::WorkspaceRecord> {
    state
        .workspace_service
        .fetch_workspace(workspace_id)
        .await
        .map_err(map_app_error)
}

pub(crate) async fn doc_permissions_for_user(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
    user_id: &str,
) -> GraphQLResult<Option<DocPermissions>> {
    doc_permissions_for_user_internal(state, workspace_id, doc_id, user_id)
        .await
        .map_err(map_app_error)
}

pub(crate) async fn require_doc_permission<F>(
    ctx: &Context<'_>,
    workspace_id: &str,
    doc_id: &str,
    predicate: F,
    error_message: &'static str,
) -> GraphQLResult<RequestUser>
where
    F: Fn(&DocPermissions) -> bool,
{
    let request_user = require_request_user(ctx)?;
    let state = ctx.data::<AppState>()?;

    let Some(perms) =
        doc_permissions_for_user(state, workspace_id, doc_id, &request_user.user_id).await?
    else {
        return Err(map_app_error(AppError::forbidden(error_message)));
    };

    if predicate(&perms) {
        Ok(request_user)
    } else {
        Err(map_app_error(AppError::forbidden(error_message)))
    }
}

pub(crate) fn map_anyhow(err: AnyError) -> GraphQLError {
    graphql_error_from_app_error(AppError::internal(err))
}

pub(crate) fn map_app_error(err: AppError) -> GraphQLError {
    graphql_error_from_app_error(err)
}

pub(crate) fn map_password(err: password_hash::Error) -> GraphQLError {
    graphql_error_from_app_error(AppError::internal(AnyError::new(err)))
}

pub(crate) fn graphql_error_from_app_error(err: AppError) -> GraphQLError {
    let (_, payload) = err.into_payload();
    graphql_error_from_payload(payload)
}

pub(crate) fn graphql_error_from_payload(payload: UserFriendlyPayload) -> GraphQLError {
    let UserFriendlyPayload {
        status,
        code,
        reason,
        error_type,
        name,
        message,
        data,
    } = payload;

    let mut gql_error = GraphQLError::new(message.clone());
    gql_error = gql_error.extend_with(move |_, ext| {
        ext.set("status", status as i32);
        ext.set("code", code.clone());
        ext.set("reason", reason.clone());
        ext.set("type", error_type);
        ext.set("name", name);
        ext.set("message", message.clone());

        if let Some(data_value) = data.clone() {
            if let Ok(value) = Value::from_json(data_value) {
                ext.set("data", value);
            }
        }
    });

    gql_error
}

pub(crate) fn timestamp_to_datetime(ts: i64) -> DateTime<Utc> {
    let seconds = ts.div_euclid(1000);
    let millis_remainder = ts.rem_euclid(1000);
    let nanos = (millis_remainder as u32) * 1_000_000;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .or_else(|| DateTime::<Utc>::from_timestamp(0, 0))
        .unwrap()
}

pub(crate) fn format_bytes(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    if bytes == 0 {
        return "0 B".to_string();
    }

    let mut value = bytes as f64;
    let mut unit = 0;
    while value.abs() >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else if (value * 10.0).fract() == 0.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

pub(crate) fn format_history_period(ms: i64) -> String {
    const ONE_MINUTE_MS: i64 = 60_000;
    const ONE_DAY_MS: i64 = ONE_MINUTE_MS * 60 * 24;

    let days = (ms / ONE_DAY_MS).max(1);
    if days == 1 {
        "1 day".to_string()
    } else {
        format!("{days} days")
    }
}

pub(crate) struct ResolvedUpload {
    pub filename: String,
    pub content_type: Option<String>,
    pub bytes: Vec<u8>,
}

pub(crate) fn read_upload_with_limit(
    ctx: &Context<'_>,
    upload: Upload,
    limit: usize,
) -> GraphQLResult<ResolvedUpload> {
    let upload_value = upload.value(ctx)?;
    let filename = upload_value.filename.clone();
    let content_type = upload_value.content_type.clone();
    let mut reader = upload_value.into_read();
    let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
    let mut buffer = [0u8; 8192];

    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|err| map_app_error(AppError::from_anyhow(AnyError::new(err))))?;
        if read == 0 {
            break;
        }

        if bytes.len() + read > limit {
            return Err(map_app_error(
                AppError::bad_request(format!("upload exceeds {} KB limit", limit / 1024))
                    .with_name("UPLOAD_TOO_LARGE"),
            ));
        }

        bytes.extend_from_slice(&buffer[..read]);
    }

    Ok(ResolvedUpload {
        filename,
        content_type,
        bytes,
    })
}

pub(crate) fn read_upload(ctx: &Context<'_>, upload: Upload) -> GraphQLResult<ResolvedUpload> {
    let upload_value = upload.value(ctx)?;
    let filename = upload_value.filename.clone();
    let content_type = upload_value.content_type.clone();
    let mut reader = upload_value.into_read();
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|err| map_app_error(AppError::from_anyhow(err.into())))?;

    Ok(ResolvedUpload {
        filename,
        content_type,
        bytes,
    })
}
