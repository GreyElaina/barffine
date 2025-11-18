// Authentication and authorization logic

use std::sync::Arc;

use argon2::{
    Argon2,
    password_hash::{
        Error as PasswordHashError, PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
        rand_core::OsRng,
    },
};
use axum::http::HeaderMap;
use barffine_core::user;
use chrono::DateTime;

use crate::{
    cookies::{clear_session_cookie, clear_user_cookie, extract_session_token},
    doc::service::DocAccessService,
    error::AppError,
    graphql::{Permission, workspace_role_from_str},
    state::{AppState, SocketRuntimeState},
    types::{AuthenticatedRestSession, RestDocAccess, SessionLookup, WorkspaceAccess},
    user::service::UserService,
    workspace::service::{AccessTokenContext, AccessTokenVerification, WorkspaceService},
};

pub(crate) trait AuthState: Send + Sync {
    fn user_service(&self) -> &UserService;
    fn workspace_service(&self) -> &WorkspaceService;
    fn doc_access_service(&self) -> &DocAccessService;
}

impl AuthState for AppState {
    fn user_service(&self) -> &UserService {
        self.user_service.as_ref()
    }

    fn workspace_service(&self) -> &WorkspaceService {
        self.workspace_service.as_ref()
    }

    fn doc_access_service(&self) -> &DocAccessService {
        self.doc_access_service.as_ref()
    }
}

impl AuthState for SocketRuntimeState {
    fn user_service(&self) -> &UserService {
        self.user_service.as_ref()
    }

    fn workspace_service(&self) -> &WorkspaceService {
        self.workspace_service.as_ref()
    }

    fn doc_access_service(&self) -> &DocAccessService {
        self.doc_access_service.as_ref()
    }
}

impl<T> AuthState for Arc<T>
where
    T: AuthState + ?Sized,
{
    fn user_service(&self) -> &UserService {
        (**self).user_service()
    }

    fn workspace_service(&self) -> &WorkspaceService {
        (**self).workspace_service()
    }

    fn doc_access_service(&self) -> &DocAccessService {
        (**self).doc_access_service()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum RpcAccessRequirement {
    Optional,
    Required,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum DocAccessIntent {
    Standard,
    RequireAuthenticatedRead,
}

impl DocAccessIntent {
    fn allows_rpc_token(self) -> bool {
        matches!(self, DocAccessIntent::Standard)
    }

    fn requires_doc_read_permission(self) -> bool {
        matches!(self, DocAccessIntent::RequireAuthenticatedRead)
    }
}

pub(crate) async fn authenticate_rest_request<S>(
    state: &S,
    headers: &HeaderMap,
) -> Result<AuthenticatedRestSession, AppError>
where
    S: AuthState,
{
    state
        .user_service()
        .authenticate_rest_request(headers)
        .await
}

pub(crate) async fn resolve_workspace_access<S>(
    state: &S,
    headers: &HeaderMap,
    workspace_id: &str,
) -> Result<WorkspaceAccess, AppError>
where
    S: AuthState,
{
    let workspace = state
        .workspace_service()
        .fetch_workspace(workspace_id)
        .await?;

    let mut set_cookies = Vec::new();
    let mut user: Option<user::UserRecord> = None;

    if extract_session_token(headers).is_some() {
        match authenticate_rest_request(state, headers).await {
            Ok(authenticated) => {
                set_cookies.extend(authenticated.set_cookies.clone());
                user = Some(authenticated.user);
            }
            Err(err) => {
                if workspace.public {
                    set_cookies.push(clear_session_cookie());
                    set_cookies.push(clear_user_cookie());
                } else {
                    return Err(err);
                }
            }
        }
    }

    if !workspace.public && user.is_none() {
        return Err(AppError::unauthorized("authentication required"));
    }

    let has_access = state
        .workspace_service()
        .has_workspace_read_access(&workspace, user.as_ref().map(|record| record.id.as_str()))
        .await?;

    if !has_access && !workspace.public {
        return Err(AppError::space_access_denied(workspace_id));
    }

    let mut workspace_role: Option<Permission> = None;
    if let Some(user_record) = user.as_ref() {
        if user_record.id == workspace.owner_id.as_str() {
            workspace_role = Some(Permission::Owner);
        } else if state.workspace_service().is_admin(&user_record.id).await? {
            workspace_role = Some(Permission::Admin);
        } else if let Some(role) = state
            .workspace_service()
            .find_active_member_role(&workspace.id, &user_record.id)
            .await?
        {
            workspace_role = Some(workspace_role_from_str(&role));
        }
    }

    if workspace_role.is_none() && workspace.public {
        workspace_role = Some(Permission::External);
    }

    Ok(WorkspaceAccess {
        workspace,
        user,
        set_cookies,
        workspace_role,
    })
}

pub(crate) async fn resolve_doc_access<S>(
    state: &S,
    headers: &HeaderMap,
    workspace_id: &str,
    doc_id: &str,
    rpc_requirement: RpcAccessRequirement,
    intent: DocAccessIntent,
) -> Result<RestDocAccess, AppError>
where
    S: AuthState,
{
    let workspace = state
        .workspace_service()
        .fetch_workspace(workspace_id)
        .await?;

    let metadata = state
        .doc_access_service()
        .ensure_metadata(&workspace, doc_id)
        .await?;

    let token_status = state.workspace_service().verify_rpc_access_token(
        headers,
        doc_id,
        &AccessTokenContext {
            workspace_id,
            doc_id: Some(doc_id),
            target: "doc",
        },
    )?;

    match token_status {
        AccessTokenVerification::Matched if intent.allows_rpc_token() => {
            return Ok(RestDocAccess {
                metadata,
                user: None,
                set_cookies: Vec::new(),
                workspace_role: Some(crate::graphql::Permission::External),
                doc_role: Some(crate::graphql::DocRole::Reader),
            });
        }
        AccessTokenVerification::NotPresent
            if matches!(rpc_requirement, RpcAccessRequirement::Required) =>
        {
            return Err(AppError::invalid_internal_request());
        }
        AccessTokenVerification::Matched | AccessTokenVerification::NotPresent => {}
    }

    let mut set_cookies: Vec<String> = Vec::new();
    let mut user: Option<user::UserRecord> = None;

    if extract_session_token(headers).is_some() {
        match authenticate_rest_request(state, headers).await {
            Ok(auth) => {
                set_cookies.extend(auth.set_cookies.clone());
                user = Some(auth.user);
            }
            Err(err) => {
                if metadata.public {
                    set_cookies.push(clear_session_cookie());
                    set_cookies.push(clear_user_cookie());
                } else {
                    return Err(err);
                }
            }
        }
    }

    if (!metadata.public || intent.requires_doc_read_permission()) && user.is_none() {
        return Err(AppError::unauthorized("authentication required"));
    }

    let authorization = state
        .doc_access_service()
        .resolve_doc_authorization(
            &workspace,
            &metadata,
            user.as_ref().map(|record| record.id.as_str()),
        )
        .await?;

    if !metadata.public || intent.requires_doc_read_permission() {
        authorization
            .ensure_permission(|perms| perms.can_read_doc(), "Doc.Read permission required")?;
    }

    Ok(RestDocAccess {
        metadata,
        user,
        set_cookies,
        workspace_role: authorization.workspace_role,
        doc_role: authorization.doc_role,
    })
}

pub(crate) async fn authenticate_with_password(
    state: &AppState,
    email: &str,
    password: &str,
) -> Result<(user::UserRecord, user::SessionRecord), AppError> {
    let Some(user) = state
        .user_store
        .find_by_email(email)
        .await
        .map_err(AppError::from_anyhow)?
    else {
        return Err(AppError::unauthorized("invalid credentials"));
    };

    if user.disabled {
        return Err(AppError::unauthorized("invalid credentials"));
    }

    if user.password_hash.trim().is_empty() {
        return Err(AppError::unauthorized("invalid credentials"));
    }

    let parsed_hash =
        PasswordHash::new(&user.password_hash).map_err(|err| AppError::internal(err.into()))?;

    Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .map_err(|_| AppError::unauthorized("invalid credentials"))?;

    let session = state
        .user_store
        .create_session(&user.id)
        .await
        .map_err(AppError::from_anyhow)?;

    Ok((user, session))
}

pub(crate) async fn pad_session_response(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<SessionLookup, AppError> {
    state.user_service.pad_session_response(headers).await
}

pub(crate) fn parse_history_timestamp(value: &str) -> Result<i64, AppError> {
    if let Ok(ms) = value.parse::<i64>() {
        return Ok(ms / 1_000);
    }

    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Ok(parsed.timestamp());
    }

    Err(AppError::bad_request("invalid history timestamp"))
}

pub fn generate_password_hash(password: &str) -> Result<String, PasswordHashError> {
    let mut rng = OsRng;
    let salt = SaltString::generate(&mut rng);
    let hash = Argon2::default().hash_password(password.as_bytes(), &salt)?;
    Ok(hash.to_string())
}
