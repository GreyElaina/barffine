use std::sync::Arc;

use axum::extract::FromRef;
use axum::http::HeaderMap;
use barffine_core::{
    doc_roles::DocumentRoleStore,
    doc_store::DocumentStore,
    user::UserStore,
    workspace::{DEFAULT_WORKSPACE_NAME, WorkspaceRecord, WorkspaceStore},
};
use tracing::warn;

use crate::{
    AppError,
    crypto::{DocTokenError, DocTokenSigner},
    state::AppState,
};

pub struct WorkspaceService {
    workspace_store: WorkspaceStore,
    user_store: UserStore,
    document_store: DocumentStore,
    doc_role_store: DocumentRoleStore,
    doc_token_signer: Arc<DocTokenSigner>,
}

impl WorkspaceService {
    pub fn new(
        workspace_store: WorkspaceStore,
        user_store: UserStore,
        document_store: DocumentStore,
        doc_role_store: DocumentRoleStore,
        doc_token_signer: Arc<DocTokenSigner>,
    ) -> Self {
        Self {
            workspace_store,
            user_store,
            document_store,
            doc_role_store,
            doc_token_signer,
        }
    }

    pub async fn workspace_exists(&self, workspace_id: &str) -> Result<bool, AppError> {
        let exists = self
            .workspace_store
            .find_by_id(workspace_id)
            .await
            .map_err(AppError::from_anyhow)?
            .is_some();
        Ok(exists)
    }

    pub async fn ensure_workspace_exists(&self, workspace_id: &str) -> Result<(), AppError> {
        if self.workspace_exists(workspace_id).await? {
            Ok(())
        } else {
            Err(AppError::workspace_not_found(workspace_id))
        }
    }

    pub async fn fetch_workspace(&self, workspace_id: &str) -> Result<WorkspaceRecord, AppError> {
        self.workspace_store
            .find_by_id(workspace_id)
            .await
            .map_err(AppError::from_anyhow)?
            .ok_or_else(|| AppError::workspace_not_found(workspace_id))
    }

    pub async fn has_workspace_read_access(
        &self,
        workspace: &WorkspaceRecord,
        user_id: Option<&str>,
    ) -> Result<bool, AppError> {
        if let Some(user_id) = user_id {
            if user_id == workspace.owner_id {
                return Ok(true);
            }

            if self
                .workspace_store
                .find_active_member_role(&workspace.id, user_id)
                .await
                .map_err(AppError::from_anyhow)?
                .is_some()
            {
                return Ok(true);
            }

            if self
                .user_store
                .is_admin(user_id)
                .await
                .map_err(AppError::from_anyhow)?
            {
                return Ok(true);
            }
        }

        Ok(workspace.public)
    }

    pub async fn find_active_member_role(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<String>, AppError> {
        self.workspace_store
            .find_active_member_role(workspace_id, user_id)
            .await
            .map_err(AppError::from_anyhow)
    }

    pub async fn is_admin(&self, user_id: &str) -> Result<bool, AppError> {
        self.user_store
            .is_admin(user_id)
            .await
            .map_err(AppError::from_anyhow)
    }

    pub fn normalized_workspace_title<'a>(name: &'a str) -> Option<&'a str> {
        if name == DEFAULT_WORKSPACE_NAME {
            return None;
        }
        let trimmed = name.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    }

    pub fn verify_rpc_access_token(
        &self,
        headers: &HeaderMap,
        expected_payload: &str,
        context: &AccessTokenContext<'_>,
    ) -> Result<AccessTokenVerification, AppError> {
        Self::verify_rpc_access_token_with_signer(
            &self.doc_token_signer,
            headers,
            expected_payload,
            context,
        )
    }

    pub fn verify_rpc_access_token_with_signer(
        signer: &DocTokenSigner,
        headers: &HeaderMap,
        expected_payload: &str,
        context: &AccessTokenContext<'_>,
    ) -> Result<AccessTokenVerification, AppError> {
        let Some(token_header) = headers.get("x-access-token") else {
            return Ok(AccessTokenVerification::NotPresent);
        };

        let token = token_header.to_str().map_err(|err| {
            warn!(
                workspace_id = context.workspace_id,
                doc_id = context.doc_id.unwrap_or("-"),
                error = ?err,
                "x-access-token header is not valid utf-8"
            );
            AppError::invalid_internal_request()
        })?;

        match signer.verify(token) {
            Ok(payload) if payload == expected_payload => Ok(AccessTokenVerification::Matched),
            Ok(payload) => {
                warn!(
                    workspace_id = context.workspace_id,
                    doc_id = context.doc_id.unwrap_or("-"),
                    signed_payload = payload.as_str(),
                    target = context.target,
                    "x-access-token payload mismatch"
                );
                Err(AppError::invalid_internal_request())
            }
            Err(err) => {
                warn!(
                    workspace_id = context.workspace_id,
                    doc_id = context.doc_id.unwrap_or("-"),
                    reason = token_error_reason(&err),
                    target = context.target,
                    "failed to verify rpc access token"
                );
                Err(AppError::invalid_internal_request())
            }
        }
    }

    pub async fn create_workspace_with_defaults(
        &self,
        params: WorkspaceCreationParams<'_>,
    ) -> Result<WorkspaceRecord, AppError> {
        let owner_exists = self
            .user_store
            .find_by_id(params.owner_id)
            .await
            .map_err(AppError::from_anyhow)?
            .is_some();

        if !owner_exists {
            return Err(AppError::bad_request("owner not found"));
        }

        let trimmed_name = params.name.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        let workspace = self
            .workspace_store
            .create(
                params.owner_id,
                trimmed_name,
                params.public,
                params.enable_ai,
                params.enable_doc_embedding,
                params.enable_url_preview,
            )
            .await
            .map_err(AppError::from_anyhow)?;

        let workspace_title = Self::normalized_workspace_title(&workspace.name);

        self.document_store
            .ensure_workspace_defaults(&workspace.id, &workspace.owner_id, workspace_title)
            .await
            .map_err(AppError::from_anyhow)?;

        self.doc_role_store
            .upsert(&workspace.id, &workspace.id, &workspace.owner_id, "owner")
            .await
            .map_err(AppError::from_anyhow)?;

        Ok(workspace)
    }
}

impl FromRef<AppState> for Arc<WorkspaceService> {
    fn from_ref(state: &AppState) -> Arc<WorkspaceService> {
        Arc::clone(&state.workspace_service)
    }
}

fn token_error_reason(err: &DocTokenError) -> &'static str {
    match err {
        DocTokenError::MalformedHeader => "malformed header",
        DocTokenError::MissingDelimiter => "missing delimiter",
        DocTokenError::InvalidSignature => "invalid signature",
        DocTokenError::InvalidEncoding => "invalid encoding",
    }
}

pub struct AccessTokenContext<'a> {
    pub workspace_id: &'a str,
    pub doc_id: Option<&'a str>,
    pub target: &'static str,
}

pub enum AccessTokenVerification {
    NotPresent,
    Matched,
}

pub struct WorkspaceCreationParams<'a> {
    pub owner_id: &'a str,
    pub name: Option<&'a str>,
    pub public: Option<bool>,
    pub enable_ai: Option<bool>,
    pub enable_doc_embedding: Option<bool>,
    pub enable_url_preview: Option<bool>,
}
