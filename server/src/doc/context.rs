use anyhow::Error as AnyError;
use std::sync::Arc;

use axum::{
    http::{HeaderMap, HeaderValue},
    response::Response,
};

use crate::{
    AppError,
    auth::{AuthState, DocAccessIntent, RpcAccessRequirement, resolve_doc_access},
    handlers::headers::{
        HEADER_DOC_ID, HEADER_DOC_ROLE, HEADER_USER_ID, HEADER_WORKSPACE_ID, HEADER_WORKSPACE_ROLE,
        doc_role_header_value, permission_header_value,
    },
    http::append_set_cookie_headers,
    state::{AppState, SocketRuntimeState},
    utils::blob_download::BlobResponseContext,
};
use barffine_core::{
    doc_store::DocumentMetadata,
    ids::{DocId, WorkspaceId},
};

use super::sync::workspace_snapshot_or_not_found;
use crate::types::RestDocAccess;

pub(crate) struct DocAccessContext<S> {
    state: S,
    workspace_id: WorkspaceId,
    doc_id: DocId,
    access: RestDocAccess,
}

impl<S> DocAccessContext<S>
where
    S: AuthState + Clone,
{
    pub(crate) fn from_access(
        state: S,
        workspace_id: WorkspaceId,
        doc_id: DocId,
        access: RestDocAccess,
    ) -> Self {
        Self {
            state,
            workspace_id,
            doc_id,
            access,
        }
    }

    pub(crate) async fn new(
        state: S,
        headers: &HeaderMap,
        workspace_id: WorkspaceId,
        doc_id: DocId,
        rpc_requirement: RpcAccessRequirement,
        intent: DocAccessIntent,
    ) -> Result<Self, AppError> {
        let access = resolve_doc_access(
            &state,
            headers,
            workspace_id.as_str(),
            doc_id.as_str(),
            rpc_requirement,
            intent,
        )
        .await?;
        Ok(Self {
            state,
            workspace_id,
            doc_id,
            access,
        })
    }

    pub(crate) fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    pub(crate) fn doc_id(&self) -> &DocId {
        &self.doc_id
    }

    pub(crate) fn access(&self) -> &RestDocAccess {
        &self.access
    }

    pub(crate) fn into_access(self) -> RestDocAccess {
        self.access
    }

    pub(crate) fn metadata(&self) -> &DocumentMetadata {
        &self.access.metadata
    }
}

impl DocAccessContext<AppState> {
    pub(crate) fn finalize_response(&self, mut response: Response) -> Result<Response, AppError> {
        append_doc_access_headers(&mut response, &self.access)?;
        append_set_cookie_headers(&mut response, &self.access.set_cookies)?;
        Ok(response)
    }

    pub(crate) fn blob_context(&self) -> BlobResponseContext<'_> {
        BlobResponseContext::Document(&self.access)
    }

    pub(crate) async fn workspace_snapshot(&self) -> Result<(Vec<u8>, i64), AppError> {
        workspace_snapshot_or_not_found(self.state(), self.workspace_id(), self.doc_id()).await
    }

    pub(crate) async fn fetch_history_snapshot(
        &self,
        timestamp: i64,
    ) -> Result<Option<Vec<u8>>, AppError> {
        self.state()
            .document_store
            .fetch_history_as_of(self.workspace_id(), self.doc_id(), timestamp)
            .await
            .map_err(AppError::from_anyhow)
    }

    pub(crate) fn state(&self) -> &AppState {
        &self.state
    }
}

impl DocAccessContext<Arc<SocketRuntimeState>> {
    pub(crate) fn runtime(&self) -> &SocketRuntimeState {
        self.state.as_ref()
    }
}

fn append_doc_access_headers(
    response: &mut Response,
    access: &RestDocAccess,
) -> Result<(), AppError> {
    let headers = response.headers_mut();
    headers.insert(
        HEADER_WORKSPACE_ID,
        HeaderValue::from_str(&access.metadata.workspace_id)
            .map_err(|err| AppError::internal(AnyError::new(err)))?,
    );
    headers.insert(
        HEADER_DOC_ID,
        HeaderValue::from_str(&access.metadata.id)
            .map_err(|err| AppError::internal(AnyError::new(err)))?,
    );

    if let Some(user) = &access.user {
        headers.insert(
            HEADER_USER_ID,
            HeaderValue::from_str(&user.id)
                .map_err(|err| AppError::internal(AnyError::new(err)))?,
        );
    }

    if let Some(role) = access.workspace_role {
        headers.insert(
            HEADER_WORKSPACE_ROLE,
            HeaderValue::from_static(permission_header_value(role)),
        );
    }

    if let Some(role) = access.doc_role {
        headers.insert(
            HEADER_DOC_ROLE,
            HeaderValue::from_static(doc_role_header_value(role)),
        );
    }

    Ok(())
}
