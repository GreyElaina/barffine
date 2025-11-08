use std::sync::Arc;

use axum::extract::FromRef;
use barffine_core::{
    doc_roles::DocumentRoleStore,
    doc_store::{DocumentMetadata, DocumentStore},
    workspace::WorkspaceRecord,
};

use crate::{
    AppError,
    graphql::{
        DocPermissions, DocRole, Permission, doc_permissions_for_role, doc_role_from_str,
        fixup_doc_role, workspace_role_from_str,
    },
    state::AppState,
    workspace::service::WorkspaceService,
};

pub struct DocAccessService {
    document_store: DocumentStore,
    doc_role_store: DocumentRoleStore,
    workspace_service: Arc<WorkspaceService>,
}

impl DocAccessService {
    pub fn new(
        document_store: DocumentStore,
        doc_role_store: DocumentRoleStore,
        workspace_service: Arc<WorkspaceService>,
    ) -> Self {
        Self {
            document_store,
            doc_role_store,
            workspace_service,
        }
    }

    pub async fn document_exists(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<bool, AppError> {
        let metadata = self
            .document_store
            .find_metadata(workspace_id, doc_id)
            .await
            .map_err(AppError::from_anyhow)?;
        Ok(metadata.is_some())
    }

    pub async fn ensure_document_exists(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<(), AppError> {
        if self.document_exists(workspace_id, doc_id).await? {
            Ok(())
        } else {
            Err(AppError::doc_not_found(workspace_id, doc_id))
        }
    }

    pub async fn ensure_metadata(
        &self,
        workspace: &WorkspaceRecord,
        doc_id: &str,
    ) -> Result<DocumentMetadata, AppError> {
        if let Some(metadata) = self
            .document_store
            .find_metadata(&workspace.id, doc_id)
            .await
            .map_err(AppError::from_anyhow)?
        {
            return Ok(metadata);
        }

        let workspace_title = WorkspaceService::normalized_workspace_title(&workspace.name);

        let metadata = match self
            .document_store
            .ensure_system_doc(&workspace.id, doc_id, &workspace.owner_id, workspace_title)
            .await
            .map_err(AppError::from_anyhow)?
        {
            Some(metadata) => metadata,
            None => self
                .document_store
                .ensure_doc_record(&workspace.id, doc_id, &workspace.owner_id, None)
                .await
                .map_err(AppError::from_anyhow)?,
        };

        self.doc_role_store
            .upsert(&workspace.id, doc_id, &workspace.owner_id, "owner")
            .await
            .map_err(AppError::from_anyhow)?;

        Ok(metadata)
    }

    pub async fn resolve_doc_authorization(
        &self,
        workspace: &WorkspaceRecord,
        metadata: &DocumentMetadata,
        user_id: Option<&str>,
    ) -> Result<DocAuthorization, AppError> {
        let mut workspace_role: Option<Permission> = None;

        if let Some(user_id) = user_id {
            if user_id == workspace.owner_id {
                workspace_role = Some(Permission::Owner);
            } else if let Some(role) = self
                .workspace_service
                .find_active_member_role(&workspace.id, user_id)
                .await?
            {
                workspace_role = Some(workspace_role_from_str(&role));
            } else if self.workspace_service.is_admin(user_id).await? {
                workspace_role = Some(Permission::Admin);
            }
        }

        if workspace_role.is_none() && (workspace.public || metadata.public) {
            workspace_role = Some(Permission::External);
        }

        let doc_role = if let Some(user_id) = user_id {
            if let Some(role) = self
                .doc_role_store
                .find_for_user(&workspace.id, &metadata.id, user_id)
                .await
                .map_err(AppError::from_anyhow)?
            {
                Some(doc_role_from_str(&role.role))
            } else {
                match workspace_role {
                    Some(Permission::Owner) => Some(DocRole::Owner),
                    Some(Permission::Admin) | Some(Permission::Collaborator) => {
                        Some(doc_role_from_str(&metadata.default_role))
                    }
                    Some(Permission::External) => metadata.public.then_some(DocRole::Reader),
                    None => metadata.public.then_some(DocRole::Reader),
                }
            }
        } else if metadata.public {
            Some(DocRole::Reader)
        } else {
            None
        };

        let resolved_role = fixup_doc_role(workspace_role, doc_role);
        let permissions = resolved_role.map(doc_permissions_for_role);

        Ok(DocAuthorization {
            workspace_role,
            doc_role: resolved_role,
            permissions,
        })
    }
}

impl FromRef<AppState> for Arc<DocAccessService> {
    fn from_ref(state: &AppState) -> Arc<DocAccessService> {
        Arc::clone(&state.doc_access_service)
    }
}

pub struct DocAuthorization {
    pub(crate) workspace_role: Option<Permission>,
    pub(crate) doc_role: Option<DocRole>,
    pub(crate) permissions: Option<DocPermissions>,
}

impl DocAuthorization {
    pub(crate) fn ensure_permission<F>(
        &self,
        predicate: F,
        error_message: &'static str,
    ) -> Result<(), AppError>
    where
        F: FnOnce(&DocPermissions) -> bool,
    {
        match self.permissions.as_ref() {
            Some(perms) if predicate(perms) => Ok(()),
            _ => Err(AppError::forbidden(error_message)),
        }
    }
}
