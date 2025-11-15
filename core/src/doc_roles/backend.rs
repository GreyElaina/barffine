use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use super::{DocumentRoleCursor, DocumentRoleRecord};
use crate::db::doc_role_repo::DocRoleRepositoryRef;

#[async_trait]
pub trait DocRoleBackend: Send + Sync {
    async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>>;

    async fn find_for_user(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
    ) -> Result<Option<DocumentRoleRecord>>;

    async fn paginate_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentRoleCursor>,
    ) -> Result<Vec<DocumentRoleRecord>>;

    async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64>;

    async fn owners_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>>;

    async fn upsert_roles(&self, roles: &[DocumentRoleRecord]) -> Result<()>;

    async fn upsert_role(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
        role: &str,
        created_at: i64,
    ) -> Result<()>;

    async fn remove_role(&self, workspace_id: &str, doc_id: &str, user_id: &str) -> Result<()>;

    async fn remove_doc_roles(&self, workspace_id: &str, doc_id: &str) -> Result<()>;
}

pub type DocRoleBackendRef = Arc<dyn DocRoleBackend>;

pub struct SqlDocRoleBackend {
    repo: DocRoleRepositoryRef,
}

impl SqlDocRoleBackend {
    pub fn new(repo: DocRoleRepositoryRef) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl DocRoleBackend for SqlDocRoleBackend {
    async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        self.repo.list_for_doc(workspace_id, doc_id).await
    }

    async fn find_for_user(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
    ) -> Result<Option<DocumentRoleRecord>> {
        self.repo.find_for_user(workspace_id, doc_id, user_id).await
    }

    async fn paginate_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentRoleCursor>,
    ) -> Result<Vec<DocumentRoleRecord>> {
        self.repo
            .paginate_for_doc(workspace_id, doc_id, limit, offset, cursor)
            .await
    }

    async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64> {
        self.repo.count_for_doc(workspace_id, doc_id).await
    }

    async fn owners_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        self.repo.owners_for_doc(workspace_id, doc_id).await
    }

    async fn upsert_roles(&self, roles: &[DocumentRoleRecord]) -> Result<()> {
        self.repo.upsert_roles(roles).await
    }

    async fn upsert_role(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
        role: &str,
        created_at: i64,
    ) -> Result<()> {
        self.repo
            .upsert_role(workspace_id, doc_id, user_id, role, created_at)
            .await
    }

    async fn remove_role(&self, workspace_id: &str, doc_id: &str, user_id: &str) -> Result<()> {
        self.repo.remove_role(workspace_id, doc_id, user_id).await
    }

    async fn remove_doc_roles(&self, workspace_id: &str, doc_id: &str) -> Result<()> {
        self.repo.remove_doc_roles(workspace_id, doc_id).await
    }
}
