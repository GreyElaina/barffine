use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use crate::doc_roles::{DocumentRoleCursor, DocumentRoleRecord};

#[async_trait]
pub trait DocRoleRepository: Send + Sync {
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

pub type DocRoleRepositoryRef = Arc<dyn DocRoleRepository>;
