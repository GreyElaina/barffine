use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::comment_attachment::{CommentAttachmentRecord, CommentAttachmentUpsert};

#[async_trait]
pub trait CommentAttachmentRepository: Send + Sync {
    async fn upsert(&self, input: CommentAttachmentUpsert<'_>) -> Result<CommentAttachmentRecord>;
    async fn get(
        &self,
        workspace_id: &str,
        doc_id: &str,
        key: &str,
    ) -> Result<Option<CommentAttachmentRecord>>;
    async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<CommentAttachmentRecord>>;
    async fn delete(&self, workspace_id: &str, doc_id: &str, key: &str) -> Result<()>;
}

pub type CommentAttachmentRepositoryRef = Arc<dyn CommentAttachmentRepository>;
