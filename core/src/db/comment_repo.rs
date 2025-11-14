use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::notification::{
    CommentChangeAction, CommentRecord, CommentRecordWithCursor, CommentReplyRecord,
};

#[derive(Debug, Clone)]
pub struct CommentChangeRow {
    pub row_id: i64,
    pub comment_id: Option<String>,
    pub reply_id: Option<String>,
    pub action: CommentChangeAction,
    pub payload: JsonValue,
    pub updated_at: DateTime<Utc>,
}

#[async_trait]
pub trait CommentRepository: Send + Sync {
    async fn insert_comment(&self, comment: &CommentRecord) -> Result<()>;

    async fn fetch_comment(&self, comment_id: &str) -> Result<Option<CommentRecord>>;

    async fn list_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<Vec<CommentRecord>>;

    async fn update_comment(&self, comment: &CommentRecord) -> Result<()>;

    async fn delete_comment(&self, comment_id: &str) -> Result<()>;

    async fn set_comment_resolved(
        &self,
        comment_id: &str,
        resolved: bool,
        updated_at: DateTime<Utc>,
    ) -> Result<()>;

    async fn insert_reply(&self, reply: &CommentReplyRecord) -> Result<()>;

    async fn fetch_reply(&self, reply_id: &str) -> Result<Option<CommentReplyRecord>>;

    async fn list_replies(&self, comment_id: &str) -> Result<Vec<CommentReplyRecord>>;

    async fn update_reply(&self, reply: &CommentReplyRecord) -> Result<()>;

    async fn delete_reply(&self, reply_id: &str) -> Result<()>;

    async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64>;

    async fn list_comments_paginated(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        after_sid: Option<i64>,
    ) -> Result<Vec<CommentRecordWithCursor>>;

    async fn insert_comment_change(
        &self,
        workspace_id: &str,
        doc_id: &str,
        comment_id: Option<&str>,
        reply_id: Option<&str>,
        action: CommentChangeAction,
        payload: &JsonValue,
        updated_at: DateTime<Utc>,
    ) -> Result<()>;

    async fn list_comment_changes(
        &self,
        workspace_id: &str,
        doc_id: &str,
        baseline_timestamp: i64,
        baseline_row_id: Option<i64>,
        limit: i64,
    ) -> Result<Vec<CommentChangeRow>>;
}

pub type CommentRepositoryRef = Arc<dyn CommentRepository>;
