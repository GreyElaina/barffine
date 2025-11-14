use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommentVisibility {
    Private,
    Workspace,
    Public,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentRecord {
    pub id: String,
    pub workspace_id: String,
    pub doc_id: String,
    pub author_id: String,
    pub body: String,
    pub visibility: CommentVisibility,
    pub metadata: JsonValue,
    pub resolved: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentReplyRecord {
    pub id: String,
    pub comment_id: String,
    pub author_id: String,
    pub body: String,
    pub metadata: JsonValue,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommentChangeAction {
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct CommentChangeRecord {
    pub action: CommentChangeAction,
    pub id: String,
    pub comment_id: Option<String>,
    pub workspace_id: String,
    pub doc_id: String,
    pub item: JsonValue,
    pub updated_at: DateTime<Utc>,
    pub change_row_id: i64,
    pub is_reply: bool,
}

#[derive(Debug, Clone)]
pub struct CommentRecordWithCursor {
    pub record: CommentRecord,
    pub sid: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CommentCursor {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sid: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment_updated_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_updated_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment_change_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_change_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationRecord {
    pub id: String,
    pub user_id: String,
    pub kind: String,
    pub payload: JsonValue,
    pub read: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[async_trait]
pub trait CommentStore: Send + Sync {
    async fn create(&self, comment: CommentRecord) -> Result<CommentRecord>;
    async fn find(&self, comment_id: &str) -> Result<Option<CommentRecord>>;
    async fn list_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<Vec<CommentRecord>>;
    async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64>;
    async fn update(&self, comment: CommentRecord) -> Result<CommentRecord>;
    async fn delete(&self, comment_id: &str) -> Result<()>;
    async fn resolve(&self, comment_id: &str, resolved: bool) -> Result<()>;
    async fn create_reply(&self, reply: CommentReplyRecord) -> Result<CommentReplyRecord>;
    async fn find_reply(&self, reply_id: &str) -> Result<Option<CommentReplyRecord>>;
    async fn list_replies(&self, comment_id: &str) -> Result<Vec<CommentReplyRecord>>;
    async fn update_reply(&self, reply: CommentReplyRecord) -> Result<CommentReplyRecord>;
    async fn delete_reply(&self, reply_id: &str) -> Result<()>;
    async fn list_comments_paginated(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        after_sid: Option<i64>,
    ) -> Result<Vec<CommentRecordWithCursor>>;
    async fn list_comment_changes(
        &self,
        workspace_id: &str,
        doc_id: &str,
        after_comment_ts: Option<i64>,
        after_reply_ts: Option<i64>,
        after_comment_change_id: Option<i64>,
        after_reply_change_id: Option<i64>,
        limit: i64,
    ) -> Result<Vec<CommentChangeRecord>>;
}

#[async_trait]
pub trait NotificationCenter: Send + Sync {
    async fn enqueue(&self, notification: NotificationRecord) -> Result<()>;
    async fn list_unread(&self, user_id: &str) -> Result<Vec<NotificationRecord>>;
    async fn list_for_user(
        &self,
        user_id: &str,
        limit: i64,
        offset: i64,
        after: Option<(i64, String)>,
    ) -> Result<Vec<NotificationRecord>>;
    async fn count_for_user(&self, user_id: &str) -> Result<i64>;
    async fn count_unread(&self, user_id: &str) -> Result<i64>;
    async fn mark_read(&self, notification_id: &str, user_id: &str) -> Result<bool>;
    async fn mark_all_read(&self, user_id: &str) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comment_visibility_defaults() {
        let comment = CommentRecord {
            id: "comment".into(),
            workspace_id: "ws".into(),
            doc_id: "doc".into(),
            author_id: "user".into(),
            body: "Hello".into(),
            resolved: false,
            visibility: CommentVisibility::Workspace,
            metadata: JsonValue::Null,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        assert_eq!(comment.visibility, CommentVisibility::Workspace);
    }
}
