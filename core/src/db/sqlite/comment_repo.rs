use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::{Pool, Row, Sqlite, sqlite::SqliteRow};

use crate::db::comment_repo::{CommentChangeRow, CommentRepository};
use crate::notification::{
    CommentChangeAction, CommentRecord, CommentRecordWithCursor, CommentReplyRecord,
    CommentVisibility,
};

pub struct SqliteCommentRepository {
    pool: Pool<Sqlite>,
}

impl SqliteCommentRepository {
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self { pool }
    }

    fn serialize_body(body: &str) -> String {
        body.to_owned()
    }

    fn deserialize_body(body: &str) -> String {
        body.to_owned()
    }

    fn serialize_metadata(metadata: &JsonValue) -> String {
        serde_json::to_string(metadata).unwrap_or_else(|_| "null".to_string())
    }

    fn deserialize_metadata(value: &str) -> JsonValue {
        serde_json::from_str(value).unwrap_or(JsonValue::Null)
    }

    fn map_comment(row: SqliteRow) -> CommentRecord {
        CommentRecord {
            id: row.get("id"),
            workspace_id: row.get("workspace_id"),
            doc_id: row.get("doc_id"),
            author_id: row.get("author_id"),
            body: Self::deserialize_body(row.get("body")),
            visibility: CommentVisibility::Workspace,
            metadata: Self::deserialize_metadata(row.get("metadata")),
            resolved: row.get::<i64, _>("resolved") != 0,
            created_at: DateTime::<Utc>::from_timestamp(row.get("created_at"), 0).unwrap(),
            updated_at: DateTime::<Utc>::from_timestamp(row.get("updated_at"), 0).unwrap(),
        }
    }

    fn map_reply(row: SqliteRow) -> CommentReplyRecord {
        CommentReplyRecord {
            id: row.get("id"),
            comment_id: row.get("comment_id"),
            author_id: row.get("author_id"),
            body: Self::deserialize_body(row.get("body")),
            metadata: Self::deserialize_metadata(row.get("metadata")),
            created_at: DateTime::<Utc>::from_timestamp(row.get("created_at"), 0).unwrap(),
            updated_at: DateTime::<Utc>::from_timestamp(row.get("updated_at"), 0).unwrap(),
        }
    }

    fn action_to_str(action: CommentChangeAction) -> &'static str {
        match action {
            CommentChangeAction::Update => "update",
            CommentChangeAction::Delete => "delete",
        }
    }

    fn action_from_str(value: &str) -> CommentChangeAction {
        match value {
            "delete" => CommentChangeAction::Delete,
            _ => CommentChangeAction::Update,
        }
    }
}

#[async_trait]
impl CommentRepository for SqliteCommentRepository {
    async fn insert_comment(&self, comment: &CommentRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO doc_comments (id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&comment.id)
        .bind(&comment.workspace_id)
        .bind(&comment.doc_id)
        .bind(&comment.author_id)
        .bind(Self::serialize_body(&comment.body))
        .bind(Self::serialize_metadata(&comment.metadata))
        .bind(if comment.resolved { 1 } else { 0 })
        .bind(comment.created_at.timestamp())
        .bind(comment.updated_at.timestamp())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn fetch_comment(&self, comment_id: &str) -> Result<Option<CommentRecord>> {
        let row = sqlx::query(
            "SELECT id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at
             FROM doc_comments WHERE id = ?",
        )
        .bind(comment_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_comment))
    }

    async fn list_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<Vec<CommentRecord>> {
        let rows = sqlx::query(
            "SELECT id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at
             FROM doc_comments WHERE workspace_id = ? AND doc_id = ? ORDER BY created_at ASC",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_comment).collect())
    }

    async fn update_comment(&self, comment: &CommentRecord) -> Result<()> {
        sqlx::query("UPDATE doc_comments SET body = ?, metadata = ?, updated_at = ? WHERE id = ?")
            .bind(Self::serialize_body(&comment.body))
            .bind(Self::serialize_metadata(&comment.metadata))
            .bind(comment.updated_at.timestamp())
            .bind(&comment.id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn delete_comment(&self, comment_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM doc_comments WHERE id = ?")
            .bind(comment_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn set_comment_resolved(
        &self,
        comment_id: &str,
        resolved: bool,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query("UPDATE doc_comments SET resolved = ?, updated_at = ? WHERE id = ?")
            .bind(if resolved { 1 } else { 0 })
            .bind(updated_at.timestamp())
            .bind(comment_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn insert_reply(&self, reply: &CommentReplyRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO doc_comment_replies (id, comment_id, author_id, body, metadata, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&reply.id)
        .bind(&reply.comment_id)
        .bind(&reply.author_id)
        .bind(Self::serialize_body(&reply.body))
        .bind(Self::serialize_metadata(&reply.metadata))
        .bind(reply.created_at.timestamp())
        .bind(reply.updated_at.timestamp())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn fetch_reply(&self, reply_id: &str) -> Result<Option<CommentReplyRecord>> {
        let row = sqlx::query(
            "SELECT id, comment_id, author_id, body, metadata, created_at, updated_at
             FROM doc_comment_replies WHERE id = ?",
        )
        .bind(reply_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_reply))
    }

    async fn list_replies(&self, comment_id: &str) -> Result<Vec<CommentReplyRecord>> {
        let rows = sqlx::query(
            "SELECT id, comment_id, author_id, body, metadata, created_at, updated_at
             FROM doc_comment_replies WHERE comment_id = ? ORDER BY created_at ASC",
        )
        .bind(comment_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_reply).collect())
    }

    async fn update_reply(&self, reply: &CommentReplyRecord) -> Result<()> {
        sqlx::query(
            "UPDATE doc_comment_replies SET body = ?, metadata = ?, updated_at = ? WHERE id = ?",
        )
        .bind(Self::serialize_body(&reply.body))
        .bind(Self::serialize_metadata(&reply.metadata))
        .bind(reply.updated_at.timestamp())
        .bind(&reply.id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete_reply(&self, reply_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM doc_comment_replies WHERE id = ?")
            .bind(reply_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM doc_comments WHERE workspace_id = ? AND doc_id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    async fn list_comments_paginated(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        after_sid: Option<i64>,
    ) -> Result<Vec<CommentRecordWithCursor>> {
        let rows = if let Some(after) = after_sid {
            sqlx::query(
                "SELECT rowid AS sid, id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at
                 FROM doc_comments
                 WHERE workspace_id = ? AND doc_id = ? AND rowid < ?
                 ORDER BY sid DESC
                 LIMIT ?",
            )
            .bind(workspace_id)
            .bind(doc_id)
            .bind(after)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT rowid AS sid, id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at
                 FROM doc_comments
                 WHERE workspace_id = ? AND doc_id = ?
                 ORDER BY sid DESC
                 LIMIT ?",
            )
            .bind(workspace_id)
            .bind(doc_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows
            .into_iter()
            .map(|row| {
                let sid = row.get::<i64, _>("sid");
                let record = Self::map_comment(row);
                CommentRecordWithCursor { record, sid }
            })
            .collect())
    }

    async fn insert_comment_change(
        &self,
        workspace_id: &str,
        doc_id: &str,
        comment_id: Option<&str>,
        reply_id: Option<&str>,
        action: CommentChangeAction,
        payload: &JsonValue,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO doc_comment_changes (workspace_id, doc_id, comment_id, reply_id, action, payload, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(comment_id)
        .bind(reply_id)
        .bind(Self::action_to_str(action))
        .bind(serde_json::to_string(payload).unwrap_or_else(|_| "{}".to_string()))
        .bind(updated_at.timestamp())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn list_comment_changes(
        &self,
        workspace_id: &str,
        doc_id: &str,
        baseline_timestamp: i64,
        baseline_row_id: Option<i64>,
        limit: i64,
    ) -> Result<Vec<CommentChangeRow>> {
        let query = if baseline_row_id.is_some() {
            "SELECT id, comment_id, reply_id, action, payload, updated_at
             FROM doc_comment_changes
             WHERE workspace_id = ? AND doc_id = ?
               AND (updated_at > ? OR (updated_at = ? AND id > ?))
             ORDER BY updated_at ASC, id ASC
             LIMIT ?"
        } else {
            "SELECT id, comment_id, reply_id, action, payload, updated_at
             FROM doc_comment_changes
             WHERE workspace_id = ? AND doc_id = ? AND updated_at > ?
             ORDER BY updated_at ASC, id ASC
             LIMIT ?"
        };

        let mut stmt = sqlx::query(query)
            .bind(workspace_id)
            .bind(doc_id)
            .bind(baseline_timestamp);
        if let Some(row_id) = baseline_row_id {
            stmt = stmt.bind(baseline_timestamp).bind(row_id);
        }
        let rows = stmt.bind(limit).fetch_all(&self.pool).await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let payload_raw: String = row.get("payload");
                let change_id: i64 = row.get("id");
                CommentChangeRow {
                    row_id: change_id,
                    comment_id: row
                        .try_get::<Option<String>, _>("comment_id")
                        .unwrap_or(None),
                    reply_id: row.try_get::<Option<String>, _>("reply_id").unwrap_or(None),
                    action: Self::action_from_str(row.get("action")),
                    payload: serde_json::from_str(&payload_raw).unwrap_or(JsonValue::Null),
                    updated_at: DateTime::<Utc>::from_timestamp(row.get("updated_at"), 0).unwrap(),
                }
            })
            .collect())
    }
}
