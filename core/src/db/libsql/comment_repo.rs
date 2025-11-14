use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use deadpool_libsql::{
    Pool,
    libsql::{self, params},
};
use serde_json::Value as JsonValue;

use crate::db::comment_repo::{CommentChangeRow, CommentRepository};
use crate::db::libsql::row_ext;
use crate::notification::{
    CommentChangeAction, CommentRecord, CommentRecordWithCursor, CommentReplyRecord,
    CommentVisibility,
};

pub struct LibsqlCommentRepository {
    pool: Pool,
}

impl LibsqlCommentRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn serialize_body(body: &str) -> String {
        body.to_owned()
    }

    fn deserialize_body(body: String) -> String {
        body
    }

    fn serialize_metadata(metadata: &JsonValue) -> String {
        serde_json::to_string(metadata).unwrap_or_else(|_| "null".to_string())
    }

    fn deserialize_metadata(value: String) -> JsonValue {
        serde_json::from_str(&value).unwrap_or(JsonValue::Null)
    }

    fn map_comment(row: &libsql::Row) -> Result<CommentRecord> {
        let created_at_ts: i64 = row_ext::get(row, "created_at")?;
        let updated_at_ts: i64 = row_ext::get(row, "updated_at")?;
        Ok(CommentRecord {
            id: row_ext::get(row, "id")?,
            workspace_id: row_ext::get(row, "workspace_id")?,
            doc_id: row_ext::get(row, "doc_id")?,
            author_id: row_ext::get(row, "author_id")?,
            body: Self::deserialize_body(row_ext::get(row, "body")?),
            visibility: CommentVisibility::Workspace,
            metadata: Self::deserialize_metadata(row_ext::get(row, "metadata")?),
            resolved: row_ext::get::<i64>(row, "resolved")? != 0,
            created_at: Utc
                .timestamp_opt(created_at_ts, 0)
                .single()
                .ok_or_else(|| anyhow!("invalid created_at timestamp"))?,
            updated_at: Utc
                .timestamp_opt(updated_at_ts, 0)
                .single()
                .ok_or_else(|| anyhow!("invalid updated_at timestamp"))?,
        })
    }

    fn map_reply(row: &libsql::Row) -> Result<CommentReplyRecord> {
        let created_at_ts: i64 = row_ext::get(row, "created_at")?;
        let updated_at_ts: i64 = row_ext::get(row, "updated_at")?;
        Ok(CommentReplyRecord {
            id: row_ext::get(row, "id")?,
            comment_id: row_ext::get(row, "comment_id")?,
            author_id: row_ext::get(row, "author_id")?,
            body: Self::deserialize_body(row_ext::get(row, "body")?),
            metadata: Self::deserialize_metadata(row_ext::get(row, "metadata")?),
            created_at: Utc
                .timestamp_opt(created_at_ts, 0)
                .single()
                .ok_or_else(|| anyhow!("invalid created_at timestamp"))?,
            updated_at: Utc
                .timestamp_opt(updated_at_ts, 0)
                .single()
                .ok_or_else(|| anyhow!("invalid updated_at timestamp"))?,
        })
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
impl CommentRepository for LibsqlCommentRepository {
    async fn insert_comment(&self, comment: &CommentRecord) -> Result<()> {
        let id_ref = comment.id.as_str();
        let workspace_id_ref = comment.workspace_id.as_str();
        let doc_id_ref = comment.doc_id.as_str();
        let author_id_ref = comment.author_id.as_str();
        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO doc_comments (id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                id_ref,
                workspace_id_ref,
                doc_id_ref,
                author_id_ref,
                Self::serialize_body(&comment.body),
                Self::serialize_metadata(&comment.metadata),
                if comment.resolved { 1 } else { 0 },
                comment.created_at.timestamp(),
                comment.updated_at.timestamp()
            ],
        )
        .await?;
        Ok(())
    }

    async fn fetch_comment(&self, comment_id: &str) -> Result<Option<CommentRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at
                 FROM doc_comments WHERE id = ?",
                params![comment_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::map_comment(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn list_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<Vec<CommentRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at
                 FROM doc_comments WHERE workspace_id = ? AND doc_id = ? ORDER BY created_at ASC",
                params![workspace_id, doc_id],
            )
            .await?;

        let mut comments = Vec::new();
        while let Some(row) = rows.next().await? {
            comments.push(Self::map_comment(&row)?);
        }
        Ok(comments)
    }

    async fn update_comment(&self, comment: &CommentRecord) -> Result<()> {
        let id_ref = comment.id.as_str();
        let conn = self.pool.get().await?;
        conn.execute(
            "UPDATE doc_comments SET body = ?, metadata = ?, updated_at = ? WHERE id = ?",
            params![
                Self::serialize_body(&comment.body),
                Self::serialize_metadata(&comment.metadata),
                comment.updated_at.timestamp(),
                id_ref
            ],
        )
        .await?;
        Ok(())
    }

    async fn delete_comment(&self, comment_id: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute("DELETE FROM doc_comments WHERE id = ?", params![comment_id])
            .await?;
        Ok(())
    }

    async fn set_comment_resolved(
        &self,
        comment_id: &str,
        resolved: bool,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "UPDATE doc_comments SET resolved = ?, updated_at = ? WHERE id = ?",
            params![
                if resolved { 1 } else { 0 },
                updated_at.timestamp(),
                comment_id
            ],
        )
        .await?;
        Ok(())
    }

    async fn insert_reply(&self, reply: &CommentReplyRecord) -> Result<()> {
        let reply_id_ref = reply.id.as_str();
        let comment_id_ref = reply.comment_id.as_str();
        let author_id_ref = reply.author_id.as_str();
        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO doc_comment_replies (id, comment_id, author_id, body, metadata, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                reply_id_ref,
                comment_id_ref,
                author_id_ref,
                Self::serialize_body(&reply.body),
                Self::serialize_metadata(&reply.metadata),
                reply.created_at.timestamp(),
                reply.updated_at.timestamp()
            ],
        )
        .await?;
        Ok(())
    }

    async fn fetch_reply(&self, reply_id: &str) -> Result<Option<CommentReplyRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, comment_id, author_id, body, metadata, created_at, updated_at
                 FROM doc_comment_replies WHERE id = ?",
                params![reply_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::map_reply(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn list_replies(&self, comment_id: &str) -> Result<Vec<CommentReplyRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, comment_id, author_id, body, metadata, created_at, updated_at
                 FROM doc_comment_replies WHERE comment_id = ? ORDER BY created_at ASC",
                params![comment_id],
            )
            .await?;

        let mut replies = Vec::new();
        while let Some(row) = rows.next().await? {
            replies.push(Self::map_reply(&row)?);
        }
        Ok(replies)
    }

    async fn update_reply(&self, reply: &CommentReplyRecord) -> Result<()> {
        let reply_id_ref = reply.id.as_str();
        let conn = self.pool.get().await?;
        conn.execute(
            "UPDATE doc_comment_replies SET body = ?, metadata = ?, updated_at = ? WHERE id = ?",
            params![
                Self::serialize_body(&reply.body),
                Self::serialize_metadata(&reply.metadata),
                reply.updated_at.timestamp(),
                reply_id_ref
            ],
        )
        .await?;
        Ok(())
    }

    async fn delete_reply(&self, reply_id: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "DELETE FROM doc_comment_replies WHERE id = ?",
            params![reply_id],
        )
        .await?;
        Ok(())
    }

    async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT COUNT(*) AS count FROM doc_comments WHERE workspace_id = ? AND doc_id = ?",
                params![workspace_id, doc_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(row_ext::get(&row, "count")?)
        } else {
            Ok(0)
        }
    }

    async fn list_comments_paginated(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        after_sid: Option<i64>,
    ) -> Result<Vec<CommentRecordWithCursor>> {
        let conn = self.pool.get().await?;
        let mut rows = if let Some(after) = after_sid {
            conn.query(
                "SELECT rowid AS sid, id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at
                 FROM doc_comments
                 WHERE workspace_id = ? AND doc_id = ? AND rowid < ?
                 ORDER BY sid DESC
                 LIMIT ?",
                params![workspace_id, doc_id, after, limit],
            )
            .await?
        } else {
            conn.query(
                "SELECT rowid AS sid, id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at
                 FROM doc_comments
                 WHERE workspace_id = ? AND doc_id = ?
                 ORDER BY sid DESC
                 LIMIT ?",
                params![workspace_id, doc_id, limit],
            )
            .await?
        };

        let mut records = Vec::new();
        while let Some(row) = rows.next().await? {
            let sid: i64 = row_ext::get(&row, "sid")?;
            let record = Self::map_comment(&row)?;
            records.push(CommentRecordWithCursor { record, sid });
        }
        Ok(records)
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
        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO doc_comment_changes (workspace_id, doc_id, comment_id, reply_id, action, payload, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                workspace_id,
                doc_id,
                comment_id,
                reply_id,
                Self::action_to_str(action),
                serde_json::to_string(payload).unwrap_or_else(|_| "{}".to_string()),
                updated_at.timestamp()
            ],
        )
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
        let conn = self.pool.get().await?;
        let mut rows = if let Some(row_id) = baseline_row_id {
            conn.query(
                "SELECT id, comment_id, reply_id, action, payload, updated_at
                 FROM doc_comment_changes
                 WHERE workspace_id = ? AND doc_id = ?
                   AND (updated_at > ? OR (updated_at = ? AND id > ?))
                 ORDER BY updated_at ASC, id ASC
                 LIMIT ?",
                params![
                    workspace_id,
                    doc_id,
                    baseline_timestamp,
                    baseline_timestamp,
                    row_id,
                    limit
                ],
            )
            .await?
        } else {
            conn.query(
                "SELECT id, comment_id, reply_id, action, payload, updated_at
                 FROM doc_comment_changes
                 WHERE workspace_id = ? AND doc_id = ? AND updated_at > ?
                 ORDER BY updated_at ASC, id ASC
                 LIMIT ?",
                params![workspace_id, doc_id, baseline_timestamp, limit],
            )
            .await?
        };

        let mut changes = Vec::new();
        while let Some(row) = rows.next().await? {
            let payload_raw: String = row_ext::get(&row, "payload")?;
            let change_id: i64 = row_ext::get(&row, "id")?;
            changes.push(CommentChangeRow {
                row_id: change_id,
                comment_id: row_ext::get(&row, "comment_id")?,
                reply_id: row_ext::get(&row, "reply_id")?,
                action: Self::action_from_str(row_ext::get::<String>(&row, "action")?.as_str()),
                payload: serde_json::from_str(&payload_raw).unwrap_or(JsonValue::Null),
                updated_at: Utc
                    .timestamp_opt(row_ext::get::<i64>(&row, "updated_at")?, 0)
                    .single()
                    .ok_or_else(|| anyhow!("invalid updated_at timestamp"))?,
            });
        }
        Ok(changes)
    }
}
