use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue, json};
use sqlx::{Pool, Row, Sqlite};

use crate::{
    db::Database,
    notification::{
        CommentChangeAction, CommentChangeRecord, CommentRecord, CommentRecordWithCursor,
        CommentReplyRecord, CommentStore, CommentVisibility,
    },
};

#[derive(Clone)]
pub struct SqliteCommentStore {
    pool: Arc<Pool<Sqlite>>,
}

impl SqliteCommentStore {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: Arc::new(database.pool().clone()),
        }
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

    fn map_comment(row: sqlx::sqlite::SqliteRow) -> CommentRecord {
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

    fn map_reply(row: sqlx::sqlite::SqliteRow) -> CommentReplyRecord {
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

    fn comment_payload(comment: &CommentRecord) -> JsonValue {
        json!({
            "id": comment.id,
            "workspaceId": comment.workspace_id,
            "docId": comment.doc_id,
            "authorId": comment.author_id,
            "body": comment.body,
            "metadata": comment.metadata,
            "resolved": comment.resolved,
            "createdAt": comment.created_at.to_rfc3339(),
            "updatedAt": comment.updated_at.to_rfc3339(),
        })
    }

    fn reply_payload(reply: &CommentReplyRecord) -> JsonValue {
        json!({
            "id": reply.id,
            "commentId": reply.comment_id,
            "authorId": reply.author_id,
            "body": reply.body,
            "metadata": reply.metadata,
            "createdAt": reply.created_at.to_rfc3339(),
            "updatedAt": reply.updated_at.to_rfc3339(),
        })
    }

    fn deletion_payload(timestamp: DateTime<Utc>) -> JsonValue {
        json!({
            "deletedAt": timestamp.to_rfc3339(),
            "updatedAt": timestamp.to_rfc3339(),
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
        .execute(&*self.pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl CommentStore for SqliteCommentStore {
    async fn create(&self, comment: CommentRecord) -> Result<CommentRecord> {
        sqlx::query(
            "INSERT INTO doc_comments (id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        .execute(&*self.pool)
        .await?;

        self.insert_comment_change(
            &comment.workspace_id,
            &comment.doc_id,
            Some(&comment.id),
            None,
            CommentChangeAction::Update,
            &Self::comment_payload(&comment),
            comment.updated_at,
        )
        .await?;

        Ok(comment)
    }

    async fn find(&self, comment_id: &str) -> Result<Option<CommentRecord>> {
        let row = sqlx::query(
            "SELECT id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at FROM doc_comments WHERE id = ?",
        )
        .bind(comment_id)
        .fetch_optional(&*self.pool)
        .await?;

        Ok(row.map(Self::map_comment))
    }

    async fn list_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<Vec<CommentRecord>> {
        let rows = sqlx::query(
            "SELECT id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at FROM doc_comments WHERE workspace_id = ? AND doc_id = ? ORDER BY created_at ASC",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_all(&*self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_comment).collect())
    }

    async fn update(&self, comment: CommentRecord) -> Result<CommentRecord> {
        sqlx::query("UPDATE doc_comments SET body = ?, metadata = ?, updated_at = ? WHERE id = ?")
            .bind(Self::serialize_body(&comment.body))
            .bind(Self::serialize_metadata(&comment.metadata))
            .bind(comment.updated_at.timestamp())
            .bind(&comment.id)
            .execute(&*self.pool)
            .await?;

        self.insert_comment_change(
            &comment.workspace_id,
            &comment.doc_id,
            Some(&comment.id),
            None,
            CommentChangeAction::Update,
            &Self::comment_payload(&comment),
            comment.updated_at,
        )
        .await?;

        Ok(comment)
    }

    async fn delete(&self, comment_id: &str) -> Result<()> {
        let existing = self.find(comment_id).await?;
        let timestamp = Utc::now();

        sqlx::query("DELETE FROM doc_comments WHERE id = ?")
            .bind(comment_id)
            .execute(&*self.pool)
            .await?;

        if let Some(comment) = existing {
            self.insert_comment_change(
                &comment.workspace_id,
                &comment.doc_id,
                Some(&comment.id),
                None,
                CommentChangeAction::Delete,
                &Self::deletion_payload(timestamp),
                timestamp,
            )
            .await?;
        }
        Ok(())
    }

    async fn resolve(&self, comment_id: &str, resolved: bool) -> Result<()> {
        sqlx::query("UPDATE doc_comments SET resolved = ?, updated_at = ? WHERE id = ?")
            .bind(if resolved { 1 } else { 0 })
            .bind(Utc::now().timestamp())
            .bind(comment_id)
            .execute(&*self.pool)
            .await?;

        if let Some(comment) = self.find(comment_id).await? {
            self.insert_comment_change(
                &comment.workspace_id,
                &comment.doc_id,
                Some(&comment.id),
                None,
                CommentChangeAction::Update,
                &Self::comment_payload(&comment),
                comment.updated_at,
            )
            .await?;
        }
        Ok(())
    }

    async fn create_reply(&self, reply: CommentReplyRecord) -> Result<CommentReplyRecord> {
        sqlx::query(
            "INSERT INTO doc_comment_replies (id, comment_id, author_id, body, metadata, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&reply.id)
        .bind(&reply.comment_id)
        .bind(&reply.author_id)
        .bind(Self::serialize_body(&reply.body))
        .bind(Self::serialize_metadata(&reply.metadata))
        .bind(reply.created_at.timestamp())
        .bind(reply.updated_at.timestamp())
        .execute(&*self.pool)
        .await?;

        if let Some(parent) = self.find(&reply.comment_id).await? {
            self.insert_comment_change(
                &parent.workspace_id,
                &parent.doc_id,
                Some(&parent.id),
                Some(&reply.id),
                CommentChangeAction::Update,
                &Self::reply_payload(&reply),
                reply.updated_at,
            )
            .await?;
        }

        Ok(reply)
    }

    async fn find_reply(&self, reply_id: &str) -> Result<Option<CommentReplyRecord>> {
        let row = sqlx::query(
            "SELECT id, comment_id, author_id, body, metadata, created_at, updated_at FROM doc_comment_replies WHERE id = ?",
        )
        .bind(reply_id)
        .fetch_optional(&*self.pool)
        .await?;

        Ok(row.map(Self::map_reply))
    }

    async fn list_replies(&self, comment_id: &str) -> Result<Vec<CommentReplyRecord>> {
        let rows = sqlx::query(
            "SELECT id, comment_id, author_id, body, metadata, created_at, updated_at FROM doc_comment_replies WHERE comment_id = ? ORDER BY created_at ASC",
        )
        .bind(comment_id)
        .fetch_all(&*self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_reply).collect())
    }

    async fn update_reply(&self, reply: CommentReplyRecord) -> Result<CommentReplyRecord> {
        sqlx::query(
            "UPDATE doc_comment_replies SET body = ?, metadata = ?, updated_at = ? WHERE id = ?",
        )
        .bind(Self::serialize_body(&reply.body))
        .bind(Self::serialize_metadata(&reply.metadata))
        .bind(reply.updated_at.timestamp())
        .bind(&reply.id)
        .execute(&*self.pool)
        .await?;

        if let Some(parent) = self.find(&reply.comment_id).await? {
            self.insert_comment_change(
                &parent.workspace_id,
                &parent.doc_id,
                Some(&parent.id),
                Some(&reply.id),
                CommentChangeAction::Update,
                &Self::reply_payload(&reply),
                reply.updated_at,
            )
            .await?;
        }

        Ok(reply)
    }

    async fn delete_reply(&self, reply_id: &str) -> Result<()> {
        let existing = self.find_reply(reply_id).await?;
        let timestamp = Utc::now();

        sqlx::query("DELETE FROM doc_comment_replies WHERE id = ?")
            .bind(reply_id)
            .execute(&*self.pool)
            .await?;

        if let Some(reply) = existing {
            if let Some(parent) = self.find(&reply.comment_id).await? {
                self.insert_comment_change(
                    &parent.workspace_id,
                    &parent.doc_id,
                    Some(&parent.id),
                    Some(&reply.id),
                    CommentChangeAction::Delete,
                    &Self::deletion_payload(timestamp),
                    timestamp,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM doc_comments WHERE workspace_id = ? AND doc_id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_one(&*self.pool)
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
        let limit = limit.max(1);
        let rows = if let Some(after) = after_sid {
            sqlx::query(
                "SELECT rowid AS sid, id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at
                 FROM doc_comments
                 WHERE workspace_id = ?
                   AND doc_id = ?
                   AND rowid < ?
                 ORDER BY sid DESC
                 LIMIT ?",
            )
            .bind(workspace_id)
            .bind(doc_id)
            .bind(after)
            .bind(limit)
            .fetch_all(&*self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT rowid AS sid, id, workspace_id, doc_id, author_id, body, metadata, resolved, created_at, updated_at
                 FROM doc_comments
                 WHERE workspace_id = ?
                   AND doc_id = ?
                 ORDER BY sid DESC
                 LIMIT ?",
            )
            .bind(workspace_id)
            .bind(doc_id)
            .bind(limit)
            .fetch_all(&*self.pool)
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

    async fn list_comment_changes(
        &self,
        workspace_id: &str,
        doc_id: &str,
        after_comment_ts: Option<i64>,
        after_reply_ts: Option<i64>,
        limit: i64,
    ) -> Result<Vec<CommentChangeRecord>> {
        let limit = limit.max(1);
        let baseline = after_comment_ts
            .into_iter()
            .chain(after_reply_ts.into_iter())
            .min()
            .unwrap_or(0);

        let fetch_limit = limit.saturating_mul(3);

        let rows = sqlx::query(
            "SELECT id, comment_id, reply_id, action, payload, updated_at
             FROM doc_comment_changes
             WHERE workspace_id = ?
               AND doc_id = ?
               AND updated_at > ?
             ORDER BY updated_at ASC, id ASC
             LIMIT ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(baseline)
        .bind(fetch_limit)
        .fetch_all(&*self.pool)
        .await?;

        let mut changes = Vec::new();
        for row in rows {
            let updated_at = DateTime::<Utc>::from_timestamp(row.get("updated_at"), 0).unwrap();
            let comment_id: Option<String> = row
                .try_get::<Option<String>, _>("comment_id")
                .unwrap_or(None);
            let reply_id: Option<String> =
                row.try_get::<Option<String>, _>("reply_id").unwrap_or(None);

            let item_id = reply_id.clone().or_else(|| comment_id.clone());
            let Some(item_id) = item_id else {
                continue;
            };

            let is_reply = reply_id.is_some();
            if is_reply {
                if let Some(ts) = after_reply_ts {
                    if updated_at.timestamp() <= ts {
                        continue;
                    }
                }
            } else if let Some(ts) = after_comment_ts {
                if updated_at.timestamp() <= ts {
                    continue;
                }
            }

            let payload_raw: String = row.get("payload");
            let payload = serde_json::from_str(&payload_raw).unwrap_or(JsonValue::Null);
            let action: String = row.get("action");

            changes.push(CommentChangeRecord {
                action: Self::action_from_str(&action),
                id: item_id,
                comment_id,
                workspace_id: workspace_id.to_string(),
                doc_id: doc_id.to_string(),
                item: payload,
                updated_at,
            });

            if changes.len() as i64 >= limit {
                break;
            }
        }

        Ok(changes)
    }
}
