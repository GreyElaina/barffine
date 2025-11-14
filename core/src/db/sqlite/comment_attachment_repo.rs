use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use sqlx::{Pool, Row, Sqlite, sqlite::SqliteRow};

use crate::{
    comment_attachment::{CommentAttachmentRecord, CommentAttachmentUpsert},
    db::comment_attachment_repo::CommentAttachmentRepository,
};

pub struct SqliteCommentAttachmentRepository {
    pool: Pool<Sqlite>,
}

impl SqliteCommentAttachmentRepository {
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self { pool }
    }

    fn map_record(row: SqliteRow) -> CommentAttachmentRecord {
        let timestamp = row.get::<i64, _>("created_at");
        let created_at = Utc
            .timestamp_opt(timestamp, 0)
            .single()
            .unwrap_or_else(Utc::now);

        CommentAttachmentRecord {
            workspace_id: row.get("workspace_id"),
            doc_id: row.get("doc_id"),
            key: row.get("key"),
            name: row.get("name"),
            mime: row.get("mime"),
            size: row.get("size"),
            created_at,
            created_by: row.get::<Option<String>, _>("created_by"),
        }
    }
}

#[async_trait]
impl CommentAttachmentRepository for SqliteCommentAttachmentRepository {
    async fn upsert(&self, input: CommentAttachmentUpsert<'_>) -> Result<CommentAttachmentRecord> {
        let created_at = Utc::now().timestamp();

        sqlx::query(
            "INSERT INTO comment_attachments (workspace_id, doc_id, key, name, mime, size, created_at, created_by)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(workspace_id, doc_id, key) DO UPDATE SET name = excluded.name, mime = excluded.mime, size = excluded.size",
        )
        .bind(input.workspace_id)
        .bind(input.doc_id)
        .bind(input.key)
        .bind(input.name)
        .bind(input.mime)
        .bind(input.size)
        .bind(created_at)
        .bind(input.created_by)
        .execute(&self.pool)
        .await?;

        self.get(input.workspace_id, input.doc_id, input.key)
            .await?
            .ok_or_else(|| anyhow!("comment attachment not found after upsert"))
    }

    async fn get(
        &self,
        workspace_id: &str,
        doc_id: &str,
        key: &str,
    ) -> Result<Option<CommentAttachmentRecord>> {
        let row = sqlx::query(
            "SELECT workspace_id, doc_id, key, name, mime, size, created_at, created_by
             FROM comment_attachments
             WHERE workspace_id = ? AND doc_id = ? AND key = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_record))
    }

    async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<CommentAttachmentRecord>> {
        let rows = sqlx::query(
            "SELECT workspace_id, doc_id, key, name, mime, size, created_at, created_by
             FROM comment_attachments
             WHERE workspace_id = ? AND doc_id = ?
             ORDER BY created_at ASC",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_record).collect())
    }

    async fn delete(&self, workspace_id: &str, doc_id: &str, key: &str) -> Result<()> {
        sqlx::query(
            "DELETE FROM comment_attachments WHERE workspace_id = ? AND doc_id = ? AND key = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
