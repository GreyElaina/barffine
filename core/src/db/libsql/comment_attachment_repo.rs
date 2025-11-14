use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use deadpool_libsql::{
    Pool,
    libsql::{self, params},
};

use crate::comment_attachment::{CommentAttachmentRecord, CommentAttachmentUpsert};
use crate::db::comment_attachment_repo::CommentAttachmentRepository;
use crate::db::libsql::row_ext;

fn row_to_record(row: libsql::Row) -> Result<CommentAttachmentRecord> {
    let created_at_ts: i64 = row_ext::get(&row, "created_at")?;
    let created_at = Utc
        .timestamp_opt(created_at_ts, 0)
        .single()
        .unwrap_or_else(Utc::now);

    Ok(CommentAttachmentRecord {
        workspace_id: row_ext::get(&row, "workspace_id")?,
        doc_id: row_ext::get(&row, "doc_id")?,
        key: row_ext::get(&row, "key")?,
        name: row_ext::get(&row, "name")?,
        mime: row_ext::get(&row, "mime")?,
        size: row_ext::get(&row, "size")?,
        created_at,
        created_by: row_ext::get(&row, "created_by")?,
    })
}

pub struct LibsqlCommentAttachmentRepository {
    pool: Pool,
}

impl LibsqlCommentAttachmentRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CommentAttachmentRepository for LibsqlCommentAttachmentRepository {
    async fn upsert(&self, input: CommentAttachmentUpsert<'_>) -> Result<CommentAttachmentRecord> {
        let conn = self.pool.get().await?;
        let created_at = Utc::now().timestamp();
        conn.execute(
            "INSERT INTO comment_attachments (workspace_id, doc_id, key, name, mime, size, created_at, created_by)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(workspace_id, doc_id, key)
             DO UPDATE SET name = excluded.name, mime = excluded.mime, size = excluded.size",
            params![
                input.workspace_id,
                input.doc_id,
                input.key,
                input.name,
                input.mime,
                input.size,
                created_at,
                input.created_by
            ],
        )
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
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT workspace_id, doc_id, key, name, mime, size, created_at, created_by
                 FROM comment_attachments
                 WHERE workspace_id = ? AND doc_id = ? AND key = ?",
                params![workspace_id, doc_id, key],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row_to_record(row)?))
        } else {
            Ok(None)
        }
    }

    async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<CommentAttachmentRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT workspace_id, doc_id, key, name, mime, size, created_at, created_by
                 FROM comment_attachments
                 WHERE workspace_id = ? AND doc_id = ?
                 ORDER BY created_at ASC",
                params![workspace_id, doc_id],
            )
            .await?;

        let mut attachments = Vec::new();
        while let Some(row) = rows.next().await? {
            attachments.push(row_to_record(row)?);
        }
        Ok(attachments)
    }

    async fn delete(&self, workspace_id: &str, doc_id: &str, key: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "DELETE FROM comment_attachments WHERE workspace_id = ? AND doc_id = ? AND key = ?",
            params![workspace_id, doc_id, key],
        )
        .await?;
        Ok(())
    }
}
