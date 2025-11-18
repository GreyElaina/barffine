use anyhow::{Result, bail};
use async_trait::async_trait;
use sqlx::{Pool, Row, Sqlite, sqlite::SqliteRow};
use std::sync::Arc;

use crate::{
    db::doc_repo::{
        AppendDocUpdatesParams, CompactionApplyParams, CompactionSource, DocRepository,
        DocumentListOrder, DuplicateDocParams, HistorySnapshotInsert, InsertDocRecordParams,
        MetadataBackfillParams, ReplaceDocSnapshotParams, SnapshotUpsertParams,
    },
    db::sqlite::doc_update_log_store::SqliteDocUpdateLogStore,
    doc_snapshot_store::DocSnapshotStore,
    doc_store::{
        DocumentCursor, DocumentHistoryRecord, DocumentMetadata, DocumentSnapshot,
        UserShareTokenRecord,
    },
    doc_update_log::DocUpdateLogReader,
    ids::{DocId, WorkspaceId},
};

pub struct SqliteDocRepository {
    pool: Pool<Sqlite>,
    doc_logs: Arc<SqliteDocUpdateLogStore>,
    doc_snapshots: Arc<DocSnapshotStore>,
}

impl SqliteDocRepository {
    pub fn new(
        pool: Pool<Sqlite>,
        doc_logs: Arc<SqliteDocUpdateLogStore>,
        doc_snapshots: Arc<DocSnapshotStore>,
    ) -> Self {
        Self {
            pool,
            doc_logs,
            doc_snapshots,
        }
    }

    fn metadata_from_row(row: SqliteRow) -> DocumentMetadata {
        let public = row.get::<i64, _>("public") != 0;
        let blocked = row.try_get::<i64, _>("blocked").unwrap_or(0) != 0;
        let share_token = row
            .try_get::<Option<String>, _>("share_token")
            .unwrap_or(None)
            .filter(|_| public);

        DocumentMetadata {
            id: DocId::from(row.get::<String, _>("id")),
            workspace_id: WorkspaceId::from(row.get::<String, _>("workspace_id")),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            default_role: row.get("default_role"),
            public,
            blocked,
            mode: row.get("mode"),
            title: row.try_get::<Option<String>, _>("title").unwrap_or(None),
            summary: row.try_get::<Option<String>, _>("summary").unwrap_or(None),
            creator_id: row
                .try_get::<Option<String>, _>("creator_id")
                .unwrap_or(None),
            updater_id: row
                .try_get::<Option<String>, _>("updater_id")
                .unwrap_or(None),
            share_token,
            trashed_at: row.try_get::<Option<i64>, _>("trashed_at").unwrap_or(None),
            trashed_by: row
                .try_get::<Option<String>, _>("trashed_by")
                .unwrap_or(None),
            snapshot: None,
        }
    }
}

#[async_trait]
impl DocRepository for SqliteDocRepository {
    async fn fetch_snapshot_with_timestamp(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentSnapshot>> {
        let row = sqlx::query(
            "SELECT snapshot, snapshot_kv_key, updated_at
             FROM documents
             WHERE workspace_id = ? AND id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let inline_snapshot: Vec<u8> = row.try_get("snapshot")?;
        let kv_key = row
            .try_get::<Option<String>, _>("snapshot_kv_key")
            .unwrap_or(None);
        let snapshot = self
            .doc_snapshots
            .resolve_snapshot(kv_key.as_deref(), inline_snapshot)?;
        let updated_at: i64 = row.try_get("updated_at")?;
        Ok(Some(DocumentSnapshot {
            snapshot,
            updated_at,
        }))
    }

    async fn insert_doc_record(&self, params: InsertDocRecordParams) -> Result<()> {
        let InsertDocRecordParams {
            workspace_id,
            doc_id,
            snapshot,
            owner_id,
            title,
            created_at,
        } = params;

        let mut tx = self.pool.begin().await?;
        let snapshot_payload =
            self.doc_snapshots
                .prepare_doc_snapshot(&workspace_id, &doc_id, &snapshot)?;

        sqlx::query(
            "INSERT INTO documents (
                 id,
                 workspace_id,
                 snapshot,
                 snapshot_kv_key,
                 created_at,
                 updated_at,
                 default_role,
                 public,
                 blocked,
                 mode,
                 title,
                 summary,
                 creator_id,
                 updater_id,
                 trashed_at,
                 trashed_by
             ) VALUES (?, ?, ?, ?, ?, ?, 'manager', 0, 0, 'page', ?, NULL, ?, ?, NULL, NULL)",
        )
        .bind(&doc_id)
        .bind(&workspace_id)
        .bind(&snapshot_payload.inline)
        .bind(snapshot_payload.kv_key.as_deref())
        .bind(created_at)
        .bind(created_at)
        .bind(title.as_deref())
        .bind(&owner_id)
        .bind(&owner_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn fetch_metadata(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let row = sqlx::query(
            "SELECT
                 d.id, d.workspace_id, d.created_at, d.updated_at,
                 d.default_role, d.public, d.blocked, d.mode,
                 d.title, d.summary, d.creator_id, d.updater_id,
                 d.trashed_at, d.trashed_by,
                 l.token AS share_token
             FROM documents d
             LEFT JOIN doc_public_links l
               ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
             WHERE d.workspace_id = ? AND d.id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::metadata_from_row))
    }

    async fn list_documents(
        &self,
        workspace_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentCursor>,
        order: DocumentListOrder,
    ) -> Result<Vec<DocumentMetadata>> {
        let rows = match order {
            DocumentListOrder::CreatedAsc => {
                list_created(&self.pool, workspace_id, limit, offset, cursor).await?
            }
            DocumentListOrder::UpdatedDesc => {
                list_updated_desc(&self.pool, workspace_id, limit, offset, cursor).await?
            }
        };
        Ok(rows.into_iter().map(Self::metadata_from_row).collect())
    }

    async fn set_doc_trashed(
        &self,
        workspace_id: &str,
        doc_id: &str,
        trashed_at: i64,
        trashed_by: &str,
    ) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE documents
             SET trashed_at = ?, trashed_by = ?, updated_at = ?, updater_id = ?
             WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
        )
        .bind(trashed_at)
        .bind(trashed_by)
        .bind(trashed_at)
        .bind(trashed_by)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn restore_doc_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        updated_at: i64,
        restored_by: &str,
    ) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE documents
             SET trashed_at = NULL, trashed_by = NULL, updated_at = ?, updater_id = ?
             WHERE workspace_id = ? AND id = ? AND trashed_at IS NOT NULL",
        )
        .bind(updated_at)
        .bind(restored_by)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn delete_doc_entry(&self, workspace_id: &str, doc_id: &str) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let history_keys = sqlx::query(
            "SELECT snapshot_kv_key FROM document_history
             WHERE workspace_id = ? AND doc_id = ? AND snapshot_kv_key IS NOT NULL",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_all(&mut *tx)
        .await?;

        let deleted_row = sqlx::query(
            "DELETE FROM documents
             WHERE workspace_id = ? AND id = ?
             RETURNING snapshot_kv_key",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = deleted_row else {
            tx.rollback().await?;
            return Ok(0);
        };

        let doc_snapshot_key = row.try_get::<Option<String>, _>("snapshot_kv_key")?;
        self.doc_snapshots
            .delete_snapshot(doc_snapshot_key.as_deref())?;

        for row in history_keys {
            let key = row.try_get::<Option<String>, _>("snapshot_kv_key")?;
            self.doc_snapshots.delete_snapshot(key.as_deref())?;
        }

        self.doc_logs
            .delete_doc_logs(&mut tx, "workspace", workspace_id, doc_id)
            .await?;
        tx.commit().await?;
        Ok(1)
    }

    async fn publish_doc_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        mode: &str,
        timestamp: i64,
        share_token: &str,
    ) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE documents
             SET public = 1, mode = ?, updated_at = ?
             WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
        )
        .bind(mode)
        .bind(timestamp)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(0);
        }

        sqlx::query(
            "INSERT INTO doc_public_links (workspace_id, doc_id, token, created_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(workspace_id, doc_id) DO NOTHING",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(share_token)
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(result.rows_affected())
    }

    async fn unpublish_doc_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        timestamp: i64,
    ) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE documents
             SET public = 0, updated_at = ?
             WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
        )
        .bind(timestamp)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(0);
        }

        sqlx::query("DELETE FROM doc_public_links WHERE workspace_id = ? AND doc_id = ?")
            .bind(workspace_id)
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(result.rows_affected())
    }

    async fn update_default_role_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        role: &str,
    ) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE documents
             SET default_role = ?
             WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
        )
        .bind(role)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn update_doc_mode(&self, workspace_id: &str, doc_id: &str, mode: &str) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE documents
             SET mode = ?
             WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
        )
        .bind(mode)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn count_documents(&self, workspace_id: &str) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) as count FROM documents WHERE workspace_id = ? AND trashed_at IS NULL",
        )
        .bind(workspace_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    async fn list_share_tokens_for_user(&self, user_id: &str) -> Result<Vec<UserShareTokenRecord>> {
        let rows = sqlx::query(
            "SELECT l.workspace_id, l.doc_id, l.token, l.created_at
             FROM doc_public_links l
             JOIN workspace_members wm
               ON wm.workspace_id = l.workspace_id
             JOIN documents d
               ON d.workspace_id = l.workspace_id AND d.id = l.doc_id
             WHERE wm.user_id = ? AND wm.status = 'Accepted'
               AND d.public = 1 AND d.trashed_at IS NULL
             ORDER BY l.created_at DESC, l.workspace_id ASC, l.doc_id ASC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| UserShareTokenRecord {
                workspace_id: WorkspaceId::from(row.get::<String, _>("workspace_id")),
                doc_id: DocId::from(row.get::<String, _>("doc_id")),
                token: row.get("token"),
                created_at: row.get::<i64, _>("created_at"),
            })
            .collect())
    }

    async fn backfill_metadata(&self, params: MetadataBackfillParams) -> Result<()> {
        let MetadataBackfillParams {
            workspace_id,
            doc_id,
            title,
            summary,
            creator_id,
            updater_id,
            created_at,
            updated_at,
        } = params;

        sqlx::query(
            "UPDATE documents
             SET title = ?, summary = ?, creator_id = ?, updater_id = ?, created_at = ?, updated_at = ?
             WHERE workspace_id = ? AND id = ?",
        )
        .bind(title.as_deref())
        .bind(summary.as_deref())
        .bind(creator_id.as_deref())
        .bind(updater_id.as_deref())
        .bind(created_at)
        .bind(updated_at)
        .bind(&workspace_id)
        .bind(&doc_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn is_doc_public(&self, workspace_id: &str, doc_id: &str) -> Result<bool> {
        let value: Option<i64> =
            sqlx::query_scalar("SELECT public FROM documents WHERE workspace_id = ? AND id = ?")
                .bind(workspace_id)
                .bind(doc_id)
                .fetch_optional(&self.pool)
                .await?;

        Ok(value.unwrap_or(0) != 0)
    }

    async fn list_public_docs(&self, workspace_id: &str) -> Result<Vec<DocumentMetadata>> {
        let rows = sqlx::query(
            "SELECT
                 d.id,
                 d.workspace_id,
                 d.created_at,
                 d.updated_at,
                 d.default_role,
                 d.public,
                 d.blocked,
                 d.mode,
                 d.title,
                 d.summary,
                 d.creator_id,
                 d.updater_id,
                 d.trashed_at,
                 d.trashed_by,
                 l.token AS share_token
             FROM documents d
             LEFT JOIN doc_public_links l
               ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
             WHERE d.workspace_id = ? AND d.public = 1 AND d.trashed_at IS NULL
             ORDER BY d.created_at ASC, d.id ASC",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::metadata_from_row).collect())
    }

    async fn duplicate_doc_entry(
        &self,
        params: DuplicateDocParams,
    ) -> Result<Option<DocumentMetadata>> {
        let DuplicateDocParams {
            source_workspace_id,
            source_doc_id,
            target_workspace_id,
            new_doc_id,
            creator_id,
            title_override,
            now,
        } = params;

        let mut tx = self.pool.begin().await?;
        let source_row = sqlx::query(
            "SELECT snapshot, snapshot_kv_key, default_role, mode, title, summary
             FROM documents WHERE workspace_id = ? AND id = ?",
        )
        .bind(&source_workspace_id)
        .bind(&source_doc_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = source_row else {
            tx.rollback().await?;
            return Ok(None);
        };

        let inline_snapshot: Vec<u8> = row.try_get("snapshot")?;
        let snapshot_kv = row
            .try_get::<Option<String>, _>("snapshot_kv_key")
            .unwrap_or(None);
        let snapshot = self
            .doc_snapshots
            .resolve_snapshot(snapshot_kv.as_deref(), inline_snapshot)?;
        let default_role: String = row.try_get("default_role")?;
        let mode: String = row.try_get("mode")?;
        let original_title: Option<String> = row.try_get("title").unwrap_or(None);
        let summary: Option<String> = row.try_get("summary").unwrap_or(None);

        let sanitized_title = title_override
            .as_deref()
            .and_then(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .or_else(|| original_title.clone());

        let snapshot_payload = self.doc_snapshots.prepare_doc_snapshot(
            &target_workspace_id,
            &new_doc_id,
            &snapshot,
        )?;

        sqlx::query(
            "INSERT INTO documents (
                 id,
                 workspace_id,
                 snapshot,
                 snapshot_kv_key,
                 created_at,
                 updated_at,
                 default_role,
                 public,
                 blocked,
                 mode,
                 title,
                 summary,
                 creator_id,
                 updater_id,
                 trashed_at,
                 trashed_by
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, NULL, NULL)",
        )
        .bind(&new_doc_id)
        .bind(&target_workspace_id)
        .bind(&snapshot_payload.inline)
        .bind(snapshot_payload.kv_key.as_deref())
        .bind(now)
        .bind(now)
        .bind(&default_role)
        .bind(&mode)
        .bind(sanitized_title.as_deref())
        .bind(summary.as_deref())
        .bind(&creator_id)
        .bind(&creator_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(Some(DocumentMetadata {
            id: DocId::from(new_doc_id),
            workspace_id: WorkspaceId::from(target_workspace_id),
            created_at: now,
            updated_at: now,
            default_role,
            public: false,
            blocked: false,
            mode,
            title: sanitized_title,
            summary,
            creator_id: Some(creator_id.clone()),
            updater_id: Some(creator_id),
            share_token: None,
            trashed_at: None,
            trashed_by: None,
            snapshot: Some(snapshot),
        }))
    }

    async fn fetch_history_snapshot(
        &self,
        workspace_id: &str,
        doc_id: &str,
        history_id: i64,
    ) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query(
            "SELECT snapshot, snapshot_kv_key FROM document_history
             WHERE id = ? AND workspace_id = ? AND doc_id = ?",
        )
        .bind(history_id)
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row
            .map(|row| {
                let inline = row.try_get("snapshot")?;
                let kv_key = row
                    .try_get::<Option<String>, _>("snapshot_kv_key")
                    .unwrap_or(None);
                self.doc_snapshots
                    .resolve_snapshot(kv_key.as_deref(), inline)
            })
            .transpose()?)
    }

    async fn find_history_id_before(
        &self,
        workspace_id: &str,
        doc_id: &str,
        timestamp: i64,
    ) -> Result<Option<i64>> {
        let history_id: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM document_history
             WHERE workspace_id = ? AND doc_id = ? AND created_at <= ?
             ORDER BY created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(timestamp)
        .fetch_optional(&self.pool)
        .await?;

        Ok(history_id)
    }

    async fn replace_doc_snapshot(&self, params: ReplaceDocSnapshotParams) -> Result<bool> {
        let ReplaceDocSnapshotParams {
            workspace_id,
            doc_id,
            snapshot,
            updated_at,
            title,
            summary,
            creator_id,
            updater_id,
            history_entry,
        } = params;

        let mut tx = self.pool.begin().await?;
        if let Some(HistorySnapshotInsert {
            snapshot: history_snapshot,
            created_at,
        }) = history_entry
        {
            let history_payload = self.doc_snapshots.prepare_history_snapshot(
                &workspace_id,
                &doc_id,
                &history_snapshot,
            )?;
            sqlx::query(
                "INSERT INTO document_history (doc_id, workspace_id, snapshot, snapshot_kv_key, created_at)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(&doc_id)
            .bind(&workspace_id)
            .bind(&history_payload.inline)
            .bind(history_payload.kv_key.as_deref())
            .bind(created_at)
            .execute(&mut *tx)
            .await?;
        }

        let snapshot_payload =
            self.doc_snapshots
                .prepare_doc_snapshot(&workspace_id, &doc_id, &snapshot)?;

        let result = sqlx::query(
            "UPDATE documents SET snapshot = ?, snapshot_kv_key = ?, updated_at = ?, title = ?, summary = ?,
             updater_id = COALESCE(?, updater_id), creator_id = COALESCE(?, creator_id)
             WHERE workspace_id = ? AND id = ?",
        )
        .bind(&snapshot_payload.inline)
        .bind(snapshot_payload.kv_key.as_deref())
        .bind(updated_at)
        .bind(title.as_deref())
        .bind(summary.as_deref())
        .bind(updater_id.as_deref())
        .bind(creator_id.as_deref())
        .bind(&workspace_id)
        .bind(&doc_id)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(false);
        }

        tx.commit().await?;
        Ok(true)
    }

    async fn docs_requiring_compaction(
        &self,
        threshold: i64,
        limit: i64,
    ) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT space_id, doc_id
             FROM doc_updates
             WHERE space_type = ?
             GROUP BY space_id, doc_id
             HAVING COUNT(*) > ?
             LIMIT ?",
        )
        .bind("workspace")
        .bind(threshold)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| (row.get("space_id"), row.get("doc_id")))
            .collect())
    }

    async fn list_doc_timestamps(
        &self,
        workspace_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<(String, i64)>> {
        let rows = if let Some(threshold) = after {
            sqlx::query(
                "SELECT id, updated_at FROM documents WHERE workspace_id = ? AND updated_at > ?",
            )
            .bind(workspace_id)
            .bind(threshold)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query("SELECT id, updated_at FROM documents WHERE workspace_id = ?")
                .bind(workspace_id)
                .fetch_all(&self.pool)
                .await?
        };

        Ok(rows
            .into_iter()
            .map(|row| (row.get("id"), row.get("updated_at")))
            .collect())
    }

    async fn list_history_entries(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
    ) -> Result<Vec<DocumentHistoryRecord>> {
        let rows = sqlx::query(
            "SELECT id, snapshot, snapshot_kv_key, created_at FROM document_history
             WHERE workspace_id = ? AND doc_id = ?
             ORDER BY created_at DESC, id DESC
             LIMIT ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let inline: Vec<u8> = row.try_get("snapshot")?;
            let kv_key = row
                .try_get::<Option<String>, _>("snapshot_kv_key")
                .unwrap_or(None);
            let snapshot = self
                .doc_snapshots
                .resolve_snapshot(kv_key.as_deref(), inline)?;
            records.push(DocumentHistoryRecord {
                id: row.get("id"),
                snapshot,
                created_at: row.get("created_at"),
            });
        }
        Ok(records)
    }

    async fn upsert_snapshot_with_updates(&self, params: SnapshotUpsertParams) -> Result<()> {
        let SnapshotUpsertParams {
            workspace_id,
            doc_id,
            snapshot,
            created_at,
            updated_at,
            title,
            summary,
            creator_id,
            updater_id,
            default_role,
            mode,
            history_entry,
            new_document,
            doc_updates,
            log_editor_id,
            log_timestamp,
            log_limit,
        } = params;

        let snapshot_payload =
            self.doc_snapshots
                .prepare_doc_snapshot(&workspace_id, &doc_id, &snapshot)?;

        let mut tx = self.pool.begin().await?;
        if new_document {
            sqlx::query(
                "INSERT INTO documents (
                     id,
                     workspace_id,
                     snapshot,
                     snapshot_kv_key,
                     created_at,
                     updated_at,
                     default_role,
                     public,
                     blocked,
                     mode,
                     title,
                     summary,
                     creator_id,
                     updater_id,
                     trashed_at,
                     trashed_by
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, NULL, NULL)",
            )
            .bind(&doc_id)
            .bind(&workspace_id)
            .bind(&snapshot_payload.inline)
            .bind(snapshot_payload.kv_key.as_deref())
            .bind(created_at)
            .bind(updated_at)
            .bind(&default_role)
            .bind(&mode)
            .bind(title.as_deref())
            .bind(summary.as_deref())
            .bind(creator_id.as_deref())
            .bind(updater_id.as_deref())
            .execute(&mut *tx)
            .await?;
        } else {
            let result = sqlx::query(
                "UPDATE documents SET snapshot = ?, snapshot_kv_key = ?, updated_at = ?, title = ?, summary = ?,
                 updater_id = COALESCE(?, updater_id), creator_id = COALESCE(?, creator_id)
                 WHERE workspace_id = ? AND id = ?",
            )
            .bind(&snapshot_payload.inline)
            .bind(snapshot_payload.kv_key.as_deref())
            .bind(updated_at)
            .bind(title.as_deref())
            .bind(summary.as_deref())
            .bind(updater_id.as_deref())
            .bind(creator_id.as_deref())
            .bind(&workspace_id)
            .bind(&doc_id)
            .execute(&mut *tx)
            .await?;

            if result.rows_affected() == 0 {
                tx.rollback().await?;
                bail!(
                    "document not found while updating snapshot: {}/{}",
                    workspace_id,
                    doc_id
                );
            }
        }

        if let Some(HistorySnapshotInsert {
            snapshot,
            created_at,
        }) = history_entry
        {
            let history_payload =
                self.doc_snapshots
                    .prepare_history_snapshot(&workspace_id, &doc_id, &snapshot)?;
            sqlx::query(
                "INSERT INTO document_history (doc_id, workspace_id, snapshot, snapshot_kv_key, created_at)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(&doc_id)
            .bind(&workspace_id)
            .bind(&history_payload.inline)
            .bind(history_payload.kv_key.as_deref())
            .bind(created_at)
            .execute(&mut *tx)
            .await?;
        }

        self.doc_logs
            .insert_updates(
                &mut tx,
                "workspace",
                &workspace_id,
                &doc_id,
                &doc_updates,
                log_editor_id.as_deref(),
                log_timestamp,
            )
            .await?;
        self.doc_logs
            .trim_updates(&mut tx, "workspace", &workspace_id, &doc_id, log_limit)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn append_doc_updates(&self, params: AppendDocUpdatesParams) -> Result<()> {
        let AppendDocUpdatesParams {
            workspace_id,
            doc_id,
            updates,
            editor_id,
            timestamp,
            log_limit,
        } = params;

        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE documents
             SET updated_at = ?, updater_id = COALESCE(?, updater_id)
             WHERE workspace_id = ? AND id = ?",
        )
        .bind(timestamp)
        .bind(editor_id.as_deref())
        .bind(&workspace_id)
        .bind(&doc_id)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.rollback().await?;
            bail!(
                "document not found while appending updates: {}/{}",
                workspace_id,
                doc_id
            );
        }

        self.doc_logs
            .insert_updates(
                &mut tx,
                "workspace",
                &workspace_id,
                &doc_id,
                &updates,
                editor_id.as_deref(),
                timestamp,
            )
            .await?;
        self.doc_logs
            .trim_updates(&mut tx, "workspace", &workspace_id, &doc_id, log_limit)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn load_compaction_source(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<CompactionSource>> {
        let row = sqlx::query(
            "SELECT snapshot, snapshot_kv_key, updated_at FROM documents WHERE workspace_id = ? AND id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let inline_snapshot: Vec<u8> = row.try_get("snapshot")?;
        let kv_key = row
            .try_get::<Option<String>, _>("snapshot_kv_key")
            .unwrap_or(None);
        let base_snapshot = self
            .doc_snapshots
            .resolve_snapshot(kv_key.as_deref(), inline_snapshot)?;
        let doc_updated_at: i64 = row.try_get("updated_at")?;
        let logs = self
            .doc_logs
            .fetch_logs("workspace", workspace_id, doc_id)
            .await?;

        Ok(Some(CompactionSource {
            base_snapshot,
            logs,
            doc_updated_at,
        }))
    }

    async fn apply_compaction_result(&self, params: CompactionApplyParams) -> Result<()> {
        let CompactionApplyParams {
            workspace_id,
            doc_id,
            snapshot,
            updated_at,
            title,
            summary,
            creator_id,
            updater_id,
            last_log_id,
            expected_updated_at,
        } = params;

        let mut tx = self.pool.begin().await?;
        let snapshot_payload =
            self.doc_snapshots
                .prepare_doc_snapshot(&workspace_id, &doc_id, &snapshot)?;
        let result = sqlx::query(
            "UPDATE documents
             SET snapshot = ?, snapshot_kv_key = ?, updated_at = ?, title = ?, summary = ?,
                 updater_id = COALESCE(?, updater_id), creator_id = COALESCE(?, creator_id)
             WHERE workspace_id = ? AND id = ? AND updated_at = ?",
        )
        .bind(&snapshot_payload.inline)
        .bind(snapshot_payload.kv_key.as_deref())
        .bind(updated_at)
        .bind(title.as_deref())
        .bind(summary.as_deref())
        .bind(updater_id.as_deref())
        .bind(creator_id.as_deref())
        .bind(&workspace_id)
        .bind(&doc_id)
        .bind(expected_updated_at)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(());
        }

        if let Some(last_id) = last_log_id {
            self.doc_logs
                .delete_logs_up_to(&mut tx, "workspace", &workspace_id, &doc_id, last_id)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

async fn list_created(
    pool: &Pool<Sqlite>,
    workspace_id: &str,
    limit: i64,
    offset: i64,
    cursor: Option<&DocumentCursor>,
) -> Result<Vec<SqliteRow>> {
    if let Some(cursor) = cursor {
        let rows = sqlx::query(
            "SELECT
                 d.id, d.workspace_id, d.created_at, d.updated_at,
                 d.default_role, d.public, d.blocked, d.mode,
                 d.title, d.summary, d.creator_id, d.updater_id,
                 d.trashed_at, d.trashed_by,
                 l.token AS share_token
             FROM documents d
             LEFT JOIN doc_public_links l
               ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
             WHERE d.workspace_id = ?
               AND d.trashed_at IS NULL
               AND (d.created_at > ? OR (d.created_at = ? AND d.id > ?))
             ORDER BY d.created_at ASC, d.id ASC
             LIMIT ?",
        )
        .bind(workspace_id)
        .bind(cursor.timestamp)
        .bind(cursor.timestamp)
        .bind(cursor.id.as_str())
        .bind(limit)
        .fetch_all(pool)
        .await?;
        Ok(rows)
    } else {
        let rows = sqlx::query(
            "SELECT
                 d.id, d.workspace_id, d.created_at, d.updated_at,
                 d.default_role, d.public, d.blocked, d.mode,
                 d.title, d.summary, d.creator_id, d.updater_id,
                 d.trashed_at, d.trashed_by,
                 l.token AS share_token
             FROM documents d
             LEFT JOIN doc_public_links l
               ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
             WHERE d.workspace_id = ?
               AND d.trashed_at IS NULL
             ORDER BY d.created_at ASC, d.id ASC
             LIMIT ? OFFSET ?",
        )
        .bind(workspace_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }
}

async fn list_updated_desc(
    pool: &Pool<Sqlite>,
    workspace_id: &str,
    limit: i64,
    offset: i64,
    cursor: Option<&DocumentCursor>,
) -> Result<Vec<SqliteRow>> {
    if let Some(cursor) = cursor {
        let rows = sqlx::query(
            "SELECT
                 d.id, d.workspace_id, d.created_at, d.updated_at,
                 d.default_role, d.public, d.blocked, d.mode,
                 d.title, d.summary, d.creator_id, d.updater_id,
                 d.trashed_at, d.trashed_by,
                 l.token AS share_token
             FROM documents d
             LEFT JOIN doc_public_links l
               ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
             WHERE d.workspace_id = ?
               AND d.trashed_at IS NULL
               AND (d.updated_at < ? OR (d.updated_at = ? AND d.id < ?))
             ORDER BY d.updated_at DESC, d.id DESC
             LIMIT ?",
        )
        .bind(workspace_id)
        .bind(cursor.timestamp)
        .bind(cursor.timestamp)
        .bind(cursor.id.as_str())
        .bind(limit)
        .fetch_all(pool)
        .await?;
        Ok(rows)
    } else {
        let rows = sqlx::query(
            "SELECT
                 d.id, d.workspace_id, d.created_at, d.updated_at,
                 d.default_role, d.public, d.blocked, d.mode,
                 d.title, d.summary, d.creator_id, d.updater_id,
                 d.trashed_at, d.trashed_by,
                 l.token AS share_token
             FROM documents d
             LEFT JOIN doc_public_links l
               ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
             WHERE d.workspace_id = ?
               AND d.trashed_at IS NULL
             ORDER BY d.updated_at DESC, d.id DESC
             LIMIT ? OFFSET ?",
        )
        .bind(workspace_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }
}
