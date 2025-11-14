use anyhow::{Result, bail};
use async_trait::async_trait;
use deadpool_libsql::{
    Pool,
    libsql::{self, params},
};
use std::sync::Arc;

use crate::{
    db::{
        doc_repo::{
            AppendDocUpdatesParams, CompactionApplyParams, CompactionSource, DocRepository,
            DocumentListOrder, DuplicateDocParams, HistorySnapshotInsert, InsertDocRecordParams,
            MetadataBackfillParams, ReplaceDocSnapshotParams, SnapshotUpsertParams,
        },
        libsql::{doc_update_log_store::LibsqlDocUpdateLogStore, row_ext},
    },
    doc_snapshot_store::DocSnapshotStore,
    doc_store::{
        DocumentCursor, DocumentHistoryRecord, DocumentMetadata, DocumentSnapshot,
        UserShareTokenRecord,
    },
    doc_update_log::DocUpdateLogReader,
};

pub struct LibsqlDocRepository {
    pool: Pool,
    doc_logs: Arc<LibsqlDocUpdateLogStore>,
    doc_snapshots: Arc<DocSnapshotStore>,
}

impl LibsqlDocRepository {
    pub fn new(
        pool: Pool,
        doc_logs: Arc<LibsqlDocUpdateLogStore>,
        doc_snapshots: Arc<DocSnapshotStore>,
    ) -> Self {
        Self {
            pool,
            doc_logs,
            doc_snapshots,
        }
    }

    fn metadata_from_row(row: &libsql::Row) -> Result<DocumentMetadata> {
        let public = row_ext::get::<Option<i64>>(row, "public")?.unwrap_or(0) != 0;
        let blocked = row_ext::get::<Option<i64>>(row, "blocked")?.unwrap_or(0) != 0;
        let share_token = row_ext::get::<Option<String>>(row, "share_token")?.filter(|_| public);

        Ok(DocumentMetadata {
            id: row_ext::get(row, "id")?,
            workspace_id: row_ext::get(row, "workspace_id")?,
            created_at: row_ext::get(row, "created_at")?,
            updated_at: row_ext::get(row, "updated_at")?,
            default_role: row_ext::get(row, "default_role")?,
            public,
            blocked,
            mode: row_ext::get(row, "mode")?,
            title: row_ext::get(row, "title")?,
            summary: row_ext::get(row, "summary")?,
            creator_id: row_ext::get(row, "creator_id")?,
            updater_id: row_ext::get(row, "updater_id")?,
            share_token,
            trashed_at: row_ext::get(row, "trashed_at")?,
            trashed_by: row_ext::get(row, "trashed_by")?,
            snapshot: None,
        })
    }
}

#[async_trait]
impl DocRepository for LibsqlDocRepository {
    async fn fetch_snapshot_with_timestamp(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentSnapshot>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT snapshot, snapshot_kv_key, updated_at
                 FROM documents
                 WHERE workspace_id = ? AND id = ?",
                params![workspace_id, doc_id],
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        let inline_snapshot: Vec<u8> = row_ext::get(&row, "snapshot")?;
        let kv_key = row_ext::get::<Option<String>>(&row, "snapshot_kv_key")?;
        let snapshot = self
            .doc_snapshots
            .resolve_snapshot(kv_key.as_deref(), inline_snapshot)?;
        let updated_at: i64 = row_ext::get(&row, "updated_at")?;
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

        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let snapshot_payload =
            self.doc_snapshots
                .prepare_doc_snapshot(&workspace_id, &doc_id, &snapshot)?;

        tx.execute(
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
            params![
                doc_id.as_str(),
                workspace_id.as_str(),
                snapshot_payload.inline.as_slice(),
                snapshot_payload.kv_key.as_deref(),
                created_at,
                created_at,
                title.as_deref(),
                owner_id.as_str(),
                owner_id.as_str()
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn fetch_metadata(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
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
                params![workspace_id, doc_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::metadata_from_row(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn list_documents(
        &self,
        workspace_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentCursor>,
        order: DocumentListOrder,
    ) -> Result<Vec<DocumentMetadata>> {
        match order {
            DocumentListOrder::CreatedAsc => {
                list_created(&self.pool, workspace_id, limit, offset, cursor).await
            }
            DocumentListOrder::UpdatedDesc => {
                list_updated_desc(&self.pool, workspace_id, limit, offset, cursor).await
            }
        }
    }

    async fn set_doc_trashed(
        &self,
        workspace_id: &str,
        doc_id: &str,
        trashed_at: i64,
        trashed_by: &str,
    ) -> Result<u64> {
        let conn = self.pool.get().await?;
        let affected = conn
            .execute(
                "UPDATE documents
                 SET trashed_at = ?, trashed_by = ?, updated_at = ?, updater_id = ?
                 WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
                params![
                    trashed_at,
                    trashed_by,
                    trashed_at,
                    trashed_by,
                    workspace_id,
                    doc_id
                ],
            )
            .await?;
        Ok(affected)
    }

    async fn restore_doc_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        updated_at: i64,
        restored_by: &str,
    ) -> Result<u64> {
        let conn = self.pool.get().await?;
        let affected = conn
            .execute(
                "UPDATE documents
                 SET trashed_at = NULL, trashed_by = NULL, updated_at = ?, updater_id = ?
                 WHERE workspace_id = ? AND id = ? AND trashed_at IS NOT NULL",
                params![updated_at, restored_by, workspace_id, doc_id],
            )
            .await?;
        Ok(affected)
    }

    async fn delete_doc_entry(&self, workspace_id: &str, doc_id: &str) -> Result<u64> {
        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let mut history_rows = tx
            .query(
                "SELECT snapshot_kv_key FROM document_history
                 WHERE workspace_id = ? AND doc_id = ? AND snapshot_kv_key IS NOT NULL",
                params![workspace_id, doc_id],
            )
            .await?;
        let mut history_keys = Vec::new();
        while let Some(row) = history_rows.next().await? {
            history_keys.push(row_ext::get::<Option<String>>(&row, "snapshot_kv_key")?);
        }

        let mut deleted_rows = tx
            .query(
                "DELETE FROM documents
                 WHERE workspace_id = ? AND id = ?
                 RETURNING snapshot_kv_key",
                params![workspace_id, doc_id],
            )
            .await?;
        let deleted_row = deleted_rows.next().await?;

        let Some(row) = deleted_row else {
            tx.rollback().await?;
            return Ok(0);
        };

        let doc_snapshot_key = row_ext::get::<Option<String>>(&row, "snapshot_kv_key")?;
        self.doc_snapshots
            .delete_snapshot(doc_snapshot_key.as_deref())?;

        for key in history_keys {
            self.doc_snapshots.delete_snapshot(key.as_deref())?;
        }

        self.doc_logs
            .delete_doc_logs(&tx, "workspace", workspace_id, doc_id)
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
        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let affected = tx
            .execute(
                "UPDATE documents
                 SET public = 1, mode = ?, updated_at = ?
                 WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
                params![mode, timestamp, workspace_id, doc_id],
            )
            .await?;

        if affected == 0 {
            tx.rollback().await?;
            return Ok(0);
        }

        tx.execute(
            "INSERT INTO doc_public_links (workspace_id, doc_id, token, created_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(workspace_id, doc_id) DO NOTHING",
            params![workspace_id, doc_id, share_token, timestamp],
        )
        .await?;

        tx.commit().await?;
        Ok(affected)
    }

    async fn unpublish_doc_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        timestamp: i64,
    ) -> Result<u64> {
        let conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let affected = tx
            .execute(
                "UPDATE documents
                 SET public = 0, updated_at = ?
                 WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
                params![timestamp, workspace_id, doc_id],
            )
            .await?;

        if affected == 0 {
            tx.rollback().await?;
            return Ok(0);
        }

        tx.execute(
            "DELETE FROM doc_public_links WHERE workspace_id = ? AND doc_id = ?",
            params![workspace_id, doc_id],
        )
        .await?;

        tx.commit().await?;
        Ok(affected)
    }

    async fn update_default_role_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        role: &str,
    ) -> Result<u64> {
        let conn = self.pool.get().await?;
        let affected = conn
            .execute(
                "UPDATE documents
                 SET default_role = ?
                 WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
                params![role, workspace_id, doc_id],
            )
            .await?;
        Ok(affected)
    }

    async fn update_doc_mode(&self, workspace_id: &str, doc_id: &str, mode: &str) -> Result<u64> {
        let conn = self.pool.get().await?;
        let affected = conn
            .execute(
                "UPDATE documents
                 SET mode = ?
                 WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
                params![mode, workspace_id, doc_id],
            )
            .await?;
        Ok(affected)
    }

    async fn count_documents(&self, workspace_id: &str) -> Result<i64> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT COUNT(*) as count FROM documents WHERE workspace_id = ? AND trashed_at IS NULL",
                params![workspace_id],
            )
            .await?;
        let row = rows
            .next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("count query returned no rows"))?;
        Ok(row_ext::get(&row, "count")?)
    }

    async fn list_share_tokens_for_user(&self, user_id: &str) -> Result<Vec<UserShareTokenRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT l.workspace_id, l.doc_id, l.token, l.created_at
                 FROM doc_public_links l
                 JOIN workspace_members wm
                   ON wm.workspace_id = l.workspace_id
                 JOIN documents d
                   ON d.workspace_id = l.workspace_id AND d.id = l.doc_id
                 WHERE wm.user_id = ? AND wm.status = 'Accepted'
                   AND d.public = 1 AND d.trashed_at IS NULL
                 ORDER BY l.created_at DESC, l.workspace_id ASC, l.doc_id ASC",
                params![user_id],
            )
            .await?;

        let mut records = Vec::new();
        while let Some(row) = rows.next().await? {
            records.push(UserShareTokenRecord {
                workspace_id: row_ext::get(&row, "workspace_id")?,
                doc_id: row_ext::get(&row, "doc_id")?,
                token: row_ext::get(&row, "token")?,
                created_at: row_ext::get(&row, "created_at")?,
            });
        }
        Ok(records)
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

        let conn = self.pool.get().await?;
        conn.execute(
            "UPDATE documents
             SET title = ?, summary = ?, creator_id = ?, updater_id = ?, created_at = ?, updated_at = ?
             WHERE workspace_id = ? AND id = ?",
            params![
                title.as_deref(),
                summary.as_deref(),
                creator_id.as_deref(),
                updater_id.as_deref(),
                created_at,
                updated_at,
                workspace_id.as_str(),
                doc_id.as_str()
            ],
        )
        .await?;

        Ok(())
    }

    async fn is_doc_public(&self, workspace_id: &str, doc_id: &str) -> Result<bool> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT public FROM documents WHERE workspace_id = ? AND id = ?",
                params![workspace_id, doc_id],
            )
            .await?;
        let value = if let Some(row) = rows.next().await? {
            row_ext::get::<i64>(&row, "public")?
        } else {
            0
        };
        Ok(value != 0)
    }

    async fn list_public_docs(&self, workspace_id: &str) -> Result<Vec<DocumentMetadata>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
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
                params![workspace_id],
            )
            .await?;

        let mut docs = Vec::new();
        while let Some(row) = rows.next().await? {
            docs.push(Self::metadata_from_row(&row)?);
        }
        Ok(docs)
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
        let source_workspace_id_ref = source_workspace_id.as_str();
        let source_doc_id_ref = source_doc_id.as_str();
        let target_workspace_id_ref = target_workspace_id.as_str();
        let new_doc_id_ref = new_doc_id.as_str();

        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let mut source_rows = tx
            .query(
                "SELECT snapshot, snapshot_kv_key, default_role, mode, title, summary
                 FROM documents WHERE workspace_id = ? AND id = ?",
                params![source_workspace_id_ref, source_doc_id_ref],
            )
            .await?;

        let Some(row) = source_rows.next().await? else {
            tx.rollback().await?;
            return Ok(None);
        };

        let inline_snapshot: Vec<u8> = row_ext::get(&row, "snapshot")?;
        let snapshot_kv = row_ext::get::<Option<String>>(&row, "snapshot_kv_key")?;
        let snapshot = self
            .doc_snapshots
            .resolve_snapshot(snapshot_kv.as_deref(), inline_snapshot)?;
        let default_role: String = row_ext::get(&row, "default_role")?;
        let mode: String = row_ext::get(&row, "mode")?;
        let original_title: Option<String> = row_ext::get(&row, "title")?;
        let summary: Option<String> = row_ext::get(&row, "summary")?;

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
            target_workspace_id_ref,
            new_doc_id_ref,
            &snapshot,
        )?;

        tx.execute(
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
            params![
                new_doc_id_ref,
                target_workspace_id_ref,
                snapshot_payload.inline.as_slice(),
                snapshot_payload.kv_key.as_deref(),
                now,
                now,
                default_role.as_str(),
                mode.as_str(),
                sanitized_title.as_deref(),
                summary.as_deref(),
                creator_id.as_str(),
                creator_id.as_str()
            ],
        )
        .await?;

        tx.commit().await?;

        Ok(Some(DocumentMetadata {
            id: new_doc_id,
            workspace_id: target_workspace_id,
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
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT snapshot, snapshot_kv_key FROM document_history
                 WHERE id = ? AND workspace_id = ? AND doc_id = ?",
                params![history_id, workspace_id, doc_id],
            )
            .await?;

        Ok(rows
            .next()
            .await?
            .map(|row| {
                let inline = row_ext::get(&row, "snapshot")?;
                let kv_key = row_ext::get::<Option<String>>(&row, "snapshot_kv_key")?;
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
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id FROM document_history
                 WHERE workspace_id = ? AND doc_id = ? AND created_at <= ?
                 ORDER BY created_at DESC, id DESC
                 LIMIT 1",
                params![workspace_id, doc_id, timestamp],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row_ext::get(&row, "id")?))
        } else {
            Ok(None)
        }
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
        let workspace_id_ref = workspace_id.as_str();
        let doc_id_ref = doc_id.as_str();

        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        if let Some(HistorySnapshotInsert {
            snapshot: history_snapshot,
            created_at,
        }) = history_entry
        {
            let history_payload = self.doc_snapshots.prepare_history_snapshot(
                workspace_id_ref,
                doc_id_ref,
                &history_snapshot,
            )?;
            tx.execute(
                "INSERT INTO document_history (doc_id, workspace_id, snapshot, snapshot_kv_key, created_at)
                 VALUES (?, ?, ?, ?, ?)",
                params![
                    doc_id_ref,
                    workspace_id_ref,
                    history_payload.inline.as_slice(),
                    history_payload.kv_key.as_deref(),
                    created_at
                ],
            )
            .await?;
        }

        let snapshot_payload =
            self.doc_snapshots
                .prepare_doc_snapshot(workspace_id_ref, doc_id_ref, &snapshot)?;

        let affected = tx
            .execute(
                "UPDATE documents SET snapshot = ?, snapshot_kv_key = ?, updated_at = ?, title = ?, summary = ?,
                 updater_id = COALESCE(?, updater_id), creator_id = COALESCE(?, creator_id)
                 WHERE workspace_id = ? AND id = ?",
                params![
                    snapshot_payload.inline.as_slice(),
                    snapshot_payload.kv_key.as_deref(),
                    updated_at,
                    title.as_deref(),
                    summary.as_deref(),
                    updater_id.as_deref(),
                    creator_id.as_deref(),
                    workspace_id_ref,
                    doc_id_ref
                ],
            )
            .await?;

        if affected == 0 {
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
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT space_id, doc_id
                 FROM doc_updates
                 WHERE space_type = ?
                 GROUP BY space_id, doc_id
                 HAVING COUNT(*) > ?
                 LIMIT ?",
                params!["workspace", threshold, limit],
            )
            .await?;

        let mut docs = Vec::new();
        while let Some(row) = rows.next().await? {
            docs.push((
                row_ext::get(&row, "space_id")?,
                row_ext::get(&row, "doc_id")?,
            ));
        }
        Ok(docs)
    }

    async fn list_doc_timestamps(
        &self,
        workspace_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<(String, i64)>> {
        let conn = self.pool.get().await?;
        let mut rows = if let Some(threshold) = after {
            conn.query(
                "SELECT id, updated_at FROM documents WHERE workspace_id = ? AND updated_at > ?",
                params![workspace_id, threshold],
            )
            .await?
        } else {
            conn.query(
                "SELECT id, updated_at FROM documents WHERE workspace_id = ?",
                params![workspace_id],
            )
            .await?
        };

        let mut docs = Vec::new();
        while let Some(row) = rows.next().await? {
            docs.push((row_ext::get(&row, "id")?, row_ext::get(&row, "updated_at")?));
        }
        Ok(docs)
    }

    async fn list_history_entries(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
    ) -> Result<Vec<DocumentHistoryRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, snapshot, snapshot_kv_key, created_at FROM document_history
                 WHERE workspace_id = ? AND doc_id = ?
                 ORDER BY created_at DESC, id DESC
                 LIMIT ?",
                params![workspace_id, doc_id, limit],
            )
            .await?;

        let mut records = Vec::new();
        while let Some(row) = rows.next().await? {
            let inline: Vec<u8> = row_ext::get(&row, "snapshot")?;
            let kv_key = row_ext::get::<Option<String>>(&row, "snapshot_kv_key")?;
            let snapshot = self
                .doc_snapshots
                .resolve_snapshot(kv_key.as_deref(), inline)?;
            records.push(DocumentHistoryRecord {
                id: row_ext::get(&row, "id")?,
                snapshot,
                created_at: row_ext::get(&row, "created_at")?,
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
        let workspace_id_ref = workspace_id.as_str();
        let doc_id_ref = doc_id.as_str();
        let default_role_ref = default_role.as_str();
        let mode_ref = mode.as_str();

        let snapshot_payload =
            self.doc_snapshots
                .prepare_doc_snapshot(workspace_id_ref, doc_id_ref, &snapshot)?;

        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        if new_document {
            tx.execute(
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
                params![
                    doc_id_ref,
                    workspace_id_ref,
                    snapshot_payload.inline.as_slice(),
                    snapshot_payload.kv_key.as_deref(),
                    created_at,
                    updated_at,
                    default_role_ref,
                    mode_ref,
                    title.as_deref(),
                    summary.as_deref(),
                    creator_id.as_deref(),
                    updater_id.as_deref()
                ],
            )
            .await?;
        } else {
            let affected = tx
                .execute(
                    "UPDATE documents SET snapshot = ?, snapshot_kv_key = ?, updated_at = ?, title = ?, summary = ?,
                     updater_id = COALESCE(?, updater_id), creator_id = COALESCE(?, creator_id)
                     WHERE workspace_id = ? AND id = ?",
                    params![
                        snapshot_payload.inline.as_slice(),
                        snapshot_payload.kv_key.as_deref(),
                        updated_at,
                        title.as_deref(),
                        summary.as_deref(),
                        updater_id.as_deref(),
                        creator_id.as_deref(),
                        workspace_id_ref,
                        doc_id_ref
                    ],
                )
                .await?;

            if affected == 0 {
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
            let history_payload = self.doc_snapshots.prepare_history_snapshot(
                workspace_id_ref,
                doc_id_ref,
                &snapshot,
            )?;
            tx.execute(
                "INSERT INTO document_history (doc_id, workspace_id, snapshot, snapshot_kv_key, created_at)
                 VALUES (?, ?, ?, ?, ?)",
                params![
                    doc_id_ref,
                    workspace_id_ref,
                    history_payload.inline.as_slice(),
                    history_payload.kv_key.as_deref(),
                    created_at
                ],
            )
            .await?;
        }

        self.doc_logs
            .insert_updates(
                &tx,
                "workspace",
                workspace_id_ref,
                doc_id_ref,
                &doc_updates,
                log_editor_id.as_deref(),
                log_timestamp,
            )
            .await?;
        self.doc_logs
            .trim_updates(&tx, "workspace", workspace_id_ref, doc_id_ref, log_limit)
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
        let workspace_id_ref = workspace_id.as_str();
        let doc_id_ref = doc_id.as_str();

        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let affected = tx
            .execute(
                "UPDATE documents
                 SET updated_at = ?, updater_id = COALESCE(?, updater_id)
                 WHERE workspace_id = ? AND id = ?",
                params![
                    timestamp,
                    editor_id.as_deref(),
                    workspace_id_ref,
                    doc_id_ref
                ],
            )
            .await?;

        if affected == 0 {
            tx.rollback().await?;
            bail!(
                "document not found while appending updates: {}/{}",
                workspace_id,
                doc_id
            );
        }

        self.doc_logs
            .insert_updates(
                &tx,
                "workspace",
                workspace_id_ref,
                doc_id_ref,
                &updates,
                editor_id.as_deref(),
                timestamp,
            )
            .await?;
        self.doc_logs
            .trim_updates(&tx, "workspace", workspace_id_ref, doc_id_ref, log_limit)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn load_compaction_source(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<CompactionSource>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT snapshot, snapshot_kv_key, updated_at FROM documents WHERE workspace_id = ? AND id = ?",
                params![workspace_id, doc_id],
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        let inline_snapshot: Vec<u8> = row_ext::get(&row, "snapshot")?;
        let kv_key = row_ext::get::<Option<String>>(&row, "snapshot_kv_key")?;
        let base_snapshot = self
            .doc_snapshots
            .resolve_snapshot(kv_key.as_deref(), inline_snapshot)?;
        let doc_updated_at: i64 = row_ext::get(&row, "updated_at")?;
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
        let workspace_id_ref = workspace_id.as_str();
        let doc_id_ref = doc_id.as_str();

        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let snapshot_payload =
            self.doc_snapshots
                .prepare_doc_snapshot(workspace_id_ref, doc_id_ref, &snapshot)?;
        let affected = tx
            .execute(
                "UPDATE documents
                 SET snapshot = ?, snapshot_kv_key = ?, updated_at = ?, title = ?, summary = ?,
                     updater_id = COALESCE(?, updater_id), creator_id = COALESCE(?, creator_id)
                 WHERE workspace_id = ? AND id = ? AND updated_at = ?",
                params![
                    snapshot_payload.inline.as_slice(),
                    snapshot_payload.kv_key.as_deref(),
                    updated_at,
                    title.as_deref(),
                    summary.as_deref(),
                    updater_id.as_deref(),
                    creator_id.as_deref(),
                    workspace_id_ref,
                    doc_id_ref,
                    expected_updated_at
                ],
            )
            .await?;

        if affected == 0 {
            tx.rollback().await?;
            return Ok(());
        }

        if let Some(last_id) = last_log_id {
            self.doc_logs
                .delete_logs_up_to(&tx, "workspace", workspace_id_ref, doc_id_ref, last_id)
                .await?;
        } else {
            self.doc_logs
                .delete_doc_logs(&tx, "workspace", workspace_id_ref, doc_id_ref)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

async fn list_created(
    pool: &Pool,
    workspace_id: &str,
    limit: i64,
    offset: i64,
    cursor: Option<&DocumentCursor>,
) -> Result<Vec<DocumentMetadata>> {
    let conn = pool.get().await?;
    let mut rows = if let Some(cursor) = cursor {
        conn.query(
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
            params![
                workspace_id,
                cursor.timestamp,
                cursor.timestamp,
                cursor.id.as_str(),
                limit
            ],
        )
        .await?
    } else {
        conn.query(
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
            params![workspace_id, limit, offset],
        )
        .await?
    };

    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(LibsqlDocRepository::metadata_from_row(&row)?);
    }
    Ok(out)
}

async fn list_updated_desc(
    pool: &Pool,
    workspace_id: &str,
    limit: i64,
    offset: i64,
    cursor: Option<&DocumentCursor>,
) -> Result<Vec<DocumentMetadata>> {
    let conn = pool.get().await?;
    let mut rows = if let Some(cursor) = cursor {
        conn.query(
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
            params![
                workspace_id,
                cursor.timestamp,
                cursor.timestamp,
                cursor.id.as_str(),
                limit
            ],
        )
        .await?
    } else {
        conn.query(
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
            params![workspace_id, limit, offset],
        )
        .await?
    };

    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(LibsqlDocRepository::metadata_from_row(&row)?);
    }
    Ok(out)
}
