use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use deadpool_libsql::{Pool, libsql::params};

use crate::{
    db::libsql::doc_update_log_store::LibsqlDocUpdateLogStore,
    db::libsql::row_ext,
    db::user_doc_repo::{
        UserDocAppendUpdatesParams, UserDocCompactionApplyParams, UserDocCompactionSource,
        UserDocRepository, UserDocSnapshotUpsertParams,
    },
    doc_snapshot_store::DocSnapshotStore,
    user_doc_store::{SPACE_TYPE_USER, UserDocumentSnapshot},
};

pub struct LibsqlUserDocRepository {
    pool: Pool,
    doc_logs: Arc<LibsqlDocUpdateLogStore>,
    doc_snapshots: Arc<DocSnapshotStore>,
}

impl LibsqlUserDocRepository {
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
}

#[async_trait]
impl UserDocRepository for LibsqlUserDocRepository {
    async fn fetch_snapshot_with_timestamp(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Option<UserDocumentSnapshot>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT snapshot, snapshot_kv_key, updated_at, editor_id
                 FROM user_documents
                 WHERE user_id = ? AND doc_id = ?",
                params![user_id, doc_id],
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
        let updated_at = row_ext::get(&row, "updated_at")?;
        let editor_id = row_ext::get(&row, "editor_id")?;

        Ok(Some(UserDocumentSnapshot {
            snapshot,
            updated_at,
            editor_id,
        }))
    }

    async fn upsert_snapshot_with_updates(
        &self,
        params: UserDocSnapshotUpsertParams,
    ) -> Result<()> {
        let UserDocSnapshotUpsertParams {
            user_id,
            doc_id,
            snapshot,
            editor_id,
            created_at,
            updated_at,
            new_document,
            doc_updates,
            log_editor_id,
            log_timestamp,
            log_limit,
        } = params;
        let user_id_ref = user_id.as_str();
        let doc_id_ref = doc_id.as_str();

        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let snapshot_payload =
            self.doc_snapshots
                .prepare_userspace_snapshot(user_id_ref, doc_id_ref, &snapshot)?;

        if new_document {
            tx.execute(
                "INSERT INTO user_documents (
                     user_id,
                     doc_id,
                     snapshot,
                     snapshot_kv_key,
                     created_at,
                     updated_at,
                     editor_id
                 ) VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    user_id_ref,
                    doc_id_ref,
                    snapshot_payload.inline.as_slice(),
                    snapshot_payload.kv_key.as_deref(),
                    created_at,
                    updated_at,
                    editor_id.as_deref()
                ],
            )
            .await?;
        } else {
            let changed = tx
                .execute(
                    "UPDATE user_documents
                     SET snapshot = ?, snapshot_kv_key = ?, updated_at = ?, editor_id = COALESCE(?, editor_id)
                     WHERE user_id = ? AND doc_id = ?",
                    params![
                        snapshot_payload.inline.as_slice(),
                        snapshot_payload.kv_key.as_deref(),
                        updated_at,
                        editor_id.as_deref(),
                        user_id_ref,
                        doc_id_ref
                    ],
                )
                .await?;

            if changed == 0 {
                tx.rollback().await?;
                bail!(
                    "user document not found while updating snapshot: {}/{}",
                    user_id,
                    doc_id
                );
            }
        }

        if !doc_updates.is_empty() {
            self.doc_logs
                .insert_updates(
                    &tx,
                    SPACE_TYPE_USER,
                    user_id_ref,
                    doc_id_ref,
                    &doc_updates,
                    log_editor_id.as_deref(),
                    log_timestamp,
                )
                .await?;
            self.doc_logs
                .trim_updates(&tx, SPACE_TYPE_USER, user_id_ref, doc_id_ref, log_limit)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn append_doc_updates(&self, params: UserDocAppendUpdatesParams) -> Result<()> {
        let UserDocAppendUpdatesParams {
            user_id,
            doc_id,
            updates,
            editor_id,
            timestamp,
            log_limit,
        } = params;
        let user_id_ref = user_id.as_str();
        let doc_id_ref = doc_id.as_str();

        if updates.is_empty() {
            return Ok(());
        }

        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let changed = tx
            .execute(
                "UPDATE user_documents
                 SET updated_at = ?, editor_id = COALESCE(?, editor_id)
                 WHERE user_id = ? AND doc_id = ?",
                params![timestamp, editor_id.as_deref(), user_id_ref, doc_id_ref],
            )
            .await?;

        if changed == 0 {
            tx.rollback().await?;
            bail!(
                "user document not found while appending updates: {}/{}",
                user_id,
                doc_id
            );
        }

        self.doc_logs
            .insert_updates(
                &tx,
                SPACE_TYPE_USER,
                user_id_ref,
                doc_id_ref,
                &updates,
                editor_id.as_deref(),
                timestamp,
            )
            .await?;
        self.doc_logs
            .trim_updates(&tx, SPACE_TYPE_USER, user_id_ref, doc_id_ref, log_limit)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn load_compaction_source(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Option<UserDocCompactionSource>> {
        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let mut rows = tx
            .query(
                "SELECT snapshot, snapshot_kv_key, updated_at
                 FROM user_documents
                 WHERE user_id = ? AND doc_id = ?",
                params![user_id, doc_id],
            )
            .await?;

        let Some(row) = rows.next().await? else {
            tx.rollback().await?;
            return Ok(None);
        };
        let inline_snapshot: Vec<u8> = row_ext::get(&row, "snapshot")?;
        let kv_key = row_ext::get::<Option<String>>(&row, "snapshot_kv_key")?;
        let base_snapshot = self
            .doc_snapshots
            .resolve_snapshot(kv_key.as_deref(), inline_snapshot)?;
        let doc_updated_at = row_ext::get(&row, "updated_at")?;
        let logs = self
            .doc_logs
            .fetch_logs_with_tx(&tx, SPACE_TYPE_USER, user_id, doc_id)
            .await?;

        tx.commit().await?;

        Ok(Some(UserDocCompactionSource {
            base_snapshot,
            logs,
            doc_updated_at,
        }))
    }

    async fn apply_compaction_result(&self, params: UserDocCompactionApplyParams) -> Result<()> {
        let UserDocCompactionApplyParams {
            user_id,
            doc_id,
            snapshot,
            updated_at,
            editor_id,
            last_log_id,
        } = params;
        let user_id_ref = user_id.as_str();
        let doc_id_ref = doc_id.as_str();

        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let snapshot_payload =
            self.doc_snapshots
                .prepare_userspace_snapshot(user_id_ref, doc_id_ref, &snapshot)?;

        let changed = tx
            .execute(
                "UPDATE user_documents
                 SET snapshot = ?, snapshot_kv_key = ?, updated_at = ?, editor_id = COALESCE(?, editor_id)
                 WHERE user_id = ? AND doc_id = ?",
                params![
                    snapshot_payload.inline.as_slice(),
                    snapshot_payload.kv_key.as_deref(),
                    updated_at,
                    editor_id.as_deref(),
                    user_id_ref,
                    doc_id_ref
                ],
            )
            .await?;

        if changed == 0 {
            tx.rollback().await?;
            return Ok(());
        }

        if let Some(last_id) = last_log_id {
            self.doc_logs
                .delete_logs_up_to(&tx, SPACE_TYPE_USER, user_id_ref, doc_id_ref, last_id)
                .await?;
        } else {
            self.doc_logs
                .delete_doc_logs(&tx, SPACE_TYPE_USER, user_id_ref, doc_id_ref)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn delete_doc(&self, user_id: &str, doc_id: &str) -> Result<bool> {
        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        let mut rows = tx
            .query(
                "DELETE FROM user_documents
                 WHERE user_id = ? AND doc_id = ?
                 RETURNING snapshot_kv_key",
                params![user_id, doc_id],
            )
            .await?;

        let Some(row) = rows.next().await? else {
            tx.rollback().await?;
            return Ok(false);
        };
        let snapshot_key = row_ext::get::<Option<String>>(&row, "snapshot_kv_key")?;
        self.doc_snapshots
            .delete_snapshot(snapshot_key.as_deref())?;
        self.doc_logs
            .delete_doc_logs(&tx, SPACE_TYPE_USER, user_id, doc_id)
            .await?;

        tx.commit().await?;
        Ok(true)
    }

    async fn timestamps_since(
        &self,
        user_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<(String, i64)>> {
        let mut sql =
            String::from("SELECT doc_id, updated_at FROM user_documents WHERE user_id = ?");
        if after.is_some() {
            sql.push_str(" AND updated_at > ?");
        }

        let conn = self.pool.get().await?;
        let mut rows = if let Some(ts) = after {
            conn.query(&sql, params![user_id, ts]).await?
        } else {
            conn.query(&sql, params![user_id]).await?
        };

        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push((
                row_ext::get(&row, "doc_id")?,
                row_ext::get(&row, "updated_at")?,
            ));
        }
        Ok(result)
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
                params![SPACE_TYPE_USER, threshold, limit],
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
}
