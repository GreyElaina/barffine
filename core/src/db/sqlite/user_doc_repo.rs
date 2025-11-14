use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use sqlx::{Pool, Row, Sqlite};

use crate::{
    db::sqlite::doc_update_log_store::SqliteDocUpdateLogStore,
    db::user_doc_repo::{
        UserDocAppendUpdatesParams, UserDocCompactionApplyParams, UserDocCompactionSource,
        UserDocRepository, UserDocSnapshotUpsertParams,
    },
    doc_snapshot_store::DocSnapshotStore,
    user_doc_store::{SPACE_TYPE_USER, UserDocumentSnapshot},
};

pub struct SqliteUserDocRepository {
    pool: Pool<Sqlite>,
    doc_logs: Arc<SqliteDocUpdateLogStore>,
    doc_snapshots: Arc<DocSnapshotStore>,
}

impl SqliteUserDocRepository {
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
}

#[async_trait]
impl UserDocRepository for SqliteUserDocRepository {
    async fn fetch_snapshot_with_timestamp(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Option<UserDocumentSnapshot>> {
        let row = sqlx::query(
            "SELECT snapshot, snapshot_kv_key, updated_at, editor_id
             FROM user_documents
             WHERE user_id = ? AND doc_id = ?",
        )
        .bind(user_id)
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
        let editor_id = row
            .try_get::<Option<String>, _>("editor_id")
            .unwrap_or(None);

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

        let mut tx = self.pool.begin().await?;
        let snapshot_payload = self
            .doc_snapshots
            .prepare_userspace_snapshot(&user_id, &doc_id, &snapshot)?;

        if new_document {
            sqlx::query(
                "INSERT INTO user_documents (
                     user_id,
                     doc_id,
                     snapshot,
                     snapshot_kv_key,
                     created_at,
                     updated_at,
                     editor_id
                 ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&user_id)
            .bind(&doc_id)
            .bind(&snapshot_payload.inline)
            .bind(snapshot_payload.kv_key.as_deref())
            .bind(created_at)
            .bind(updated_at)
            .bind(editor_id.as_deref())
            .execute(&mut *tx)
            .await?;
        } else {
            let result = sqlx::query(
                "UPDATE user_documents
                 SET snapshot = ?, snapshot_kv_key = ?, updated_at = ?, editor_id = COALESCE(?, editor_id)
                 WHERE user_id = ? AND doc_id = ?",
            )
            .bind(&snapshot_payload.inline)
            .bind(snapshot_payload.kv_key.as_deref())
            .bind(updated_at)
            .bind(editor_id.as_deref())
            .bind(&user_id)
            .bind(&doc_id)
            .execute(&mut *tx)
            .await?;

            if result.rows_affected() == 0 {
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
                    &mut tx,
                    SPACE_TYPE_USER,
                    &user_id,
                    &doc_id,
                    &doc_updates,
                    log_editor_id.as_deref(),
                    log_timestamp,
                )
                .await?;
            self.doc_logs
                .trim_updates(&mut tx, SPACE_TYPE_USER, &user_id, &doc_id, log_limit)
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

        if updates.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE user_documents
             SET updated_at = ?, editor_id = COALESCE(?, editor_id)
             WHERE user_id = ? AND doc_id = ?",
        )
        .bind(timestamp)
        .bind(editor_id.as_deref())
        .bind(&user_id)
        .bind(&doc_id)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.rollback().await?;
            bail!(
                "user document not found while appending updates: {}/{}",
                user_id,
                doc_id
            );
        }

        self.doc_logs
            .insert_updates(
                &mut tx,
                SPACE_TYPE_USER,
                &user_id,
                &doc_id,
                &updates,
                editor_id.as_deref(),
                timestamp,
            )
            .await?;
        self.doc_logs
            .trim_updates(&mut tx, SPACE_TYPE_USER, &user_id, &doc_id, log_limit)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn load_compaction_source(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Option<UserDocCompactionSource>> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT snapshot, snapshot_kv_key, updated_at
             FROM user_documents
             WHERE user_id = ? AND doc_id = ?",
        )
        .bind(user_id)
        .bind(doc_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            tx.rollback().await?;
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
            .fetch_logs_via_executor(tx.as_mut(), SPACE_TYPE_USER, user_id, doc_id)
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

        let mut tx = self.pool.begin().await?;
        let snapshot_payload = self
            .doc_snapshots
            .prepare_userspace_snapshot(&user_id, &doc_id, &snapshot)?;

        let result = sqlx::query(
            "UPDATE user_documents
             SET snapshot = ?, snapshot_kv_key = ?, updated_at = ?, editor_id = COALESCE(?, editor_id)
             WHERE user_id = ? AND doc_id = ?",
        )
        .bind(&snapshot_payload.inline)
        .bind(snapshot_payload.kv_key.as_deref())
        .bind(updated_at)
        .bind(editor_id.as_deref())
        .bind(&user_id)
        .bind(&doc_id)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(());
        }

        if let Some(last_id) = last_log_id {
            self.doc_logs
                .delete_logs_up_to(&mut tx, SPACE_TYPE_USER, &user_id, &doc_id, last_id)
                .await?;
        } else {
            self.doc_logs
                .delete_doc_logs(&mut tx, SPACE_TYPE_USER, &user_id, &doc_id)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn delete_doc(&self, user_id: &str, doc_id: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "DELETE FROM user_documents
             WHERE user_id = ? AND doc_id = ?
             RETURNING snapshot_kv_key",
        )
        .bind(user_id)
        .bind(doc_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(false);
        };

        let snapshot_key = row.try_get::<Option<String>, _>("snapshot_kv_key")?;
        self.doc_snapshots
            .delete_snapshot(snapshot_key.as_deref())?;
        self.doc_logs
            .delete_doc_logs(&mut tx, SPACE_TYPE_USER, user_id, doc_id)
            .await?;

        tx.commit().await?;
        Ok(true)
    }

    async fn timestamps_since(
        &self,
        user_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<(String, i64)>> {
        let mut query =
            String::from("SELECT doc_id, updated_at FROM user_documents WHERE user_id = ?");
        if after.is_some() {
            query.push_str(" AND updated_at > ?");
        }

        let mut stmt = sqlx::query(&query).bind(user_id);
        if let Some(ts) = after {
            stmt = stmt.bind(ts);
        }

        let rows = stmt.fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("doc_id"), row.get("updated_at")))
            .collect())
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
        .bind(SPACE_TYPE_USER)
        .bind(threshold)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| (row.get("space_id"), row.get("doc_id")))
            .collect())
    }
}
