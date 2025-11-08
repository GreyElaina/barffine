use anyhow::{Context, Result, anyhow};
use sqlx::{Pool, Row, Sqlite};
use std::collections::HashMap;

use crate::{
    db::Database,
    doc::DocEngine,
    doc_store::{
        DOC_UPDATE_LOG_LIMIT, ParsedDocMeta, SnapshotComputation, build_snapshot_from_updates,
    },
    doc_update_log::{
        DocUpdateRecord, fetch_doc_logs, fetch_doc_logs_via_executor, insert_doc_updates,
        trim_doc_updates,
    },
};

const SPACE_TYPE_USER: &str = "userspace";

#[derive(Debug, Clone)]
pub struct UserDocumentSnapshot {
    pub snapshot: Vec<u8>,
    pub updated_at: i64,
    pub editor_id: Option<String>,
}

#[derive(Clone)]
pub struct UserDocStore {
    pool: Pool<Sqlite>,
}

impl UserDocStore {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: database.pool().clone(),
        }
    }

    fn now_millis() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    pub async fn fetch_snapshot_with_timestamp(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Option<UserDocumentSnapshot>> {
        let row = sqlx::query(
            "SELECT snapshot, updated_at, editor_id FROM user_documents WHERE user_id = ? AND doc_id = ?",
        )
        .bind(user_id)
        .bind(doc_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let mut snapshot: Vec<u8> = row
            .try_get("snapshot")
            .context("failed to read snapshot column")?;
        let mut updated_at: i64 = row.try_get("updated_at")?;
        let editor_id = row.try_get("editor_id").unwrap_or(None);

        let logs = self.fetch_doc_logs(user_id, doc_id).await?;
        if !logs.is_empty() {
            let log_updates = logs
                .iter()
                .map(|log| log.update.clone())
                .collect::<Vec<_>>();
            snapshot = DocEngine::apply_updates_to_snapshot(Some(&snapshot), &log_updates)
                .context("apply userspace doc logs to snapshot")?;
            if let Some(last) = logs.last() {
                updated_at = updated_at.max(last.created_at);
            }
        }

        Ok(Some(UserDocumentSnapshot {
            snapshot,
            updated_at,
            editor_id,
        }))
    }

    pub async fn ensure_doc_record(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<UserDocumentSnapshot> {
        if let Some(snapshot) = self.fetch_snapshot_with_timestamp(user_id, doc_id).await? {
            return Ok(snapshot);
        }

        let snapshot = DocEngine::new().snapshot()?;
        let now = Self::now_millis();
        self.insert_snapshot(
            user_id,
            doc_id,
            &snapshot,
            ParsedDocMeta::default(),
            None,
            now,
            now,
        )
        .await?;

        self.fetch_snapshot_with_timestamp(user_id, doc_id)
            .await?
            .ok_or_else(|| anyhow!("user doc inserted but not found"))
    }

    pub async fn apply_updates(
        &self,
        user_id: &str,
        doc_id: &str,
        updates: &[Vec<u8>],
        editor_id: Option<&str>,
    ) -> Result<UserDocumentSnapshot> {
        if updates.is_empty() {
            return Err(anyhow!("updates payload must not be empty"));
        }

        let mut tx = self.pool.begin().await?;
        let existing_row = sqlx::query(
            "SELECT snapshot, updated_at, editor_id FROM user_documents WHERE user_id = ? AND doc_id = ?",
        )
        .bind(user_id)
        .bind(doc_id)
        .fetch_optional(&mut *tx)
        .await?;

        let existing_snapshot: Option<Vec<u8>> = existing_row
            .as_ref()
            .map(|row| row.try_get("snapshot"))
            .transpose()
            .context("failed to read snapshot column")?;

        let SnapshotComputation {
            snapshot: new_snapshot,
            created_at,
            updated_at,
            ..
        } = build_snapshot_from_updates(
            doc_id,
            existing_snapshot.as_deref(),
            updates,
            Self::now_millis,
        )?;

        if existing_snapshot.is_none() {
            self.insert_snapshot_inner(
                &mut tx,
                user_id,
                doc_id,
                &new_snapshot,
                editor_id,
                created_at,
                updated_at,
            )
            .await?;
        } else {
            self.update_snapshot_inner(
                &mut tx,
                user_id,
                doc_id,
                &new_snapshot,
                editor_id,
                updated_at,
            )
            .await?;
        }

        insert_doc_updates(
            &mut tx,
            SPACE_TYPE_USER,
            user_id,
            doc_id,
            updates,
            editor_id,
            updated_at,
        )
        .await?;
        trim_doc_updates(
            &mut tx,
            SPACE_TYPE_USER,
            user_id,
            doc_id,
            DOC_UPDATE_LOG_LIMIT,
        )
        .await?;

        tx.commit().await?;

        Ok(UserDocumentSnapshot {
            snapshot: new_snapshot,
            updated_at,
            editor_id: editor_id.map(str::to_owned).or_else(|| {
                existing_row
                    .as_ref()
                    .and_then(|row| row.try_get("editor_id").ok().flatten())
            }),
        })
    }

    pub async fn append_doc_updates(
        &self,
        user_id: &str,
        doc_id: &str,
        updates: &[Vec<u8>],
        editor_id: Option<&str>,
        timestamp: i64,
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE user_documents
             SET updated_at = ?, editor_id = COALESCE(?, editor_id)
             WHERE user_id = ? AND doc_id = ?",
        )
        .bind(timestamp)
        .bind(editor_id)
        .bind(user_id)
        .bind(doc_id)
        .execute(&mut *tx)
        .await?;

        insert_doc_updates(
            &mut tx,
            SPACE_TYPE_USER,
            user_id,
            doc_id,
            updates,
            editor_id,
            timestamp,
        )
        .await?;
        trim_doc_updates(
            &mut tx,
            SPACE_TYPE_USER,
            user_id,
            doc_id,
            DOC_UPDATE_LOG_LIMIT,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn compact_doc(&self, user_id: &str, doc_id: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row =
            sqlx::query("SELECT snapshot FROM user_documents WHERE user_id = ? AND doc_id = ?")
                .bind(user_id)
                .bind(doc_id)
                .fetch_optional(&mut *tx)
                .await?;

        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(());
        };

        let base_snapshot: Vec<u8> = row.try_get("snapshot")?;
        let logs =
            fetch_doc_logs_via_executor(tx.as_mut(), SPACE_TYPE_USER, user_id, doc_id).await?;
        if logs.is_empty() {
            tx.commit().await?;
            return Ok(());
        }

        let updates = logs
            .iter()
            .map(|log| log.update.clone())
            .collect::<Vec<_>>();
        let SnapshotComputation {
            snapshot: merged_snapshot,
            meta,
            mut updated_at,
            ..
        } = build_snapshot_from_updates(doc_id, Some(&base_snapshot), &updates, Self::now_millis)
            .context("merge userspace doc updates during compaction")?;
        if meta.updated_at.is_none() {
            if let Some(last) = logs.last() {
                updated_at = last.created_at;
            }
        }

        sqlx::query(
            "UPDATE user_documents
             SET snapshot = ?, updated_at = ?, editor_id = COALESCE(?, editor_id)
             WHERE user_id = ? AND doc_id = ?",
        )
        .bind(&merged_snapshot)
        .bind(updated_at)
        .bind(meta.updater_id.as_deref())
        .bind(user_id)
        .bind(doc_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query("DELETE FROM doc_updates WHERE space_type = ? AND space_id = ? AND doc_id = ?")
            .bind(SPACE_TYPE_USER)
            .bind(user_id)
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn delete_doc(&self, user_id: &str, doc_id: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query("DELETE FROM user_documents WHERE user_id = ? AND doc_id = ?")
            .bind(user_id)
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM doc_updates WHERE space_type = ? AND space_id = ? AND doc_id = ?")
            .bind(SPACE_TYPE_USER)
            .bind(user_id)
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn timestamps_since(
        &self,
        user_id: &str,
        after: Option<i64>,
    ) -> Result<HashMap<String, i64>> {
        let mut query =
            String::from("SELECT doc_id, updated_at FROM user_documents WHERE user_id = ?");

        if after.is_some() {
            query.push_str(" AND updated_at > ?");
        }

        let mut rows = sqlx::query(&query).bind(user_id);

        if let Some(timestamp) = after {
            rows = rows.bind(timestamp);
        }

        let rows = rows.fetch_all(&self.pool).await?;
        let mut result = HashMap::with_capacity(rows.len());

        for row in rows {
            let doc_id: String = row.try_get("doc_id")?;
            let updated_at: i64 = row.try_get("updated_at")?;
            result.insert(doc_id, updated_at);
        }

        Ok(result)
    }

    pub async fn fetch_doc_logs(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocUpdateRecord>> {
        fetch_doc_logs(self.pool.clone(), SPACE_TYPE_USER, user_id, doc_id).await
    }

    pub async fn docs_requiring_compaction(
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

        let mut docs = Vec::with_capacity(rows.len());
        for row in rows {
            docs.push((row.try_get("space_id")?, row.try_get("doc_id")?));
        }
        Ok(docs)
    }

    async fn insert_snapshot(
        &self,
        user_id: &str,
        doc_id: &str,
        snapshot: &[u8],
        meta: ParsedDocMeta,
        editor_id: Option<&str>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.insert_snapshot_inner(
            &mut tx,
            user_id,
            doc_id,
            snapshot,
            editor_id,
            meta.created_at.unwrap_or(created_at),
            meta.updated_at.unwrap_or(updated_at),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn insert_snapshot_inner(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        user_id: &str,
        doc_id: &str,
        snapshot: &[u8],
        editor_id: Option<&str>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO user_documents (user_id, doc_id, snapshot, created_at, updated_at, editor_id)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(user_id)
        .bind(doc_id)
        .bind(snapshot)
        .bind(created_at)
        .bind(updated_at)
        .bind(editor_id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn update_snapshot_inner(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        user_id: &str,
        doc_id: &str,
        snapshot: &[u8],
        editor_id: Option<&str>,
        updated_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE user_documents
             SET snapshot = ?, updated_at = ?, editor_id = COALESCE(?, editor_id)
             WHERE user_id = ? AND doc_id = ?",
        )
        .bind(snapshot)
        .bind(updated_at)
        .bind(editor_id)
        .bind(user_id)
        .bind(doc_id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }
}
