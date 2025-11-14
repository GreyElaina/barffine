use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::{Pool, Postgres, Row, Transaction, postgres::PgRow};

use crate::doc_data::DocDataBackend;
use crate::doc_update_log::{DocUpdateLogReader, DocUpdateRecord, LogPayloadStore};

pub struct PostgresDocUpdateLogStore {
    pool: Pool<Postgres>,
    payloads: LogPayloadStore,
}

impl PostgresDocUpdateLogStore {
    pub fn new(pool: Pool<Postgres>, doc_data: Option<Arc<dyn DocDataBackend>>) -> Self {
        Self {
            pool,
            payloads: LogPayloadStore::new(doc_data),
        }
    }

    fn rows_to_records(&self, rows: Vec<PgRow>) -> Result<Vec<DocUpdateRecord>> {
        let mut logs = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i64 = row.try_get("id")?;
            let created_at: i64 = row.try_get("created_at")?;
            let editor_id = row.try_get::<Option<String>, _>("editor_id")?;
            let kv_key = row.try_get::<Option<String>, _>("kv_key")?;
            let update = if let Some(key) = kv_key {
                self.payloads.load_external(&key)?
            } else {
                row.try_get::<Vec<u8>, _>("update_blob")?
            };
            logs.push(DocUpdateRecord {
                id,
                update,
                created_at,
                editor_id,
            });
        }
        Ok(logs)
    }

    pub fn uses_external_storage(&self) -> bool {
        self.payloads.uses_external_storage()
    }

    pub async fn insert_updates(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
        updates: &[Vec<u8>],
        editor_id: Option<&str>,
        timestamp: i64,
    ) -> Result<()> {
        for update in updates {
            let external = self.uses_external_storage();
            let inline_blob = if external { Vec::new() } else { update.clone() };

            let inserted_id: i64 = pg_query_scalar!(
                "INSERT INTO doc_updates (
                     space_type,
                     space_id,
                     doc_id,
                     editor_id,
                     update_blob,
                     created_at,
                     kv_key
                 ) VALUES (?, ?, ?, ?, ?, ?, NULL)
                 RETURNING id",
            )
            .bind(space_type)
            .bind(space_id)
            .bind(doc_id)
            .bind(editor_id)
            .bind(&inline_blob)
            .bind(timestamp)
            .fetch_one(tx.as_mut())
            .await?;

            if external {
                let kv_key = format!("log:{inserted_id}");
                self.payloads.put_external(&kv_key, update)?;
                pg_query!("UPDATE doc_updates SET kv_key = ? WHERE id = ?")
                    .bind(&kv_key)
                    .bind(inserted_id)
                    .execute(tx.as_mut())
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn trim_updates(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
        limit: i64,
    ) -> Result<()> {
        let count: i64 = pg_query_scalar!(
            "SELECT COUNT(*) FROM doc_updates WHERE space_type = ? AND space_id = ? AND doc_id = ?",
        )
        .bind(space_type)
        .bind(space_id)
        .bind(doc_id)
        .fetch_one(tx.as_mut())
        .await?;

        if count <= limit {
            return Ok(());
        }

        let delete_count = count - limit;
        let rows = pg_query!(
            "DELETE FROM doc_updates
             WHERE id IN (
                 SELECT id FROM doc_updates
                 WHERE space_type = ? AND space_id = ? AND doc_id = ?
                 ORDER BY id ASC
                 LIMIT ?
             )
             RETURNING kv_key",
        )
        .bind(space_type)
        .bind(space_id)
        .bind(doc_id)
        .bind(delete_count)
        .fetch_all(tx.as_mut())
        .await?;

        let keys = rows
            .into_iter()
            .map(|row| row.try_get::<Option<String>, _>("kv_key"))
            .collect::<Result<Vec<_>, _>>()?;
        self.payloads.cleanup_keys(keys)
    }

    pub async fn delete_doc_logs(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<()> {
        let rows = pg_query!(
            "DELETE FROM doc_updates
             WHERE space_type = ? AND space_id = ? AND doc_id = ?
             RETURNING kv_key",
        )
        .bind(space_type)
        .bind(space_id)
        .bind(doc_id)
        .fetch_all(tx.as_mut())
        .await?;

        let keys = rows
            .into_iter()
            .map(|row| row.try_get::<Option<String>, _>("kv_key"))
            .collect::<Result<Vec<_>, _>>()?;
        self.payloads.cleanup_keys(keys)
    }

    pub async fn delete_logs_up_to(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
        last_id: i64,
    ) -> Result<()> {
        let rows = pg_query!(
            "DELETE FROM doc_updates
             WHERE space_type = ? AND space_id = ? AND doc_id = ? AND id <= ?
             RETURNING kv_key",
        )
        .bind(space_type)
        .bind(space_id)
        .bind(doc_id)
        .bind(last_id)
        .fetch_all(tx.as_mut())
        .await?;

        let keys = rows
            .into_iter()
            .map(|row| row.try_get::<Option<String>, _>("kv_key"))
            .collect::<Result<Vec<_>, _>>()?;
        self.payloads.cleanup_keys(keys)
    }

    pub async fn fetch_logs_via_executor<'e, E>(
        &self,
        executor: E,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocUpdateRecord>>
    where
        E: sqlx::Executor<'e, Database = Postgres>,
    {
        let rows = pg_query!(
            "SELECT id, update_blob, created_at, editor_id, kv_key
             FROM doc_updates
             WHERE space_type = ? AND space_id = ? AND doc_id = ?
             ORDER BY id ASC",
        )
        .bind(space_type)
        .bind(space_id)
        .bind(doc_id)
        .fetch_all(executor)
        .await?;

        self.rows_to_records(rows)
    }
}

#[async_trait]
impl DocUpdateLogReader for PostgresDocUpdateLogStore {
    async fn fetch_logs(
        &self,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocUpdateRecord>> {
        let rows = pg_query!(
            "SELECT id, update_blob, created_at, editor_id, kv_key
             FROM doc_updates
             WHERE space_type = ? AND space_id = ? AND doc_id = ?
             ORDER BY id ASC",
        )
        .bind(space_type)
        .bind(space_id)
        .bind(doc_id)
        .fetch_all(&self.pool)
        .await?;

        self.rows_to_records(rows)
    }
}
