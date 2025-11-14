use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use deadpool_libsql::{
    Pool,
    libsql::{self, params},
};

use crate::db::libsql::row_ext;
use crate::doc_data::DocDataBackend;
use crate::doc_update_log::{DocUpdateLogReader, DocUpdateRecord, LogPayloadStore};

pub struct LibsqlDocUpdateLogStore {
    pool: Pool,
    payloads: LogPayloadStore,
}

impl LibsqlDocUpdateLogStore {
    pub fn new(pool: Pool, doc_data: Option<Arc<dyn DocDataBackend>>) -> Self {
        Self {
            pool,
            payloads: LogPayloadStore::new(doc_data),
        }
    }

    async fn rows_to_records(&self, mut rows: libsql::Rows) -> Result<Vec<DocUpdateRecord>> {
        let mut logs = Vec::new();
        while let Some(row) = rows.next().await? {
            let id: i64 = row_ext::get(&row, "id")?;
            let created_at: i64 = row_ext::get(&row, "created_at")?;
            let editor_id: Option<String> = row_ext::get(&row, "editor_id")?;
            let kv_key: Option<String> = row_ext::get(&row, "kv_key")?;
            let update = if let Some(key) = kv_key {
                self.payloads.load_external(&key)?
            } else {
                row_ext::get::<Vec<u8>>(&row, "update_blob")?
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

    pub async fn insert_updates(
        &self,
        tx: &libsql::Transaction,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
        updates: &[Vec<u8>],
        editor_id: Option<&str>,
        timestamp: i64,
    ) -> Result<()> {
        for update in updates {
            let external = self.payloads.uses_external_storage();
            let inline_blob = if external { Vec::new() } else { update.clone() };
            let mut rows = tx
                .query(
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
                    params![
                        space_type,
                        space_id,
                        doc_id,
                        editor_id,
                        inline_blob,
                        timestamp
                    ],
                )
                .await?;

            if let Some(row) = rows.next().await? {
                let inserted_id: i64 = row_ext::get(&row, "id")?;
                if external {
                    let kv_key = format!("log:{inserted_id}");
                    self.payloads.put_external(&kv_key, update)?;
                    tx.execute(
                        "UPDATE doc_updates SET kv_key = ? WHERE id = ?",
                        params![kv_key, inserted_id],
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    pub async fn trim_updates(
        &self,
        tx: &libsql::Transaction,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
        limit: i64,
    ) -> Result<()> {
        let mut count_rows = tx
            .query(
                "SELECT COUNT(*) as count FROM doc_updates WHERE space_type = ? AND space_id = ? AND doc_id = ?",
                params![space_type, space_id, doc_id],
            )
            .await?;
        let Some(row) = count_rows.next().await? else {
            return Ok(());
        };
        let count: i64 = row_ext::get(&row, "count")?;
        if count <= limit {
            return Ok(());
        }

        let delete_count = count - limit;
        let mut rows = tx
            .query(
                "DELETE FROM doc_updates
                 WHERE id IN (
                     SELECT id FROM doc_updates
                     WHERE space_type = ? AND space_id = ? AND doc_id = ?
                     ORDER BY id ASC
                     LIMIT ?
                 )
                 RETURNING kv_key",
                params![space_type, space_id, doc_id, delete_count],
            )
            .await?;

        let mut keys = Vec::new();
        while let Some(row) = rows.next().await? {
            keys.push(row_ext::get(&row, "kv_key")?);
        }
        self.payloads.cleanup_keys(keys)
    }

    pub async fn delete_doc_logs(
        &self,
        tx: &libsql::Transaction,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<()> {
        let mut rows = tx
            .query(
                "DELETE FROM doc_updates
                 WHERE space_type = ? AND space_id = ? AND doc_id = ?
                 RETURNING kv_key",
                params![space_type, space_id, doc_id],
            )
            .await?;

        let mut keys = Vec::new();
        while let Some(row) = rows.next().await? {
            keys.push(row_ext::get(&row, "kv_key")?);
        }
        self.payloads.cleanup_keys(keys)
    }

    pub async fn delete_logs_up_to(
        &self,
        tx: &libsql::Transaction,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
        last_id: i64,
    ) -> Result<()> {
        let mut rows = tx
            .query(
                "DELETE FROM doc_updates
                 WHERE space_type = ? AND space_id = ? AND doc_id = ? AND id <= ?
                 RETURNING kv_key",
                params![space_type, space_id, doc_id, last_id],
            )
            .await?;

        let mut keys = Vec::new();
        while let Some(row) = rows.next().await? {
            keys.push(row_ext::get(&row, "kv_key")?);
        }
        self.payloads.cleanup_keys(keys)
    }

    pub async fn fetch_logs_with_tx(
        &self,
        tx: &libsql::Transaction,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocUpdateRecord>> {
        let rows = tx
            .query(
                "SELECT id, update_blob, created_at, editor_id, kv_key
                 FROM doc_updates
                 WHERE space_type = ? AND space_id = ? AND doc_id = ?
                 ORDER BY id ASC",
                params![space_type, space_id, doc_id],
            )
            .await?;

        self.rows_to_records(rows).await
    }
}

#[async_trait]
impl DocUpdateLogReader for LibsqlDocUpdateLogStore {
    async fn fetch_logs(
        &self,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocUpdateRecord>> {
        let conn = self.pool.get().await?;
        let rows = conn
            .query(
                "SELECT id, update_blob, created_at, editor_id, kv_key
                 FROM doc_updates
                 WHERE space_type = ? AND space_id = ? AND doc_id = ?
                 ORDER BY id ASC",
                params![space_type, space_id, doc_id],
            )
            .await?;

        self.rows_to_records(rows).await
    }
}
