use anyhow::{Context, Result, anyhow};
use sqlx::{Pool, Row, Sqlite};

use crate::doc_data::DocDataBackend;

/// SQLite-backed implementation of `DocDataBackend` for log payloads.
///
/// For the SQLite backend we still store snapshots inline in the
/// main tables, so the snapshot and cache-related methods are
/// currently unimplemented.
pub struct SqliteDocDataStore {
    pool: Pool<Sqlite>,
}

impl SqliteDocDataStore {
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self { pool }
    }

    fn parse_log_key(key: &str) -> Result<i64> {
        let Some(id_str) = key.strip_prefix("log:") else {
            return Err(anyhow!(
                "invalid log payload key '{key}' (expected 'log:<id>')"
            ));
        };
        let id: i64 = id_str
            .parse()
            .with_context(|| format!("invalid log id in key '{key}'"))?;
        Ok(id)
    }
}

impl DocDataBackend for SqliteDocDataStore {
    fn put_log_payload(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let id = Self::parse_log_key(key)?;
        let fut = sqlx::query(
            "UPDATE doc_updates
             SET update_blob = ?
             WHERE id = ?",
        )
        .bind(bytes)
        .bind(id)
        .execute(&self.pool);

        futures::executor::block_on(fut)?;
        Ok(())
    }

    fn delete_log_payload(&self, key: &str) -> Result<()> {
        let id = Self::parse_log_key(key)?;
        // Clear the inline blob; it's fine if the row no longer exists.
        let fut = sqlx::query(
            "UPDATE doc_updates
             SET update_blob = X''
             WHERE id = ?",
        )
        .bind(id)
        .execute(&self.pool);

        futures::executor::block_on(fut)?;
        Ok(())
    }

    fn load_log_payload(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let id = Self::parse_log_key(key)?;
        let fut = sqlx::query(
            "SELECT update_blob
             FROM doc_updates
             WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool);

        let row = futures::executor::block_on(fut)?;

        if let Some(row) = row {
            let bytes: Vec<u8> = row.try_get("update_blob")?;
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    fn put_snapshot(&self, _key: &str, _bytes: &[u8]) -> Result<()> {
        Err(anyhow!(
            "snapshot storage is not implemented for SQLite DocData backend"
        ))
    }

    fn get_snapshot(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        Err(anyhow!(
            "snapshot storage is not implemented for SQLite DocData backend"
        ))
    }

    fn delete_snapshot(&self, _key: &str) -> Result<()> {
        Err(anyhow!(
            "snapshot storage is not implemented for SQLite DocData backend"
        ))
    }

    fn put_cache_entry(&self, _key: &str, _bytes: &[u8]) -> Result<()> {
        Err(anyhow!(
            "doc cache persistence is not implemented for SQLite DocData backend"
        ))
    }

    fn get_cache_entry(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        Err(anyhow!(
            "doc cache persistence is not implemented for SQLite DocData backend"
        ))
    }

    fn delete_cache_entry(&self, _key: &str) -> Result<()> {
        Err(anyhow!(
            "doc cache persistence is not implemented for SQLite DocData backend"
        ))
    }
}
