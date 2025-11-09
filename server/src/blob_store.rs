use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::RwLock;
use tracing::debug;

use barffine_core::{
    blob::ListedBlobRecord,
    blob::{BlobDescriptor, BlobDownload, BlobLocation, BlobMetadata, BlobStorage},
    db::Database,
};
use sqlx::{Pool, Row, Sqlite, sqlite::SqliteRow};

/// Naive in-memory blob storage used for local development and tests.
pub struct InMemoryBlobStorage {
    entries: RwLock<HashMap<String, (BlobMetadata, Vec<u8>)>>,
    trash: RwLock<HashMap<String, (BlobMetadata, Vec<u8>)>>,
}

impl Default for InMemoryBlobStorage {
    fn default() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            trash: RwLock::new(HashMap::new()),
        }
    }
}

impl InMemoryBlobStorage {
    fn key(descriptor: &BlobDescriptor) -> String {
        format!("{}/{}", descriptor.workspace_id, descriptor.key)
    }
}

#[async_trait]
impl BlobStorage for InMemoryBlobStorage {
    async fn put(
        &self,
        descriptor: &BlobDescriptor,
        content: &[u8],
        mut metadata: BlobMetadata,
    ) -> Result<BlobLocation> {
        if metadata.content_length.is_none() {
            metadata.content_length = Some(content.len() as u64);
        }

        if metadata.last_modified.is_none() {
            metadata.last_modified = Some(Utc::now());
        }

        let key = Self::key(descriptor);
        let mut entries = self.entries.write().await;
        entries.insert(key.clone(), (metadata.clone(), content.to_vec()));
        let mut trash = self.trash.write().await;
        trash.remove(&key);

        Ok(BlobLocation::new(
            format!(
                "/workspaces/{}/blobs/{}",
                descriptor.workspace_id, descriptor.key
            ),
            None,
        ))
    }

    async fn get(
        &self,
        descriptor: &BlobDescriptor,
        _prefer_presigned: bool,
    ) -> Result<Option<BlobDownload>> {
        let entries = self.entries.read().await;
        Ok(entries
            .get(&Self::key(descriptor))
            .cloned()
            .map(|(meta, bytes)| BlobDownload::from_bytes(meta, bytes)))
    }

    async fn delete(&self, descriptor: &BlobDescriptor, permanently: bool) -> Result<()> {
        let key = Self::key(descriptor);
        if permanently {
            let mut entries = self.entries.write().await;
            entries.remove(&key);
            let mut trash = self.trash.write().await;
            trash.remove(&key);
        } else {
            let mut entries = self.entries.write().await;
            if let Some(entry) = entries.remove(&key) {
                let mut trash = self.trash.write().await;
                trash.insert(key, entry);
            }
        }
        Ok(())
    }

    async fn presign_download(
        &self,
        descriptor: &BlobDescriptor,
        _ttl: Duration,
    ) -> Result<BlobLocation> {
        Ok(BlobLocation::new(
            format!(
                "/workspaces/{}/blobs/{}",
                descriptor.workspace_id, descriptor.key
            ),
            None,
        ))
    }

    async fn release_deleted(&self, workspace_id: &str) -> Result<()> {
        let mut trash = self.trash.write().await;
        let prefix = format!("{workspace_id}/");
        trash.retain(|key, _| !key.starts_with(&prefix));
        Ok(())
    }

    async fn list(&self, workspace_id: &str) -> Result<Vec<ListedBlobRecord>> {
        let entries = self.entries.read().await;
        let prefix = format!("{workspace_id}/");
        let mut blobs = Vec::new();
        for (key, (meta, bytes)) in entries.iter() {
            if key.starts_with(&prefix) {
                let key_part = key[prefix.len()..].to_string();
                let size = meta
                    .content_length
                    .unwrap_or(bytes.len() as u64)
                    .min(i64::MAX as u64) as i64;
                let created_at = meta.last_modified.map(|dt| dt.timestamp());
                blobs.push(ListedBlobRecord {
                    key: key_part,
                    mime: meta.content_type.clone(),
                    size,
                    created_at,
                });
            }
        }

        blobs.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(blobs)
    }

    async fn total_size(&self, workspace_id: &str) -> Result<i64> {
        let entries = self.entries.read().await;
        let prefix = format!("{workspace_id}/");
        let total = entries
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(_, (meta, bytes))| {
                meta.content_length
                    .unwrap_or(bytes.len() as u64)
                    .min(i64::MAX as u64) as i64
            })
            .sum();
        Ok(total)
    }
}

/// SQLite-backed blob store that keeps bytes in the same database as the rest of the app state.
pub struct SqliteBlobStorage {
    pool: Pool<Sqlite>,
}

impl SqliteBlobStorage {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: database.pool().clone(),
        }
    }

    fn now_timestamp() -> i64 {
        Utc::now().timestamp()
    }

    fn encode_last_modified(meta: &BlobMetadata) -> Option<i64> {
        meta.last_modified.map(|value| value.timestamp())
    }

    fn decode_last_modified(value: Option<i64>) -> Option<DateTime<Utc>> {
        value.and_then(|ts| Utc.timestamp_opt(ts, 0).single())
    }

    fn clamp_length(len: u64) -> i64 {
        (len.min(i64::MAX as u64)) as i64
    }

    fn build_metadata(row: &SqliteRow) -> Result<BlobMetadata> {
        let content_type = row.try_get::<Option<String>, _>("content_type")?;
        let content_length = row
            .try_get::<Option<i64>, _>("content_length")?
            .map(|len| len.max(0) as u64);
        let etag = row.try_get::<Option<String>, _>("etag")?;
        let last_modified =
            Self::decode_last_modified(row.try_get::<Option<i64>, _>("last_modified")?);

        Ok(BlobMetadata {
            content_type,
            content_length,
            etag,
            last_modified,
        })
    }
}

#[async_trait]
impl BlobStorage for SqliteBlobStorage {
    async fn put(
        &self,
        descriptor: &BlobDescriptor,
        content: &[u8],
        mut metadata: BlobMetadata,
    ) -> Result<BlobLocation> {
        if metadata.content_length.is_none() {
            metadata.content_length = Some(content.len() as u64);
        }

        if metadata.last_modified.is_none() {
            metadata.last_modified = Some(Utc::now());
        }

        let content_length = metadata
            .content_length
            .unwrap_or_else(|| content.len() as u64);
        let last_modified = Self::encode_last_modified(&metadata);
        let now = Self::now_timestamp();

        sqlx::query(
            r#"
            INSERT INTO workspace_blobs (
                workspace_id, blob_key, content, content_type, content_length,
                etag, last_modified, created_at, deleted_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT(workspace_id, blob_key) DO UPDATE SET
                content = excluded.content,
                content_type = excluded.content_type,
                content_length = excluded.content_length,
                etag = excluded.etag,
                last_modified = excluded.last_modified,
                deleted_at = NULL,
                created_at = CASE
                    WHEN workspace_blobs.deleted_at IS NULL THEN workspace_blobs.created_at
                    ELSE excluded.created_at
                END
            "#,
        )
        .bind(&descriptor.workspace_id)
        .bind(&descriptor.key)
        .bind(content)
        .bind(metadata.content_type.clone())
        .bind(Self::clamp_length(content_length))
        .bind(metadata.etag.clone())
        .bind(last_modified)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(BlobLocation::new(
            format!(
                "/workspaces/{}/blobs/{}",
                descriptor.workspace_id, descriptor.key
            ),
            None,
        ))
    }

    async fn get(
        &self,
        descriptor: &BlobDescriptor,
        _prefer_presigned: bool,
    ) -> Result<Option<BlobDownload>> {
        let row = sqlx::query(
            r#"
            SELECT content, content_type, content_length, etag, last_modified
            FROM workspace_blobs
            WHERE workspace_id = ? AND blob_key = ? AND deleted_at IS NULL
            "#,
        )
        .bind(&descriptor.workspace_id)
        .bind(&descriptor.key)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            debug!(
                workspace_id = %descriptor.workspace_id,
                blob_key = %descriptor.key,
                "blob not found in sqlite storage"
            );
            return Ok(None);
        };

        let metadata = Self::build_metadata(&row)?;
        let bytes: Vec<u8> = row.try_get("content")?;

        Ok(Some(BlobDownload::from_bytes(metadata, bytes)))
    }

    async fn delete(&self, descriptor: &BlobDescriptor, permanently: bool) -> Result<()> {
        if permanently {
            sqlx::query(
                r#"
                DELETE FROM workspace_blobs
                WHERE workspace_id = ? AND blob_key = ?
                "#,
            )
            .bind(&descriptor.workspace_id)
            .bind(&descriptor.key)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(
                r#"
                UPDATE workspace_blobs
                SET deleted_at = ?
                WHERE workspace_id = ? AND blob_key = ?
                "#,
            )
            .bind(Self::now_timestamp())
            .bind(&descriptor.workspace_id)
            .bind(&descriptor.key)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    async fn presign_download(
        &self,
        descriptor: &BlobDescriptor,
        _ttl: Duration,
    ) -> Result<BlobLocation> {
        Ok(BlobLocation::new(
            format!(
                "/workspaces/{}/blobs/{}",
                descriptor.workspace_id, descriptor.key
            ),
            None,
        ))
    }

    async fn release_deleted(&self, workspace_id: &str) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM workspace_blobs
            WHERE workspace_id = ? AND deleted_at IS NOT NULL
            "#,
        )
        .bind(workspace_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list(&self, workspace_id: &str) -> Result<Vec<ListedBlobRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT blob_key, content_type, content_length, last_modified, created_at
            FROM workspace_blobs
            WHERE workspace_id = ? AND deleted_at IS NULL
            ORDER BY blob_key
            "#,
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;

        let mut blobs = Vec::with_capacity(rows.len());
        for row in rows {
            let key: String = row.try_get("blob_key")?;
            let mime = row
                .try_get::<Option<String>, _>("content_type")?
                .filter(|value| !value.is_empty());
            let size = row
                .try_get::<Option<i64>, _>("content_length")?
                .unwrap_or(0);
            let created_at = row.try_get::<Option<i64>, _>("created_at")?;
            blobs.push(ListedBlobRecord {
                key,
                mime,
                size,
                created_at,
            });
        }

        Ok(blobs)
    }

    async fn total_size(&self, workspace_id: &str) -> Result<i64> {
        let row = sqlx::query(
            r#"
            SELECT COALESCE(SUM(content_length), 0) AS total
            FROM workspace_blobs
            WHERE workspace_id = ? AND deleted_at IS NULL
            "#,
        )
        .bind(workspace_id)
        .fetch_one(&self.pool)
        .await?;

        let total = row.try_get::<i64, _>("total")?;
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barffine_core::{blob::BlobDescriptor, config::AppConfig, db::Database};
    use tempfile::TempDir;

    use crate::utils::db::run_migrations;

    async fn create_sqlite_store() -> (TempDir, Database, SqliteBlobStorage) {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut config = AppConfig::default();
        config.database_path = temp_dir
            .path()
            .join("blobs.db")
            .to_string_lossy()
            .into_owned();

        let database = Database::connect(&config).await.expect("connect db");
        run_migrations(database.pool())
            .await
            .expect("run migrations");

        let now = Utc::now().timestamp();
        sqlx::query("INSERT INTO users (id, email, password_hash, created_at) VALUES (?, ?, ?, ?)")
            .bind("owner")
            .bind("owner@example.com")
            .bind("hash")
            .bind(now)
            .execute(database.pool())
            .await
            .expect("insert user");

        sqlx::query("INSERT INTO workspaces (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)")
            .bind("workspace")
            .bind("Workspace")
            .bind("owner")
            .bind(now)
            .execute(database.pool())
            .await
            .expect("insert workspace");

        let store = SqliteBlobStorage::new(&database);
        (temp_dir, database, store)
    }

    #[tokio::test]
    async fn sqlite_blob_storage_roundtrip() {
        let (_dir, _db, store) = create_sqlite_store().await;
        let descriptor = BlobDescriptor::new("workspace", "logo.png");
        let mut metadata = BlobMetadata::default();
        metadata.content_type = Some("image/png".into());
        metadata.last_modified = Some(Utc::now());

        let bytes = b"png-bytes".to_vec();
        store
            .put(&descriptor, &bytes, metadata.clone())
            .await
            .expect("store blob");

        let download = store
            .get(&descriptor, false)
            .await
            .expect("fetch blob")
            .expect("blob exists");
        assert_eq!(download.bytes.unwrap(), bytes);
        assert_eq!(
            download.metadata.unwrap().content_type,
            metadata.content_type
        );

        let listed = store.list("workspace").await.expect("list blobs");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].key, "logo.png");
    }

    #[tokio::test]
    async fn sqlite_blob_storage_soft_delete_and_release() {
        let (_dir, _db, store) = create_sqlite_store().await;
        let descriptor = BlobDescriptor::new("workspace", "doc.bin");
        let mut metadata = BlobMetadata::default();
        metadata.content_type = Some("application/octet-stream".into());

        store
            .put(&descriptor, b"bytes", metadata)
            .await
            .expect("store blob");

        store
            .delete(&descriptor, false)
            .await
            .expect("soft delete blob");
        assert!(
            store
                .get(&descriptor, false)
                .await
                .expect("fetch soft deleted")
                .is_none()
        );

        store
            .release_deleted("workspace")
            .await
            .expect("release deleted");

        let listed = store
            .list("workspace")
            .await
            .expect("list blobs after purge");
        assert!(listed.is_empty());
    }
}
