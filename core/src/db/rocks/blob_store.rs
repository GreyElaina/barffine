use std::{fs, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, DEFAULT_COLUMN_FAMILY_NAME,
    Direction, IteratorMode, Options,
};
use serde::{Deserialize, Serialize};

use crate::blob::{
    BlobDescriptor, BlobDownload, BlobLocation, BlobMetadata, BlobScope, BlobStorage,
    ListedBlobRecord,
};

fn workspace_scope(workspace_id: &str) -> BlobScope {
    BlobScope::workspace(workspace_id.to_owned())
}

pub struct RocksBlobStorage {
    db: Arc<DB>,
}

#[derive(Serialize, Deserialize)]
struct RocksBlobEntry {
    metadata: BlobMetadata,
    bytes: Vec<u8>,
    created_at: i64,
    deleted_at: Option<i64>,
}

impl RocksBlobStorage {
    const ACTIVE_CF: &'static str = "active";
    const TRASH_CF: &'static str = "trash";

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create rocksdb parent directory {}",
                    parent.display()
                )
            })?;
        }

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_compression_type(DBCompressionType::Lz4);

        let mut cf_opts = Options::default();
        cf_opts.set_compression_type(DBCompressionType::Lz4);

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(DEFAULT_COLUMN_FAMILY_NAME, cf_opts.clone()),
            ColumnFamilyDescriptor::new(Self::ACTIVE_CF, cf_opts.clone()),
            ColumnFamilyDescriptor::new(Self::TRASH_CF, cf_opts),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)
            .with_context(|| format!("failed to open rocksdb blob store at {}", path.display()))?;

        Ok(Self { db: Arc::new(db) })
    }

    fn now_timestamp() -> i64 {
        Utc::now().timestamp()
    }

    fn prefix(scope: &BlobScope) -> String {
        match scope {
            BlobScope::Workspace { workspace_id } => format!("ws:{workspace_id}/"),
            BlobScope::Namespace { namespace } => format!("ns:{namespace}/"),
        }
    }

    fn make_key(scope: &BlobScope, key: &str) -> String {
        format!("{}{}", Self::prefix(scope), key)
    }

    fn active_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle(Self::ACTIVE_CF)
            .expect("active column family present")
    }

    fn trash_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle(Self::TRASH_CF)
            .expect("trash column family present")
    }

    fn read_entry(&self, cf: &ColumnFamily, key: &str) -> Result<Option<RocksBlobEntry>> {
        let value = self.db.get_cf(cf, key.as_bytes())?;
        if let Some(bytes) = value {
            let entry: RocksBlobEntry =
                bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                    .context("failed to deserialize blob record from rocksdb")?
                    .0;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    fn write_entry(&self, cf: &ColumnFamily, key: &str, entry: &RocksBlobEntry) -> Result<()> {
        let bytes = bincode::serde::encode_to_vec(entry, bincode::config::standard())
            .context("failed to serialize blob record for rocksdb")?;
        self.db.put_cf(cf, key.as_bytes(), bytes)?;
        Ok(())
    }

    fn delete_key(&self, cf: &ColumnFamily, key: &str) -> Result<()> {
        self.db.delete_cf(cf, key.as_bytes())?;
        Ok(())
    }

    fn scan_prefix<F>(&self, cf: &ColumnFamily, prefix: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&str, &RocksBlobEntry) -> Result<()>,
    {
        let iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );

        for item in iter {
            let (key_bytes, value_bytes): (Box<[u8]>, Box<[u8]>) = item?;
            let key_str = match std::str::from_utf8(&key_bytes) {
                Ok(value) => value,
                Err(_) => continue,
            };

            if !key_str.starts_with(prefix) {
                break;
            }

            let stripped = &key_str[prefix.len()..];
            let entry: RocksBlobEntry =
                bincode::serde::decode_from_slice(&value_bytes, bincode::config::standard())
                    .context("failed to deserialize blob record from rocksdb")?
                    .0;

            f(stripped, &entry)?;
        }

        Ok(())
    }
}

#[async_trait]
impl BlobStorage for RocksBlobStorage {
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

        let scope = workspace_scope(&descriptor.workspace_id);
        let key = Self::make_key(&scope, &descriptor.key);

        let entry = RocksBlobEntry {
            metadata: metadata.clone(),
            bytes: content.to_vec(),
            created_at: Self::now_timestamp(),
            deleted_at: None,
        };

        self.write_entry(self.active_cf(), &key, &entry)?;

        Ok(BlobLocation::new(
            format!(
                "/api/workspaces/{}/blobs/{}",
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
        let scope = workspace_scope(&descriptor.workspace_id);
        let key = Self::make_key(&scope, &descriptor.key);

        let entry = self.read_entry(self.active_cf(), &key)?;
        Ok(entry.map(|entry| BlobDownload::from_bytes(entry.metadata, entry.bytes)))
    }

    async fn delete(&self, descriptor: &BlobDescriptor, permanently: bool) -> Result<()> {
        let scope = workspace_scope(&descriptor.workspace_id);
        let key = Self::make_key(&scope, &descriptor.key);

        if permanently {
            self.delete_key(self.active_cf(), &key)?;
            self.delete_key(self.trash_cf(), &key)?;
            return Ok(());
        }

        if let Some(mut entry) = self.read_entry(self.active_cf(), &key)? {
            entry.deleted_at = Some(Self::now_timestamp());
            self.delete_key(self.active_cf(), &key)?;
            self.write_entry(self.trash_cf(), &key, &entry)?;
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
                "/api/workspaces/{}/blobs/{}",
                descriptor.workspace_id, descriptor.key
            ),
            None,
        ))
    }

    async fn release_deleted(&self, workspace_id: &str) -> Result<()> {
        let scope = workspace_scope(workspace_id);
        let prefix = Self::prefix(&scope);

        let mut keys_to_delete = Vec::new();
        self.scan_prefix(self.trash_cf(), &prefix, |key, _| {
            keys_to_delete.push(key.to_string());
            Ok(())
        })?;

        for key in keys_to_delete {
            let full_key = format!("{prefix}{key}");
            let _ = self.delete_key(self.trash_cf(), &full_key);
        }
        Ok(())
    }

    async fn list(&self, workspace_id: &str) -> Result<Vec<ListedBlobRecord>> {
        let scope = workspace_scope(workspace_id);
        let prefix = Self::prefix(&scope);
        let mut blobs = Vec::new();

        self.scan_prefix(self.active_cf(), &prefix, |key, entry| {
            let size = entry
                .metadata
                .content_length
                .unwrap_or(entry.bytes.len() as u64)
                .min(i64::MAX as u64) as i64;
            let mime = entry
                .metadata
                .content_type
                .clone()
                .filter(|value| !value.is_empty());
            blobs.push(ListedBlobRecord {
                key: key.to_string(),
                mime,
                size,
                created_at: Some(entry.created_at),
            });
            Ok(())
        })?;

        blobs.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(blobs)
    }

    async fn total_size(&self, workspace_id: &str) -> Result<i64> {
        let scope = workspace_scope(workspace_id);
        let prefix = Self::prefix(&scope);
        let mut total = 0_i64;

        self.scan_prefix(self.active_cf(), &prefix, |_key, entry| {
            let size = entry
                .metadata
                .content_length
                .unwrap_or(entry.bytes.len() as u64)
                .min(i64::MAX as u64) as i64;
            total += size;
            Ok(())
        })?;

        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn rocks_blob_storage_roundtrip() -> Result<()> {
        let (dir, store) = create_rocks_store()?;
        let descriptor = BlobDescriptor::new("workspace", "logo.png");
        let mut metadata = BlobMetadata::default();
        metadata.content_type = Some("image/png".into());
        metadata.last_modified = Some(Utc::now());

        let bytes = b"png-bytes".to_vec();
        store
            .put(&descriptor, &bytes, metadata.clone())
            .await
            .expect("store blob");

        let download = store.get(&descriptor, false).await?.expect("blob exists");
        assert_eq!(download.bytes.unwrap(), bytes);
        assert_eq!(
            download.metadata.unwrap().content_type,
            metadata.content_type
        );

        let listed = store.list("workspace").await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].key, "logo.png");

        drop(dir);
        Ok(())
    }

    #[tokio::test]
    async fn rocks_blob_storage_soft_delete_and_release() -> Result<()> {
        let (dir, store) = create_rocks_store()?;
        let descriptor = BlobDescriptor::new("workspace", "doc.bin");
        let metadata = BlobMetadata {
            content_type: Some("application/octet-stream".into()),
            ..Default::default()
        };

        store
            .put(&descriptor, b"bytes", metadata)
            .await
            .expect("store blob");

        store
            .delete(&descriptor, false)
            .await
            .expect("soft delete blob");
        assert!(store.get(&descriptor, false).await?.is_none());

        store.release_deleted("workspace").await?;
        let listed = store.list("workspace").await?;
        assert!(listed.is_empty());

        drop(dir);
        Ok(())
    }

    fn create_rocks_store() -> Result<(TempDir, RocksBlobStorage)> {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("rocks-blobs");
        let store = RocksBlobStorage::open(&path)?;
        Ok((dir, store))
    }
}
