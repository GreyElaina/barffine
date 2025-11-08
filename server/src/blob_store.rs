use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::RwLock;

use barffine_core::blob::ListedBlobRecord;
use barffine_core::blob::{BlobDescriptor, BlobDownload, BlobLocation, BlobMetadata, BlobStorage};

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
