use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;

use barffine_core::blob::{
    BlobDescriptor, BlobDownload, BlobLocation, BlobMetadata, BlobStorage, ListedBlobRecord,
};

/// Naive in-memory blob storage used for local development and tests.
pub struct InMemoryBlobStorage {
    entries: DashMap<String, (BlobMetadata, Vec<u8>)>,
    trash: DashMap<String, (BlobMetadata, Vec<u8>)>,
}

impl Default for InMemoryBlobStorage {
    fn default() -> Self {
        Self {
            entries: DashMap::new(),
            trash: DashMap::new(),
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
        self.entries
            .insert(key.clone(), (metadata.clone(), content.to_vec()));
        self.trash.remove(&key);

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
        Ok(self.entries.get(&Self::key(descriptor)).map(|entry| {
            let (meta, bytes) = entry.value();
            BlobDownload::from_bytes(meta.clone(), bytes.clone())
        }))
    }

    async fn delete(&self, descriptor: &BlobDescriptor, permanently: bool) -> Result<()> {
        let key = Self::key(descriptor);
        if permanently {
            self.entries.remove(&key);
            self.trash.remove(&key);
        } else {
            if let Some((_, entry)) = self.entries.remove(&key) {
                self.trash.insert(key, entry);
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
                "/api/workspaces/{}/blobs/{}",
                descriptor.workspace_id, descriptor.key
            ),
            None,
        ))
    }

    async fn release_deleted(&self, workspace_id: &str) -> Result<()> {
        let prefix = format!("{workspace_id}/");
        let doomed: Vec<String> = self
            .trash
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| entry.key().clone())
            .collect();
        for key in doomed {
            self.trash.remove(&key);
        }
        Ok(())
    }

    async fn list(&self, workspace_id: &str) -> Result<Vec<ListedBlobRecord>> {
        let prefix = format!("{workspace_id}/");
        let mut blobs = Vec::new();
        for entry in self.entries.iter() {
            let key = entry.key();
            let (meta, bytes) = entry.value();
            if !key.starts_with(&prefix) {
                continue;
            }

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

        blobs.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(blobs)
    }

    async fn total_size(&self, workspace_id: &str) -> Result<i64> {
        let prefix = format!("{workspace_id}/");
        let total = self
            .entries
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| {
                let (meta, bytes) = entry.value();
                meta.content_length
                    .unwrap_or(bytes.len() as u64)
                    .min(i64::MAX as u64) as i64
            })
            .sum();
        Ok(total)
    }
}
