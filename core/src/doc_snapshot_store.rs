use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

use crate::doc_data::DocDataBackend;

#[derive(Debug, Clone)]
pub struct SnapshotPayload {
    pub inline: Vec<u8>,
    pub kv_key: Option<String>,
}

#[derive(Clone)]
pub struct DocSnapshotStore {
    doc_data: Option<Arc<dyn DocDataBackend>>,
}

impl DocSnapshotStore {
    pub fn new(doc_data: Option<Arc<dyn DocDataBackend>>) -> Self {
        Self { doc_data }
    }

    pub fn prepare_doc_snapshot(
        &self,
        workspace_id: &str,
        doc_id: &str,
        bytes: &[u8],
    ) -> Result<SnapshotPayload> {
        let key = format!("doc:{workspace_id}:{doc_id}");
        self.store_snapshot(key, bytes)
    }

    pub fn prepare_history_snapshot(
        &self,
        workspace_id: &str,
        doc_id: &str,
        bytes: &[u8],
    ) -> Result<SnapshotPayload> {
        let key = format!("hist:{workspace_id}:{doc_id}:{}", Uuid::new_v4());
        self.store_snapshot(key, bytes)
    }

    pub fn prepare_userspace_snapshot(
        &self,
        user_id: &str,
        doc_id: &str,
        bytes: &[u8],
    ) -> Result<SnapshotPayload> {
        let key = format!("user:{user_id}:{doc_id}");
        self.store_snapshot(key, bytes)
    }

    pub fn resolve_snapshot(&self, kv_key: Option<&str>, inline: Vec<u8>) -> Result<Vec<u8>> {
        if let Some(key) = kv_key {
            let store = self
                .doc_data
                .as_ref()
                .context("doc snapshot backend not configured")?;
            let bytes = store
                .get_snapshot(key)
                .with_context(|| format!("load doc snapshot from key {key}"))?
                .ok_or_else(|| anyhow!("doc snapshot missing for key {key}"))?;
            Ok(bytes)
        } else {
            Ok(inline)
        }
    }

    pub fn delete_snapshot(&self, kv_key: Option<&str>) -> Result<()> {
        if let Some(key) = kv_key {
            if let Some(store) = &self.doc_data {
                store
                    .delete_snapshot(key)
                    .with_context(|| format!("delete doc snapshot {key}"))?;
            }
        }
        Ok(())
    }

    fn store_snapshot(&self, key: String, bytes: &[u8]) -> Result<SnapshotPayload> {
        if let Some(store) = &self.doc_data {
            store
                .put_snapshot(&key, bytes)
                .with_context(|| format!("write snapshot {key} to rocksdb"))?;
            Ok(SnapshotPayload {
                inline: Vec::new(),
                kv_key: Some(key),
            })
        } else {
            Ok(SnapshotPayload {
                inline: bytes.to_vec(),
                kv_key: None,
            })
        }
    }
}
