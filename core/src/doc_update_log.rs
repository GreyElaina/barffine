use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;

use crate::doc_data::DocDataBackend;

#[derive(Debug, Clone)]
pub struct DocUpdateRecord {
    pub id: i64,
    pub update: Vec<u8>,
    pub created_at: i64,
    pub editor_id: Option<String>,
}

#[async_trait]
pub trait DocUpdateLogReader: Send + Sync {
    async fn fetch_logs(
        &self,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocUpdateRecord>>;
}

pub type DocUpdateLogReaderRef = Arc<dyn DocUpdateLogReader>;

#[derive(Clone)]
pub struct LogPayloadStore {
    doc_data: Option<Arc<dyn DocDataBackend>>,
}

impl LogPayloadStore {
    pub fn new(doc_data: Option<Arc<dyn DocDataBackend>>) -> Self {
        Self { doc_data }
    }

    pub fn uses_external_storage(&self) -> bool {
        self.doc_data.is_some()
    }

    pub fn put_external(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let store = self
            .doc_data
            .as_ref()
            .context("doc data backend not configured for doc logs")?;
        store
            .put_log_payload(key, bytes)
            .with_context(|| format!("failed to store doc update log payload for key {key}"))?;
        Ok(())
    }

    pub fn delete_external(&self, key: &str) -> Result<()> {
        if let Some(store) = self.doc_data.as_ref() {
            store
                .delete_log_payload(key)
                .with_context(|| format!("failed to delete doc log payload for key {key}"))?;
        }
        Ok(())
    }

    pub fn load_external(&self, key: &str) -> Result<Vec<u8>> {
        let store = self
            .doc_data
            .as_ref()
            .context("doc data backend not configured for doc logs")?;
        let value = store
            .load_log_payload(key)
            .with_context(|| format!("failed to read doc log payload for key {key}"))?;
        value.ok_or_else(|| anyhow!("doc log payload {key} missing from doc data store"))
    }

    pub fn cleanup_keys(&self, keys: Vec<Option<String>>) -> Result<()> {
        for key in keys {
            if let Some(kv_key) = key {
                self.delete_external(&kv_key)?;
            }
        }
        Ok(())
    }
}
