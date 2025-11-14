use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use crate::blob::{BlobMetadata, BlobScope, ListedBlobRecord};

#[derive(Debug, Clone)]
pub struct BlobRecord {
    pub metadata: BlobMetadata,
    pub bytes: Vec<u8>,
}

#[async_trait]
pub trait BlobRepository: Send + Sync {
    async fn upsert_blob(
        &self,
        scope: &BlobScope,
        key: &str,
        content: &[u8],
        metadata: &BlobMetadata,
        now: i64,
    ) -> Result<()>;

    async fn fetch_blob(&self, scope: &BlobScope, key: &str) -> Result<Option<BlobRecord>>;

    async fn delete_blob(
        &self,
        scope: &BlobScope,
        key: &str,
        permanently: bool,
        timestamp: i64,
    ) -> Result<()>;

    async fn release_deleted(&self, scope: &BlobScope) -> Result<()>;

    async fn list_blobs(&self, scope: &BlobScope) -> Result<Vec<ListedBlobRecord>>;

    async fn total_size(&self, scope: &BlobScope) -> Result<i64>;
}

pub type BlobRepositoryRef = Arc<dyn BlobRepository>;
