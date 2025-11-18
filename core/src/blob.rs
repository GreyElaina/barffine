use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Pseudo workspace namespace for storing user avatars inside the generic blob store.
///
/// This constant is shared between the server's avatar handlers and
/// the database-backed blob stores so that namespace data can be
/// partitioned cleanly from normal workspace blobs.
pub const AVATAR_STORAGE_NAMESPACE: &str = "__avatars__";

/// Descriptor for a blob belonging to a workspace or document context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobDescriptor {
    /// Logical bucket for the blob. For normal blobs this is a `WorkspaceId`,
    /// for special namespaces (e.g. avatars) it is the raw namespace string.
    pub workspace_id: String,
    pub key: String,
}

impl BlobDescriptor {
    pub fn new(workspace_id: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            workspace_id: workspace_id.into(),
            key: key.into(),
        }
    }
}

/// Metadata that accompanies a blob upload or download.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobMetadata {
    pub content_type: Option<String>,
    pub content_length: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<DateTime<Utc>>,
}

/// Returned location for blobs that support direct client access.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobLocation {
    pub uri: String,
    pub expires_at: Option<DateTime<Utc>>,
}

impl BlobLocation {
    pub fn new(uri: impl Into<String>, expires_at: Option<DateTime<Utc>>) -> Self {
        Self {
            uri: uri.into(),
            expires_at,
        }
    }
}

/// Streaming representation for blob downloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobDownload {
    pub metadata: Option<BlobMetadata>,
    pub bytes: Option<Vec<u8>>,
    pub location: Option<BlobLocation>,
}

impl BlobDownload {
    pub fn from_bytes(metadata: BlobMetadata, bytes: Vec<u8>) -> Self {
        Self {
            metadata: Some(metadata),
            bytes: Some(bytes),
            location: None,
        }
    }

    pub fn from_location(location: BlobLocation, metadata: Option<BlobMetadata>) -> Self {
        Self {
            metadata,
            bytes: None,
            location: Some(location),
        }
    }
}

#[derive(Debug)]
pub struct BlobStream<'a> {
    pub bytes: &'a [u8],
}

/// Lightweight listing record used for workspace blob enumeration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListedBlobRecord {
    pub key: String,
    pub mime: Option<String>,
    pub size: i64,
    pub created_at: Option<i64>,
}

/// Storage abstraction that can power `/blobs` and attachment endpoints.
#[async_trait]
pub trait BlobStorage: Send + Sync {
    async fn put(
        &self,
        descriptor: &BlobDescriptor,
        content: &[u8],
        metadata: BlobMetadata,
    ) -> Result<BlobLocation>;

    async fn get(
        &self,
        descriptor: &BlobDescriptor,
        prefer_presigned: bool,
    ) -> Result<Option<BlobDownload>>;

    async fn delete(&self, descriptor: &BlobDescriptor, permanently: bool) -> Result<()>;

    async fn presign_download(
        &self,
        descriptor: &BlobDescriptor,
        ttl: Duration,
    ) -> Result<BlobLocation>;

    async fn release_deleted(&self, _workspace_id: &str) -> Result<()> {
        Ok(())
    }

    async fn list(&self, _workspace_id: &str) -> Result<Vec<ListedBlobRecord>> {
        Ok(Vec::new())
    }

    async fn total_size(&self, _workspace_id: &str) -> Result<i64> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_roundtrip() {
        let descriptor = BlobDescriptor::new("workspace", "path/to/blob");
        assert_eq!(descriptor.workspace_id, "workspace");
        assert_eq!(descriptor.key, "path/to/blob");
    }
}

/// Logical scope for blob persistence (workspace vs. special namespaces such as avatars).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobScope {
    Workspace { workspace_id: crate::ids::WorkspaceId },
    Namespace { namespace: String },
}

impl BlobScope {
    pub fn workspace(workspace_id: impl Into<crate::ids::WorkspaceId>) -> Self {
        Self::Workspace {
            workspace_id: workspace_id.into(),
        }
    }

    pub fn namespace(namespace: impl Into<String>) -> Self {
        Self::Namespace {
            namespace: namespace.into(),
        }
    }

    pub fn workspace_id(&self) -> Option<&str> {
        match self {
            Self::Workspace { workspace_id } => Some(workspace_id.as_str()),
            _ => None,
        }
    }

    pub fn identifier(&self) -> &str {
        match self {
            Self::Workspace { workspace_id } => workspace_id,
            Self::Namespace { namespace } => namespace,
        }
    }
}
