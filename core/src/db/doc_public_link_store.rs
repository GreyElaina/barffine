use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A minimal view of a row in the `doc_public_links` table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocPublicLinkRecord {
    pub workspace_id: String,
    pub doc_id: String,
    pub token: String,
    pub created_at: i64,
}

/// Backend-agnostic interface for manipulating and querying
/// `doc_public_links` together with `workspace_members`.
///
/// Rocks-backed doc repositories rely on this trait to:
/// - keep `doc_public_links` up to date when publishing/unpublishing
///   documents; and
/// - discover candidate share tokens for a given user based on their
///   workspace memberships.
#[async_trait]
pub trait DocPublicLinkStore: Send + Sync {
    /// Insert or update a public link entry for the given document.
    async fn insert_link(
        &self,
        workspace_id: &str,
        doc_id: &str,
        token: &str,
        created_at: i64,
    ) -> Result<()>;

    /// Remove any public link entry for the given document.
    async fn delete_link(&self, workspace_id: &str, doc_id: &str) -> Result<()>;

    /// List all public link rows that are visible to the given user
    /// based on workspace membership. This does *not* filter by
    /// document state; callers are expected to validate against their
    /// own metadata (e.g. Rocks snapshots).
    async fn list_links_for_user(&self, user_id: &str) -> Result<Vec<DocPublicLinkRecord>>;
}

pub type DocPublicLinkStoreRef = Arc<dyn DocPublicLinkStore>;
