use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::{doc_update_log::DocUpdateRecord, user_doc_store::UserDocumentSnapshot};

#[derive(Debug, Clone)]
pub struct UserDocSnapshotUpsertParams {
    pub user_id: String,
    pub doc_id: String,
    pub snapshot: Vec<u8>,
    pub editor_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub new_document: bool,
    pub doc_updates: Vec<Vec<u8>>,
    pub log_editor_id: Option<String>,
    pub log_timestamp: i64,
    pub log_limit: i64,
}

#[derive(Debug, Clone)]
pub struct UserDocAppendUpdatesParams {
    pub user_id: String,
    pub doc_id: String,
    pub updates: Vec<Vec<u8>>,
    pub editor_id: Option<String>,
    pub timestamp: i64,
    pub log_limit: i64,
}

#[derive(Debug, Clone)]
pub struct UserDocCompactionSource {
    pub base_snapshot: Vec<u8>,
    pub logs: Vec<DocUpdateRecord>,
    pub doc_updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct UserDocCompactionApplyParams {
    pub user_id: String,
    pub doc_id: String,
    pub snapshot: Vec<u8>,
    pub updated_at: i64,
    pub editor_id: Option<String>,
    pub last_log_id: Option<i64>,
}

#[async_trait]
pub trait UserDocRepository: Send + Sync {
    async fn fetch_snapshot_with_timestamp(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Option<UserDocumentSnapshot>>;

    async fn upsert_snapshot_with_updates(&self, params: UserDocSnapshotUpsertParams)
    -> Result<()>;

    async fn append_doc_updates(&self, params: UserDocAppendUpdatesParams) -> Result<()>;

    async fn load_compaction_source(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Option<UserDocCompactionSource>>;

    async fn apply_compaction_result(&self, params: UserDocCompactionApplyParams) -> Result<()>;

    async fn delete_doc(&self, user_id: &str, doc_id: &str) -> Result<bool>;

    async fn timestamps_since(
        &self,
        user_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<(String, i64)>>;

    async fn docs_requiring_compaction(
        &self,
        threshold: i64,
        limit: i64,
    ) -> Result<Vec<(String, String)>>;
}

pub type UserDocRepositoryRef = Arc<dyn UserDocRepository>;
