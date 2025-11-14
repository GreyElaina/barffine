use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    doc_store::{DocumentCursor, DocumentHistoryRecord, DocumentMetadata, DocumentSnapshot},
    doc_update_log::DocUpdateRecord,
};

#[derive(Debug, Clone)]
pub struct InsertDocRecordParams {
    pub workspace_id: String,
    pub doc_id: String,
    pub snapshot: Vec<u8>,
    pub owner_id: String,
    pub title: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct MetadataBackfillParams {
    pub workspace_id: String,
    pub doc_id: String,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub creator_id: Option<String>,
    pub updater_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct DuplicateDocParams {
    pub source_workspace_id: String,
    pub source_doc_id: String,
    pub target_workspace_id: String,
    pub new_doc_id: String,
    pub creator_id: String,
    pub title_override: Option<String>,
    pub now: i64,
}

#[derive(Debug, Clone)]
pub struct HistorySnapshotInsert {
    pub snapshot: Vec<u8>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct ReplaceDocSnapshotParams {
    pub workspace_id: String,
    pub doc_id: String,
    pub snapshot: Vec<u8>,
    pub updated_at: i64,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub creator_id: Option<String>,
    pub updater_id: Option<String>,
    pub history_entry: Option<HistorySnapshotInsert>,
}

#[derive(Debug, Clone)]
pub struct SnapshotUpsertParams {
    pub workspace_id: String,
    pub doc_id: String,
    pub snapshot: Vec<u8>,
    pub created_at: i64,
    pub updated_at: i64,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub creator_id: Option<String>,
    pub updater_id: Option<String>,
    pub default_role: String,
    pub mode: String,
    pub history_entry: Option<HistorySnapshotInsert>,
    pub new_document: bool,
    pub doc_updates: Vec<Vec<u8>>,
    pub log_editor_id: Option<String>,
    pub log_timestamp: i64,
    pub log_limit: i64,
}

#[derive(Debug, Clone)]
pub struct AppendDocUpdatesParams {
    pub workspace_id: String,
    pub doc_id: String,
    pub updates: Vec<Vec<u8>>,
    pub editor_id: Option<String>,
    pub timestamp: i64,
    pub log_limit: i64,
}

#[derive(Debug, Clone)]
pub struct CompactionSource {
    pub base_snapshot: Vec<u8>,
    pub logs: Vec<DocUpdateRecord>,
    pub doc_updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct CompactionApplyParams {
    pub workspace_id: String,
    pub doc_id: String,
    pub snapshot: Vec<u8>,
    pub updated_at: i64,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub creator_id: Option<String>,
    pub updater_id: Option<String>,
    pub last_log_id: Option<i64>,
    pub expected_updated_at: i64,
}

#[derive(Debug, Clone, Copy)]
pub enum DocumentListOrder {
    CreatedAsc,
    UpdatedDesc,
}

#[async_trait]
pub trait DocRepository: Send + Sync {
    async fn fetch_snapshot_with_timestamp(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentSnapshot>>;

    async fn insert_doc_record(&self, params: InsertDocRecordParams) -> Result<()>;

    async fn fetch_metadata(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentMetadata>>;

    async fn backfill_metadata(&self, params: MetadataBackfillParams) -> Result<()>;

    async fn list_documents(
        &self,
        workspace_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentCursor>,
        order: DocumentListOrder,
    ) -> Result<Vec<DocumentMetadata>>;

    async fn set_doc_trashed(
        &self,
        workspace_id: &str,
        doc_id: &str,
        trashed_at: i64,
        trashed_by: &str,
    ) -> Result<u64>;

    async fn restore_doc_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        updated_at: i64,
        restored_by: &str,
    ) -> Result<u64>;

    async fn delete_doc_entry(&self, workspace_id: &str, doc_id: &str) -> Result<u64>;

    async fn publish_doc_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        mode: &str,
        timestamp: i64,
        share_token: &str,
    ) -> Result<u64>;

    async fn unpublish_doc_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        timestamp: i64,
    ) -> Result<u64>;

    async fn update_default_role_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        role: &str,
    ) -> Result<u64>;

    async fn update_doc_mode(&self, workspace_id: &str, doc_id: &str, mode: &str) -> Result<u64>;

    async fn count_documents(&self, workspace_id: &str) -> Result<i64>;

    async fn list_share_tokens_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<crate::doc_store::UserShareTokenRecord>>;

    async fn is_doc_public(&self, workspace_id: &str, doc_id: &str) -> Result<bool>;

    async fn list_public_docs(&self, workspace_id: &str) -> Result<Vec<DocumentMetadata>>;

    async fn duplicate_doc_entry(
        &self,
        params: DuplicateDocParams,
    ) -> Result<Option<DocumentMetadata>>;

    async fn fetch_history_snapshot(
        &self,
        workspace_id: &str,
        doc_id: &str,
        history_id: i64,
    ) -> Result<Option<Vec<u8>>>;

    async fn find_history_id_before(
        &self,
        workspace_id: &str,
        doc_id: &str,
        timestamp: i64,
    ) -> Result<Option<i64>>;

    async fn replace_doc_snapshot(&self, params: ReplaceDocSnapshotParams) -> Result<bool>;

    async fn docs_requiring_compaction(
        &self,
        threshold: i64,
        limit: i64,
    ) -> Result<Vec<(String, String)>>;

    async fn list_doc_timestamps(
        &self,
        workspace_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<(String, i64)>>;

    async fn list_history_entries(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
    ) -> Result<Vec<DocumentHistoryRecord>>;

    async fn upsert_snapshot_with_updates(&self, params: SnapshotUpsertParams) -> Result<()>;

    async fn append_doc_updates(&self, params: AppendDocUpdatesParams) -> Result<()>;

    async fn load_compaction_source(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<CompactionSource>>;

    async fn apply_compaction_result(&self, params: CompactionApplyParams) -> Result<()>;
}

pub type DocRepositoryRef = Arc<dyn DocRepository>;
