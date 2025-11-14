use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};
use uuid::Uuid;

use crate::{
    db::{
        Database,
        doc_repo::{
            AppendDocUpdatesParams, CompactionApplyParams, DocRepositoryRef, DocumentListOrder,
            DuplicateDocParams, HistorySnapshotInsert, InsertDocRecordParams,
            MetadataBackfillParams, ReplaceDocSnapshotParams, SnapshotUpsertParams,
        },
    },
    doc::DocEngine,
    doc_update_log::{DocUpdateLogReaderRef, DocUpdateRecord},
};

pub const DOC_UPDATE_LOG_LIMIT: i64 = 200;
pub const SPACE_TYPE_WORKSPACE: &str = "workspace";
const WORKSPACE_DB_DOC_SUFFIXES: &[&str] = &[
    "folders",
    "docProperties",
    "docCustomPropertyInfo",
    "pinnedCollections",
    "explorerIcon",
];

const WORKSPACE_USERDATA_DOC_SUFFIXES: &[&str] = &["favorite", "settings", "docIntegrationRef"];

#[derive(Debug, PartialEq, Eq)]
enum SystemDocKind<'a> {
    WorkspaceRoot,
    WorkspaceDb { suffix: &'a str },
    Userdata { user_id: &'a str, suffix: &'a str },
}

fn classify_system_doc<'a>(workspace_id: &'a str, doc_id: &'a str) -> Option<SystemDocKind<'a>> {
    if doc_id == workspace_id {
        return Some(SystemDocKind::WorkspaceRoot);
    }

    let parts: Vec<&str> = doc_id.split('$').collect();
    match parts.as_slice() {
        ["db", ws_id, suffix]
            if *ws_id == workspace_id && WORKSPACE_DB_DOC_SUFFIXES.contains(suffix) =>
        {
            Some(SystemDocKind::WorkspaceDb { suffix })
        }
        ["userdata", user_id, ws_id, suffix]
            if *ws_id == workspace_id && WORKSPACE_USERDATA_DOC_SUFFIXES.contains(suffix) =>
        {
            Some(SystemDocKind::Userdata { user_id, suffix })
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct DocumentHistoryRecord {
    pub id: i64,
    pub snapshot: Vec<u8>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct DocumentSnapshot {
    pub snapshot: Vec<u8>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentMetadata {
    pub id: String,
    pub workspace_id: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub default_role: String,
    pub public: bool,
    pub blocked: bool,
    pub mode: String,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub creator_id: Option<String>,
    pub updater_id: Option<String>,
    pub share_token: Option<String>,
    pub trashed_at: Option<i64>,
    pub trashed_by: Option<String>,
    pub snapshot: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct UserShareTokenRecord {
    pub workspace_id: String,
    pub doc_id: String,
    pub token: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct DocumentCursor {
    pub timestamp: i64,
    pub id: String,
}

#[derive(Clone)]
pub struct DocumentStore {
    doc_repo: DocRepositoryRef,
    doc_logs: DocUpdateLogReaderRef,
}

impl DocumentStore {
    pub fn new(database: &Database) -> Self {
        let doc_repo = database.repositories().doc_repo();
        let doc_logs = database.doc_update_logs();
        Self { doc_repo, doc_logs }
    }

    fn now_millis() -> i64 {
        Utc::now().timestamp_millis()
    }

    fn metadata_needs_backfill(metadata: &DocumentMetadata) -> bool {
        metadata.title.is_none()
            || metadata.summary.is_none()
            || metadata.creator_id.is_none()
            || metadata.updater_id.is_none()
            || metadata.created_at == 0
            || metadata.updated_at == 0
    }

    fn metadata_backfill_changed(original: &DocumentMetadata, updated: &DocumentMetadata) -> bool {
        original.title != updated.title
            || original.summary != updated.summary
            || original.creator_id != updated.creator_id
            || original.updater_id != updated.updater_id
            || original.created_at != updated.created_at
            || original.updated_at != updated.updated_at
    }

    fn merge_parsed_metadata(metadata: &mut DocumentMetadata, parsed: ParsedDocMeta) {
        let ParsedDocMeta {
            title,
            summary,
            created_at,
            updated_at,
            creator_id,
            updater_id,
        } = parsed;

        if metadata.title.is_none() {
            metadata.title = title;
        }

        if metadata.summary.is_none() {
            metadata.summary = summary;
        }

        if metadata.creator_id.is_none() {
            metadata.creator_id = creator_id;
        }

        if metadata.updater_id.is_none() {
            metadata.updater_id = updater_id;
        }

        if let Some(parsed_created_at) = created_at {
            if metadata.created_at == 0 || parsed_created_at < metadata.created_at {
                metadata.created_at = parsed_created_at;
            }
        }

        if let Some(parsed_updated_at) = updated_at {
            if parsed_updated_at > metadata.updated_at {
                metadata.updated_at = parsed_updated_at;
            }
        }
    }

    async fn hydrate_metadata_with_snapshot(
        &self,
        metadata: DocumentMetadata,
        snapshot: Vec<u8>,
    ) -> Result<DocumentMetadata> {
        let mut updated = metadata.clone();
        updated.snapshot = Some(snapshot.clone());
        let parsed = extract_doc_meta(&snapshot, &metadata.id);
        Self::merge_parsed_metadata(&mut updated, parsed);

        if Self::metadata_backfill_changed(&metadata, &updated) {
            self.doc_repo
                .backfill_metadata(MetadataBackfillParams {
                    workspace_id: updated.workspace_id.clone(),
                    doc_id: updated.id.clone(),
                    title: updated.title.clone(),
                    summary: updated.summary.clone(),
                    creator_id: updated.creator_id.clone(),
                    updater_id: updated.updater_id.clone(),
                    created_at: updated.created_at,
                    updated_at: updated.updated_at,
                })
                .await?;
        }

        Ok(updated)
    }

    /// Backfill document metadata using a provided snapshot, if needed.
    ///
    /// This is primarily used by the doc cache flush path, where we already
    /// have an up-to-date snapshot in memory and want to avoid re-parsing it
    /// on read paths (e.g. document listing).
    pub async fn backfill_metadata_from_snapshot(
        &self,
        workspace_id: &str,
        doc_id: &str,
        snapshot: &[u8],
    ) -> Result<()> {
        let Some(metadata) = self.doc_repo.fetch_metadata(workspace_id, doc_id).await? else {
            return Ok(());
        };

        if !Self::metadata_needs_backfill(&metadata) {
            return Ok(());
        }

        // Reuse the existing snapshot-based hydration logic – it will update
        // metadata fields and persist them when they actually change.
        let _ = self
            .hydrate_metadata_with_snapshot(metadata, snapshot.to_vec())
            .await?;

        Ok(())
    }

    pub async fn hydrate_metadata(&self, metadata: DocumentMetadata) -> Result<DocumentMetadata> {
        if !Self::metadata_needs_backfill(&metadata) {
            return Ok(metadata);
        }

        let snapshot = if let Some(existing) = metadata.snapshot.clone() {
            Some(existing)
        } else {
            self.fetch_snapshot(&metadata.workspace_id, &metadata.id)
                .await?
        };

        let Some(snapshot) = snapshot else {
            return Ok(metadata);
        };

        self.hydrate_metadata_with_snapshot(metadata, snapshot)
            .await
    }

    fn generate_share_token() -> String {
        Uuid::new_v4().to_string().replace('-', "")
    }

    pub async fn apply_updates(
        &self,
        workspace_id: &str,
        doc_id: &str,
        updates: &[Vec<u8>],
    ) -> Result<Vec<u8>> {
        if updates.is_empty() {
            return Err(anyhow!("updates payload must not be empty"));
        }

        let existing_snapshot = self
            .doc_repo
            .fetch_snapshot_with_timestamp(workspace_id, doc_id)
            .await?
            .map(|snapshot| snapshot.snapshot);

        let existing_snapshot_ref = existing_snapshot.as_deref();

        let SnapshotComputation {
            snapshot: new_snapshot,
            meta,
            created_at,
            updated_at,
        } = build_snapshot_from_updates(doc_id, existing_snapshot_ref, updates, Self::now_millis)?;

        let had_existing = existing_snapshot_ref.is_some();
        let history_entry = existing_snapshot.map(|snapshot| HistorySnapshotInsert {
            snapshot,
            created_at: updated_at,
        });

        self.doc_repo
            .upsert_snapshot_with_updates(SnapshotUpsertParams {
                workspace_id: workspace_id.to_owned(),
                doc_id: doc_id.to_owned(),
                snapshot: new_snapshot.clone(),
                created_at,
                updated_at,
                title: meta.title.clone(),
                summary: meta.summary.clone(),
                creator_id: meta.creator_id.clone(),
                updater_id: meta.updater_id.clone(),
                default_role: "manager".to_string(),
                mode: "page".to_string(),
                history_entry,
                new_document: !had_existing,
                doc_updates: updates.to_vec(),
                log_editor_id: meta.updater_id.clone(),
                log_timestamp: updated_at,
                log_limit: DOC_UPDATE_LOG_LIMIT,
            })
            .await?;

        Ok(new_snapshot)
    }

    pub async fn append_doc_updates(
        &self,
        workspace_id: &str,
        doc_id: &str,
        updates: &[Vec<u8>],
        editor_id: Option<&str>,
        timestamp: i64,
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        self.doc_repo
            .append_doc_updates(AppendDocUpdatesParams {
                workspace_id: workspace_id.to_owned(),
                doc_id: doc_id.to_owned(),
                updates: updates.to_vec(),
                editor_id: editor_id.map(|id| id.to_owned()),
                timestamp,
                log_limit: DOC_UPDATE_LOG_LIMIT,
            })
            .await
    }

    pub async fn compact_doc(&self, workspace_id: &str, doc_id: &str) -> Result<()> {
        let Some(source) = self
            .doc_repo
            .load_compaction_source(workspace_id, doc_id)
            .await?
        else {
            return Ok(());
        };

        if source.logs.is_empty() {
            return Ok(());
        }

        let updates = source
            .logs
            .iter()
            .map(|log| log.update.clone())
            .collect::<Vec<_>>();
        let merged_snapshot =
            DocEngine::apply_updates_to_snapshot(Some(&source.base_snapshot), &updates)
                .context("merge doc updates during compaction")?;
        let meta = extract_doc_meta(&merged_snapshot, doc_id);
        let updated_at = meta
            .updated_at
            .or_else(|| source.logs.last().map(|log| log.created_at))
            .unwrap_or_else(Self::now_millis);

        self.doc_repo
            .apply_compaction_result(CompactionApplyParams {
                workspace_id: workspace_id.to_owned(),
                doc_id: doc_id.to_owned(),
                snapshot: merged_snapshot,
                updated_at,
                title: meta.title,
                summary: meta.summary,
                creator_id: meta.creator_id,
                updater_id: meta.updater_id,
                last_log_id: source.logs.last().map(|log| log.id),
                expected_updated_at: source.doc_updated_at,
            })
            .await?;

        Ok(())
    }

    pub async fn ensure_doc_record(
        &self,
        workspace_id: &str,
        doc_id: &str,
        owner_id: &str,
        title: Option<&str>,
    ) -> Result<DocumentMetadata> {
        if let Some(metadata) = self.doc_repo.fetch_metadata(workspace_id, doc_id).await? {
            return Ok(metadata);
        }

        let snapshot = DocEngine::new().snapshot()?;
        let now_ts = Self::now_millis();
        let title_value = title
            .map(str::trim)
            .filter(|trimmed| !trimmed.is_empty())
            .map(ToOwned::to_owned);
        let insert_params = InsertDocRecordParams {
            workspace_id: workspace_id.to_string(),
            doc_id: doc_id.to_string(),
            snapshot: snapshot.clone(),
            owner_id: owner_id.to_string(),
            title: title_value.clone(),
            created_at: now_ts,
        };
        self.doc_repo.insert_doc_record(insert_params).await?;

        let metadata = DocumentMetadata {
            id: doc_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            created_at: now_ts,
            updated_at: now_ts,
            default_role: "manager".to_string(),
            public: false,
            blocked: false,
            mode: "page".to_string(),
            title: title_value,
            summary: None,
            creator_id: Some(owner_id.to_owned()),
            updater_id: Some(owner_id.to_owned()),
            share_token: None,
            trashed_at: None,
            trashed_by: None,
            snapshot: Some(snapshot),
        };

        Ok(metadata)
    }

    pub async fn fetch_snapshot(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<Vec<u8>>> {
        let snapshot = self
            .fetch_snapshot_with_timestamp(workspace_id, doc_id)
            .await?
            .map(|doc| doc.snapshot);
        Ok(snapshot)
    }

    pub async fn fetch_snapshot_with_timestamp(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentSnapshot>> {
        let Some(mut snapshot_with_ts) = self
            .doc_repo
            .fetch_snapshot_with_timestamp(workspace_id, doc_id)
            .await?
        else {
            return Ok(None);
        };

        let logs = self.fetch_doc_logs(workspace_id, doc_id).await?;
        if !logs.is_empty() {
            let log_updates = logs
                .iter()
                .map(|log| log.update.clone())
                .collect::<Vec<_>>();
            snapshot_with_ts.snapshot = DocEngine::apply_updates_to_snapshot(
                Some(&snapshot_with_ts.snapshot),
                &log_updates,
            )
            .context("apply doc updates from log to snapshot")?;
            if let Some(last) = logs.last() {
                snapshot_with_ts.updated_at = snapshot_with_ts.updated_at.max(last.created_at);
            }
        }

        Ok(Some(snapshot_with_ts))
    }

    pub async fn list_doc_timestamps(
        &self,
        workspace_id: &str,
        after: Option<i64>,
    ) -> Result<HashMap<String, i64>> {
        let entries = self
            .doc_repo
            .list_doc_timestamps(workspace_id, after)
            .await?;

        let mut map = HashMap::with_capacity(entries.len());
        for (id, updated_at) in entries {
            map.insert(id, updated_at);
        }

        Ok(map)
    }

    pub async fn list_history(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
    ) -> Result<Vec<DocumentHistoryRecord>> {
        self.doc_repo
            .list_history_entries(workspace_id, doc_id, limit)
            .await
    }

    pub async fn fetch_history_as_of(
        &self,
        workspace_id: &str,
        doc_id: &str,
        timestamp: i64,
    ) -> Result<Option<Vec<u8>>> {
        let Some(history_id) = self
            .doc_repo
            .find_history_id_before(workspace_id, doc_id, timestamp)
            .await?
        else {
            return Ok(None);
        };

        self.doc_repo
            .fetch_history_snapshot(workspace_id, doc_id, history_id)
            .await
    }

    pub async fn find_metadata(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentMetadata>> {
        if let Some(metadata) = self.doc_repo.fetch_metadata(workspace_id, doc_id).await? {
            let hydrated = self.hydrate_metadata(metadata).await?;
            Ok(Some(hydrated))
        } else {
            Ok(None)
        }
    }

    pub async fn list_by_workspace_created(
        &self,
        workspace_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentCursor>,
    ) -> Result<Vec<DocumentMetadata>> {
        let mut records = self
            .doc_repo
            .list_documents(
                workspace_id,
                limit + 1,
                offset,
                cursor,
                DocumentListOrder::CreatedAsc,
            )
            .await?;
        for record in &mut records {
            if Self::metadata_needs_backfill(record) {
                *record = self.hydrate_metadata(record.clone()).await?;
            }
        }

        Ok(records)
    }

    pub async fn list_by_workspace_recent(
        &self,
        workspace_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentCursor>,
    ) -> Result<Vec<DocumentMetadata>> {
        let mut records = self
            .doc_repo
            .list_documents(
                workspace_id,
                limit + 1,
                offset,
                cursor,
                DocumentListOrder::UpdatedDesc,
            )
            .await?;
        for record in &mut records {
            if Self::metadata_needs_backfill(record) {
                *record = self.hydrate_metadata(record.clone()).await?;
            }
        }

        Ok(records)
    }

    pub async fn count_by_workspace(&self, workspace_id: &str) -> Result<i64> {
        self.doc_repo.count_documents(workspace_id).await
    }

    pub async fn list_share_tokens_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<UserShareTokenRecord>> {
        self.doc_repo.list_share_tokens_for_user(user_id).await
    }

    pub async fn publish_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        mode: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let now = Self::now_millis();
        let token = Self::generate_share_token();
        let rows = self
            .doc_repo
            .publish_doc_entry(workspace_id, doc_id, mode, now, &token)
            .await?;
        if rows == 0 {
            return Ok(None);
        }
        self.find_metadata(workspace_id, doc_id).await
    }

    pub async fn unpublish_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let now = Self::now_millis();
        let rows = self
            .doc_repo
            .unpublish_doc_entry(workspace_id, doc_id, now)
            .await?;
        if rows == 0 {
            return Ok(None);
        }

        self.find_metadata(workspace_id, doc_id).await
    }

    pub async fn update_default_role(
        &self,
        workspace_id: &str,
        doc_id: &str,
        role: &str,
    ) -> Result<bool> {
        let rows = self
            .doc_repo
            .update_default_role_entry(workspace_id, doc_id, role)
            .await?;
        Ok(rows > 0)
    }

    pub async fn duplicate_doc(
        &self,
        source_workspace_id: &str,
        source_doc_id: &str,
        target_workspace_id: &str,
        new_doc_id: &str,
        creator_id: &str,
        title_override: Option<&str>,
    ) -> Result<Option<DocumentMetadata>> {
        let params = DuplicateDocParams {
            source_workspace_id: source_workspace_id.to_owned(),
            source_doc_id: source_doc_id.to_owned(),
            target_workspace_id: target_workspace_id.to_owned(),
            new_doc_id: new_doc_id.to_owned(),
            creator_id: creator_id.to_owned(),
            title_override: title_override.map(str::to_owned),
            now: Self::now_millis(),
        };

        self.doc_repo.duplicate_doc_entry(params).await
    }

    pub async fn ensure_workspace_defaults(
        &self,
        workspace_id: &str,
        owner_id: &str,
        workspace_name: Option<&str>,
    ) -> Result<Vec<DocumentMetadata>> {
        let mut ensured = Vec::with_capacity(1 + WORKSPACE_DB_DOC_SUFFIXES.len());
        ensured.push(
            self.ensure_doc_record(workspace_id, workspace_id, owner_id, workspace_name)
                .await?,
        );

        for suffix in WORKSPACE_DB_DOC_SUFFIXES {
            let doc_id = format!("db${workspace_id}${suffix}");
            ensured.push(
                self.ensure_doc_record(workspace_id, &doc_id, owner_id, None)
                    .await?,
            );
        }

        Ok(ensured)
    }

    pub async fn ensure_system_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        owner_id: &str,
        workspace_name: Option<&str>,
    ) -> Result<Option<DocumentMetadata>> {
        match classify_system_doc(workspace_id, doc_id) {
            Some(SystemDocKind::WorkspaceRoot) => self
                .ensure_doc_record(workspace_id, doc_id, owner_id, workspace_name)
                .await
                .map(Some),
            Some(SystemDocKind::WorkspaceDb { .. }) => self
                .ensure_doc_record(workspace_id, doc_id, owner_id, None)
                .await
                .map(Some),
            Some(SystemDocKind::Userdata { .. }) => self
                .ensure_doc_record(workspace_id, doc_id, owner_id, None)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    pub async fn trash_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        trashed_by: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let now = Self::now_millis();
        let rows = self
            .doc_repo
            .set_doc_trashed(workspace_id, doc_id, now, trashed_by)
            .await?;
        if rows == 0 {
            return Ok(None);
        }

        self.find_metadata(workspace_id, doc_id).await
    }

    pub async fn restore_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        restored_by: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let now = Self::now_millis();
        let rows = self
            .doc_repo
            .restore_doc_entry(workspace_id, doc_id, now, restored_by)
            .await?;
        if rows == 0 {
            return Ok(None);
        }

        self.find_metadata(workspace_id, doc_id).await
    }

    pub async fn delete_doc(&self, workspace_id: &str, doc_id: &str) -> Result<bool> {
        let affected = self.doc_repo.delete_doc_entry(workspace_id, doc_id).await?;
        Ok(affected > 0)
    }

    pub async fn restore_doc_history(
        &self,
        workspace_id: &str,
        doc_id: &str,
        history_id: i64,
        updater_id: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let target_snapshot = match self
            .doc_repo
            .fetch_history_snapshot(workspace_id, doc_id, history_id)
            .await?
        {
            Some(snapshot) => snapshot,
            None => return Ok(None),
        };

        let Some(current_snapshot) = self
            .doc_repo
            .fetch_snapshot_with_timestamp(workspace_id, doc_id)
            .await?
        else {
            return Ok(None);
        };

        let mut parsed = extract_doc_meta(&target_snapshot, doc_id);
        if parsed.updater_id.is_none() {
            parsed.updater_id = Some(updater_id.to_string());
        }

        let updated_at = Self::now_millis();
        let history_entry = HistorySnapshotInsert {
            snapshot: current_snapshot.snapshot,
            created_at: updated_at,
        };

        let replaced = self
            .doc_repo
            .replace_doc_snapshot(ReplaceDocSnapshotParams {
                workspace_id: workspace_id.to_owned(),
                doc_id: doc_id.to_owned(),
                snapshot: target_snapshot.clone(),
                updated_at,
                title: parsed.title.clone(),
                summary: parsed.summary.clone(),
                creator_id: parsed.creator_id.clone(),
                updater_id: parsed.updater_id.clone(),
                history_entry: Some(history_entry),
            })
            .await?;

        if !replaced {
            return Ok(None);
        }

        let metadata = self.find_metadata(workspace_id, doc_id).await?;
        Ok(metadata.map(|mut value| {
            value.snapshot = Some(target_snapshot);
            value
        }))
    }

    pub async fn restore_doc_at_timestamp(
        &self,
        workspace_id: &str,
        doc_id: &str,
        timestamp: i64,
        updater_id: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let Some(history_id) = self
            .doc_repo
            .find_history_id_before(workspace_id, doc_id, timestamp)
            .await?
        else {
            return Ok(None);
        };

        self.restore_doc_history(workspace_id, doc_id, history_id, updater_id)
            .await
    }

    pub async fn is_public(&self, workspace_id: &str, doc_id: &str) -> Result<bool> {
        self.doc_repo.is_doc_public(workspace_id, doc_id).await
    }

    pub async fn list_public(&self, workspace_id: &str) -> Result<Vec<DocumentMetadata>> {
        let mut records = self.doc_repo.list_public_docs(workspace_id).await?;
        for record in &mut records {
            if Self::metadata_needs_backfill(record) {
                *record = self.hydrate_metadata(record.clone()).await?;
            }
        }

        Ok(records)
    }

    pub async fn docs_requiring_compaction(
        &self,
        threshold: i64,
        limit: i64,
    ) -> Result<Vec<(String, String)>> {
        self.doc_repo
            .docs_requiring_compaction(threshold, limit)
            .await
    }
}

impl DocumentStore {
    pub async fn fetch_doc_logs(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocUpdateRecord>> {
        self.doc_logs
            .fetch_logs(SPACE_TYPE_WORKSPACE, workspace_id, doc_id)
            .await
    }
}

static YRS_META_SUCCESS: AtomicU64 = AtomicU64::new(0);
static YRS_META_FAILURE: AtomicU64 = AtomicU64::new(0);

#[derive(Default, Clone)]
pub(crate) struct ParsedDocMeta {
    pub(crate) title: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) created_at: Option<i64>,
    pub(crate) updated_at: Option<i64>,
    pub(crate) creator_id: Option<String>,
    pub(crate) updater_id: Option<String>,
}

pub(crate) fn extract_doc_meta(snapshot: &[u8], doc_id: &str) -> ParsedDocMeta {
    if let Some(parsed) = try_extract_doc_meta_with_yrs(snapshot, doc_id) {
        return parsed;
    }

    use y_octo::Doc;

    let doc = match Doc::try_from_binary_v1(snapshot) {
        Ok(doc) => doc,
        Err(_) => return ParsedDocMeta::default(),
    };

    let map = match doc.get_map("meta") {
        Ok(map) => map,
        Err(_) => return ParsedDocMeta::default(),
    };

    let Some(pages_value) = map.get("pages") else {
        return ParsedDocMeta::default();
    };

    match serde_json::to_value(&pages_value) {
        Ok(json) => parse_meta_from_json(json, doc_id),
        Err(_) => ParsedDocMeta::default(),
    }
}

fn try_extract_doc_meta_with_yrs(snapshot: &[u8], doc_id: &str) -> Option<ParsedDocMeta> {
    match parse_doc_meta_with_yrs(snapshot, doc_id) {
        Ok(parsed) => {
            YRS_META_SUCCESS.fetch_add(1, Ordering::Relaxed);
            Some(parsed)
        }
        Err(error) => {
            let failures = YRS_META_FAILURE.fetch_add(1, Ordering::Relaxed) + 1;
            if failures <= 5 || failures.is_power_of_two() {
                tracing::debug!(%doc_id, failures, ?error, "yrs doc meta fallback");
            }
            None
        }
    }
}

fn parse_doc_meta_with_yrs(snapshot: &[u8], doc_id: &str) -> Result<ParsedDocMeta> {
    use yrs::types::ToJson;
    use yrs::updates::decoder::Decode;
    use yrs::{Doc as YrsDoc, Map, ReadTxn, Transact, Update};

    let update = Update::decode_v1(snapshot).context("decode snapshot as yrs update")?;

    let doc = YrsDoc::new();
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(update)
            .context("apply snapshot update to yrs doc")?;
    }

    let txn = doc.transact();
    let Some(meta) = txn.get_map("meta") else {
        return Ok(ParsedDocMeta::default());
    };
    let Some(pages) = meta.get(&txn, "pages") else {
        return Ok(ParsedDocMeta::default());
    };
    let pages_json = pages.to_json(&txn);
    let json = match serde_json::to_value(pages_json) {
        Ok(value) => value,
        Err(_) => return Ok(ParsedDocMeta::default()),
    };

    Ok(parse_meta_from_json(json, doc_id))
}

fn number_to_millis(value: &JsonValue) -> Option<i64> {
    if let Some(int) = value.as_i64() {
        return Some(int);
    }

    value.as_f64().map(|float| float.round() as i64)
}

fn parse_meta_from_json(json: JsonValue, doc_id: &str) -> ParsedDocMeta {
    let mut meta = ParsedDocMeta::default();

    let JsonValue::Array(entries) = json else {
        return meta;
    };

    for entry in entries {
        let JsonValue::Object(obj) = entry else {
            continue;
        };

        let Some(id) = obj.get("id").and_then(JsonValue::as_str) else {
            continue;
        };

        if id != doc_id {
            continue;
        }

        meta.title = obj
            .get("title")
            .and_then(JsonValue::as_str)
            .map(|s| s.to_owned());
        meta.summary = obj
            .get("summary")
            .and_then(JsonValue::as_str)
            .map(|s| s.to_owned());
        meta.created_at = obj.get("createDate").and_then(number_to_millis);
        meta.updated_at = obj.get("updatedDate").and_then(number_to_millis);
        meta.creator_id = obj
            .get("creator")
            .and_then(JsonValue::as_str)
            .map(|s| s.to_owned());
        meta.updater_id = obj
            .get("lastUpdatedBy")
            .and_then(JsonValue::as_str)
            .map(|s| s.to_owned());
        break;
    }

    meta
}

pub(crate) struct SnapshotComputation {
    pub snapshot: Vec<u8>,
    pub meta: ParsedDocMeta,
    pub created_at: i64,
    pub updated_at: i64,
}

pub(crate) fn build_snapshot_from_updates<F>(
    doc_id: &str,
    existing_snapshot: Option<&[u8]>,
    updates: &[Vec<u8>],
    now_fn: F,
) -> Result<SnapshotComputation>
where
    F: FnOnce() -> i64,
{
    let snapshot = DocEngine::apply_updates_to_snapshot(existing_snapshot, updates)?;
    let meta = extract_doc_meta(&snapshot, doc_id);
    let fallback_timestamp = now_fn();
    let updated_at = meta.updated_at.unwrap_or(fallback_timestamp);
    let created_at = meta.created_at.unwrap_or(updated_at);

    Ok(SnapshotComputation {
        snapshot,
        meta,
        created_at,
        updated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_doc_meta_reads_title_and_timestamps() {
        let json = serde_json::json!([
            {
                "id": "doc-1",
                "title": "Hello",
                "summary": "Summary",
                "createDate": 1_234_000,
                "updatedDate": 5_678_000,
                "creator": "user-1",
                "lastUpdatedBy": "user-2"
            },
            {
                "id": "other",
                "title": "Other",
                "createDate": 9_000_000,
                "updatedDate": 9_000_000
            }
        ]);

        let parsed = parse_meta_from_json(json, "doc-1");

        assert_eq!(parsed.title.as_deref(), Some("Hello"));
        assert_eq!(parsed.summary.as_deref(), Some("Summary"));
        assert_eq!(parsed.created_at, Some(1234000));
        assert_eq!(parsed.updated_at, Some(5678000));
        assert_eq!(parsed.creator_id.as_deref(), Some("user-1"));
        assert_eq!(parsed.updater_id.as_deref(), Some("user-2"));
    }

    #[test]
    fn parse_meta_from_json_gracefully_skips_missing_doc() {
        let json = serde_json::json!([
            {
                "id": "doc-2",
                "title": "Other",
                "createDate": 9_000_000,
                "updatedDate": 9_000_000
            }
        ]);

        let parsed = parse_meta_from_json(json, "doc-1");

        assert!(parsed.title.is_none());
        assert!(parsed.summary.is_none());
        assert!(parsed.created_at.is_none());
        assert!(parsed.updated_at.is_none());
        assert!(parsed.creator_id.is_none());
        assert!(parsed.updater_id.is_none());
    }

    #[test]
    fn parse_meta_from_json_allows_missing_fields() {
        let json = serde_json::json!([
            {
                "id": "doc-1",
                "title": "Hello"
            }
        ]);

        let parsed = parse_meta_from_json(json, "doc-1");

        assert_eq!(parsed.title.as_deref(), Some("Hello"));
        assert!(parsed.summary.is_none());
        assert!(parsed.created_at.is_none());
        assert!(parsed.updated_at.is_none());
        assert!(parsed.creator_id.is_none());
        assert!(parsed.updater_id.is_none());
    }

    #[test]
    fn merge_parsed_metadata_fills_missing_fields() {
        let mut metadata = DocumentMetadata {
            id: "doc-1".to_string(),
            workspace_id: "ws-1".to_string(),
            created_at: 0,
            updated_at: 0,
            default_role: "manager".to_string(),
            public: false,
            blocked: false,
            mode: "page".to_string(),
            title: None,
            summary: None,
            creator_id: None,
            updater_id: None,
            share_token: None,
            trashed_at: None,
            trashed_by: None,
            snapshot: None,
        };

        let parsed = ParsedDocMeta {
            title: Some("Title".to_string()),
            summary: Some("Summary".to_string()),
            created_at: Some(1_000),
            updated_at: Some(2_000),
            creator_id: Some("user-1".to_string()),
            updater_id: Some("user-2".to_string()),
        };

        DocumentStore::merge_parsed_metadata(&mut metadata, parsed);

        assert_eq!(metadata.title.as_deref(), Some("Title"));
        assert_eq!(metadata.summary.as_deref(), Some("Summary"));
        assert_eq!(metadata.creator_id.as_deref(), Some("user-1"));
        assert_eq!(metadata.updater_id.as_deref(), Some("user-2"));
        assert_eq!(metadata.created_at, 1_000);
        assert_eq!(metadata.updated_at, 2_000);
    }

    #[test]
    fn merge_parsed_metadata_preserves_existing_values() {
        let mut metadata = DocumentMetadata {
            id: "doc-2".to_string(),
            workspace_id: "ws-2".to_string(),
            created_at: 5_000,
            updated_at: 6_000,
            default_role: "manager".to_string(),
            public: true,
            blocked: false,
            mode: "page".to_string(),
            title: Some("Existing".to_string()),
            summary: Some("Present".to_string()),
            creator_id: Some("author".to_string()),
            updater_id: Some("editor".to_string()),
            share_token: Some("token".to_string()),
            trashed_at: None,
            trashed_by: None,
            snapshot: None,
        };

        let parsed = ParsedDocMeta {
            title: Some("New Title".to_string()),
            summary: Some("New Summary".to_string()),
            created_at: Some(4_000),
            updated_at: Some(7_000),
            creator_id: Some("other-author".to_string()),
            updater_id: Some("other-editor".to_string()),
        };

        DocumentStore::merge_parsed_metadata(&mut metadata, parsed);

        assert_eq!(metadata.title.as_deref(), Some("Existing"));
        assert_eq!(metadata.summary.as_deref(), Some("Present"));
        assert_eq!(metadata.creator_id.as_deref(), Some("author"));
        assert_eq!(metadata.updater_id.as_deref(), Some("editor"));
        assert_eq!(metadata.created_at, 4_000);
        assert_eq!(metadata.updated_at, 7_000);
        assert_eq!(metadata.share_token.as_deref(), Some("token"));
    }
}
