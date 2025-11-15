use std::sync::Arc;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use rocksdb::{Direction, IteratorMode, WriteBatch};
use serde::{Deserialize, Serialize};

use crate::{
    db::{
        doc_public_link_store::{DocPublicLinkRecord, DocPublicLinkStoreRef},
        doc_repo::{
            AppendDocUpdatesParams, CompactionApplyParams, CompactionSource, DocRepository,
            DocumentListOrder, DuplicateDocParams, HistorySnapshotInsert, InsertDocRecordParams,
            MetadataBackfillParams, ReplaceDocSnapshotParams, SnapshotUpsertParams,
        },
        rocks::{doc_data::DocDataStore, doc_update_log_store::RocksDocUpdateLogStore},
    },
    doc_store::{
        DocumentCursor, DocumentHistoryRecord, DocumentMetadata, DocumentSnapshot,
        UserShareTokenRecord,
    },
    doc_update_log::DocUpdateLogReader,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RocksDocumentRecord {
    metadata: DocumentMetadata,
    /// Base snapshot for the document. Incremental updates are not stored in
    /// Rocks-mode; snapshots are treated as authoritative.
    snapshot: Vec<u8>,
}

impl RocksDocumentRecord {
    fn new(
        workspace_id: String,
        doc_id: String,
        snapshot: Vec<u8>,
        owner_id: String,
        title: Option<String>,
        created_at: i64,
    ) -> Self {
        let metadata = DocumentMetadata {
            id: doc_id,
            workspace_id,
            created_at,
            updated_at: created_at,
            default_role: "manager".to_string(),
            public: false,
            blocked: false,
            mode: "page".to_string(),
            title,
            summary: None,
            creator_id: Some(owner_id.clone()),
            updater_id: Some(owner_id),
            share_token: None,
            trashed_at: None,
            trashed_by: None,
            snapshot: None,
        };

        Self { metadata, snapshot }
    }
}

pub struct RocksDocRepository {
    store: Arc<DocDataStore>,
    logs: Arc<RocksDocUpdateLogStore>,
    links: DocPublicLinkStoreRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RocksHistoryValue {
    created_at: i64,
    snapshot: Vec<u8>,
}

impl RocksDocRepository {
    pub fn new(
        store: Arc<DocDataStore>,
        logs: Arc<RocksDocUpdateLogStore>,
        links: DocPublicLinkStoreRef,
    ) -> Self {
        Self { store, logs, links }
    }

    fn doc_key(workspace_id: &str, doc_id: &str) -> String {
        format!("doc:{workspace_id}:{doc_id}")
    }

    fn decode_record(&self, bytes: &[u8]) -> Result<RocksDocumentRecord> {
        serde_json::from_slice(bytes)
            .with_context(|| "failed to decode Rocks document record from JSON")
    }

    fn encode_record(&self, record: &RocksDocumentRecord) -> Result<Vec<u8>> {
        serde_json::to_vec(record).with_context(|| "failed to encode Rocks document record as JSON")
    }

    fn get_record(&self, workspace_id: &str, doc_id: &str) -> Result<Option<RocksDocumentRecord>> {
        let key = Self::doc_key(workspace_id, doc_id);
        let value = self
            .store
            .db()
            .get_cf(self.store.doc_store_cf(), key.as_bytes())
            .with_context(|| format!("failed to read Rocks doc {key}"))?;
        Ok(match value {
            Some(bytes) => Some(self.decode_record(&bytes)?),
            None => None,
        })
    }

    fn put_record(&self, record: &RocksDocumentRecord) -> Result<()> {
        let key = Self::doc_key(&record.metadata.workspace_id, &record.metadata.id);
        let bytes = self.encode_record(record)?;
        self.store
            .db()
            .put_cf(self.store.doc_store_cf(), key.as_bytes(), &bytes)
            .with_context(|| format!("failed to write Rocks doc {key}"))
    }

    fn delete_record(&self, workspace_id: &str, doc_id: &str) -> Result<bool> {
        let key = Self::doc_key(workspace_id, doc_id);
        let db = self.store.db();
        let cf = self.store.doc_store_cf();
        let existed = db
            .get_cf(cf, key.as_bytes())
            .with_context(|| format!("failed to check existence of Rocks doc {key}"))?
            .is_some();
        if existed {
            db.delete_cf(cf, key.as_bytes())
                .with_context(|| format!("failed to delete Rocks doc {key}"))?;
        }
        Ok(existed)
    }

    fn list_workspace_records(&self, workspace_id: &str) -> Result<Vec<RocksDocumentRecord>> {
        let prefix = format!("doc:{workspace_id}:");
        let cf = self.store.doc_store_cf();
        let db = self.store.db();
        let mut records = Vec::new();
        let iter = db.iterator_cf(
            cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            let record = self.decode_record(&value)?;
            records.push(record);
        }
        Ok(records)
    }

    fn list_all_records(&self) -> Result<Vec<RocksDocumentRecord>> {
        let cf = self.store.doc_store_cf();
        let db = self.store.db();
        let mut records = Vec::new();
        let iter = db.iterator_cf(cf, IteratorMode::Start);
        for item in iter {
            let (_key, value) = item?;
            let record = self.decode_record(&value)?;
            records.push(record);
        }
        Ok(records)
    }

    fn delete_doc_history(&self, workspace_id: &str, doc_id: &str) -> Result<()> {
        let prefix = Self::history_prefix(workspace_id, doc_id);
        let cf = self.store.history_cf();
        let db = self.store.db();
        let mut batch = WriteBatch::default();
        let mut touched = false;
        let iter = db.iterator_cf(
            cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            batch.delete_cf(cf, &key);
            touched = true;
        }

        if touched {
            db.write(batch)
                .with_context(|| "failed to purge Rocks doc history entries")?;
        }

        Ok(())
    }

    fn history_key(workspace_id: &str, doc_id: &str, id: i64) -> String {
        format!("hist:{workspace_id}:{doc_id}:{id:016x}")
    }

    fn history_prefix(workspace_id: &str, doc_id: &str) -> String {
        format!("hist:{workspace_id}:{doc_id}:")
    }

    fn iter_doc_history(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<(i64, DocumentHistoryRecord)>> {
        let prefix = Self::history_prefix(workspace_id, doc_id);
        let cf = self.store.history_cf();
        let db = self.store.db();
        let mut records = Vec::new();
        let iter = db.iterator_cf(
            cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            let key_str = std::str::from_utf8(&key)?;
            let parts: Vec<&str> = key_str.rsplitn(2, ':').collect();
            if parts.len() != 2 {
                continue;
            }
            let id_str = parts[0];
            let id = i64::from_str_radix(id_str, 16)?;
            let decoded: RocksHistoryValue = serde_json::from_slice(&value)?;
            records.push((
                id,
                DocumentHistoryRecord {
                    id,
                    snapshot: decoded.snapshot,
                    created_at: decoded.created_at,
                },
            ));
        }
        Ok(records)
    }

    fn append_history_snapshot(
        &self,
        workspace_id: &str,
        doc_id: &str,
        snapshot: &[u8],
        created_at: i64,
    ) -> Result<i64> {
        let existing = self.iter_doc_history(workspace_id, doc_id)?;
        let next_id = existing
            .iter()
            .map(|(id, _)| *id)
            .max()
            .unwrap_or(0)
            .saturating_add(1);

        let value = RocksHistoryValue {
            created_at,
            snapshot: snapshot.to_vec(),
        };
        let encoded = serde_json::to_vec(&value)?;
        let key = Self::history_key(workspace_id, doc_id, next_id);
        let cf = self.store.history_cf();
        let db = self.store.db();
        db.put_cf(cf, key.as_bytes(), &encoded)?;
        Ok(next_id)
    }
}

#[async_trait]
impl DocRepository for RocksDocRepository {
    async fn fetch_snapshot_with_timestamp(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentSnapshot>> {
        let record = self.get_record(workspace_id, doc_id)?;
        Ok(record.map(|rec| DocumentSnapshot {
            snapshot: rec.snapshot,
            updated_at: rec.metadata.updated_at,
        }))
    }

    async fn insert_doc_record(&self, params: InsertDocRecordParams) -> Result<()> {
        let InsertDocRecordParams {
            workspace_id,
            doc_id,
            snapshot,
            owner_id,
            title,
            created_at,
        } = params;

        let record =
            RocksDocumentRecord::new(workspace_id, doc_id, snapshot, owner_id, title, created_at);
        self.put_record(&record)
    }

    async fn fetch_metadata(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentMetadata>> {
        Ok(self
            .get_record(workspace_id, doc_id)?
            .map(|rec| rec.metadata))
    }

    async fn backfill_metadata(&self, params: MetadataBackfillParams) -> Result<()> {
        let MetadataBackfillParams {
            workspace_id,
            doc_id,
            title,
            summary,
            creator_id,
            updater_id,
            created_at,
            updated_at,
        } = params;

        let mut record = match self.get_record(&workspace_id, &doc_id)? {
            Some(rec) => rec,
            None => return Ok(()),
        };

        record.metadata.title = title;
        record.metadata.summary = summary;
        record.metadata.creator_id = creator_id;
        record.metadata.updater_id = updater_id;
        record.metadata.created_at = created_at;
        record.metadata.updated_at = updated_at;

        self.put_record(&record)
    }

    async fn list_documents(
        &self,
        workspace_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentCursor>,
        order: DocumentListOrder,
    ) -> Result<Vec<DocumentMetadata>> {
        let mut records = self.list_workspace_records(workspace_id)?;
        // Filter out trashed documents from listings, matching SQL semantics.
        records.retain(|rec| rec.metadata.trashed_at.is_none());

        match order {
            DocumentListOrder::CreatedAsc => {
                records.sort_by_key(|rec| (rec.metadata.created_at, rec.metadata.id.clone()));
                if let Some(cursor) = cursor {
                    records.retain(|rec| {
                        rec.metadata.created_at > cursor.timestamp
                            || (rec.metadata.created_at == cursor.timestamp
                                && rec.metadata.id > cursor.id)
                    });
                }
            }
            DocumentListOrder::UpdatedDesc => {
                records.sort_by_key(|rec| {
                    (
                        std::cmp::Reverse(rec.metadata.updated_at),
                        std::cmp::Reverse(rec.metadata.id.clone()),
                    )
                });
                if let Some(cursor) = cursor {
                    records.retain(|rec| {
                        rec.metadata.updated_at < cursor.timestamp
                            || (rec.metadata.updated_at == cursor.timestamp
                                && rec.metadata.id < cursor.id)
                    });
                }
            }
        }

        let start = offset.max(0) as usize;
        let end = (start + limit.max(0) as usize).min(records.len());
        let slice = if start >= records.len() {
            &[]
        } else {
            &records[start..end]
        };

        Ok(slice
            .iter()
            .map(|rec| rec.metadata.clone())
            .collect::<Vec<_>>())
    }

    async fn set_doc_trashed(
        &self,
        workspace_id: &str,
        doc_id: &str,
        trashed_at: i64,
        trashed_by: &str,
    ) -> Result<u64> {
        let mut record = match self.get_record(workspace_id, doc_id)? {
            Some(rec) => rec,
            None => return Ok(0),
        };

        if record.metadata.trashed_at.is_some() {
            return Ok(0);
        }

        record.metadata.trashed_at = Some(trashed_at);
        record.metadata.trashed_by = Some(trashed_by.to_owned());
        record.metadata.updated_at = trashed_at;
        record.metadata.updater_id = Some(trashed_by.to_owned());
        self.put_record(&record)?;
        Ok(1)
    }

    async fn restore_doc_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        updated_at: i64,
        restored_by: &str,
    ) -> Result<u64> {
        let mut record = match self.get_record(workspace_id, doc_id)? {
            Some(rec) => rec,
            None => return Ok(0),
        };

        if record.metadata.trashed_at.is_none() {
            return Ok(0);
        }

        record.metadata.trashed_at = None;
        record.metadata.trashed_by = None;
        record.metadata.updated_at = updated_at;
        record.metadata.updater_id = Some(restored_by.to_owned());
        self.put_record(&record)?;
        Ok(1)
    }

    async fn delete_doc_entry(&self, workspace_id: &str, doc_id: &str) -> Result<u64> {
        let deleted = self.delete_record(workspace_id, doc_id)?;
        if deleted {
            // Ensure any lingering public link entries and history/log
            // records are removed now that the document has been deleted.
            self.links.delete_link(workspace_id, doc_id).await?;
            self.delete_doc_history(workspace_id, doc_id)?;
            self.logs
                .delete_doc_logs(crate::doc_store::SPACE_TYPE_WORKSPACE, workspace_id, doc_id)
                .await?;
        }
        Ok(if deleted { 1 } else { 0 })
    }

    async fn publish_doc_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        mode: &str,
        timestamp: i64,
        share_token: &str,
    ) -> Result<u64> {
        let mut record = match self.get_record(workspace_id, doc_id)? {
            Some(rec) => rec,
            None => return Ok(0),
        };

        if record.metadata.trashed_at.is_some() {
            return Ok(0);
        }

        record.metadata.public = true;
        record.metadata.mode = mode.to_owned();
        record.metadata.updated_at = timestamp;
        record.metadata.share_token = Some(share_token.to_owned());
        self.put_record(&record)?;

        // Maintain doc_public_links via the shared link store for
        // compatibility with SQL semantics and existing queries.
        self.links
            .insert_link(workspace_id, doc_id, share_token, timestamp)
            .await?;

        Ok(1)
    }

    async fn unpublish_doc_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        timestamp: i64,
    ) -> Result<u64> {
        let mut record = match self.get_record(workspace_id, doc_id)? {
            Some(rec) => rec,
            None => return Ok(0),
        };

        if record.metadata.trashed_at.is_some() || !record.metadata.public {
            return Ok(0);
        }

        record.metadata.public = false;
        record.metadata.share_token = None;
        record.metadata.updated_at = timestamp;
        self.put_record(&record)?;

        self.links.delete_link(workspace_id, doc_id).await?;

        Ok(1)
    }

    async fn update_default_role_entry(
        &self,
        workspace_id: &str,
        doc_id: &str,
        role: &str,
    ) -> Result<u64> {
        let mut record = match self.get_record(workspace_id, doc_id)? {
            Some(rec) => rec,
            None => return Ok(0),
        };

        if record.metadata.trashed_at.is_some() {
            return Ok(0);
        }

        record.metadata.default_role = role.to_owned();
        self.put_record(&record)?;
        Ok(1)
    }

    async fn update_doc_mode(&self, workspace_id: &str, doc_id: &str, mode: &str) -> Result<u64> {
        let mut record = match self.get_record(workspace_id, doc_id)? {
            Some(rec) => rec,
            None => return Ok(0),
        };

        if record.metadata.trashed_at.is_some() {
            return Ok(0);
        }

        record.metadata.mode = mode.to_owned();
        self.put_record(&record)?;
        Ok(1)
    }

    async fn count_documents(&self, workspace_id: &str) -> Result<i64> {
        let records = self.list_workspace_records(workspace_id)?;
        Ok(records
            .into_iter()
            .filter(|rec| rec.metadata.trashed_at.is_none())
            .count() as i64)
    }

    async fn list_share_tokens_for_user(&self, user_id: &str) -> Result<Vec<UserShareTokenRecord>> {
        let mut tokens = Vec::new();
        let rows: Vec<DocPublicLinkRecord> = self.links.list_links_for_user(user_id).await?;
        for row in rows {
            let DocPublicLinkRecord {
                workspace_id,
                doc_id,
                token,
                created_at,
            } = row;

            let rec = match self.get_record(&workspace_id, &doc_id)? {
                Some(rec) => rec,
                None => continue,
            };

            let meta = rec.metadata;
            if meta.public
                && meta.trashed_at.is_none()
                && meta.share_token.as_deref() == Some(token.as_str())
            {
                tokens.push(UserShareTokenRecord {
                    workspace_id,
                    doc_id,
                    token,
                    created_at,
                });
            }
        }

        // Keep ordering consistent with SQL implementation: newest token first,
        // then workspace/doc identifiers for tie-breaking.
        tokens.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.workspace_id.cmp(&b.workspace_id))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });

        Ok(tokens)
    }

    async fn is_doc_public(&self, workspace_id: &str, doc_id: &str) -> Result<bool> {
        let record = self.get_record(workspace_id, doc_id)?;
        Ok(match record {
            Some(rec) => rec.metadata.public && rec.metadata.trashed_at.is_none(),
            None => false,
        })
    }

    async fn list_public_docs(&self, workspace_id: &str) -> Result<Vec<DocumentMetadata>> {
        let mut records = self.list_workspace_records(workspace_id)?;
        records.retain(|rec| rec.metadata.public && rec.metadata.trashed_at.is_none());
        records.sort_by_key(|rec| (rec.metadata.created_at, rec.metadata.id.clone()));
        Ok(records
            .into_iter()
            .map(|rec| rec.metadata)
            .collect::<Vec<_>>())
    }

    async fn duplicate_doc_entry(
        &self,
        params: DuplicateDocParams,
    ) -> Result<Option<DocumentMetadata>> {
        let DuplicateDocParams {
            source_workspace_id,
            source_doc_id,
            target_workspace_id,
            new_doc_id,
            creator_id,
            title_override,
            now,
        } = params;

        let source = match self.get_record(&source_workspace_id, &source_doc_id)? {
            Some(rec) => rec,
            None => return Ok(None),
        };

        let mut new_meta = source.metadata.clone();
        new_meta.id = new_doc_id.clone();
        new_meta.workspace_id = target_workspace_id.clone();
        new_meta.created_at = now;
        new_meta.updated_at = now;
        new_meta.public = false;
        new_meta.share_token = None;
        new_meta.trashed_at = None;
        new_meta.trashed_by = None;
        new_meta.creator_id = Some(creator_id.clone());
        new_meta.updater_id = Some(creator_id.clone());

        if let Some(title) = title_override.as_deref().and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        }) {
            new_meta.title = Some(title);
        }

        let record = RocksDocumentRecord {
            metadata: new_meta.clone(),
            snapshot: source.snapshot.clone(),
        };
        self.put_record(&record)?;

        let mut returned = new_meta.clone();
        returned.snapshot = Some(source.snapshot);
        Ok(Some(returned))
    }

    async fn fetch_history_snapshot(
        &self,
        workspace_id: &str,
        doc_id: &str,
        history_id: i64,
    ) -> Result<Option<Vec<u8>>> {
        let records = self.iter_doc_history(workspace_id, doc_id)?;
        for (id, record) in records {
            if id == history_id {
                return Ok(Some(record.snapshot));
            }
        }
        Ok(None)
    }

    async fn find_history_id_before(
        &self,
        workspace_id: &str,
        doc_id: &str,
        timestamp: i64,
    ) -> Result<Option<i64>> {
        let records = self.iter_doc_history(workspace_id, doc_id)?;
        let mut candidate: Option<i64> = None;
        let mut candidate_ts: i64 = 0;
        for (_id, record) in records {
            if record.created_at <= timestamp {
                if candidate.is_none() || record.created_at > candidate_ts {
                    candidate = Some(record.id);
                    candidate_ts = record.created_at;
                }
            }
        }
        Ok(candidate)
    }

    async fn replace_doc_snapshot(&self, params: ReplaceDocSnapshotParams) -> Result<bool> {
        let ReplaceDocSnapshotParams {
            workspace_id,
            doc_id,
            snapshot,
            updated_at,
            title,
            summary,
            creator_id,
            updater_id,
            history_entry,
        } = params;

        let mut record = match self.get_record(&workspace_id, &doc_id)? {
            Some(rec) => rec,
            None => return Ok(false),
        };

        if let Some(HistorySnapshotInsert {
            snapshot: hist_snapshot,
            created_at,
        }) = history_entry
        {
            let _ =
                self.append_history_snapshot(&workspace_id, &doc_id, &hist_snapshot, created_at)?;
        }

        record.snapshot = snapshot;
        if let Some(title) = title {
            record.metadata.title = Some(title);
        }
        if let Some(summary) = summary {
            record.metadata.summary = Some(summary);
        }
        if let Some(creator_id) = creator_id {
            record.metadata.creator_id = Some(creator_id);
        }
        if let Some(updater_id) = updater_id {
            record.metadata.updater_id = Some(updater_id);
        }
        record.metadata.updated_at = updated_at;

        self.put_record(&record)?;
        Ok(true)
    }

    async fn docs_requiring_compaction(
        &self,
        threshold: i64,
        limit: i64,
    ) -> Result<Vec<(String, String)>> {
        self.logs
            .docs_requiring_compaction("workspace", threshold, limit)
            .await
    }

    async fn list_doc_timestamps(
        &self,
        workspace_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<(String, i64)>> {
        let records = self.list_workspace_records(workspace_id)?;
        Ok(records
            .into_iter()
            .filter(|rec| after.map_or(true, |threshold| rec.metadata.updated_at > threshold))
            .map(|rec| (rec.metadata.id, rec.metadata.updated_at))
            .collect())
    }

    async fn list_history_entries(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
    ) -> Result<Vec<DocumentHistoryRecord>> {
        let mut history = self
            .iter_doc_history(workspace_id, doc_id)?
            .into_iter()
            .map(|(_, record)| record)
            .collect::<Vec<_>>();

        // Order by created_at/id desc (newest first), then apply limit.
        history.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| b.id.cmp(&a.id))
        });
        if history.len() as i64 > limit {
            history.truncate(limit as usize);
        }

        Ok(history)
    }

    async fn upsert_snapshot_with_updates(&self, params: SnapshotUpsertParams) -> Result<()> {
        let SnapshotUpsertParams {
            workspace_id,
            doc_id,
            snapshot,
            created_at,
            updated_at,
            title,
            summary,
            creator_id,
            updater_id,
            default_role,
            mode,
            history_entry,
            new_document,
            doc_updates,
            log_editor_id,
            log_timestamp,
            log_limit,
        } = params;

        let mut record = if new_document {
            RocksDocumentRecord::new(
                workspace_id.clone(),
                doc_id.clone(),
                snapshot.clone(),
                creator_id.clone().unwrap_or_default(),
                title.clone(),
                created_at,
            )
        } else {
            match self.get_record(&workspace_id, &doc_id)? {
                Some(mut rec) => {
                    rec.snapshot = snapshot.clone();
                    rec
                }
                None => RocksDocumentRecord::new(
                    workspace_id.clone(),
                    doc_id.clone(),
                    snapshot.clone(),
                    creator_id.clone().unwrap_or_default(),
                    title.clone(),
                    created_at,
                ),
            }
        };

        if new_document {
            record.metadata.created_at = created_at;
        }
        record.metadata.updated_at = updated_at;
        record.metadata.title = title;
        record.metadata.summary = summary;
        if !default_role.is_empty() {
            record.metadata.default_role = default_role;
        }
        if !mode.is_empty() {
            record.metadata.mode = mode;
        }
        if let Some(creator_id) = creator_id {
            record.metadata.creator_id = Some(creator_id);
        }
        if let Some(updater_id) = updater_id {
            record.metadata.updater_id = Some(updater_id);
        }

        self.put_record(&record)?;

        if let Some(HistorySnapshotInsert {
            snapshot: hist_snapshot,
            created_at,
        }) = history_entry
        {
            let _ =
                self.append_history_snapshot(&workspace_id, &doc_id, &hist_snapshot, created_at)?;
        }

        if !doc_updates.is_empty() {
            self.logs
                .insert_updates(
                    crate::doc_store::SPACE_TYPE_WORKSPACE,
                    &workspace_id,
                    &doc_id,
                    &doc_updates,
                    log_editor_id.as_deref(),
                    log_timestamp,
                )
                .await?;
            self.logs
                .trim_updates(
                    crate::doc_store::SPACE_TYPE_WORKSPACE,
                    &workspace_id,
                    &doc_id,
                    log_limit,
                )
                .await?;
        }

        Ok(())
    }

    async fn append_doc_updates(&self, params: AppendDocUpdatesParams) -> Result<()> {
        let AppendDocUpdatesParams {
            workspace_id,
            doc_id,
            updates,
            editor_id,
            timestamp,
            log_limit,
        } = params;

        if updates.is_empty() {
            return Ok(());
        }

        let mut record = match self.get_record(&workspace_id, &doc_id)? {
            Some(rec) => rec,
            None => {
                bail!(
                    "document not found while appending updates: {}/{}",
                    workspace_id,
                    doc_id
                );
            }
        };

        record.metadata.updated_at = timestamp;
        if let Some(editor) = editor_id.as_deref() {
            record.metadata.updater_id = Some(editor.to_owned());
        }
        self.put_record(&record)?;

        self.logs
            .insert_updates(
                crate::doc_store::SPACE_TYPE_WORKSPACE,
                &workspace_id,
                &doc_id,
                &updates,
                editor_id.as_deref(),
                timestamp,
            )
            .await?;
        self.logs
            .trim_updates(
                crate::doc_store::SPACE_TYPE_WORKSPACE,
                &workspace_id,
                &doc_id,
                log_limit,
            )
            .await?;

        Ok(())
    }

    async fn load_compaction_source(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<CompactionSource>> {
        let record = match self.get_record(workspace_id, doc_id)? {
            Some(rec) => rec,
            None => return Ok(None),
        };

        let logs = self
            .logs
            .fetch_logs(crate::doc_store::SPACE_TYPE_WORKSPACE, workspace_id, doc_id)
            .await?;

        Ok(Some(CompactionSource {
            base_snapshot: record.snapshot,
            logs,
            doc_updated_at: record.metadata.updated_at,
        }))
    }

    async fn apply_compaction_result(&self, params: CompactionApplyParams) -> Result<()> {
        let CompactionApplyParams {
            workspace_id,
            doc_id,
            snapshot,
            updated_at,
            title,
            summary,
            creator_id,
            updater_id,
            last_log_id,
            expected_updated_at,
        } = params;

        let mut record = match self.get_record(&workspace_id, &doc_id)? {
            Some(rec) => rec,
            None => return Ok(()),
        };

        // Only apply the compaction result if the document has not been
        // concurrently updated.
        if record.metadata.updated_at != expected_updated_at {
            return Ok(());
        }

        record.snapshot = snapshot;
        if let Some(title) = title {
            record.metadata.title = Some(title);
        }
        if let Some(summary) = summary {
            record.metadata.summary = Some(summary);
        }
        if let Some(creator_id) = creator_id {
            record.metadata.creator_id = Some(creator_id);
        }
        if let Some(updater_id) = updater_id {
            record.metadata.updater_id = Some(updater_id);
        }
        record.metadata.updated_at = updated_at;

        self.put_record(&record)?;

        if let Some(last_id) = last_log_id {
            self.logs
                .delete_logs_up_to(
                    crate::doc_store::SPACE_TYPE_WORKSPACE,
                    &workspace_id,
                    &doc_id,
                    last_id,
                )
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::doc_repo::{
        AppendDocUpdatesParams, HistorySnapshotInsert, InsertDocRecordParams, SnapshotUpsertParams,
    };
    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::TempDir;

    #[tokio::test]
    async fn insert_and_fetch_roundtrip() {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        let logs = Arc::new(
            crate::db::rocks::doc_update_log_store::RocksDocUpdateLogStore::new(store.clone()),
        );
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        let links = Arc::new(
            crate::db::sqlite::doc_public_link_store::SqliteDocPublicLinkStore::new(pool.clone()),
        );
        let repo = RocksDocRepository::new(store, logs, links);

        let created_at = 1_700_000_000_000_i64;
        repo.insert_doc_record(InsertDocRecordParams {
            workspace_id: "ws".to_string(),
            doc_id: "doc".to_string(),
            snapshot: b"hello".to_vec(),
            owner_id: "user".to_string(),
            title: Some("Title".to_string()),
            created_at,
        })
        .await
        .unwrap();

        let snapshot = repo
            .fetch_snapshot_with_timestamp("ws", "doc")
            .await
            .unwrap()
            .expect("snapshot present");
        assert_eq!(snapshot.snapshot, b"hello".to_vec());
        assert_eq!(snapshot.updated_at, created_at);

        let meta = repo
            .fetch_metadata("ws", "doc")
            .await
            .unwrap()
            .expect("metadata present");
        assert_eq!(meta.id, "doc");
        assert_eq!(meta.workspace_id, "ws");
        assert_eq!(meta.title.as_deref(), Some("Title"));
        assert_eq!(meta.created_at, created_at);
        assert_eq!(meta.updated_at, created_at);
    }

    #[tokio::test]
    async fn upsert_and_append_write_rocks_logs() {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        let logs = Arc::new(
            crate::db::rocks::doc_update_log_store::RocksDocUpdateLogStore::new(store.clone()),
        );
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        let links = Arc::new(
            crate::db::sqlite::doc_public_link_store::SqliteDocPublicLinkStore::new(pool.clone()),
        );
        let repo = RocksDocRepository::new(store.clone(), logs.clone(), links);

        // 初始插入文档
        let created_at = 1_700_000_000_000_i64;
        let workspace_id = "ws".to_string();
        let doc_id = "doc".to_string();
        repo.insert_doc_record(InsertDocRecordParams {
            workspace_id: workspace_id.clone(),
            doc_id: doc_id.clone(),
            snapshot: b"base".to_vec(),
            owner_id: "user".to_string(),
            title: Some("Title".to_string()),
            created_at,
        })
        .await
        .unwrap();

        // 通过 upsert_snapshot_with_updates 写入一批更新
        let updates = vec![b"u1".to_vec(), b"u2".to_vec()];
        let log_ts = created_at + 1_000;
        repo.upsert_snapshot_with_updates(SnapshotUpsertParams {
            workspace_id: workspace_id.clone(),
            doc_id: doc_id.clone(),
            snapshot: b"after-upsert".to_vec(),
            created_at,
            updated_at: log_ts,
            title: None,
            summary: None,
            creator_id: None,
            updater_id: Some("user-1".to_string()),
            default_role: "manager".to_string(),
            mode: "page".to_string(),
            history_entry: None,
            new_document: false,
            doc_updates: updates.clone(),
            log_editor_id: Some("user-1".to_string()),
            log_timestamp: log_ts,
            log_limit: 10,
        })
        .await
        .unwrap();

        let all_logs = logs
            .fetch_logs(
                crate::doc_store::SPACE_TYPE_WORKSPACE,
                &workspace_id,
                &doc_id,
            )
            .await
            .unwrap();
        assert_eq!(all_logs.len(), 2);

        // append 再追加一批更新，并检查日志数量与 trim 行为
        repo.append_doc_updates(AppendDocUpdatesParams {
            workspace_id: workspace_id.clone(),
            doc_id: doc_id.clone(),
            updates: vec![b"u3".to_vec()],
            editor_id: Some("user-2".to_string()),
            timestamp: log_ts + 1_000,
            log_limit: 2,
        })
        .await
        .unwrap();

        let trimmed = logs
            .fetch_logs(
                crate::doc_store::SPACE_TYPE_WORKSPACE,
                &workspace_id,
                &doc_id,
            )
            .await
            .unwrap();
        // log_limit=2，应保留最新 2 条
        assert_eq!(trimmed.len(), 2);

        // 日志按 id 升序排列，最后一条应对应 append 写入的更新
        assert_eq!(trimmed[1].update, b"u3".to_vec());
    }

    #[tokio::test]
    async fn list_share_tokens_respects_membership_and_doc_state() {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        let logs = Arc::new(
            crate::db::rocks::doc_update_log_store::RocksDocUpdateLogStore::new(store.clone()),
        );
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        // 建立最小化 SQL schema，仅包含 list_share_tokens 所需表
        sqlx::query(
            "CREATE TABLE workspace_members (
                 workspace_id TEXT NOT NULL,
                 user_id TEXT NOT NULL,
                 status TEXT NOT NULL
             )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE doc_public_links (
                 workspace_id TEXT NOT NULL,
                 doc_id TEXT NOT NULL,
                 token TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 PRIMARY KEY (workspace_id, doc_id)
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        // user-1 是 ws1 的 Accepted 成员
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, status)
             VALUES (?, ?, 'Accepted')",
        )
        .bind("ws1")
        .bind("user-1")
        .execute(&pool)
        .await
        .unwrap();
        // user-1 不是 ws2 的成员
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, status)
             VALUES (?, ?, 'Pending')",
        )
        .bind("ws2")
        .bind("user-1")
        .execute(&pool)
        .await
        .unwrap();

        let links = Arc::new(
            crate::db::sqlite::doc_public_link_store::SqliteDocPublicLinkStore::new(pool.clone()),
        );
        let repo = RocksDocRepository::new(store, logs, links);

        let base_ts = 1_700_000_000_000_i64;

        // ws1/doc-ok: public=true, 未删除
        repo.insert_doc_record(InsertDocRecordParams {
            workspace_id: "ws1".to_string(),
            doc_id: "doc-ok".to_string(),
            snapshot: b"ok".to_vec(),
            owner_id: "owner".to_string(),
            title: Some("ok".to_string()),
            created_at: base_ts,
        })
        .await
        .unwrap();
        repo.publish_doc_entry("ws1", "doc-ok", "page", base_ts + 1_000, "token-ok")
            .await
            .unwrap();

        // ws1/doc-trashed: public=true 但已被删除
        repo.insert_doc_record(InsertDocRecordParams {
            workspace_id: "ws1".to_string(),
            doc_id: "doc-trashed".to_string(),
            snapshot: b"trash".to_vec(),
            owner_id: "owner".to_string(),
            title: Some("trash".to_string()),
            created_at: base_ts,
        })
        .await
        .unwrap();
        repo.publish_doc_entry("ws1", "doc-trashed", "page", base_ts + 2_000, "token-trash")
            .await
            .unwrap();
        repo.set_doc_trashed("ws1", "doc-trashed", base_ts + 3_000, "owner")
            .await
            .unwrap();

        // ws2/doc-other: public=true 且未删除，但 user-1 不是 Accepted 成员
        repo.insert_doc_record(InsertDocRecordParams {
            workspace_id: "ws2".to_string(),
            doc_id: "doc-other".to_string(),
            snapshot: b"other".to_vec(),
            owner_id: "owner".to_string(),
            title: Some("other".to_string()),
            created_at: base_ts,
        })
        .await
        .unwrap();
        repo.publish_doc_entry("ws2", "doc-other", "page", base_ts + 4_000, "token-other")
            .await
            .unwrap();

        let tokens = repo.list_share_tokens_for_user("user-1").await.unwrap();

        // 只应返回 ws1/doc-ok
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].workspace_id, "ws1");
        assert_eq!(tokens[0].doc_id, "doc-ok");
        assert_eq!(tokens[0].token, "token-ok");
    }

    #[tokio::test]
    async fn rocks_history_layer_persists_and_reads_snapshots() {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        let logs = Arc::new(
            crate::db::rocks::doc_update_log_store::RocksDocUpdateLogStore::new(store.clone()),
        );
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        let links = Arc::new(
            crate::db::sqlite::doc_public_link_store::SqliteDocPublicLinkStore::new(pool.clone()),
        );
        let repo = RocksDocRepository::new(store, logs, links);

        let workspace_id = "ws-hist".to_string();
        let doc_id = "doc-hist".to_string();
        let created_at = 1_700_000_000_000_i64;

        // 初始插入文档 snapshot=v1
        repo.insert_doc_record(InsertDocRecordParams {
            workspace_id: workspace_id.clone(),
            doc_id: doc_id.clone(),
            snapshot: b"v1".to_vec(),
            owner_id: "owner".to_string(),
            title: Some("Doc".to_string()),
            created_at,
        })
        .await
        .unwrap();

        // 通过 upsert 将 snapshot 更新为 v2，并把 v1 作为历史快照写入冷层
        let history_created_at = created_at + 500;
        repo.upsert_snapshot_with_updates(SnapshotUpsertParams {
            workspace_id: workspace_id.clone(),
            doc_id: doc_id.clone(),
            snapshot: b"v2".to_vec(),
            created_at,
            updated_at: created_at + 1_000,
            title: None,
            summary: None,
            creator_id: None,
            updater_id: Some("user-hist".to_string()),
            default_role: "manager".to_string(),
            mode: "page".to_string(),
            history_entry: Some(HistorySnapshotInsert {
                snapshot: b"v1".to_vec(),
                created_at: history_created_at,
            }),
            new_document: false,
            doc_updates: Vec::new(),
            log_editor_id: None,
            log_timestamp: created_at + 1_000,
            log_limit: 10,
        })
        .await
        .unwrap();

        // list_history_entries 应该能看到一条历史，snapshot=v1，created_at=history_created_at
        let history = repo
            .list_history_entries(&workspace_id, &doc_id, 10)
            .await
            .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].snapshot, b"v1".to_vec());
        assert_eq!(history[0].created_at, history_created_at);

        // find_history_id_before(timestamp) 应该返回这条记录的 id
        let cutoff_ts = history_created_at + 1;
        let maybe_id = repo
            .find_history_id_before(&workspace_id, &doc_id, cutoff_ts)
            .await
            .unwrap();
        let history_id = maybe_id.expect("history id present");

        // fetch_history_snapshot 应能拿到同样的 snapshot
        let snapshot = repo
            .fetch_history_snapshot(&workspace_id, &doc_id, history_id)
            .await
            .unwrap()
            .expect("history snapshot present");
        assert_eq!(snapshot, b"v1".to_vec());
    }

    #[tokio::test]
    async fn delete_doc_cleans_history_entries() {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        let logs = Arc::new(
            crate::db::rocks::doc_update_log_store::RocksDocUpdateLogStore::new(store.clone()),
        );
        // Use a no-op public link store; this test focuses on history/log
        // cleanup rather than SQL doc_public_links behavior.
        use crate::db::doc_public_link_store::{DocPublicLinkRecord, DocPublicLinkStore};
        use async_trait::async_trait;

        struct NoopPublicLinkStore;

        #[async_trait]
        impl DocPublicLinkStore for NoopPublicLinkStore {
            async fn insert_link(
                &self,
                _workspace_id: &str,
                _doc_id: &str,
                _token: &str,
                _created_at: i64,
            ) -> anyhow::Result<()> {
                Ok(())
            }

            async fn delete_link(&self, _workspace_id: &str, _doc_id: &str) -> anyhow::Result<()> {
                Ok(())
            }

            async fn list_links_for_user(
                &self,
                _user_id: &str,
            ) -> anyhow::Result<Vec<DocPublicLinkRecord>> {
                Ok(Vec::new())
            }
        }

        let repo = RocksDocRepository::new(store, logs, Arc::new(NoopPublicLinkStore));

        repo.insert_doc_record(InsertDocRecordParams {
            workspace_id: "ws-del".to_string(),
            doc_id: "doc-del".to_string(),
            snapshot: b"base".to_vec(),
            owner_id: "owner".to_string(),
            title: Some("Doc".to_string()),
            created_at: 1,
        })
        .await
        .unwrap();

        repo.upsert_snapshot_with_updates(SnapshotUpsertParams {
            workspace_id: "ws-del".to_string(),
            doc_id: "doc-del".to_string(),
            snapshot: b"v2".to_vec(),
            created_at: 1,
            updated_at: 2,
            title: None,
            summary: None,
            creator_id: None,
            updater_id: Some("user".to_string()),
            default_role: "manager".to_string(),
            mode: "page".to_string(),
            history_entry: Some(HistorySnapshotInsert {
                snapshot: b"base".to_vec(),
                created_at: 3,
            }),
            new_document: false,
            doc_updates: Vec::new(),
            log_editor_id: None,
            log_timestamp: 2,
            log_limit: 10,
        })
        .await
        .unwrap();

        let history = repo
            .list_history_entries("ws-del", "doc-del", 10)
            .await
            .unwrap();
        assert_eq!(
            history.len(),
            1,
            "history snapshot should exist before delete"
        );

        let deleted = repo.delete_doc_entry("ws-del", "doc-del").await.unwrap();
        assert_eq!(deleted, 1);

        let history_after = repo
            .list_history_entries("ws-del", "doc-del", 10)
            .await
            .unwrap();
        assert!(
            history_after.is_empty(),
            "history entries should be removed"
        );
    }
}
