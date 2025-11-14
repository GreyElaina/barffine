use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;

use crate::{
    db::{
        Database,
        user_doc_repo::{
            UserDocAppendUpdatesParams, UserDocCompactionApplyParams, UserDocCompactionSource,
            UserDocRepositoryRef, UserDocSnapshotUpsertParams,
        },
    },
    doc::DocEngine,
    doc_store::{DOC_UPDATE_LOG_LIMIT, SnapshotComputation, build_snapshot_from_updates},
    doc_update_log::{DocUpdateLogReaderRef, DocUpdateRecord},
};

pub const SPACE_TYPE_USER: &str = "userspace";

#[derive(Debug, Clone)]
pub struct UserDocumentSnapshot {
    pub snapshot: Vec<u8>,
    pub updated_at: i64,
    pub editor_id: Option<String>,
}

#[derive(Clone)]
pub struct UserDocStore {
    repo: UserDocRepositoryRef,
    doc_logs: DocUpdateLogReaderRef,
}

impl UserDocStore {
    pub fn new(database: &Database) -> Self {
        Self {
            repo: database.repositories().user_doc_repo(),
            doc_logs: database.doc_update_logs(),
        }
    }

    fn now_millis() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    async fn fetch_raw_snapshot(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Option<UserDocumentSnapshot>> {
        self.repo
            .fetch_snapshot_with_timestamp(user_id, doc_id)
            .await
    }

    pub async fn fetch_snapshot_with_timestamp(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Option<UserDocumentSnapshot>> {
        let Some(mut base_snapshot) = self.fetch_raw_snapshot(user_id, doc_id).await? else {
            return Ok(None);
        };

        let logs = self.fetch_doc_logs(user_id, doc_id).await?;
        if !logs.is_empty() {
            let log_updates = logs
                .iter()
                .map(|log| log.update.clone())
                .collect::<Vec<_>>();
            base_snapshot.snapshot =
                DocEngine::apply_updates_to_snapshot(Some(&base_snapshot.snapshot), &log_updates)
                    .context("apply userspace doc logs to snapshot")?;
            if let Some(last) = logs.last() {
                base_snapshot.updated_at = base_snapshot.updated_at.max(last.created_at);
            }
        }

        Ok(Some(base_snapshot))
    }

    pub async fn ensure_doc_record(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<UserDocumentSnapshot> {
        if let Some(snapshot) = self.fetch_snapshot_with_timestamp(user_id, doc_id).await? {
            return Ok(snapshot);
        }

        let snapshot = DocEngine::new().snapshot()?;
        let now = Self::now_millis();
        self.repo
            .upsert_snapshot_with_updates(UserDocSnapshotUpsertParams {
                user_id: user_id.to_owned(),
                doc_id: doc_id.to_owned(),
                snapshot: snapshot.clone(),
                editor_id: None,
                created_at: now,
                updated_at: now,
                new_document: true,
                doc_updates: Vec::new(),
                log_editor_id: None,
                log_timestamp: now,
                log_limit: DOC_UPDATE_LOG_LIMIT,
            })
            .await?;

        self.fetch_snapshot_with_timestamp(user_id, doc_id)
            .await?
            .ok_or_else(|| anyhow!("user doc inserted but not found"))
    }

    pub async fn apply_updates(
        &self,
        user_id: &str,
        doc_id: &str,
        updates: &[Vec<u8>],
        editor_id: Option<&str>,
    ) -> Result<UserDocumentSnapshot> {
        if updates.is_empty() {
            return Err(anyhow!("updates payload must not be empty"));
        }

        let existing_snapshot = self.fetch_raw_snapshot(user_id, doc_id).await?;
        let existing_editor = existing_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.editor_id.clone());
        let existing_snapshot_ref = existing_snapshot
            .as_ref()
            .map(|snapshot| snapshot.snapshot.as_slice());

        let SnapshotComputation {
            snapshot: new_snapshot,
            created_at,
            updated_at,
            ..
        } = build_snapshot_from_updates(doc_id, existing_snapshot_ref, updates, Self::now_millis)?;

        self.repo
            .upsert_snapshot_with_updates(UserDocSnapshotUpsertParams {
                user_id: user_id.to_owned(),
                doc_id: doc_id.to_owned(),
                snapshot: new_snapshot.clone(),
                editor_id: editor_id.map(str::to_owned),
                created_at,
                updated_at,
                new_document: existing_snapshot.is_none(),
                doc_updates: updates.to_vec(),
                log_editor_id: editor_id.map(str::to_owned),
                log_timestamp: updated_at,
                log_limit: DOC_UPDATE_LOG_LIMIT,
            })
            .await?;

        Ok(UserDocumentSnapshot {
            snapshot: new_snapshot,
            updated_at,
            editor_id: editor_id.map(str::to_owned).or(existing_editor),
        })
    }

    pub async fn append_doc_updates(
        &self,
        user_id: &str,
        doc_id: &str,
        updates: &[Vec<u8>],
        editor_id: Option<&str>,
        timestamp: i64,
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        self.repo
            .append_doc_updates(UserDocAppendUpdatesParams {
                user_id: user_id.to_owned(),
                doc_id: doc_id.to_owned(),
                updates: updates.to_vec(),
                editor_id: editor_id.map(str::to_owned),
                timestamp,
                log_limit: DOC_UPDATE_LOG_LIMIT,
            })
            .await
    }

    pub async fn compact_doc(&self, user_id: &str, doc_id: &str) -> Result<()> {
        let Some(source) = self.repo.load_compaction_source(user_id, doc_id).await? else {
            return Ok(());
        };

        if source.logs.is_empty() {
            return Ok(());
        }

        let UserDocCompactionSource {
            base_snapshot,
            logs,
            doc_updated_at: _,
        } = source;

        let updates = logs
            .iter()
            .map(|log| log.update.clone())
            .collect::<Vec<_>>();
        let SnapshotComputation {
            snapshot: merged_snapshot,
            meta,
            mut updated_at,
            ..
        } = build_snapshot_from_updates(doc_id, Some(&base_snapshot), &updates, Self::now_millis)
            .context("merge userspace doc updates during compaction")?;
        if meta.updated_at.is_none() {
            if let Some(last) = logs.last() {
                updated_at = last.created_at;
            }
        }

        self.repo
            .apply_compaction_result(UserDocCompactionApplyParams {
                user_id: user_id.to_owned(),
                doc_id: doc_id.to_owned(),
                snapshot: merged_snapshot,
                updated_at,
                editor_id: meta.updater_id,
                last_log_id: logs.last().map(|log| log.id),
            })
            .await
    }

    pub async fn delete_doc(&self, user_id: &str, doc_id: &str) -> Result<bool> {
        self.repo.delete_doc(user_id, doc_id).await
    }

    pub async fn timestamps_since(
        &self,
        user_id: &str,
        after: Option<i64>,
    ) -> Result<HashMap<String, i64>> {
        let entries = self.repo.timestamps_since(user_id, after).await?;
        let mut result = HashMap::with_capacity(entries.len());
        for (doc_id, updated_at) in entries {
            result.insert(doc_id, updated_at);
        }
        Ok(result)
    }

    pub async fn fetch_doc_logs(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocUpdateRecord>> {
        self.doc_logs
            .fetch_logs(SPACE_TYPE_USER, user_id, doc_id)
            .await
    }

    pub async fn docs_requiring_compaction(
        &self,
        threshold: i64,
        limit: i64,
    ) -> Result<Vec<(String, String)>> {
        self.repo.docs_requiring_compaction(threshold, limit).await
    }
}
