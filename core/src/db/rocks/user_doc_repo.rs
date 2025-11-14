use std::sync::Arc;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use rocksdb::{Direction, IteratorMode};
use serde::{Deserialize, Serialize};

use crate::{
    db::rocks::{doc_data::DocDataStore, doc_update_log_store::RocksDocUpdateLogStore},
    db::user_doc_repo::{
        UserDocAppendUpdatesParams, UserDocCompactionApplyParams, UserDocCompactionSource,
        UserDocRepository, UserDocSnapshotUpsertParams,
    },
    doc_update_log::DocUpdateLogReader,
    user_doc_store::UserDocumentSnapshot,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RocksUserDocRecord {
    user_id: String,
    doc_id: String,
    snapshot: Vec<u8>,
    created_at: i64,
    updated_at: i64,
    editor_id: Option<String>,
}

impl RocksUserDocRecord {
    fn new(
        user_id: String,
        doc_id: String,
        snapshot: Vec<u8>,
        created_at: i64,
        updated_at: i64,
        editor_id: Option<String>,
    ) -> Self {
        Self {
            user_id,
            doc_id,
            snapshot,
            created_at,
            updated_at,
            editor_id,
        }
    }
}

pub struct RocksUserDocRepository {
    store: Arc<DocDataStore>,
    logs: Arc<RocksDocUpdateLogStore>,
}

impl RocksUserDocRepository {
    pub fn new(store: Arc<DocDataStore>, logs: Arc<RocksDocUpdateLogStore>) -> Self {
        Self { store, logs }
    }

    fn key(user_id: &str, doc_id: &str) -> String {
        format!("user:{user_id}:{doc_id}")
    }

    fn decode_record(&self, bytes: &[u8]) -> Result<RocksUserDocRecord> {
        serde_json::from_slice(bytes)
            .with_context(|| "failed to decode Rocks user doc record from JSON")
    }

    fn encode_record(&self, record: &RocksUserDocRecord) -> Result<Vec<u8>> {
        serde_json::to_vec(record).with_context(|| "failed to encode Rocks user doc record as JSON")
    }

    fn get_record(&self, user_id: &str, doc_id: &str) -> Result<Option<RocksUserDocRecord>> {
        let key = Self::key(user_id, doc_id);
        let value = self
            .store
            .db()
            .get_cf(self.store.doc_store_cf(), key.as_bytes())
            .with_context(|| format!("failed to read Rocks user doc {key}"))?;
        Ok(match value {
            Some(bytes) => Some(self.decode_record(&bytes)?),
            None => None,
        })
    }

    fn put_record(&self, record: &RocksUserDocRecord) -> Result<()> {
        let key = Self::key(&record.user_id, &record.doc_id);
        let bytes = self.encode_record(record)?;
        self.store
            .db()
            .put_cf(self.store.doc_store_cf(), key.as_bytes(), &bytes)
            .with_context(|| format!("failed to write Rocks user doc {key}"))
    }

    fn delete_record(&self, user_id: &str, doc_id: &str) -> Result<bool> {
        let key = Self::key(user_id, doc_id);
        let db = self.store.db();
        let cf = self.store.doc_store_cf();
        let existed = db
            .get_cf(cf, key.as_bytes())
            .with_context(|| format!("failed to check existence of Rocks user doc {key}"))?
            .is_some();
        if existed {
            db.delete_cf(cf, key.as_bytes())
                .with_context(|| format!("failed to delete Rocks user doc {key}"))?;
        }
        Ok(existed)
    }

    fn list_user_records(&self, user_id: &str) -> Result<Vec<RocksUserDocRecord>> {
        let prefix = format!("user:{user_id}:");
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
}

#[async_trait]
impl UserDocRepository for RocksUserDocRepository {
    async fn fetch_snapshot_with_timestamp(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Option<UserDocumentSnapshot>> {
        let record = self.get_record(user_id, doc_id)?;
        Ok(record.map(|rec| UserDocumentSnapshot {
            snapshot: rec.snapshot,
            updated_at: rec.updated_at,
            editor_id: rec.editor_id,
        }))
    }

    async fn upsert_snapshot_with_updates(
        &self,
        params: UserDocSnapshotUpsertParams,
    ) -> Result<()> {
        let UserDocSnapshotUpsertParams {
            user_id,
            doc_id,
            snapshot,
            editor_id,
            created_at,
            updated_at,
            new_document,
            doc_updates,
            log_editor_id,
            log_timestamp,
            log_limit,
        } = params;

        let mut record = if new_document {
            RocksUserDocRecord::new(
                user_id.clone(),
                doc_id.clone(),
                snapshot.clone(),
                created_at,
                updated_at,
                editor_id.clone(),
            )
        } else {
            match self.get_record(&user_id, &doc_id)? {
                Some(mut rec) => {
                    rec.snapshot = snapshot.clone();
                    rec
                }
                None => RocksUserDocRecord::new(
                    user_id.clone(),
                    doc_id.clone(),
                    snapshot.clone(),
                    created_at,
                    updated_at,
                    editor_id.clone(),
                ),
            }
        };

        if new_document {
            record.created_at = created_at;
        }
        record.updated_at = updated_at;
        if editor_id.is_some() {
            record.editor_id = editor_id;
        }

        self.put_record(&record)?;

        if !doc_updates.is_empty() {
            self.logs
                .insert_updates(
                    crate::user_doc_store::SPACE_TYPE_USER,
                    &user_id,
                    &doc_id,
                    &doc_updates,
                    log_editor_id.as_deref(),
                    log_timestamp,
                )
                .await?;
            self.logs
                .trim_updates(
                    crate::user_doc_store::SPACE_TYPE_USER,
                    &user_id,
                    &doc_id,
                    log_limit,
                )
                .await?;
        }

        Ok(())
    }

    async fn append_doc_updates(&self, params: UserDocAppendUpdatesParams) -> Result<()> {
        let UserDocAppendUpdatesParams {
            user_id,
            doc_id,
            updates,
            editor_id,
            timestamp,
            log_limit,
        } = params;

        if updates.is_empty() {
            return Ok(());
        }

        let mut record = match self.get_record(&user_id, &doc_id)? {
            Some(rec) => rec,
            None => {
                bail!(
                    "user document not found while appending updates: {}/{}",
                    user_id,
                    doc_id
                );
            }
        };

        record.updated_at = timestamp;
        if let Some(editor) = editor_id.as_deref() {
            record.editor_id = Some(editor.to_owned());
        }
        self.put_record(&record)?;

        self.logs
            .insert_updates(
                crate::user_doc_store::SPACE_TYPE_USER,
                &user_id,
                &doc_id,
                &updates,
                editor_id.as_deref(),
                timestamp,
            )
            .await?;
        self.logs
            .trim_updates(
                crate::user_doc_store::SPACE_TYPE_USER,
                &user_id,
                &doc_id,
                log_limit,
            )
            .await?;

        Ok(())
    }

    async fn load_compaction_source(
        &self,
        user_id: &str,
        doc_id: &str,
    ) -> Result<Option<UserDocCompactionSource>> {
        let record = match self.get_record(user_id, doc_id)? {
            Some(rec) => rec,
            None => return Ok(None),
        };

        let logs = self
            .logs
            .fetch_logs(crate::user_doc_store::SPACE_TYPE_USER, user_id, doc_id)
            .await?;

        Ok(Some(UserDocCompactionSource {
            base_snapshot: record.snapshot,
            logs,
            doc_updated_at: record.updated_at,
        }))
    }

    async fn apply_compaction_result(&self, params: UserDocCompactionApplyParams) -> Result<()> {
        let UserDocCompactionApplyParams {
            user_id,
            doc_id,
            snapshot,
            updated_at,
            editor_id,
            last_log_id,
        } = params;

        let mut record = match self.get_record(&user_id, &doc_id)? {
            Some(rec) => rec,
            None => return Ok(()),
        };

        record.snapshot = snapshot;
        record.updated_at = updated_at;
        if let Some(editor) = editor_id {
            record.editor_id = Some(editor);
        }
        self.put_record(&record)?;

        if let Some(last_id) = last_log_id {
            self.logs
                .delete_logs_up_to(
                    crate::user_doc_store::SPACE_TYPE_USER,
                    &user_id,
                    &doc_id,
                    last_id,
                )
                .await?;
        } else {
            self.logs
                .delete_doc_logs(crate::user_doc_store::SPACE_TYPE_USER, &user_id, &doc_id)
                .await?;
        }

        Ok(())
    }

    async fn delete_doc(&self, user_id: &str, doc_id: &str) -> Result<bool> {
        self.delete_record(user_id, doc_id)
    }

    async fn timestamps_since(
        &self,
        user_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<(String, i64)>> {
        let records = self.list_user_records(user_id)?;
        Ok(records
            .into_iter()
            .filter(|rec| after.map_or(true, |ts| rec.updated_at > ts))
            .map(|rec| (rec.doc_id, rec.updated_at))
            .collect())
    }

    async fn docs_requiring_compaction(
        &self,
        threshold: i64,
        limit: i64,
    ) -> Result<Vec<(String, String)>> {
        self.logs
            .docs_requiring_compaction(crate::user_doc_store::SPACE_TYPE_USER, threshold, limit)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::user_doc_repo::{UserDocAppendUpdatesParams, UserDocSnapshotUpsertParams};
    use tempfile::TempDir;

    #[tokio::test]
    async fn insert_and_fetch_userspace_roundtrip() {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        let logs = Arc::new(
            crate::db::rocks::doc_update_log_store::RocksDocUpdateLogStore::new(store.clone()),
        );
        let repo = RocksUserDocRepository::new(store, logs);

        let created_at = 1_700_000_000_000_i64;
        let updated_at = created_at + 1_000;
        repo.upsert_snapshot_with_updates(UserDocSnapshotUpsertParams {
            user_id: "user".to_string(),
            doc_id: "doc".to_string(),
            snapshot: b"usersnap".to_vec(),
            editor_id: Some("user".to_string()),
            created_at,
            updated_at,
            new_document: true,
            doc_updates: Vec::new(),
            log_editor_id: None,
            log_timestamp: updated_at,
            log_limit: 0,
        })
        .await
        .unwrap();

        let snapshot = repo
            .fetch_snapshot_with_timestamp("user", "doc")
            .await
            .unwrap()
            .expect("userspace snapshot present");
        assert_eq!(snapshot.snapshot, b"usersnap".to_vec());
        assert_eq!(snapshot.updated_at, updated_at);
        assert_eq!(snapshot.editor_id.as_deref(), Some("user"));
    }

    #[tokio::test]
    async fn upsert_and_append_userspace_write_logs() {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        let logs = Arc::new(
            crate::db::rocks::doc_update_log_store::RocksDocUpdateLogStore::new(store.clone()),
        );
        let repo = RocksUserDocRepository::new(store.clone(), logs.clone());

        let created_at = 1_700_000_000_000_i64;
        let updated_at = created_at + 1_000;
        let user_id = "user".to_string();
        let doc_id = "doc".to_string();
        repo.upsert_snapshot_with_updates(UserDocSnapshotUpsertParams {
            user_id: user_id.clone(),
            doc_id: doc_id.clone(),
            snapshot: b"usersnap".to_vec(),
            editor_id: Some("user".to_string()),
            created_at,
            updated_at,
            new_document: true,
            doc_updates: vec![b"u1".to_vec()],
            log_editor_id: Some("user".to_string()),
            log_timestamp: updated_at,
            log_limit: 10,
        })
        .await
        .unwrap();

        let all_logs = logs
            .fetch_logs(crate::user_doc_store::SPACE_TYPE_USER, &user_id, &doc_id)
            .await
            .unwrap();
        assert_eq!(all_logs.len(), 1);

        repo.append_doc_updates(UserDocAppendUpdatesParams {
            user_id: user_id.clone(),
            doc_id: doc_id.clone(),
            updates: vec![b"u2".to_vec(), b"u3".to_vec()],
            editor_id: Some("user-2".to_string()),
            timestamp: updated_at + 1_000,
            log_limit: 2,
        })
        .await
        .unwrap();

        let trimmed = logs
            .fetch_logs(crate::user_doc_store::SPACE_TYPE_USER, &user_id, &doc_id)
            .await
            .unwrap();
        assert_eq!(trimmed.len(), 2);
    }
}
