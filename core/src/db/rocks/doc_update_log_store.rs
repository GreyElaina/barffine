use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use rocksdb::{Direction, IteratorMode};
use serde::{Deserialize, Serialize};

use crate::{
    db::rocks::doc_data::DocDataStore,
    doc_update_log::{DocUpdateLogReader, DocUpdateRecord},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RocksLogRecord {
    id: i64,
    created_at: i64,
    editor_id: Option<String>,
    update: Vec<u8>,
}

pub struct RocksDocUpdateLogStore {
    store: Arc<DocDataStore>,
}

impl RocksDocUpdateLogStore {
    pub fn new(store: Arc<DocDataStore>) -> Self {
        Self { store }
    }

    /// KV key used to persist the per-doc monotonically increasing log
    /// sequence counter. This lives in the same column family as the
    /// log records themselves.
    fn counter_key(space_type: &str, space_id: &str, doc_id: &str) -> String {
        format!("logseq:{space_type}:{space_id}:{doc_id}")
    }

    fn key(space_type: &str, space_id: &str, doc_id: &str, id: i64) -> String {
        format!("log:{space_type}:{space_id}:{doc_id}:{id:016x}")
    }

    fn prefix(space_type: &str, space_id: &str, doc_id: &str) -> String {
        format!("log:{space_type}:{space_id}:{doc_id}:")
    }

    fn space_prefix(space_type: &str) -> String {
        format!("log:{space_type}:")
    }

    fn encode_record(&self, record: &RocksLogRecord) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(record)?)
    }

    fn decode_record(&self, bytes: &[u8]) -> Result<RocksLogRecord> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn iter_doc_logs(
        &self,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<Vec<(Vec<u8>, RocksLogRecord)>> {
        let prefix = Self::prefix(space_type, space_id, doc_id);
        let cf = self.store.logs_cf();
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
            records.push((key.to_vec(), record));
        }
        Ok(records)
    }

    /// Allocate a batch of strictly increasing log IDs for the given
    /// document, backed by a small counter stored in RocksDB.
    ///
    /// The counter is kept in the same column family as the logs using
    /// the `logseq:` prefix. On first use (when the counter key is
    /// missing) we scan existing log records for this doc to derive the
    /// current maximum ID, so we stay monotonic across upgrades.
    fn allocate_ids(
        &self,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
        count: usize,
    ) -> Result<Vec<i64>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let cf = self.store.logs_cf();
        let db = self.store.db();
        let counter_key = Self::counter_key(space_type, space_id, doc_id);

        // Load current counter value, if any.
        let current_bytes = db.get_cf(cf, counter_key.as_bytes())?;
        let mut last_id: i64 = if let Some(bytes) = current_bytes {
            if bytes.len() == 8 {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes[..8]);
                i64::from_be_bytes(buf)
            } else {
                0
            }
        } else {
            // No counter yet – derive max existing ID from current logs
            // so that we don't regress after an upgrade.
            let records = self.iter_doc_logs(space_type, space_id, doc_id)?;
            records
                .iter()
                .map(|(_, record)| record.id)
                .max()
                .unwrap_or(0)
        };

        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            last_id = last_id.saturating_add(1);
            ids.push(last_id);
        }

        let encoded = last_id.to_be_bytes();
        db.put_cf(cf, counter_key.as_bytes(), &encoded)?;

        Ok(ids)
    }

    pub async fn insert_updates(
        &self,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
        updates: &[Vec<u8>],
        editor_id: Option<&str>,
        timestamp: i64,
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let cf = self.store.logs_cf();
        let db = self.store.db();
        let ids = self.allocate_ids(space_type, space_id, doc_id, updates.len())?;

        for (id, update) in ids.into_iter().zip(updates.iter()) {
            let record = RocksLogRecord {
                id,
                created_at: timestamp,
                editor_id: editor_id.map(|s| s.to_owned()),
                update: update.clone(),
            };
            let key = Self::key(space_type, space_id, doc_id, id);
            let bytes = self.encode_record(&record)?;
            db.put_cf(cf, key.as_bytes(), &bytes)?;
        }
        Ok(())
    }

    pub async fn trim_updates(
        &self,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
        limit: i64,
    ) -> Result<()> {
        let mut records = self.iter_doc_logs(space_type, space_id, doc_id)?;
        let count = records.len() as i64;
        if count <= limit {
            return Ok(());
        }

        let delete_count = (count - limit) as usize;
        records.sort_by_key(|(_, record)| record.id);
        let cf = self.store.logs_cf();
        let db = self.store.db();
        for (key, _) in records.into_iter().take(delete_count) {
            db.delete_cf(cf, &key)?;
        }
        Ok(())
    }

    pub async fn delete_doc_logs(
        &self,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<()> {
        let records = self.iter_doc_logs(space_type, space_id, doc_id)?;
        let cf = self.store.logs_cf();
        let db = self.store.db();
        for (key, _) in records {
            db.delete_cf(cf, &key)?;
        }
        Ok(())
    }

    pub async fn delete_logs_up_to(
        &self,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
        last_id: i64,
    ) -> Result<()> {
        let records = self.iter_doc_logs(space_type, space_id, doc_id)?;
        let cf = self.store.logs_cf();
        let db = self.store.db();
        for (key, record) in records {
            if record.id <= last_id {
                db.delete_cf(cf, &key)?;
            }
        }
        Ok(())
    }

    pub async fn docs_requiring_compaction(
        &self,
        space_type: &str,
        threshold: i64,
        limit: i64,
    ) -> Result<Vec<(String, String)>> {
        use std::collections::HashMap;

        let prefix = Self::space_prefix(space_type);
        let cf = self.store.logs_cf();
        let db = self.store.db();
        let mut counts: HashMap<(String, String), i64> = HashMap::new();
        let iter = db.iterator_cf(
            cf,
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }

            let key_str = match std::str::from_utf8(&key) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let parts: Vec<&str> = key_str.split(':').collect();
            if parts.len() < 5 {
                continue;
            }
            let space = parts[1];
            if space != space_type {
                continue;
            }
            let space_id = parts[2].to_owned();
            let doc_id = parts[3].to_owned();
            let entry = counts.entry((space_id, doc_id)).or_insert(0);
            *entry += 1;
        }

        let mut docs: Vec<(String, String)> = counts
            .into_iter()
            .filter_map(|((space_id, doc_id), count)| {
                if count > threshold {
                    Some((space_id, doc_id))
                } else {
                    None
                }
            })
            .collect();

        // Keep deterministic order: by space_id/doc_id, then truncate to limit.
        docs.sort_by(|a, b| a.cmp(b));
        if docs.len() as i64 > limit {
            docs.truncate(limit as usize);
        }

        Ok(docs)
    }
}

#[async_trait]
impl DocUpdateLogReader for RocksDocUpdateLogStore {
    async fn fetch_logs(
        &self,
        space_type: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocUpdateRecord>> {
        let mut records = self.iter_doc_logs(space_type, space_id, doc_id)?;
        records.sort_by_key(|(_, record)| record.id);
        Ok(records
            .into_iter()
            .map(|(_, record)| DocUpdateRecord {
                id: record.id,
                update: record.update,
                created_at: record.created_at,
                editor_id: record.editor_id,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn insert_trim_and_fetch_logs_roundtrip() {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        let logs = RocksDocUpdateLogStore::new(store);

        let ts = 1_700_000_000_000_i64;
        // 插入 3 条更新
        logs.insert_updates(
            "workspace",
            "ws",
            "doc",
            &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            Some("user-1"),
            ts,
        )
        .await
        .unwrap();

        let all = logs.fetch_logs("workspace", "ws", "doc").await.unwrap();
        assert_eq!(all.len(), 3);
        assert!(all.windows(2).all(|w| w[0].id < w[1].id));

        // trim 到最近 2 条
        logs.trim_updates("workspace", "ws", "doc", 2)
            .await
            .unwrap();
        let trimmed = logs.fetch_logs("workspace", "ws", "doc").await.unwrap();
        assert_eq!(trimmed.len(), 2);
        assert_eq!(trimmed[0].update, b"b".to_vec());
        assert_eq!(trimmed[1].update, b"c".to_vec());
    }

    /// 连续调用 insert_updates 应当为同一文档生成严格递增的 ID，
    /// 即使 timestamp 相同，也不会出现 ID 冲突或乱序。
    #[tokio::test]
    async fn log_ids_are_monotonic_across_multiple_insert_calls() {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        let logs = RocksDocUpdateLogStore::new(store);

        let ts = 1_700_000_000_000_i64;

        logs.insert_updates(
            "workspace",
            "ws",
            "doc",
            &[b"a".to_vec(), b"b".to_vec()],
            Some("user-1"),
            ts,
        )
        .await
        .unwrap();

        let first_batch = logs.fetch_logs("workspace", "ws", "doc").await.unwrap();
        assert_eq!(first_batch.len(), 2);
        assert!(first_batch[0].id < first_batch[1].id);

        let last_id_first_batch = first_batch.last().unwrap().id;

        // 第二次插入，使用相同的 timestamp，模拟高并发写入场景。
        logs.insert_updates(
            "workspace",
            "ws",
            "doc",
            &[b"c".to_vec()],
            Some("user-2"),
            ts,
        )
        .await
        .unwrap();

        let all = logs.fetch_logs("workspace", "ws", "doc").await.unwrap();
        assert_eq!(all.len(), 3);
        assert!(all.windows(2).all(|w| w[0].id < w[1].id));
        assert!(all.last().unwrap().id > last_id_first_batch);
    }

    /// 当升级前已经存在老格式日志（没有 logseq 计数器键）时，
    /// 第一次为该文档分配 ID 应当从现有最大 ID 开始递增，
    /// 确保不会复用老 ID。
    #[tokio::test]
    async fn allocate_ids_derives_start_from_existing_logs_when_counter_missing() {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        let logs = RocksDocUpdateLogStore::new(store.clone());

        let cf = store.logs_cf();
        let db = store.db();

        // 手工写入一条“旧”日志记录，模拟升级前的数据。
        let legacy_id = 42_i64;
        let legacy_ts = 1_650_000_000_000_i64;
        let legacy_record = RocksLogRecord {
            id: legacy_id,
            created_at: legacy_ts,
            editor_id: Some("legacy-user".to_string()),
            update: b"legacy".to_vec(),
        };
        let legacy_key = RocksDocUpdateLogStore::key("workspace", "ws", "doc", legacy_id);
        let legacy_bytes = logs.encode_record(&legacy_record).unwrap();
        db.put_cf(cf, legacy_key.as_bytes(), &legacy_bytes).unwrap();

        // 此时还没有 counter 键，第一次 insert_updates 应当从 legacy_id + 1 开始分配。
        let ts = 1_700_000_000_000_i64;
        logs.insert_updates(
            "workspace",
            "ws",
            "doc",
            &[b"a".to_vec()],
            Some("user-1"),
            ts,
        )
        .await
        .unwrap();

        let all = logs.fetch_logs("workspace", "ws", "doc").await.unwrap();

        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id, legacy_id);
        assert!(all[1].id > legacy_id);
    }
}
