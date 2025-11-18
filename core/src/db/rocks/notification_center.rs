use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use rocksdb::{Direction, IteratorMode, WriteBatch};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    doc_data::RocksDocDataStore,
    ids::UserId,
    notification::{NotificationCenter, NotificationRecord},
};

pub struct RocksNotificationCenter {
    store: Arc<RocksDocDataStore>,
}

impl RocksNotificationCenter {
    pub fn new(store: Arc<RocksDocDataStore>) -> Self {
        Self { store }
    }

    fn db(&self) -> Arc<rocksdb::DB> {
        self.store.db()
    }

    fn cf(&self) -> &rocksdb::ColumnFamily {
        self.store.notifications_cf()
    }

    fn list_prefix(user_id: &str) -> String {
        format!("list:{user_id}:")
    }

    fn list_key(user_id: &str, timestamp: i64, id: &str) -> String {
        let rev = Self::encode_rev(timestamp);
        format!("{}{:016x}:{id}", Self::list_prefix(user_id), rev)
    }

    fn id_key(id: &str) -> String {
        format!("id:{id}")
    }

    fn ack_value(user_id: &str) -> String {
        format!("ack:{user_id}")
    }

    fn parse_ack_value(value: &str) -> Option<&str> {
        value.strip_prefix("ack:")
    }

    fn count_key(user_id: &str) -> String {
        format!("cnt:{user_id}")
    }

    fn encode_rev(timestamp: i64) -> u64 {
        let ts = if timestamp < 0 { 0 } else { timestamp as u64 };
        u64::MAX - ts
    }

    fn serialize_record(record: &NotificationRecord) -> Result<Vec<u8>> {
        let stored = StoredNotification {
            id: record.id.clone(),
            user_id: record.user_id.to_string(),
            kind: record.kind.clone(),
            payload: record.payload.clone(),
            read: record.read,
            created_at: record.created_at.timestamp(),
            updated_at: record.updated_at.timestamp(),
        };
        Ok(serde_json::to_vec(&stored)?)
    }

    fn deserialize_record(bytes: &[u8]) -> Result<NotificationRecord> {
        let stored: StoredNotification = serde_json::from_slice(bytes)?;
        let created_at = Utc
            .timestamp_opt(stored.created_at, 0)
            .single()
            .ok_or_else(|| anyhow!("invalid created_at timestamp"))?;
        let updated_at = Utc
            .timestamp_opt(stored.updated_at, 0)
            .single()
            .ok_or_else(|| anyhow!("invalid updated_at timestamp"))?;
        Ok(NotificationRecord {
            id: stored.id,
            user_id: UserId::from(stored.user_id),
            kind: stored.kind,
            payload: stored.payload,
            read: stored.read,
            created_at,
            updated_at,
        })
    }

    fn write_count(&self, user_id: &str, value: i64) -> Result<()> {
        self.db().put_cf(
            self.cf(),
            Self::count_key(user_id).as_bytes(),
            value.to_be_bytes(),
        )?;
        Ok(())
    }

    fn increment_count(&self, user_id: &str, delta: i64) -> Result<()> {
        let current = self.get_count(user_id)?;
        let next = (current + delta).max(0);
        self.write_count(user_id, next)?;
        Ok(())
    }

    fn get_count(&self, user_id: &str) -> Result<i64> {
        let value = self
            .db()
            .get_cf(self.cf(), Self::count_key(user_id).as_bytes())?;
        if let Some(bytes) = value {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&bytes);
            Ok(i64::from_be_bytes(arr))
        } else {
            Ok(0)
        }
    }

    fn delete_list_entry(&self, list_key: &str, notification_id: &str) -> Result<()> {
        let id_key = Self::id_key(notification_id);
        self.db().delete_cf(self.cf(), list_key.as_bytes())?;
        self.db().delete_cf(self.cf(), id_key.as_bytes())?;
        Ok(())
    }

    fn collect_list_keys(&self, user_id: &str) -> Result<Vec<(String, NotificationRecord)>> {
        let prefix = Self::list_prefix(user_id);
        let mut entries = Vec::new();
        let db = self.db();
        let iter = db.iterator_cf(
            self.cf(),
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        );
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8(key.to_vec())?;
            if !key_str.starts_with(&prefix) {
                break;
            }
            let record = Self::deserialize_record(&value)?;
            entries.push((key_str, record));
        }
        Ok(entries)
    }

    fn apply_cursor_filter(
        records: &[NotificationRecord],
        limit: i64,
        offset: i64,
        after: &Option<(i64, String)>,
    ) -> Vec<NotificationRecord> {
        let mut filtered: Vec<NotificationRecord> = records
            .iter()
            .filter(|record| {
                if let Some((ts, id)) = after {
                    let created_ts = record.created_at.timestamp();
                    created_ts < *ts || (created_ts == *ts && record.id.as_str() < id.as_str())
                } else {
                    true
                }
            })
            .cloned()
            .collect();
        if after.is_none() && offset > 0 {
            let skip = offset.max(0) as usize;
            if skip < filtered.len() {
                filtered.drain(0..skip);
            } else {
                filtered.clear();
            }
        }
        let limit = limit.max(0) as usize;
        if limit > 0 && filtered.len() > limit {
            filtered.truncate(limit);
        }
        filtered
    }
}

#[derive(Serialize, Deserialize)]
struct StoredNotification {
    id: String,
    user_id: String,
    kind: String,
    payload: JsonValue,
    read: bool,
    created_at: i64,
    updated_at: i64,
}

#[async_trait]
impl NotificationCenter for RocksNotificationCenter {
    async fn enqueue(&self, mut notification: NotificationRecord) -> Result<()> {
        notification.read = false;
        let timestamp = notification.created_at.timestamp();
        let list_key = Self::list_key(notification.user_id.as_str(), timestamp, &notification.id);
        let id_key = Self::id_key(&notification.id);
        let value = Self::serialize_record(&notification)?;

        let db = self.db();
        let cf = self.cf();
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, list_key.as_bytes(), value);
        batch.put_cf(cf, id_key.as_bytes(), list_key.as_bytes());
        db.write(batch)?;
        self.increment_count(notification.user_id.as_str(), 1)?;
        Ok(())
    }

    async fn list_unread(&self, user_id: &str) -> Result<Vec<NotificationRecord>> {
        let records = self.collect_list_keys(user_id)?;
        Ok(records.into_iter().map(|(_, record)| record).collect())
    }

    async fn list_for_user(
        &self,
        user_id: &str,
        limit: i64,
        offset: i64,
        after: Option<(i64, String)>,
    ) -> Result<Vec<NotificationRecord>> {
        let records = self.collect_list_keys(user_id)?;
        let just_records: Vec<NotificationRecord> =
            records.into_iter().map(|(_, record)| record).collect();
        Ok(Self::apply_cursor_filter(
            &just_records,
            limit,
            offset,
            &after,
        ))
    }

    async fn count_for_user(&self, user_id: &str) -> Result<i64> {
        self.get_count(user_id)
    }

    async fn count_unread(&self, user_id: &str) -> Result<i64> {
        self.get_count(user_id)
    }

    async fn mark_read(&self, notification_id: &str, user_id: &str) -> Result<bool> {
        let id_key = Self::id_key(notification_id);
        let Some(list_key_bytes) = self.db().get_cf(self.cf(), id_key.as_bytes())? else {
            return Ok(false);
        };
        let list_key = String::from_utf8(list_key_bytes)?;
        if let Some(owner) = Self::parse_ack_value(&list_key) {
            return Ok(owner == user_id);
        }

        if !list_key.starts_with(&Self::list_prefix(user_id)) {
            return Ok(false);
        }

        self.delete_list_entry(&list_key, notification_id)?;
        self.db().put_cf(
            self.cf(),
            id_key.as_bytes(),
            Self::ack_value(user_id).into_bytes(),
        )?;
        self.increment_count(user_id, -1)?;
        Ok(true)
    }

    async fn mark_all_read(&self, user_id: &str) -> Result<u64> {
        let entries = self.collect_list_keys(user_id)?;
        if entries.is_empty() {
            return Ok(0);
        }
        let db = self.db();
        let cf = self.cf();
        let mut batch = WriteBatch::default();
        for (list_key, record) in &entries {
            let id_key = Self::id_key(&record.id);
            batch.delete_cf(cf, list_key.as_bytes());
            batch.put_cf(cf, id_key.as_bytes(), Self::ack_value(user_id).into_bytes());
        }
        db.write(batch)?;
        self.write_count(user_id, 0)?;
        Ok(entries.len() as u64)
    }
}
