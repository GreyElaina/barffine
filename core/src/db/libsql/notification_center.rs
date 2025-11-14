use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deadpool_libsql::{
    Pool,
    libsql::{self, params},
};
use serde_json::Value as JsonValue;

use crate::{
    db::{Database, libsql::row_ext},
    notification::{NotificationCenter, NotificationRecord},
};

pub struct LibsqlNotificationCenter {
    pool: Pool,
}

impl LibsqlNotificationCenter {
    pub fn new(database: &Database) -> Self {
        let pool = database
            .libsql_pool()
            .expect("libsql pool unavailable")
            .pool()
            .clone();

        Self { pool }
    }

    fn deserialize_payload(raw: &str) -> JsonValue {
        serde_json::from_str(raw).unwrap_or(JsonValue::Null)
    }

    fn serialize_payload(payload: &JsonValue) -> Result<String> {
        Ok(serde_json::to_string(payload)?)
    }

    fn map_row(row: &libsql::Row) -> Result<NotificationRecord> {
        let id: String = row_ext::get(row, "id")?;
        let user_id: String = row_ext::get(row, "user_id")?;
        let kind: String = row_ext::get(row, "kind")?;
        let payload_raw: String = row_ext::get(row, "payload")?;
        let read_raw: i64 = row_ext::get(row, "read")?;
        let created_at_ts: i64 = row_ext::get(row, "created_at")?;
        let updated_at_ts: i64 = row_ext::get(row, "updated_at")?;

        let payload = Self::deserialize_payload(&payload_raw);
        let created_at = DateTime::<Utc>::from_timestamp(created_at_ts, 0).unwrap();
        let updated_at = DateTime::<Utc>::from_timestamp(updated_at_ts, 0).unwrap();

        Ok(NotificationRecord {
            id,
            user_id,
            kind,
            payload,
            read: read_raw != 0,
            created_at,
            updated_at,
        })
    }
}

#[async_trait]
impl NotificationCenter for LibsqlNotificationCenter {
    async fn enqueue(&self, notification: NotificationRecord) -> Result<()> {
        let payload = Self::serialize_payload(&notification.payload)?;

        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO notifications (id, user_id, kind, payload, read, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                notification.id.as_str(),
                notification.user_id.as_str(),
                notification.kind.as_str(),
                payload.as_str(),
                if notification.read { 1_i64 } else { 0_i64 },
                notification.created_at.timestamp(),
                notification.updated_at.timestamp()
            ],
        )
        .await?;

        Ok(())
    }

    async fn list_unread(&self, user_id: &str) -> Result<Vec<NotificationRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, user_id, kind, payload, read, created_at, updated_at
                 FROM notifications
                 WHERE user_id = ? AND read = 0
                 ORDER BY created_at DESC, id DESC",
                params![user_id],
            )
            .await?;

        let mut records = Vec::new();
        while let Some(row) = rows.next().await? {
            records.push(Self::map_row(&row)?);
        }

        Ok(records)
    }

    async fn list_for_user(
        &self,
        user_id: &str,
        limit: i64,
        offset: i64,
        after: Option<(i64, String)>,
    ) -> Result<Vec<NotificationRecord>> {
        let conn = self.pool.get().await?;

        let mut rows = if let Some((timestamp, id)) = after {
            conn.query(
                "SELECT id, user_id, kind, payload, read, created_at, updated_at
                 FROM notifications
                 WHERE user_id = ?
                   AND read = 0
                   AND (created_at < ? OR (created_at = ? AND id < ?))
                 ORDER BY created_at DESC, id DESC
                 LIMIT ?",
                params![user_id, timestamp, timestamp, id.as_str(), limit],
            )
            .await?
        } else {
            conn.query(
                "SELECT id, user_id, kind, payload, read, created_at, updated_at
                 FROM notifications
                 WHERE user_id = ?
                   AND read = 0
                 ORDER BY created_at DESC, id DESC
                 LIMIT ? OFFSET ?",
                params![user_id, limit, offset],
            )
            .await?
        };

        let mut records = Vec::new();
        while let Some(row) = rows.next().await? {
            records.push(Self::map_row(&row)?);
        }

        Ok(records)
    }

    async fn count_for_user(&self, user_id: &str) -> Result<i64> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT COUNT(*) as count FROM notifications WHERE user_id = ? AND read = 0",
                params![user_id],
            )
            .await?;
        let row = rows
            .next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("count query returned no rows"))?;
        let count: i64 = row_ext::get(&row, "count")?;
        Ok(count)
    }

    async fn count_unread(&self, user_id: &str) -> Result<i64> {
        self.count_for_user(user_id).await
    }

    async fn mark_read(&self, notification_id: &str, user_id: &str) -> Result<bool> {
        let conn = self.pool.get().await?;
        let affected = conn
            .execute(
                "UPDATE notifications
                 SET read = 1
                 WHERE id = ? AND user_id = ? AND read = 0",
                params![notification_id, user_id],
            )
            .await?;

        if affected > 0 {
            return Ok(true);
        }

        let mut rows = conn
            .query(
                "SELECT COUNT(1) as count FROM notifications WHERE id = ? AND user_id = ?",
                params![notification_id, user_id],
            )
            .await?;
        let row = rows
            .next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("count query returned no rows"))?;
        let exists: i64 = row_ext::get(&row, "count")?;

        Ok(exists > 0)
    }

    async fn mark_all_read(&self, user_id: &str) -> Result<u64> {
        let conn = self.pool.get().await?;
        let affected = conn
            .execute(
                "UPDATE notifications
                 SET read = 1
                 WHERE user_id = ? AND read = 0",
                params![user_id],
            )
            .await?;

        Ok(affected)
    }
}
