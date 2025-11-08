use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::{Pool, Row, Sqlite};

use crate::{
    db::Database,
    notification::{NotificationCenter, NotificationRecord},
};

#[derive(Clone)]
pub struct SqliteNotificationCenter {
    pool: Arc<Pool<Sqlite>>,
}

impl SqliteNotificationCenter {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: Arc::new(database.pool().clone()),
        }
    }

    fn deserialize_payload(raw: &str) -> JsonValue {
        serde_json::from_str(raw).unwrap_or(JsonValue::Null)
    }

    fn serialize_payload(payload: &JsonValue) -> Result<String> {
        Ok(serde_json::to_string(payload)?)
    }

    fn map_row(row: sqlx::sqlite::SqliteRow) -> NotificationRecord {
        NotificationRecord {
            id: row.get("id"),
            user_id: row.get("user_id"),
            kind: row.get("kind"),
            payload: Self::deserialize_payload(row.get("payload")),
            read: row.get::<i64, _>("read") != 0,
            created_at: DateTime::<Utc>::from_timestamp(row.get("created_at"), 0).unwrap(),
            updated_at: DateTime::<Utc>::from_timestamp(row.get("updated_at"), 0).unwrap(),
        }
    }
}

#[async_trait]
impl NotificationCenter for SqliteNotificationCenter {
    async fn enqueue(&self, notification: NotificationRecord) -> Result<()> {
        let payload = Self::serialize_payload(&notification.payload)?;

        sqlx::query(
            "INSERT INTO notifications (id, user_id, kind, payload, read, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&notification.id)
        .bind(&notification.user_id)
        .bind(&notification.kind)
        .bind(payload)
        .bind(if notification.read { 1_i64 } else { 0_i64 })
        .bind(notification.created_at.timestamp())
        .bind(notification.updated_at.timestamp())
        .execute(&*self.pool)
        .await?;

        Ok(())
    }

    async fn list_unread(&self, user_id: &str) -> Result<Vec<NotificationRecord>> {
        let rows = sqlx::query(
            "SELECT id, user_id, kind, payload, read, created_at, updated_at
             FROM notifications
             WHERE user_id = ? AND read = 0
             ORDER BY created_at DESC, id DESC",
        )
        .bind(user_id)
        .fetch_all(&*self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_row).collect())
    }

    async fn list_for_user(
        &self,
        user_id: &str,
        limit: i64,
        offset: i64,
        after: Option<(i64, String)>,
    ) -> Result<Vec<NotificationRecord>> {
        let rows = if let Some((timestamp, id)) = after {
            sqlx::query(
                "SELECT id, user_id, kind, payload, read, created_at, updated_at
                 FROM notifications
                 WHERE user_id = ?
                   AND read = 0
                   AND (created_at < ? OR (created_at = ? AND id < ?))
                 ORDER BY created_at DESC, id DESC
                 LIMIT ?",
            )
            .bind(user_id)
            .bind(timestamp)
            .bind(timestamp)
            .bind(id)
            .bind(limit)
            .fetch_all(&*self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT id, user_id, kind, payload, read, created_at, updated_at
                 FROM notifications
                 WHERE user_id = ?
                   AND read = 0
                 ORDER BY created_at DESC, id DESC
                 LIMIT ? OFFSET ?",
            )
            .bind(user_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&*self.pool)
            .await?
        };

        Ok(rows.into_iter().map(Self::map_row).collect())
    }

    async fn count_for_user(&self, user_id: &str) -> Result<i64> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM notifications WHERE user_id = ? AND read = 0")
                .bind(user_id)
                .fetch_one(&*self.pool)
                .await?;

        Ok(count)
    }

    async fn count_unread(&self, user_id: &str) -> Result<i64> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM notifications WHERE user_id = ? AND read = 0")
                .bind(user_id)
                .fetch_one(&*self.pool)
                .await?;

        Ok(count)
    }

    async fn mark_read(&self, notification_id: &str, user_id: &str) -> Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "UPDATE notifications SET read = 1, updated_at = ?
             WHERE id = ? AND user_id = ?",
        )
        .bind(now)
        .bind(notification_id)
        .bind(user_id)
        .execute(&*self.pool)
        .await?;

        if result.rows_affected() > 0 {
            return Ok(true);
        }

        let exists: i64 =
            sqlx::query_scalar("SELECT COUNT(1) FROM notifications WHERE id = ? AND user_id = ?")
                .bind(notification_id)
                .bind(user_id)
                .fetch_one(&*self.pool)
                .await?;

        Ok(exists > 0)
    }

    async fn mark_all_read(&self, user_id: &str) -> Result<u64> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "UPDATE notifications
             SET read = 1, updated_at = ?
             WHERE user_id = ? AND read = 0",
        )
        .bind(now)
        .bind(user_id)
        .execute(&*self.pool)
        .await?;

        Ok(result.rows_affected())
    }
}
