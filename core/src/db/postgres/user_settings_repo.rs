use anyhow::Result;
use async_trait::async_trait;
use sqlx::{Pool, Postgres, Row, postgres::PgRow};

use crate::{
    db::{postgres::util::bool_to_i64, user_settings_repo::UserSettingsRepository},
    user_settings::{NotificationPreferenceKind, UserNotificationSettings},
};

pub struct PostgresUserSettingsRepository {
    pool: Pool<Postgres>,
}

impl PostgresUserSettingsRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    fn map_row(row: PgRow) -> UserNotificationSettings {
        UserNotificationSettings {
            receive_comment_email: row.get::<i64, _>("receive_comment_email") != 0,
            receive_invitation_email: row.get::<i64, _>("receive_invitation_email") != 0,
            receive_mention_email: row.get::<i64, _>("receive_mention_email") != 0,
            notify_doc_activity: row.get::<i64, _>("notify_doc_activity") != 0,
            notify_permission_change: row.get::<i64, _>("notify_permission_change") != 0,
        }
    }
}

#[async_trait]
impl UserSettingsRepository for PostgresUserSettingsRepository {
    async fn get(&self, user_id: &str) -> Result<UserNotificationSettings> {
        let row = pg_query!(
            "SELECT receive_comment_email, receive_invitation_email, receive_mention_email, \
                    notify_doc_activity, notify_permission_change \
             FROM user_settings WHERE user_id = ?",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_row).unwrap_or_default())
    }

    async fn upsert(&self, user_id: &str, settings: UserNotificationSettings) -> Result<()> {
        pg_query!(
            "INSERT INTO user_settings (user_id, receive_comment_email, receive_invitation_email, \
                 receive_mention_email, notify_doc_activity, notify_permission_change)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(user_id) DO UPDATE SET
                 receive_comment_email = excluded.receive_comment_email,
                 receive_invitation_email = excluded.receive_invitation_email,
                 receive_mention_email = excluded.receive_mention_email,
                 notify_doc_activity = excluded.notify_doc_activity,
                 notify_permission_change = excluded.notify_permission_change",
        )
        .bind(user_id)
        .bind(bool_to_i64(settings.receive_comment_email))
        .bind(bool_to_i64(settings.receive_invitation_email))
        .bind(bool_to_i64(settings.receive_mention_email))
        .bind(bool_to_i64(settings.notify_doc_activity))
        .bind(bool_to_i64(settings.notify_permission_change))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn should_notify(
        &self,
        user_id: &str,
        preference: NotificationPreferenceKind,
    ) -> Result<bool> {
        let settings = self.get(user_id).await?;
        Ok(settings.allows(preference))
    }
}
