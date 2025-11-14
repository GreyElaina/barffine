use anyhow::Result;
use async_trait::async_trait;
use deadpool_libsql::{
    Pool,
    libsql::{self, params},
};

use crate::{
    db::{libsql::row_ext, user_settings_repo::UserSettingsRepository},
    user_settings::{NotificationPreferenceKind, UserNotificationSettings},
};

fn row_to_settings(row: libsql::Row) -> Result<UserNotificationSettings> {
    Ok(UserNotificationSettings {
        receive_comment_email: row_ext::get::<i64>(&row, "receive_comment_email")? != 0,
        receive_invitation_email: row_ext::get::<i64>(&row, "receive_invitation_email")? != 0,
        receive_mention_email: row_ext::get::<i64>(&row, "receive_mention_email")? != 0,
        notify_doc_activity: row_ext::get::<i64>(&row, "notify_doc_activity")? != 0,
        notify_permission_change: row_ext::get::<i64>(&row, "notify_permission_change")? != 0,
    })
}

fn bool_to_int(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

pub struct LibsqlUserSettingsRepository {
    pool: Pool,
}

impl LibsqlUserSettingsRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UserSettingsRepository for LibsqlUserSettingsRepository {
    async fn get(&self, user_id: &str) -> Result<UserNotificationSettings> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT receive_comment_email, receive_invitation_email, receive_mention_email, \
                        notify_doc_activity, notify_permission_change \
                 FROM user_settings WHERE user_id = ?",
                params![user_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            row_to_settings(row)
        } else {
            Ok(UserNotificationSettings::default())
        }
    }

    async fn upsert(&self, user_id: &str, settings: UserNotificationSettings) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO user_settings (
                 user_id,
                 receive_comment_email,
                 receive_invitation_email,
                 receive_mention_email,
                 notify_doc_activity,
                 notify_permission_change
             ) VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(user_id) DO UPDATE SET
                 receive_comment_email = excluded.receive_comment_email,
                 receive_invitation_email = excluded.receive_invitation_email,
                 receive_mention_email = excluded.receive_mention_email,
                 notify_doc_activity = excluded.notify_doc_activity,
                 notify_permission_change = excluded.notify_permission_change",
            params![
                user_id,
                bool_to_int(settings.receive_comment_email),
                bool_to_int(settings.receive_invitation_email),
                bool_to_int(settings.receive_mention_email),
                bool_to_int(settings.notify_doc_activity),
                bool_to_int(settings.notify_permission_change)
            ],
        )
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
