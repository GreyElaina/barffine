use anyhow::Result;
use sqlx::{Pool, Row, Sqlite, sqlite::SqliteRow};

use crate::db::Database;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationPreferenceKind {
    Comment,
    CommentMention,
    Invitation,
    DocActivity,
    PermissionChange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserNotificationSettings {
    pub receive_comment_email: bool,
    pub receive_invitation_email: bool,
    pub receive_mention_email: bool,
    pub notify_doc_activity: bool,
    pub notify_permission_change: bool,
}

impl Default for UserNotificationSettings {
    fn default() -> Self {
        Self {
            receive_comment_email: true,
            receive_invitation_email: true,
            receive_mention_email: true,
            notify_doc_activity: true,
            notify_permission_change: true,
        }
    }
}

impl UserNotificationSettings {
    pub fn allows(&self, preference: NotificationPreferenceKind) -> bool {
        match preference {
            NotificationPreferenceKind::Comment => self.receive_comment_email,
            NotificationPreferenceKind::CommentMention => self.receive_mention_email,
            NotificationPreferenceKind::Invitation => self.receive_invitation_email,
            NotificationPreferenceKind::DocActivity => self.notify_doc_activity,
            NotificationPreferenceKind::PermissionChange => self.notify_permission_change,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct UserNotificationSettingsUpdate {
    pub receive_comment_email: Option<bool>,
    pub receive_invitation_email: Option<bool>,
    pub receive_mention_email: Option<bool>,
    pub notify_doc_activity: Option<bool>,
    pub notify_permission_change: Option<bool>,
}

#[derive(Clone)]
pub struct UserSettingsStore {
    pool: Pool<Sqlite>,
}

impl UserSettingsStore {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: database.pool().clone(),
        }
    }

    fn map_row(row: SqliteRow) -> UserNotificationSettings {
        UserNotificationSettings {
            receive_comment_email: row.get::<i64, _>("receive_comment_email") != 0,
            receive_invitation_email: row.get::<i64, _>("receive_invitation_email") != 0,
            receive_mention_email: row.get::<i64, _>("receive_mention_email") != 0,
            notify_doc_activity: row.get::<i64, _>("notify_doc_activity") != 0,
            notify_permission_change: row.get::<i64, _>("notify_permission_change") != 0,
        }
    }

    pub async fn get(&self, user_id: &str) -> Result<UserNotificationSettings> {
        let row = sqlx::query(
            "SELECT receive_comment_email, receive_invitation_email, receive_mention_email, \
                    notify_doc_activity, notify_permission_change \
             FROM user_settings WHERE user_id = ?",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_row).unwrap_or_default())
    }

    pub async fn update(
        &self,
        user_id: &str,
        update: UserNotificationSettingsUpdate,
    ) -> Result<UserNotificationSettings> {
        let current = self.get(user_id).await?;

        let next = UserNotificationSettings {
            receive_comment_email: update
                .receive_comment_email
                .unwrap_or(current.receive_comment_email),
            receive_invitation_email: update
                .receive_invitation_email
                .unwrap_or(current.receive_invitation_email),
            receive_mention_email: update
                .receive_mention_email
                .unwrap_or(current.receive_mention_email),
            notify_doc_activity: update
                .notify_doc_activity
                .unwrap_or(current.notify_doc_activity),
            notify_permission_change: update
                .notify_permission_change
                .unwrap_or(current.notify_permission_change),
        };

        sqlx::query(
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
        .bind(bool_to_int(next.receive_comment_email))
        .bind(bool_to_int(next.receive_invitation_email))
        .bind(bool_to_int(next.receive_mention_email))
        .bind(bool_to_int(next.notify_doc_activity))
        .bind(bool_to_int(next.notify_permission_change))
        .execute(&self.pool)
        .await?;

        Ok(next)
    }

    pub async fn should_notify(
        &self,
        user_id: &str,
        preference: NotificationPreferenceKind,
    ) -> Result<bool> {
        let settings = self.get(user_id).await?;
        Ok(settings.allows(preference))
    }
}

fn bool_to_int(value: bool) -> i64 {
    if value { 1 } else { 0 }
}
