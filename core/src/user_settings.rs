use crate::db::{Database, user_settings_repo::UserSettingsRepositoryRef};
use anyhow::Result;

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
    repo: UserSettingsRepositoryRef,
}

impl UserSettingsStore {
    pub fn new(database: &Database) -> Self {
        Self {
            repo: database.repositories().user_settings_repo(),
        }
    }

    pub async fn get(&self, user_id: &str) -> Result<UserNotificationSettings> {
        self.repo.get(user_id).await
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

        self.repo.upsert(user_id, next.clone()).await?;

        Ok(next)
    }

    pub async fn should_notify(
        &self,
        user_id: &str,
        preference: NotificationPreferenceKind,
    ) -> Result<bool> {
        self.repo.should_notify(user_id, preference).await
    }
}
