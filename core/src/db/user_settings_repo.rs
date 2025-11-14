use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::user_settings::{NotificationPreferenceKind, UserNotificationSettings};

#[async_trait]
pub trait UserSettingsRepository: Send + Sync {
    async fn get(&self, user_id: &str) -> Result<UserNotificationSettings>;
    async fn upsert(&self, user_id: &str, payload: UserNotificationSettings) -> Result<()>;
    async fn should_notify(
        &self,
        user_id: &str,
        preference: NotificationPreferenceKind,
    ) -> Result<bool>;
}

pub type UserSettingsRepositoryRef = Arc<dyn UserSettingsRepository>;
