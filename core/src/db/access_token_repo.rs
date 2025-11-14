use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::access_token::AccessTokenRecord;

#[async_trait]
pub trait AccessTokenRepository: Send + Sync {
    async fn list(&self, user_id: &str) -> Result<Vec<AccessTokenRecord>>;

    async fn list_revealed(&self, user_id: &str) -> Result<Vec<AccessTokenRecord>>;

    async fn insert(
        &self,
        id: &str,
        user_id: &str,
        name: &str,
        token: &str,
        created_at: i64,
        expires_at: Option<i64>,
    ) -> Result<()>;

    async fn delete(&self, id: &str, user_id: &str) -> Result<()>;

    async fn find_active_by_token(
        &self,
        token: &str,
        now_timestamp: i64,
    ) -> Result<Option<AccessTokenRecord>>;
}

pub type AccessTokenRepositoryRef = Arc<dyn AccessTokenRepository>;
