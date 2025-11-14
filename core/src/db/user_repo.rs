use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use crate::user::{
    ConnectedAccountRecord, ConnectedAccountTokens, NewConnectedAccount, SessionRecord, UserCursor,
    UserRecord,
};

#[derive(Debug, Clone)]
pub struct CreateUserParams {
    pub id: String,
    pub email: String,
    pub password_hash: String,
    pub name: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct CreateSessionParams {
    pub id: String,
    pub user_id: String,
    pub created_at: i64,
    pub expires_at: i64,
}

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn create_user(&self, params: CreateUserParams) -> Result<UserRecord>;

    async fn fetch_user_by_email(&self, email: &str) -> Result<Option<UserRecord>>;

    async fn fetch_user_by_id(&self, id: &str) -> Result<Option<UserRecord>>;

    async fn fetch_users_by_ids(&self, ids: &[String]) -> Result<Vec<UserRecord>>;

    async fn create_session(&self, params: CreateSessionParams) -> Result<SessionRecord>;

    async fn fetch_session(&self, session_id: &str) -> Result<Option<SessionRecord>>;

    async fn delete_session(&self, session_id: &str) -> Result<()>;

    async fn delete_sessions_by_user(&self, user_id: &str) -> Result<()>;

    async fn update_password_hash(&self, user_id: &str, password_hash: &str) -> Result<()>;

    async fn fetch_connected_account(
        &self,
        provider: &str,
        provider_account_id: &str,
    ) -> Result<Option<ConnectedAccountRecord>>;

    async fn fetch_connected_account_by_id(
        &self,
        account_id: &str,
    ) -> Result<Option<ConnectedAccountRecord>>;

    async fn create_connected_account(
        &self,
        id: String,
        input: NewConnectedAccount<'_>,
        timestamp: i64,
    ) -> Result<ConnectedAccountRecord>;

    async fn update_connected_account_tokens(
        &self,
        account_id: &str,
        tokens: ConnectedAccountTokens<'_>,
        timestamp: i64,
    ) -> Result<()>;

    async fn delete_connected_account(&self, account_id: &str) -> Result<()>;

    async fn expire_session(&self, session_id: &str, expires_at: i64) -> Result<()>;

    async fn list_users_paginated(
        &self,
        skip: i64,
        limit: i64,
        keyword: Option<&str>,
        cursor: Option<&UserCursor>,
    ) -> Result<Vec<UserRecord>>;

    async fn update_user_profile(
        &self,
        user_id: &str,
        name: Option<Option<&str>>,
        avatar_url: Option<Option<&str>>,
    ) -> Result<()>;

    async fn update_user_account(
        &self,
        user_id: &str,
        email: Option<&str>,
        name: Option<Option<&str>>,
    ) -> Result<()>;

    async fn set_email_verified_at(&self, user_id: &str, timestamp: Option<i64>) -> Result<()>;

    async fn count_users(&self, keyword: Option<&str>) -> Result<i64>;

    async fn upsert_admin_user(&self, user_id: &str, timestamp: i64) -> Result<()>;

    async fn is_admin_user(&self, user_id: &str) -> Result<bool>;

    async fn set_user_disabled(&self, user_id: &str, disabled: bool) -> Result<()>;
}

pub type UserRepositoryRef = Arc<dyn UserRepository>;
