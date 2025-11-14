use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use sqlx::{Pool, Postgres, QueryBuilder, Row, postgres::PgRow};

use crate::{
    db::user_repo::{CreateSessionParams, CreateUserParams, UserRepository},
    user::{
        ConnectedAccountRecord, ConnectedAccountTokens, NewConnectedAccount, SessionRecord,
        UserCursor, UserRecord,
    },
};

pub struct PostgresUserRepository {
    pool: Pool<Postgres>,
}

impl PostgresUserRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    fn map_user_row(row: PgRow) -> UserRecord {
        UserRecord {
            id: row.get("id"),
            email: row.get("email"),
            password_hash: row.get("password_hash"),
            name: row.get("name"),
            avatar_url: row.get("avatar_url"),
            email_verified_at: row.get("email_verified_at"),
            disabled: row.get::<i64, _>("disabled") != 0,
            created_at: row.get("created_at"),
        }
    }

    fn map_session_row(row: PgRow) -> SessionRecord {
        SessionRecord {
            id: row.get("id"),
            user_id: row.get("user_id"),
            created_at: row.get("created_at"),
            expires_at: row.get("expires_at"),
        }
    }

    fn map_connected_account(row: PgRow) -> ConnectedAccountRecord {
        ConnectedAccountRecord {
            id: row.get("id"),
            user_id: row.get("user_id"),
            provider: row.get("provider"),
            provider_account_id: row.get("provider_account_id"),
            scope: row.get("scope"),
            access_token: row.get("access_token"),
            refresh_token: row.get("refresh_token"),
            expires_at: row.get("expires_at"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }
}

#[async_trait]
impl UserRepository for PostgresUserRepository {
    async fn create_user(&self, params: CreateUserParams) -> Result<UserRecord> {
        let CreateUserParams {
            id,
            email,
            password_hash,
            name,
            created_at,
        } = params;

        pg_query!(
            "INSERT INTO users (id, email, password_hash, created_at, name)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&email)
        .bind(&password_hash)
        .bind(created_at)
        .bind(name.as_deref())
        .execute(&self.pool)
        .await
        .with_context(|| "failed to insert user".to_string())?;

        Ok(UserRecord {
            id,
            email,
            password_hash,
            name,
            avatar_url: None,
            email_verified_at: None,
            disabled: false,
            created_at,
        })
    }

    async fn fetch_user_by_email(&self, email: &str) -> Result<Option<UserRecord>> {
        let row = pg_query!(
            "SELECT id, email, password_hash, name, avatar_url, email_verified_at, disabled, created_at
             FROM users WHERE email = ?",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_user_row))
    }

    async fn fetch_user_by_id(&self, id: &str) -> Result<Option<UserRecord>> {
        let row = pg_query!(
            "SELECT id, email, password_hash, name, avatar_url, email_verified_at, disabled, created_at
             FROM users WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_user_row))
    }

    async fn fetch_users_by_ids(&self, ids: &[String]) -> Result<Vec<UserRecord>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut builder = QueryBuilder::new(
            "SELECT id, email, password_hash, name, avatar_url, email_verified_at, disabled, created_at
             FROM users WHERE id IN (",
        );
        {
            let mut separated = builder.separated(", ");
            for id in ids {
                separated.push_bind(id);
            }
        }
        builder.push(")");

        let rows = builder.build().fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(Self::map_user_row).collect())
    }

    async fn create_session(&self, params: CreateSessionParams) -> Result<SessionRecord> {
        let CreateSessionParams {
            id,
            user_id,
            created_at,
            expires_at,
        } = params;

        pg_query!(
            "INSERT INTO sessions (id, user_id, created_at, expires_at)
             VALUES (?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&user_id)
        .bind(created_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;

        Ok(SessionRecord {
            id,
            user_id,
            created_at,
            expires_at,
        })
    }

    async fn fetch_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        let row =
            pg_query!("SELECT id, user_id, created_at, expires_at FROM sessions WHERE id = ?")
                .bind(session_id)
                .fetch_optional(&self.pool)
                .await?;

        Ok(row.map(Self::map_session_row))
    }

    async fn delete_session(&self, session_id: &str) -> Result<()> {
        pg_query!("DELETE FROM sessions WHERE id = ?")
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_sessions_by_user(&self, user_id: &str) -> Result<()> {
        pg_query!("DELETE FROM sessions WHERE user_id = ?")
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn update_password_hash(&self, user_id: &str, password_hash: &str) -> Result<()> {
        pg_query!("UPDATE users SET password_hash = ? WHERE id = ?")
            .bind(password_hash)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn fetch_connected_account(
        &self,
        provider: &str,
        provider_account_id: &str,
    ) -> Result<Option<ConnectedAccountRecord>> {
        let row = pg_query!(
            "SELECT id, user_id, provider, provider_account_id, scope, access_token, refresh_token, expires_at, created_at, updated_at
             FROM user_connected_accounts
             WHERE provider = ? AND provider_account_id = ?",
        )
        .bind(provider)
        .bind(provider_account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_connected_account))
    }

    async fn fetch_connected_account_by_id(
        &self,
        account_id: &str,
    ) -> Result<Option<ConnectedAccountRecord>> {
        let row = pg_query!(
            "SELECT id, user_id, provider, provider_account_id, scope, access_token, refresh_token, expires_at, created_at, updated_at
             FROM user_connected_accounts WHERE id = ?",
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_connected_account))
    }

    async fn create_connected_account(
        &self,
        id: String,
        input: NewConnectedAccount<'_>,
        timestamp: i64,
    ) -> Result<ConnectedAccountRecord> {
        pg_query!(
            "INSERT INTO user_connected_accounts (
                 id, user_id, provider, provider_account_id, scope,
                 access_token, refresh_token, expires_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(input.user_id)
        .bind(input.provider)
        .bind(input.provider_account_id)
        .bind(input.scope)
        .bind(input.access_token)
        .bind(input.refresh_token)
        .bind(input.expires_at)
        .bind(timestamp)
        .bind(timestamp)
        .execute(&self.pool)
        .await?;

        Ok(ConnectedAccountRecord {
            id,
            user_id: input.user_id.to_owned(),
            provider: input.provider.to_owned(),
            provider_account_id: input.provider_account_id.to_owned(),
            scope: input.scope.map(|value| value.to_owned()),
            access_token: input.access_token.map(|value| value.to_owned()),
            refresh_token: input.refresh_token.map(|value| value.to_owned()),
            expires_at: input.expires_at,
            created_at: timestamp,
            updated_at: timestamp,
        })
    }

    async fn update_connected_account_tokens(
        &self,
        account_id: &str,
        tokens: ConnectedAccountTokens<'_>,
        timestamp: i64,
    ) -> Result<()> {
        let result = pg_query!(
            "UPDATE user_connected_accounts
             SET scope = ?, access_token = ?, refresh_token = ?, expires_at = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(tokens.scope)
        .bind(tokens.access_token)
        .bind(tokens.refresh_token)
        .bind(tokens.expires_at)
        .bind(timestamp)
        .bind(account_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            Err(anyhow!("connected account not found: {account_id}"))
        } else {
            Ok(())
        }
    }

    async fn delete_connected_account(&self, account_id: &str) -> Result<()> {
        pg_query!("DELETE FROM user_connected_accounts WHERE id = ?")
            .bind(account_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn expire_session(&self, session_id: &str, expires_at: i64) -> Result<()> {
        pg_query!("UPDATE sessions SET expires_at = ? WHERE id = ?")
            .bind(expires_at)
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_users_paginated(
        &self,
        skip: i64,
        limit: i64,
        keyword: Option<&str>,
        cursor: Option<&UserCursor>,
    ) -> Result<Vec<UserRecord>> {
        let mut query = QueryBuilder::new(
            "SELECT id, email, password_hash, name, avatar_url, email_verified_at, disabled, created_at \
             FROM users",
        );

        let mut has_filter = false;

        if let Some(term) = keyword {
            let like = format!("%{}%", term.to_ascii_lowercase());
            query.push(" WHERE (LOWER(email) LIKE ");
            query.push_bind(like.clone());
            query.push(" OR LOWER(COALESCE(name, '')) LIKE ");
            query.push_bind(like);
            query.push(")");
            has_filter = true;
        }

        if let Some(cursor) = cursor {
            if has_filter {
                query.push(" AND ");
            } else {
                query.push(" WHERE ");
            }
            query.push("(created_at < ");
            query.push_bind(cursor.created_at);
            query.push(" OR (created_at = ");
            query.push_bind(cursor.created_at);
            query.push(" AND id < ");
            query.push_bind(&cursor.id);
            query.push("))");
        }

        query
            .push(" ORDER BY created_at DESC, id DESC LIMIT ")
            .push_bind(limit);
        query.push(" OFFSET ").push_bind(skip.max(0));

        let rows = query.build().fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(Self::map_user_row).collect())
    }

    async fn update_user_profile(
        &self,
        user_id: &str,
        name: Option<Option<&str>>,
        avatar_url: Option<Option<&str>>,
    ) -> Result<()> {
        if name.is_none() && avatar_url.is_none() {
            return Ok(());
        }

        let mut builder = QueryBuilder::new("UPDATE users SET ");
        let mut first = true;

        if let Some(name) = name {
            builder.push("name = ");
            builder.push_bind(name);
            first = false;
        }

        if let Some(avatar) = avatar_url {
            if !first {
                builder.push(", ");
            }
            builder.push("avatar_url = ");
            builder.push_bind(avatar);
        }

        builder.push(" WHERE id = ");
        builder.push_bind(user_id);
        builder.build().execute(&self.pool).await?;
        Ok(())
    }

    async fn update_user_account(
        &self,
        user_id: &str,
        email: Option<&str>,
        name: Option<Option<&str>>,
    ) -> Result<()> {
        if email.is_none() && name.is_none() {
            return Ok(());
        }

        let mut builder = QueryBuilder::new("UPDATE users SET ");
        let mut first = true;

        if let Some(email_value) = email {
            builder.push("email = ");
            builder.push_bind(email_value);
            first = false;
        }

        if let Some(name_value) = name {
            if !first {
                builder.push(", ");
            }
            builder.push("name = ");
            builder.push_bind(name_value);
        }

        builder.push(" WHERE id = ");
        builder.push_bind(user_id);
        builder.build().execute(&self.pool).await?;
        Ok(())
    }

    async fn set_email_verified_at(&self, user_id: &str, timestamp: Option<i64>) -> Result<()> {
        pg_query!("UPDATE users SET email_verified_at = ? WHERE id = ?")
            .bind(timestamp)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn count_users(&self, keyword: Option<&str>) -> Result<i64> {
        if let Some(term) = keyword {
            let like = format!("%{}%", term.to_ascii_lowercase());
            let row = pg_query!(
                "SELECT COUNT(*) as count FROM users \
                 WHERE LOWER(email) LIKE ? OR LOWER(COALESCE(name, '')) LIKE ?",
            )
            .bind(&like)
            .bind(&like)
            .fetch_one(&self.pool)
            .await?;
            return Ok(row.get::<i64, _>("count"));
        }

        let row = pg_query!("SELECT COUNT(*) as count FROM users")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>("count"))
    }

    async fn upsert_admin_user(&self, user_id: &str, timestamp: i64) -> Result<()> {
        pg_query!(
            "INSERT INTO admin_users (user_id, created_at)
             VALUES (?, ?)
             ON CONFLICT(user_id) DO UPDATE SET created_at = excluded.created_at",
        )
        .bind(user_id)
        .bind(timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn is_admin_user(&self, user_id: &str) -> Result<bool> {
        let exists: Option<i64> =
            pg_query_scalar!("SELECT 1::BIGINT FROM admin_users WHERE user_id = ?")
                .bind(user_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(exists.is_some())
    }

    async fn set_user_disabled(&self, user_id: &str, disabled: bool) -> Result<()> {
        pg_query!("UPDATE users SET disabled = ? WHERE id = ?")
            .bind(if disabled { 1_i64 } else { 0_i64 })
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
