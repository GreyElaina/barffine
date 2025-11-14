use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use deadpool_libsql::{
    Pool,
    libsql::{self, params, params_from_iter},
};

use crate::{
    db::{
        libsql::row_ext,
        user_repo::{CreateSessionParams, CreateUserParams, UserRepository},
    },
    user::{
        ConnectedAccountRecord, ConnectedAccountTokens, NewConnectedAccount, SessionRecord,
        UserCursor, UserRecord,
    },
};

fn like_pattern(keyword: &str) -> String {
    format!("%{}%", keyword.to_ascii_lowercase())
}

pub struct LibsqlUserRepository {
    pool: Pool,
}

impl LibsqlUserRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn map_user_row(row: &libsql::Row) -> Result<UserRecord> {
        Ok(UserRecord {
            id: row_ext::get(row, "id")?,
            email: row_ext::get(row, "email")?,
            password_hash: row_ext::get(row, "password_hash")?,
            name: row_ext::get(row, "name")?,
            avatar_url: row_ext::get(row, "avatar_url")?,
            email_verified_at: row_ext::get(row, "email_verified_at")?,
            disabled: row_ext::get::<i64>(row, "disabled")? != 0,
            created_at: row_ext::get(row, "created_at")?,
        })
    }

    fn map_session_row(row: &libsql::Row) -> Result<SessionRecord> {
        Ok(SessionRecord {
            id: row_ext::get(row, "id")?,
            user_id: row_ext::get(row, "user_id")?,
            created_at: row_ext::get(row, "created_at")?,
            expires_at: row_ext::get(row, "expires_at")?,
        })
    }

    fn map_connected_account(row: &libsql::Row) -> Result<ConnectedAccountRecord> {
        Ok(ConnectedAccountRecord {
            id: row_ext::get(row, "id")?,
            user_id: row_ext::get(row, "user_id")?,
            provider: row_ext::get(row, "provider")?,
            provider_account_id: row_ext::get(row, "provider_account_id")?,
            scope: row_ext::get(row, "scope")?,
            access_token: row_ext::get(row, "access_token")?,
            refresh_token: row_ext::get(row, "refresh_token")?,
            expires_at: row_ext::get(row, "expires_at")?,
            created_at: row_ext::get(row, "created_at")?,
            updated_at: row_ext::get(row, "updated_at")?,
        })
    }
}

#[async_trait]
impl UserRepository for LibsqlUserRepository {
    async fn create_user(&self, params: CreateUserParams) -> Result<UserRecord> {
        let CreateUserParams {
            id,
            email,
            password_hash,
            name,
            created_at,
        } = params;
        let id_ref = id.as_str();
        let email_ref = email.as_str();
        let password_hash_ref = password_hash.as_str();

        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO users (id, email, password_hash, created_at, name)
             VALUES (?, ?, ?, ?, ?)",
            params![
                id_ref,
                email_ref,
                password_hash_ref,
                created_at,
                name.as_deref()
            ],
        )
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
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, email, password_hash, name, avatar_url, email_verified_at, disabled, created_at
                 FROM users WHERE email = ?",
                params![email],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::map_user_row(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn fetch_user_by_id(&self, id: &str) -> Result<Option<UserRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, email, password_hash, name, avatar_url, email_verified_at, disabled, created_at
                 FROM users WHERE id = ?",
                params![id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::map_user_row(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn fetch_users_by_ids(&self, ids: &[String]) -> Result<Vec<UserRecord>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut sql = String::from(
            "SELECT id, email, password_hash, name, avatar_url, email_verified_at, disabled, created_at
             FROM users WHERE id IN (",
        );
        for (idx, _) in ids.iter().enumerate() {
            if idx > 0 {
                sql.push_str(", ");
            }
            sql.push('?');
        }
        sql.push(')');

        let bindings = ids.iter().cloned().collect::<Vec<_>>();
        let conn = self.pool.get().await?;
        let mut rows = conn.query(&sql, params_from_iter(bindings)).await?;

        let mut users = Vec::new();
        while let Some(row) = rows.next().await? {
            users.push(Self::map_user_row(&row)?);
        }
        Ok(users)
    }

    async fn create_session(&self, params: CreateSessionParams) -> Result<SessionRecord> {
        let CreateSessionParams {
            id,
            user_id,
            created_at,
            expires_at,
        } = params;
        let session_id_ref = id.as_str();
        let user_id_ref = user_id.as_str();

        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO sessions (id, user_id, created_at, expires_at)
             VALUES (?, ?, ?, ?)",
            params![session_id_ref, user_id_ref, created_at, expires_at],
        )
        .await?;

        Ok(SessionRecord {
            id,
            user_id,
            created_at,
            expires_at,
        })
    }

    async fn fetch_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, user_id, created_at, expires_at FROM sessions WHERE id = ?",
                params![session_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::map_session_row(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn delete_session(&self, session_id: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute("DELETE FROM sessions WHERE id = ?", params![session_id])
            .await?;
        Ok(())
    }

    async fn delete_sessions_by_user(&self, user_id: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute("DELETE FROM sessions WHERE user_id = ?", params![user_id])
            .await?;
        Ok(())
    }

    async fn update_password_hash(&self, user_id: &str, password_hash: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            params![password_hash, user_id],
        )
        .await?;
        Ok(())
    }

    async fn fetch_connected_account(
        &self,
        provider: &str,
        provider_account_id: &str,
    ) -> Result<Option<ConnectedAccountRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, user_id, provider, provider_account_id, scope, access_token, refresh_token, expires_at, created_at, updated_at
                 FROM user_connected_accounts
                 WHERE provider = ? AND provider_account_id = ?",
                params![provider, provider_account_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::map_connected_account(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn fetch_connected_account_by_id(
        &self,
        account_id: &str,
    ) -> Result<Option<ConnectedAccountRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, user_id, provider, provider_account_id, scope, access_token, refresh_token, expires_at, created_at, updated_at
                 FROM user_connected_accounts WHERE id = ?",
                params![account_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::map_connected_account(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn create_connected_account(
        &self,
        id: String,
        input: NewConnectedAccount<'_>,
        timestamp: i64,
    ) -> Result<ConnectedAccountRecord> {
        let id_ref = id.as_str();
        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO user_connected_accounts (
                 id, user_id, provider, provider_account_id, scope,
                 access_token, refresh_token, expires_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                id_ref,
                input.user_id,
                input.provider,
                input.provider_account_id,
                input.scope,
                input.access_token,
                input.refresh_token,
                input.expires_at,
                timestamp,
                timestamp
            ],
        )
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
        let conn = self.pool.get().await?;
        let changed = conn
            .execute(
                "UPDATE user_connected_accounts
                 SET scope = ?, access_token = ?, refresh_token = ?, expires_at = ?, updated_at = ?
                 WHERE id = ?",
                params![
                    tokens.scope,
                    tokens.access_token,
                    tokens.refresh_token,
                    tokens.expires_at,
                    timestamp,
                    account_id
                ],
            )
            .await?;

        if changed == 0 {
            Err(anyhow!("connected account not found: {account_id}"))
        } else {
            Ok(())
        }
    }

    async fn delete_connected_account(&self, account_id: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "DELETE FROM user_connected_accounts WHERE id = ?",
            params![account_id],
        )
        .await?;
        Ok(())
    }

    async fn expire_session(&self, session_id: &str, expires_at: i64) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "UPDATE sessions SET expires_at = ? WHERE id = ?",
            params![expires_at, session_id],
        )
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
        let mut sql = String::from(
            "SELECT id, email, password_hash, name, avatar_url, email_verified_at, disabled, created_at FROM users",
        );
        let mut conditions = Vec::new();
        let mut bindings: Vec<libsql::Value> = Vec::new();

        if let Some(term) = keyword {
            let like = like_pattern(term);
            conditions.push("(LOWER(email) LIKE ? OR LOWER(COALESCE(name, '')) LIKE ? )");
            bindings.push(like.clone().into());
            bindings.push(like.into());
        }

        if let Some(cursor) = cursor {
            conditions.push("(created_at < ? OR (created_at = ? AND id < ?))");
            bindings.push(cursor.created_at.into());
            bindings.push(cursor.created_at.into());
            bindings.push(cursor.id.clone().into());
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        sql.push_str(" ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?");
        bindings.push(limit.into());
        bindings.push(skip.max(0).into());

        let conn = self.pool.get().await?;
        let mut rows = conn.query(&sql, params_from_iter(bindings)).await?;

        let mut users = Vec::new();
        while let Some(row) = rows.next().await? {
            users.push(Self::map_user_row(&row)?);
        }
        Ok(users)
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

        let name_ref = name.and_then(|value| value);
        let avatar_ref = avatar_url.and_then(|value| value);

        let conn = self.pool.get().await?;
        conn.execute(
            "UPDATE users SET
                 name = CASE WHEN ?1 THEN ?2 ELSE name END,
                 avatar_url = CASE WHEN ?3 THEN ?4 ELSE avatar_url END
             WHERE id = ?5",
            params![
                bool_to_i64(name_ref.is_some()),
                name_ref,
                bool_to_i64(avatar_ref.is_some()),
                avatar_ref,
                user_id
            ],
        )
        .await?;
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

        let name_ref = name.and_then(|value| value);

        let conn = self.pool.get().await?;
        conn.execute(
            "UPDATE users SET
                 email = CASE WHEN ?1 THEN ?2 ELSE email END,
                 name = CASE WHEN ?3 THEN ?4 ELSE name END
             WHERE id = ?5",
            params![
                bool_to_i64(email.is_some()),
                email,
                bool_to_i64(name_ref.is_some()),
                name_ref,
                user_id
            ],
        )
        .await?;
        Ok(())
    }

    async fn set_email_verified_at(&self, user_id: &str, timestamp: Option<i64>) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "UPDATE users SET email_verified_at = ? WHERE id = ?",
            params![timestamp, user_id],
        )
        .await?;
        Ok(())
    }

    async fn count_users(&self, keyword: Option<&str>) -> Result<i64> {
        let conn = self.pool.get().await?;
        if let Some(term) = keyword {
            let like = like_pattern(term);
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) AS count FROM users
                     WHERE LOWER(email) LIKE ? OR LOWER(COALESCE(name, '')) LIKE ?",
                    params![like.clone(), like],
                )
                .await?;

            if let Some(row) = rows.next().await? {
                return Ok(row_ext::get(&row, "count")?);
            }
            return Ok(0);
        }

        let mut rows = conn
            .query("SELECT COUNT(*) AS count FROM users", params![])
            .await?;
        if let Some(row) = rows.next().await? {
            Ok(row_ext::get(&row, "count")?)
        } else {
            Ok(0)
        }
    }

    async fn upsert_admin_user(&self, user_id: &str, timestamp: i64) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO admin_users (user_id, created_at)
             VALUES (?, ?)
             ON CONFLICT(user_id) DO UPDATE SET created_at = excluded.created_at",
            params![user_id, timestamp],
        )
        .await?;
        Ok(())
    }

    async fn is_admin_user(&self, user_id: &str) -> Result<bool> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT 1 FROM admin_users WHERE user_id = ?",
                params![user_id],
            )
            .await?;
        Ok(rows.next().await?.is_some())
    }

    async fn set_user_disabled(&self, user_id: &str, disabled: bool) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "UPDATE users SET disabled = ? WHERE id = ?",
            params![bool_to_i64(disabled), user_id],
        )
        .await?;
        Ok(())
    }
}

fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}
