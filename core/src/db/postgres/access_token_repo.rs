use anyhow::Result;
use async_trait::async_trait;
use sqlx::{Pool, Postgres, Row};

use crate::{access_token::AccessTokenRecord, db::access_token_repo::AccessTokenRepository};

pub struct PostgresAccessTokenRepository {
    pool: Pool<Postgres>,
}

impl PostgresAccessTokenRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AccessTokenRepository for PostgresAccessTokenRepository {
    async fn list(&self, user_id: &str) -> Result<Vec<AccessTokenRecord>> {
        let rows = pg_query!(
            "SELECT id, user_id, name, created_at, expires_at \
             FROM access_tokens WHERE user_id = ? ORDER BY created_at DESC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AccessTokenRecord {
                id: row.get("id"),
                user_id: row.get("user_id"),
                name: row.get("name"),
                token: None,
                created_at: row.get("created_at"),
                expires_at: row.get("expires_at"),
            })
            .collect())
    }

    async fn list_revealed(&self, user_id: &str) -> Result<Vec<AccessTokenRecord>> {
        let rows = pg_query!(
            "SELECT id, user_id, name, token, created_at, expires_at \
             FROM access_tokens WHERE user_id = ? ORDER BY created_at DESC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AccessTokenRecord {
                id: row.get("id"),
                user_id: row.get("user_id"),
                name: row.get("name"),
                token: Some(row.get("token")),
                created_at: row.get("created_at"),
                expires_at: row.get("expires_at"),
            })
            .collect())
    }

    async fn insert(
        &self,
        id: &str,
        user_id: &str,
        name: &str,
        token: &str,
        created_at: i64,
        expires_at: Option<i64>,
    ) -> Result<()> {
        pg_query!(
            "INSERT INTO access_tokens (id, user_id, name, token, created_at, expires_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(user_id)
        .bind(name)
        .bind(token)
        .bind(created_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, id: &str, user_id: &str) -> Result<()> {
        pg_query!("DELETE FROM access_tokens WHERE id = ? AND user_id = ?")
            .bind(id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn find_active_by_token(
        &self,
        token: &str,
        now_timestamp: i64,
    ) -> Result<Option<AccessTokenRecord>> {
        let row = pg_query!(
            "SELECT id, user_id, name, token, created_at, expires_at \
             FROM access_tokens \
             WHERE token = ? AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(token)
        .bind(now_timestamp)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| AccessTokenRecord {
            id: row.get("id"),
            user_id: row.get("user_id"),
            name: row.get("name"),
            token: Some(row.get("token")),
            created_at: row.get("created_at"),
            expires_at: row.get("expires_at"),
        }))
    }
}
