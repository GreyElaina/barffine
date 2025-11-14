use anyhow::Result;
use async_trait::async_trait;
use deadpool_libsql::{
    Pool,
    libsql::{self, params},
};

use crate::access_token::AccessTokenRecord;
use crate::db::access_token_repo::AccessTokenRepository;
use crate::db::libsql::row_ext;

fn map_row(row: libsql::Row, include_token: bool) -> Result<AccessTokenRecord> {
    Ok(AccessTokenRecord {
        id: row_ext::get(&row, "id")?,
        user_id: row_ext::get(&row, "user_id")?,
        name: row_ext::get(&row, "name")?,
        token: if include_token {
            Some(row_ext::get(&row, "token")?)
        } else {
            None
        },
        created_at: row_ext::get(&row, "created_at")?,
        expires_at: row_ext::get(&row, "expires_at")?,
    })
}

pub struct LibsqlAccessTokenRepository {
    pool: Pool,
}

impl LibsqlAccessTokenRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AccessTokenRepository for LibsqlAccessTokenRepository {
    async fn list(&self, user_id: &str) -> Result<Vec<AccessTokenRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, user_id, name, created_at, expires_at \
                 FROM access_tokens WHERE user_id = ? ORDER BY created_at DESC",
                params![user_id],
            )
            .await?;

        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(AccessTokenRecord {
                id: row_ext::get(&row, "id")?,
                user_id: row_ext::get(&row, "user_id")?,
                name: row_ext::get(&row, "name")?,
                token: None,
                created_at: row_ext::get(&row, "created_at")?,
                expires_at: row_ext::get(&row, "expires_at")?,
            });
        }
        Ok(result)
    }

    async fn list_revealed(&self, user_id: &str) -> Result<Vec<AccessTokenRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, user_id, name, token, created_at, expires_at \
                 FROM access_tokens WHERE user_id = ? ORDER BY created_at DESC",
                params![user_id],
            )
            .await?;

        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(map_row(row, true)?);
        }
        Ok(result)
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
        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO access_tokens (id, user_id, name, token, created_at, expires_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
            params![id, user_id, name, token, created_at, expires_at],
        )
        .await?;
        Ok(())
    }

    async fn delete(&self, id: &str, user_id: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "DELETE FROM access_tokens WHERE id = ? AND user_id = ?",
            params![id, user_id],
        )
        .await?;
        Ok(())
    }

    async fn find_active_by_token(
        &self,
        token: &str,
        now_timestamp: i64,
    ) -> Result<Option<AccessTokenRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT id, user_id, name, token, created_at, expires_at \
                 FROM access_tokens \
                WHERE token = ? AND (expires_at IS NULL OR expires_at > ?)",
                params![token, now_timestamp],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(map_row(row, true)?))
        } else {
            Ok(None)
        }
    }
}
