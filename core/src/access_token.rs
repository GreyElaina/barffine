use anyhow::{Context, Result};
use chrono::Utc;
use sqlx::{Pool, Row, Sqlite};
use uuid::Uuid;

use crate::db::Database;

#[derive(Debug, Clone)]
pub struct AccessTokenRecord {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub token: Option<String>,
    pub created_at: i64,
    pub expires_at: Option<i64>,
}

#[derive(Clone)]
pub struct AccessTokenStore {
    pool: Pool<Sqlite>,
}

impl AccessTokenStore {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: database.pool().clone(),
        }
    }

    pub async fn list(&self, user_id: &str) -> Result<Vec<AccessTokenRecord>> {
        let rows = sqlx::query(
            "SELECT id, user_id, name, created_at, expires_at \
             FROM access_tokens WHERE user_id = ? ORDER BY created_at DESC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| "failed to list access tokens".to_string())?;

        Ok(rows
            .into_iter()
            .map(|row| AccessTokenRecord {
                id: row.get("id"),
                user_id: row.get("user_id"),
                name: row.get("name"),
                token: None,
                created_at: row.get::<i64, _>("created_at"),
                expires_at: row.get::<Option<i64>, _>("expires_at"),
            })
            .collect())
    }

    pub async fn list_revealed(&self, user_id: &str) -> Result<Vec<AccessTokenRecord>> {
        let rows = sqlx::query(
            "SELECT id, user_id, name, token, created_at, expires_at \
             FROM access_tokens WHERE user_id = ? ORDER BY created_at DESC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| "failed to list revealed access tokens".to_string())?;

        Ok(rows
            .into_iter()
            .map(|row| AccessTokenRecord {
                id: row.get("id"),
                user_id: row.get("user_id"),
                name: row.get("name"),
                token: Some(row.get("token")),
                created_at: row.get::<i64, _>("created_at"),
                expires_at: row.get::<Option<i64>, _>("expires_at"),
            })
            .collect())
    }

    pub async fn create(
        &self,
        user_id: &str,
        name: &str,
        expires_at: Option<i64>,
    ) -> Result<AccessTokenRecord> {
        let id = Uuid::new_v4().to_string();
        let token = generate_token();
        let created_at = Utc::now().timestamp();

        sqlx::query(
            "INSERT INTO access_tokens (id, user_id, name, token, created_at, expires_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(user_id)
        .bind(name)
        .bind(&token)
        .bind(created_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .with_context(|| "failed to create access token".to_string())?;

        Ok(AccessTokenRecord {
            id,
            user_id: user_id.to_owned(),
            name: name.to_owned(),
            token: Some(token),
            created_at,
            expires_at,
        })
    }

    pub async fn revoke(&self, id: &str, user_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM access_tokens WHERE id = ? AND user_id = ?")
            .bind(id)
            .bind(user_id)
            .execute(&self.pool)
            .await
            .with_context(|| "failed to revoke access token".to_string())?;

        Ok(())
    }

    pub async fn find_active_by_token(&self, token: &str) -> Result<Option<AccessTokenRecord>> {
        let now = Utc::now().timestamp();
        let row = sqlx::query(
            "SELECT id, user_id, name, token, created_at, expires_at \
             FROM access_tokens \
             WHERE token = ? AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(token)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| "failed to query access token".to_string())?;

        Ok(row.map(|row| AccessTokenRecord {
            id: row.get("id"),
            user_id: row.get("user_id"),
            name: row.get("name"),
            token: Some(row.get("token")),
            created_at: row.get::<i64, _>("created_at"),
            expires_at: row.get::<Option<i64>, _>("expires_at"),
        }))
    }
}

fn generate_token() -> String {
    let mut token = format!("ut_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    token.truncate(40);
    token
}
