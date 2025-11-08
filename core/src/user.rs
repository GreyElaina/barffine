use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use sqlx::{Pool, QueryBuilder, Row, Sqlite, sqlite::SqliteRow};
use uuid::Uuid;

use crate::db::Database;

pub const SESSION_TTL_SECONDS: i64 = 60 * 60 * 24 * 14;

#[derive(Debug, Clone)]
pub struct UserRecord {
    pub id: String,
    pub email: String,
    pub password_hash: String,
    pub name: Option<String>,
    pub avatar_url: Option<String>,
    pub email_verified_at: Option<i64>,
    pub disabled: bool,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserCursor {
    pub created_at: i64,
    pub id: String,
}

#[derive(Debug, Clone)]
pub struct SessionRecord {
    pub id: String,
    pub user_id: String,
    pub created_at: i64,
    pub expires_at: i64,
}

impl SessionRecord {
    pub fn is_expired(&self, now: i64) -> bool {
        self.expires_at <= now
    }
}

#[derive(Clone)]
pub struct UserStore {
    pool: Pool<Sqlite>,
}

impl UserStore {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: database.pool().clone(),
        }
    }

    pub async fn create(
        &self,
        email: &str,
        password_hash: &str,
        name: Option<&str>,
    ) -> Result<UserRecord> {
        let id = Uuid::new_v4().to_string();
        let created_at = Utc::now().timestamp();

        sqlx::query(
            "INSERT INTO users (id, email, password_hash, created_at, name) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(email)
        .bind(password_hash)
        .bind(created_at)
        .bind(name)
        .execute(&self.pool)
        .await
        .with_context(|| "failed to insert user".to_string())?;

        Ok(UserRecord {
            id,
            email: email.to_owned(),
            password_hash: password_hash.to_owned(),
            name: name.map(|value| value.to_owned()),
            avatar_url: None,
            email_verified_at: None,
            disabled: false,
            created_at,
        })
    }

    pub async fn find_by_email(&self, email: &str) -> Result<Option<UserRecord>> {
        let row = sqlx::query(
            "SELECT id, email, password_hash, name, avatar_url, email_verified_at, disabled, created_at \
             FROM users WHERE email = ?",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_row))
    }

    pub async fn find_by_id(&self, id: &str) -> Result<Option<UserRecord>> {
        let row = sqlx::query(
            "SELECT id, email, password_hash, name, avatar_url, email_verified_at, disabled, created_at \
             FROM users WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_row))
    }

    pub async fn find_by_ids(&self, ids: &[String]) -> Result<Vec<UserRecord>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut builder = QueryBuilder::new(
            "SELECT id, email, password_hash, name, avatar_url, email_verified_at, disabled, created_at FROM users WHERE id IN (",
        );
        let mut separated = builder.separated(", ");
        for id in ids {
            separated.push_bind(id);
        }
        builder.push(")");

        let rows = builder.build().fetch_all(&self.pool).await?;

        Ok(rows.into_iter().map(Self::map_row).collect())
    }

    pub async fn create_session(&self, user_id: &str) -> Result<SessionRecord> {
        let id = Uuid::new_v4().to_string();
        let created_at = Utc::now().timestamp();
        let expires_at = created_at + SESSION_TTL_SECONDS;

        sqlx::query(
            "INSERT INTO sessions (id, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(user_id)
        .bind(created_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;

        Ok(SessionRecord {
            id,
            user_id: user_id.to_owned(),
            created_at,
            expires_at,
        })
    }

    pub async fn update_password(&self, user_id: &str, password_hash: &str) -> Result<()> {
        sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
            .bind(password_hash)
            .bind(user_id)
            .execute(&self.pool)
            .await?
            .rows_affected();

        Ok(())
    }

    pub async fn find_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        let row =
            sqlx::query("SELECT id, user_id, created_at, expires_at FROM sessions WHERE id = ?")
                .bind(session_id)
                .fetch_optional(&self.pool)
                .await?;

        let now = Utc::now().timestamp();

        let record = row.map(|row| SessionRecord {
            id: row.get("id"),
            user_id: row.get("user_id"),
            created_at: row.get::<i64, _>("created_at"),
            expires_at: row.get::<i64, _>("expires_at"),
        });

        if let Some(record) = record {
            if record.is_expired(now) {
                self.delete_session(&record.id).await?;
                Ok(None)
            } else {
                Ok(Some(record))
            }
        } else {
            Ok(None)
        }
    }

    pub async fn delete_session(&self, session_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM sessions WHERE id = ?")
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_sessions_by_user(&self, user_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM sessions WHERE user_id = ?")
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn refresh_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        if let Some(mut record) = self.find_session(session_id).await? {
            record.expires_at = Utc::now().timestamp() + SESSION_TTL_SECONDS;
            sqlx::query("UPDATE sessions SET expires_at = ? WHERE id = ?")
                .bind(record.expires_at)
                .bind(session_id)
                .execute(&self.pool)
                .await?;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    pub async fn list_paginated(
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
        Ok(rows.into_iter().map(Self::map_row).collect())
    }

    pub async fn update_profile(
        &self,
        user_id: &str,
        name: Option<Option<String>>,
        avatar_url: Option<Option<String>>,
    ) -> Result<UserRecord> {
        let name_value = name.clone();
        let avatar_value = avatar_url.clone();

        match (name_value, avatar_value) {
            (None, None) => {
                return self
                    .find_by_id(user_id)
                    .await?
                    .ok_or_else(|| anyhow!("user not found"));
            }
            (Some(name_opt), None) => {
                sqlx::query("UPDATE users SET name = ? WHERE id = ?")
                    .bind(name_opt.as_deref())
                    .bind(user_id)
                    .execute(&self.pool)
                    .await?;
            }
            (None, Some(avatar_opt)) => {
                sqlx::query("UPDATE users SET avatar_url = ? WHERE id = ?")
                    .bind(avatar_opt.as_deref())
                    .bind(user_id)
                    .execute(&self.pool)
                    .await?;
            }
            (Some(name_opt), Some(avatar_opt)) => {
                sqlx::query("UPDATE users SET name = ?, avatar_url = ? WHERE id = ?")
                    .bind(name_opt.as_deref())
                    .bind(avatar_opt.as_deref())
                    .bind(user_id)
                    .execute(&self.pool)
                    .await?;
            }
        }

        self.find_by_id(user_id)
            .await?
            .ok_or_else(|| anyhow!("user not found"))
    }

    pub async fn update_account(
        &self,
        user_id: &str,
        email: Option<&str>,
        name: Option<Option<&str>>,
    ) -> Result<UserRecord> {
        match (email, name) {
            (Some(email_value), Some(name_value)) => {
                sqlx::query("UPDATE users SET email = ?, name = ? WHERE id = ?")
                    .bind(email_value)
                    .bind(name_value)
                    .bind(user_id)
                    .execute(&self.pool)
                    .await?
            }
            (Some(email_value), None) => {
                sqlx::query("UPDATE users SET email = ? WHERE id = ?")
                    .bind(email_value)
                    .bind(user_id)
                    .execute(&self.pool)
                    .await?
            }
            (None, Some(name_value)) => {
                sqlx::query("UPDATE users SET name = ? WHERE id = ?")
                    .bind(name_value)
                    .bind(user_id)
                    .execute(&self.pool)
                    .await?
            }
            (None, None) => {
                return self
                    .find_by_id(user_id)
                    .await?
                    .ok_or_else(|| anyhow!("user not found"));
            }
        };

        self.find_by_id(user_id)
            .await?
            .ok_or_else(|| anyhow!("user not found"))
    }

    pub async fn set_email_verified(&self, user_id: &str, verified: bool) -> Result<UserRecord> {
        let timestamp = if verified {
            Some(Utc::now().timestamp())
        } else {
            None
        };

        sqlx::query("UPDATE users SET email_verified_at = ? WHERE id = ?")
            .bind(timestamp)
            .bind(user_id)
            .execute(&self.pool)
            .await?;

        self.find_by_id(user_id)
            .await?
            .ok_or_else(|| anyhow!("user not found"))
    }

    pub async fn count(&self, keyword: Option<&str>) -> Result<i64> {
        if let Some(term) = keyword {
            let like = format!("%{}%", term.to_ascii_lowercase());
            let row = sqlx::query(
                "SELECT COUNT(*) as count FROM users \
                 WHERE LOWER(email) LIKE ? OR LOWER(COALESCE(name, '')) LIKE ?",
            )
            .bind(&like)
            .bind(&like)
            .fetch_one(&self.pool)
            .await?;
            return Ok(row.get::<i64, _>("count"));
        }

        let row = sqlx::query("SELECT COUNT(*) as count FROM users")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>("count"))
    }

    pub async fn add_admin(&self, user_id: &str) -> Result<()> {
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO admin_users (user_id, created_at)
             VALUES (?, ?)
             ON CONFLICT(user_id) DO UPDATE SET created_at = excluded.created_at",
        )
        .bind(user_id)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn is_admin(&self, user_id: &str) -> Result<bool> {
        let exists: Option<i64> = sqlx::query_scalar("SELECT 1 FROM admin_users WHERE user_id = ?")
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(exists.is_some())
    }
}

impl UserStore {
    fn map_row(row: SqliteRow) -> UserRecord {
        let disabled = row.get::<i64, _>("disabled") != 0;
        UserRecord {
            id: row.get("id"),
            email: row.get("email"),
            password_hash: row.get("password_hash"),
            name: row.get::<Option<String>, _>("name"),
            avatar_url: row.get::<Option<String>, _>("avatar_url"),
            email_verified_at: row.get::<Option<i64>, _>("email_verified_at"),
            disabled,
            created_at: row.get::<i64, _>("created_at"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::AppConfig, db::Database};
    use std::path::PathBuf;
    use uuid::Uuid;

    async fn setup_database() -> anyhow::Result<(Database, PathBuf)> {
        let mut config = AppConfig::default();
        let db_path =
            std::env::temp_dir().join(format!("barffine-user-tests-{}.db", Uuid::new_v4()));
        config.database_path = db_path.to_string_lossy().to_string();

        let database = Database::connect(&config).await?;
        sqlx::migrate!("../server/migrations")
            .run(database.pool())
            .await?;
        Ok((database, db_path))
    }

    #[tokio::test]
    async fn list_paginated_returns_most_recent_first() -> anyhow::Result<()> {
        let (database, db_path) = setup_database().await?;
        let store = UserStore::new(&database);

        let pool = database.pool().clone();
        let rows = vec![
            ("alice@example.com", 1_000_i64),
            ("bob@example.com", 2_000_i64),
            ("carol@example.com", 3_000_i64),
        ];

        for (email, created_at) in &rows {
            sqlx::query(
                "INSERT INTO users (id, email, password_hash, created_at) VALUES (?, ?, ?, ?)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(email)
            .bind("hash")
            .bind(created_at)
            .execute(&pool)
            .await?;
        }

        let page = store.list_paginated(0, 2, None, None).await?;
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].email, "carol@example.com");
        assert_eq!(page[1].email, "bob@example.com");

        let tail = store.list_paginated(2, 2, None, None).await?;
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].email, "alice@example.com");

        let total = store.count(None).await?;
        assert_eq!(total, rows.len() as i64);

        drop(store);
        drop(database);
        if let Err(err) = std::fs::remove_file(&db_path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(err.into());
            }
        }

        for suffix in ["db-wal", "db-shm"] {
            let sidecar = db_path.with_extension(suffix);
            let _ = std::fs::remove_file(sidecar);
        }

        Ok(())
    }

    #[tokio::test]
    async fn list_paginated_supports_keyword() -> anyhow::Result<()> {
        let (database, db_path) = setup_database().await?;
        let store = UserStore::new(&database);
        let pool = database.pool().clone();

        let entries = vec![
            ("alice@example.com", Some("Alice Smith")),
            ("bob@example.com", Some("Robert Brown")),
            ("carol@sample.com", None),
        ];

        for (email, name) in &entries {
            sqlx::query(
                "INSERT INTO users (id, email, password_hash, created_at, name, disabled) \
                 VALUES (?, ?, ?, ?, ?, 0)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(email)
            .bind("hash")
            .bind(1_000_i64)
            .bind(name)
            .execute(&pool)
            .await?;
        }

        let results = store.list_paginated(0, 10, Some("alice"), None).await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].email, "alice@example.com");

        let results = store.list_paginated(0, 10, Some("robert"), None).await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].email, "bob@example.com");

        let results = store.list_paginated(0, 10, Some("@sample"), None).await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].email, "carol@sample.com");

        let total = store.count(Some("example")).await?;
        assert_eq!(total, 2);

        drop(store);
        drop(database);
        if let Err(err) = std::fs::remove_file(&db_path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(err.into());
            }
        }

        for suffix in ["db-wal", "db-shm"] {
            let sidecar = db_path.with_extension(suffix);
            let _ = std::fs::remove_file(sidecar);
        }

        Ok(())
    }

    #[tokio::test]
    async fn list_paginated_supports_cursor() -> anyhow::Result<()> {
        let (database, db_path) = setup_database().await?;
        let store = UserStore::new(&database);
        let pool = database.pool().clone();

        let rows = vec![
            ("alice@example.com", 1_000_i64),
            ("bob@example.com", 2_000_i64),
            ("carol@example.com", 3_000_i64),
        ];

        for (email, created_at) in &rows {
            sqlx::query(
                "INSERT INTO users (id, email, password_hash, created_at) VALUES (?, ?, ?, ?)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(email)
            .bind("hash")
            .bind(created_at)
            .execute(&pool)
            .await?;
        }

        let first_page = store.list_paginated(0, 2, None, None).await?;
        assert_eq!(first_page.len(), 2);
        let cursor = super::UserCursor {
            created_at: first_page.last().unwrap().created_at,
            id: first_page.last().unwrap().id.clone(),
        };

        let second_page = store.list_paginated(0, 2, None, Some(&cursor)).await?;
        assert_eq!(second_page.len(), 1);
        assert!(second_page[0].created_at <= cursor.created_at);
        assert_ne!(second_page[0].id, cursor.id);

        drop(store);
        drop(database);
        if let Err(err) = std::fs::remove_file(&db_path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(err.into());
            }
        }

        for suffix in ["db-wal", "db-shm"] {
            let sidecar = db_path.with_extension(suffix);
            let _ = std::fs::remove_file(sidecar);
        }

        Ok(())
    }
}
