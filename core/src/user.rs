use anyhow::{Result, anyhow};
use chrono::Utc;
use uuid::Uuid;

use crate::db::{
    Database,
    user_repo::{CreateSessionParams, CreateUserParams, UserRepositoryRef},
};

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

#[derive(Debug, Clone)]
pub struct ConnectedAccountRecord {
    pub id: String,
    pub user_id: String,
    pub provider: String,
    pub provider_account_id: String,
    pub scope: Option<String>,
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub expires_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug)]
pub struct NewConnectedAccount<'a> {
    pub user_id: &'a str,
    pub provider: &'a str,
    pub provider_account_id: &'a str,
    pub scope: Option<&'a str>,
    pub access_token: Option<&'a str>,
    pub refresh_token: Option<&'a str>,
    pub expires_at: Option<i64>,
}

#[derive(Debug)]
pub struct ConnectedAccountTokens<'a> {
    pub scope: Option<&'a str>,
    pub access_token: Option<&'a str>,
    pub refresh_token: Option<&'a str>,
    pub expires_at: Option<i64>,
}

#[derive(Clone)]
pub struct UserStore {
    user_repo: UserRepositoryRef,
}

impl UserStore {
    pub fn new(database: &Database) -> Self {
        Self {
            user_repo: database.repositories().user_repo(),
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

        self.user_repo
            .create_user(CreateUserParams {
                id,
                email: email.to_owned(),
                password_hash: password_hash.to_owned(),
                name: name.map(|value| value.to_owned()),
                created_at,
            })
            .await
    }

    pub async fn find_by_email(&self, email: &str) -> Result<Option<UserRecord>> {
        self.user_repo.fetch_user_by_email(email).await
    }

    pub async fn find_by_id(&self, id: &str) -> Result<Option<UserRecord>> {
        self.user_repo.fetch_user_by_id(id).await
    }

    pub async fn find_by_ids(&self, ids: &[String]) -> Result<Vec<UserRecord>> {
        self.user_repo.fetch_users_by_ids(ids).await
    }

    pub async fn create_session(&self, user_id: &str) -> Result<SessionRecord> {
        let id = Uuid::new_v4().to_string();
        let created_at = Utc::now().timestamp();
        let expires_at = created_at + SESSION_TTL_SECONDS;

        self.user_repo
            .create_session(CreateSessionParams {
                id,
                user_id: user_id.to_owned(),
                created_at,
                expires_at,
            })
            .await
    }

    pub async fn update_password(&self, user_id: &str, password_hash: &str) -> Result<()> {
        self.user_repo
            .update_password_hash(user_id, password_hash)
            .await
    }

    pub async fn find_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        let row = self.user_repo.fetch_session(session_id).await?;

        let now = Utc::now().timestamp();

        if let Some(record) = row {
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
        self.user_repo.delete_session(session_id).await
    }

    pub async fn delete_sessions_by_user(&self, user_id: &str) -> Result<()> {
        self.user_repo.delete_sessions_by_user(user_id).await
    }

    pub async fn get_connected_account(
        &self,
        provider: &str,
        provider_account_id: &str,
    ) -> Result<Option<ConnectedAccountRecord>> {
        self.user_repo
            .fetch_connected_account(provider, provider_account_id)
            .await
    }

    pub async fn create_connected_account(
        &self,
        input: NewConnectedAccount<'_>,
    ) -> Result<ConnectedAccountRecord> {
        let id = Uuid::new_v4().to_string();
        let timestamp = Utc::now().timestamp();
        self.user_repo
            .create_connected_account(id, input, timestamp)
            .await
    }

    pub async fn update_connected_account_tokens(
        &self,
        account_id: &str,
        tokens: ConnectedAccountTokens<'_>,
    ) -> Result<ConnectedAccountRecord> {
        let timestamp = Utc::now().timestamp();
        self.user_repo
            .update_connected_account_tokens(account_id, tokens, timestamp)
            .await?;

        self.user_repo
            .fetch_connected_account_by_id(account_id)
            .await?
            .ok_or_else(|| anyhow!("connected account not found"))
    }

    pub async fn delete_connected_account(&self, account_id: &str) -> Result<()> {
        self.user_repo.delete_connected_account(account_id).await
    }

    pub async fn refresh_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        // Reduce write amplification by only extending the session TTL when it is actually
        // approaching expiry. Most requests do not need to bump the expiry if plenty of TTL is
        // left. This avoids an UPDATE on every request (a hotspot in SQLite/WAL).
        if let Some(mut record) = self.find_session(session_id).await? {
            let now = Utc::now().timestamp();
            let remaining = record.expires_at.saturating_sub(now);
            // Only refresh when less than this threshold remains. Start conservative at 25% of TTL.
            let refresh_threshold = SESSION_TTL_SECONDS / 4;
            if remaining <= refresh_threshold {
                record.expires_at = now + SESSION_TTL_SECONDS;
                self.user_repo
                    .expire_session(session_id, record.expires_at)
                    .await?;
            }
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
        self.user_repo
            .list_users_paginated(skip, limit, keyword, cursor)
            .await
    }

    pub async fn update_profile(
        &self,
        user_id: &str,
        name: Option<Option<String>>,
        avatar_url: Option<Option<String>>,
    ) -> Result<UserRecord> {
        if name.is_none() && avatar_url.is_none() {
            return self
                .find_by_id(user_id)
                .await?
                .ok_or_else(|| anyhow!("user not found"));
        }

        let name_ref = name.as_ref().map(|value| value.as_deref());
        let avatar_ref = avatar_url.as_ref().map(|value| value.as_deref());

        self.user_repo
            .update_user_profile(user_id, name_ref, avatar_ref)
            .await?;

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
        if email.is_none() && name.is_none() {
            return self
                .find_by_id(user_id)
                .await?
                .ok_or_else(|| anyhow!("user not found"));
        }

        self.user_repo
            .update_user_account(user_id, email, name)
            .await?;

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

        self.user_repo
            .set_email_verified_at(user_id, timestamp)
            .await?;

        self.find_by_id(user_id)
            .await?
            .ok_or_else(|| anyhow!("user not found"))
    }

    pub async fn count(&self, keyword: Option<&str>) -> Result<i64> {
        self.user_repo.count_users(keyword).await
    }

    pub async fn add_admin(&self, user_id: &str) -> Result<()> {
        let now = Utc::now().timestamp();
        self.user_repo.upsert_admin_user(user_id, now).await
    }

    pub async fn is_admin(&self, user_id: &str) -> Result<bool> {
        self.user_repo.is_admin_user(user_id).await
    }

    pub async fn set_disabled(&self, user_id: &str, disabled: bool) -> Result<()> {
        self.user_repo.set_user_disabled(user_id, disabled).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{AppConfig, BlobStoreBackend, DocDataBackend},
        db::Database,
    };
    use tempfile::TempDir;
    use uuid::Uuid;

    async fn setup_database() -> anyhow::Result<(Database, TempDir)> {
        let temp_dir = tempfile::tempdir()?;
        let mut config = AppConfig::default();
        config.database_path = temp_dir.path().to_string_lossy().to_string();
        config.doc_data_backend = DocDataBackend::Sqlite;
        config.doc_data_path = temp_dir
            .path()
            .join("doc-kv")
            .to_string_lossy()
            .into_owned();
        config.blob_store_backend = BlobStoreBackend::Sql;
        config.blob_store_path = temp_dir
            .path()
            .join("blob-store")
            .to_string_lossy()
            .into_owned();

        let database = Database::connect(&config).await?;
        Ok((database, temp_dir))
    }

    #[tokio::test]
    async fn list_paginated_returns_most_recent_first() -> anyhow::Result<()> {
        let (database, _temp_dir) = setup_database().await?;
        let store = UserStore::new(&database);

        let user_repo = database.repositories().user_repo();
        let rows = vec![
            ("alice@example.com", 1_000_i64),
            ("bob@example.com", 2_000_i64),
            ("carol@example.com", 3_000_i64),
        ];

        for (email, created_at) in &rows {
            user_repo
                .create_user(CreateUserParams {
                    id: Uuid::new_v4().to_string(),
                    email: (*email).to_string(),
                    password_hash: "hash".to_string(),
                    name: None,
                    created_at: *created_at,
                })
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

        Ok(())
    }

    #[tokio::test]
    async fn list_paginated_supports_keyword() -> anyhow::Result<()> {
        let (database, _temp_dir) = setup_database().await?;
        let store = UserStore::new(&database);
        let user_repo = database.repositories().user_repo();
        let entries = vec![
            ("alice@example.com", Some("Alice Smith")),
            ("bob@example.com", Some("Robert Brown")),
            ("carol@sample.com", None),
        ];

        for (email, name) in &entries {
            user_repo
                .create_user(CreateUserParams {
                    id: Uuid::new_v4().to_string(),
                    email: (*email).to_string(),
                    password_hash: "hash".to_string(),
                    name: name.map(|value| value.to_string()),
                    created_at: 1_000_i64,
                })
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

        Ok(())
    }

    #[tokio::test]
    async fn list_paginated_supports_cursor() -> anyhow::Result<()> {
        let (database, _temp_dir) = setup_database().await?;
        let store = UserStore::new(&database);
        let user_repo = database.repositories().user_repo();

        let rows = vec![
            ("alice@example.com", 1_000_i64),
            ("bob@example.com", 2_000_i64),
            ("carol@example.com", 3_000_i64),
        ];

        for (email, created_at) in &rows {
            user_repo
                .create_user(CreateUserParams {
                    id: Uuid::new_v4().to_string(),
                    email: (*email).to_string(),
                    password_hash: "hash".to_string(),
                    name: None,
                    created_at: *created_at,
                })
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

        Ok(())
    }

    #[tokio::test]
    async fn connected_account_lifecycle_roundtrips() -> anyhow::Result<()> {
        let (database, _temp_dir) = setup_database().await?;
        let store = UserStore::new(&database);

        let user = store
            .create("oauth@example.com", "", Some("OAuth User"))
            .await?;

        let created = store
            .create_connected_account(NewConnectedAccount {
                user_id: &user.id,
                provider: "github",
                provider_account_id: "acct-1",
                scope: Some("user"),
                access_token: Some("token-1"),
                refresh_token: None,
                expires_at: Some(1_000),
            })
            .await?;

        assert_eq!(created.provider, "github");
        assert_eq!(created.provider_account_id, "acct-1");
        assert_eq!(created.access_token.as_deref(), Some("token-1"));

        let fetched = store
            .get_connected_account("github", "acct-1")
            .await?
            .expect("connected account present");
        assert_eq!(fetched.id, created.id);
        assert_eq!(fetched.user_id, user.id);

        let updated = store
            .update_connected_account_tokens(
                &created.id,
                ConnectedAccountTokens {
                    scope: None,
                    access_token: Some("token-2"),
                    refresh_token: Some("refresh-2"),
                    expires_at: Some(2_000),
                },
            )
            .await?;

        assert_eq!(updated.access_token.as_deref(), Some("token-2"));
        assert_eq!(updated.refresh_token.as_deref(), Some("refresh-2"));
        assert_eq!(updated.expires_at, Some(2_000));

        store.delete_connected_account(&created.id).await?;
        assert!(
            store
                .get_connected_account("github", "acct-1")
                .await?
                .is_none()
        );

        Ok(())
    }
}
