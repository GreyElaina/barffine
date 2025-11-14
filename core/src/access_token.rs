use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

use crate::db::{Database, access_token_repo::AccessTokenRepositoryRef};

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
    repo: AccessTokenRepositoryRef,
}

impl AccessTokenStore {
    pub fn new(database: &Database) -> Self {
        Self {
            repo: database.repositories().access_token_repo(),
        }
    }

    pub async fn list(&self, user_id: &str) -> Result<Vec<AccessTokenRecord>> {
        self.repo.list(user_id).await
    }

    pub async fn list_revealed(&self, user_id: &str) -> Result<Vec<AccessTokenRecord>> {
        self.repo.list_revealed(user_id).await
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

        self.repo
            .insert(&id, user_id, name, &token, created_at, expires_at)
            .await?;

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
        self.repo.delete(id, user_id).await
    }

    pub async fn find_active_by_token(&self, token: &str) -> Result<Option<AccessTokenRecord>> {
        let now = Utc::now().timestamp();
        self.repo.find_active_by_token(token, now).await
    }
}

fn generate_token() -> String {
    let mut token = format!("ut_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    token.truncate(40);
    token
}
