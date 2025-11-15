use std::sync::Arc;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use rocksdb::{Direction, IteratorMode, WriteBatch};

use crate::db::{
    doc_public_link_store::{DocPublicLinkRecord, DocPublicLinkStore},
    workspace_repo::WorkspaceRepositoryRef,
};

use super::doc_data::DocDataStore;

pub struct RocksDocPublicLinkStore {
    store: Arc<DocDataStore>,
    workspace_repo: WorkspaceRepositoryRef,
}

impl RocksDocPublicLinkStore {
    pub fn new(store: Arc<DocDataStore>, workspace_repo: WorkspaceRepositoryRef) -> Self {
        Self {
            store,
            workspace_repo,
        }
    }

    fn doc_key(workspace_id: &str, doc_id: &str) -> String {
        format!("plink:doc:{workspace_id}:{doc_id}")
    }

    fn doc_prefix(workspace_id: &str) -> String {
        format!("plink:doc:{workspace_id}:")
    }

    fn token_key(token: &str) -> String {
        format!("plink:token:{token}")
    }

    fn get_doc_record(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocPublicLinkRecord>> {
        let key = Self::doc_key(workspace_id, doc_id);
        let value = self
            .store
            .db()
            .get_cf(self.store.doc_public_links_cf(), key.as_bytes())
            .context("failed to read Rocks doc public link record")?;
        Ok(match value {
            Some(bytes) => Some(
                serde_json::from_slice(&bytes)
                    .context("failed to decode Rocks doc public link record")?,
            ),
            None => None,
        })
    }

    fn save_doc_record(&self, record: &DocPublicLinkRecord, batch: &mut WriteBatch) -> Result<()> {
        let key = Self::doc_key(&record.workspace_id, &record.doc_id);
        let value =
            serde_json::to_vec(record).context("failed to encode Rocks doc public link record")?;
        batch.put_cf(self.store.doc_public_links_cf(), key.as_bytes(), &value);
        Ok(())
    }

    fn put_token_mapping(
        &self,
        token: &str,
        workspace_id: &str,
        doc_id: &str,
        batch: &mut WriteBatch,
    ) {
        let token_key = Self::token_key(token);
        let doc_key = Self::doc_key(workspace_id, doc_id);
        batch.put_cf(
            self.store.doc_public_links_cf(),
            token_key.as_bytes(),
            doc_key.as_bytes(),
        );
    }

    fn delete_token_mapping(&self, token: &str, batch: &mut WriteBatch) {
        let token_key = Self::token_key(token);
        batch.delete_cf(self.store.doc_public_links_cf(), token_key.as_bytes());
    }

    fn validate_token_available(
        &self,
        token: &str,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<()> {
        let existing = self
            .store
            .db()
            .get_cf(
                self.store.doc_public_links_cf(),
                Self::token_key(token).as_bytes(),
            )
            .context("failed to read Rocks doc public link token mapping")?;
        if let Some(bytes) = existing {
            let existing_key = String::from_utf8(bytes.to_vec())
                .context("invalid UTF-8 in Rocks doc public link token mapping")?;
            let expected = Self::doc_key(workspace_id, doc_id);
            if existing_key != expected {
                bail!("share token already assigned to another document");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl DocPublicLinkStore for RocksDocPublicLinkStore {
    async fn insert_link(
        &self,
        workspace_id: &str,
        doc_id: &str,
        token: &str,
        created_at: i64,
    ) -> Result<()> {
        self.validate_token_available(token, workspace_id, doc_id)?;

        let mut batch = WriteBatch::default();
        if let Some(existing) = self.get_doc_record(workspace_id, doc_id)? {
            if existing.token != token {
                self.delete_token_mapping(&existing.token, &mut batch);
            }
        }

        let record = DocPublicLinkRecord {
            workspace_id: workspace_id.to_owned(),
            doc_id: doc_id.to_owned(),
            token: token.to_owned(),
            created_at,
        };
        self.save_doc_record(&record, &mut batch)?;
        self.put_token_mapping(token, workspace_id, doc_id, &mut batch);

        self.store
            .db()
            .write(batch)
            .context("failed to write Rocks doc public link record")
    }

    async fn delete_link(&self, workspace_id: &str, doc_id: &str) -> Result<()> {
        let Some(existing) = self.get_doc_record(workspace_id, doc_id)? else {
            return Ok(());
        };

        let mut batch = WriteBatch::default();
        batch.delete_cf(
            self.store.doc_public_links_cf(),
            Self::doc_key(workspace_id, doc_id).as_bytes(),
        );
        self.delete_token_mapping(&existing.token, &mut batch);

        self.store
            .db()
            .write(batch)
            .context("failed to delete Rocks doc public link record")
    }

    async fn list_links_for_user(&self, user_id: &str) -> Result<Vec<DocPublicLinkRecord>> {
        let memberships = self
            .workspace_repo
            .list_memberships_for_user(user_id)
            .await?
            .into_iter()
            .map(|membership| membership.workspace_id)
            .collect::<Vec<_>>();

        if memberships.is_empty() {
            return Ok(Vec::new());
        }

        let cf = self.store.doc_public_links_cf();
        let db = self.store.db();
        let mut records = Vec::new();
        for workspace_id in memberships {
            let prefix = Self::doc_prefix(&workspace_id);
            let iter = db.iterator_cf(
                cf,
                IteratorMode::From(prefix.as_bytes(), Direction::Forward),
            );
            for item in iter {
                let (key, value) = item?;
                if !key.starts_with(prefix.as_bytes()) {
                    break;
                }
                let record: DocPublicLinkRecord = serde_json::from_slice(&value)
                    .context("failed to decode Rocks doc public link record")?;
                records.push(record);
            }
        }

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::sqlite::workspace_repo::SqliteWorkspaceRepository;
    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::TempDir;

    async fn setup_store() -> (TempDir, RocksDocPublicLinkStore, sqlx::SqlitePool) {
        let dir = TempDir::new().expect("create temp dir");
        let store = Arc::new(DocDataStore::open(dir.path().join("doc-kv")).unwrap());
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::query(
            "CREATE TABLE workspaces (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL,
                 owner_id TEXT NOT NULL,
                 created_at BIGINT NOT NULL
             )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE workspace_members (
                 workspace_id TEXT NOT NULL,
                 user_id TEXT NOT NULL,
                 status TEXT NOT NULL,
                 role TEXT NOT NULL
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let workspace_repo =
            Arc::new(SqliteWorkspaceRepository::new(pool.clone())) as WorkspaceRepositoryRef;
        let link_store = RocksDocPublicLinkStore::new(store, workspace_repo);
        (dir, link_store, pool)
    }

    async fn insert_membership(pool: &sqlx::SqlitePool, workspace_id: &str, user_id: &str) {
        sqlx::query("INSERT INTO workspaces (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)")
            .bind(workspace_id)
            .bind("Demo")
            .bind("owner")
            .bind(0_i64)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, status, role)
             VALUES (?, ?, 'Accepted', 'member')",
        )
        .bind(workspace_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn insert_and_list_links() {
        let (_dir, store, pool) = setup_store().await;
        insert_membership(&pool, "ws", "user-1").await;

        store
            .insert_link("ws", "doc-1", "token-1", 123)
            .await
            .unwrap();
        let links = store.list_links_for_user("user-1").await.unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].doc_id, "doc-1");

        store.delete_link("ws", "doc-1").await.unwrap();
        let empty = store.list_links_for_user("user-1").await.unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn enforces_unique_tokens() {
        let (_dir, store, pool) = setup_store().await;
        insert_membership(&pool, "ws", "user-1").await;

        store
            .insert_link("ws", "doc-1", "token-1", 1)
            .await
            .unwrap();
        let err = store
            .insert_link("ws", "doc-2", "token-1", 2)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("share token"));
    }
}
