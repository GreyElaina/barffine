use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::db::{Database, comment_attachment_repo::CommentAttachmentRepositoryRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommentAttachmentRecord {
    pub workspace_id: String,
    pub doc_id: String,
    pub key: String,
    pub name: String,
    pub mime: String,
    pub size: i64,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CommentAttachmentUpsert<'a> {
    pub workspace_id: &'a str,
    pub doc_id: &'a str,
    pub key: &'a str,
    pub name: &'a str,
    pub mime: &'a str,
    pub size: i64,
    pub created_by: Option<&'a str>,
}

#[derive(Clone)]
pub struct CommentAttachmentStore {
    repo: CommentAttachmentRepositoryRef,
}

impl CommentAttachmentStore {
    pub fn new(database: &Database) -> Self {
        Self {
            repo: database.repositories().comment_attachment_repo(),
        }
    }

    pub async fn upsert(
        &self,
        input: CommentAttachmentUpsert<'_>,
    ) -> Result<CommentAttachmentRecord> {
        self.repo.upsert(input).await
    }

    pub async fn get(
        &self,
        workspace_id: &str,
        doc_id: &str,
        key: &str,
    ) -> Result<Option<CommentAttachmentRecord>> {
        self.repo.get(workspace_id, doc_id, key).await
    }

    pub async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<CommentAttachmentRecord>> {
        self.repo.list_for_doc(workspace_id, doc_id).await
    }

    pub async fn delete(&self, workspace_id: &str, doc_id: &str, key: &str) -> Result<()> {
        self.repo.delete(workspace_id, doc_id, key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{AppConfig, BlobStoreBackend, DocDataBackend},
        db::{user_repo::CreateUserParams, workspace_repo::CreateWorkspaceParams},
        doc_store::DocumentStore,
    };
    use chrono::Utc;
    use tempfile::TempDir;

    async fn setup_store() -> (TempDir, Database, CommentAttachmentStore) {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut config = AppConfig::default();
        let db_path = temp_dir.path().join("comment_attachments.db");
        config.database_path = db_path.to_string_lossy().into_owned();
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

        let database = Database::connect(&config).await.expect("connect database");

        let now = Utc::now().timestamp();
        let repos = database.repositories();
        repos
            .user_repo()
            .create_user(CreateUserParams {
                id: "user".to_string(),
                email: "user@example.com".to_string(),
                password_hash: "password-hash".to_string(),
                name: None,
                created_at: now,
            })
            .await
            .expect("seed user");

        repos
            .workspace_repo()
            .create_workspace(CreateWorkspaceParams {
                id: "workspace".to_string(),
                owner_id: "user".to_string(),
                name: "Test Workspace".to_string(),
                created_at: now,
                public: false,
                enable_ai: true,
                enable_doc_embedding: true,
                enable_url_preview: false,
                avatar_key: None,
                indexed: false,
                last_check_embeddings: 0,
            })
            .await
            .expect("seed workspace");

        let doc_store = DocumentStore::new(&database);
        doc_store
            .ensure_doc_record("workspace", "doc", "user", Some("Doc"))
            .await
            .expect("seed document");

        let store = CommentAttachmentStore::new(&database);
        (temp_dir, database, store)
    }

    #[tokio::test]
    async fn upsert_and_get_comment_attachment() {
        let (_dir, _db, store) = setup_store().await;

        let record = store
            .upsert(CommentAttachmentUpsert {
                workspace_id: "workspace",
                doc_id: "doc",
                key: "key",
                name: "file.txt",
                mime: "text/plain",
                size: 42,
                created_by: Some("user"),
            })
            .await
            .expect("upsert attachment");

        assert_eq!(record.workspace_id, "workspace");
        assert_eq!(record.doc_id, "doc");
        assert_eq!(record.key, "key");
        assert_eq!(record.name, "file.txt");
        assert_eq!(record.mime, "text/plain");
        assert_eq!(record.size, 42);
        assert_eq!(record.created_by.as_deref(), Some("user"));

        let fetched = store
            .get("workspace", "doc", "key")
            .await
            .expect("get attachment");
        assert!(fetched.is_some());
    }

    #[tokio::test]
    async fn list_comment_attachments_orders_by_created_at() {
        let (_dir, _db, store) = setup_store().await;

        store
            .upsert(CommentAttachmentUpsert {
                workspace_id: "workspace",
                doc_id: "doc",
                key: "a",
                name: "a.txt",
                mime: "text/plain",
                size: 1,
                created_by: None,
            })
            .await
            .expect("insert a");

        store
            .upsert(CommentAttachmentUpsert {
                workspace_id: "workspace",
                doc_id: "doc",
                key: "b",
                name: "b.txt",
                mime: "text/plain",
                size: 2,
                created_by: None,
            })
            .await
            .expect("insert b");

        let records = store
            .list_for_doc("workspace", "doc")
            .await
            .expect("list attachments");

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, "a");
        assert_eq!(records[1].key, "b");
    }

    #[tokio::test]
    async fn delete_attachment_removes_record() {
        let (_dir, _db, store) = setup_store().await;

        store
            .upsert(CommentAttachmentUpsert {
                workspace_id: "workspace",
                doc_id: "doc",
                key: "delete",
                name: "file",
                mime: "text/plain",
                size: 10,
                created_by: None,
            })
            .await
            .expect("insert");

        store
            .delete("workspace", "doc", "delete")
            .await
            .expect("delete");

        let record = store.get("workspace", "doc", "delete").await.expect("get");
        assert!(record.is_none());
    }
}
