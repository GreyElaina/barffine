use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use tracing::debug;

use crate::{
    blob::{
        AVATAR_STORAGE_NAMESPACE, BlobDescriptor, BlobDownload, BlobLocation, BlobMetadata,
        BlobScope, BlobStorage, ListedBlobRecord,
    },
    db::{Database, blob_repo::BlobRepositoryRef},
};

fn scope_from_descriptor(descriptor: &BlobDescriptor) -> BlobScope {
    if descriptor.workspace_id == AVATAR_STORAGE_NAMESPACE {
        BlobScope::namespace(descriptor.workspace_id.clone())
    } else {
        BlobScope::workspace(descriptor.workspace_id.clone())
    }
}

fn workspace_scope(workspace_id: &str) -> BlobScope {
    BlobScope::workspace(workspace_id.to_owned())
}

/// SQL-backed blob store that keeps bytes in the same database as the rest of the app state.
///
/// 该实现通过 [`BlobRepositoryRef`] 工作，因此可以复用到
/// SQLite / Postgres 等所有 SQL 数据库后端。
pub struct SqlBlobStorage {
    blob_repo: BlobRepositoryRef,
}

impl SqlBlobStorage {
    pub fn new(database: &Database) -> Self {
        Self {
            blob_repo: database.repositories().blob_repo(),
        }
    }

    fn now_timestamp() -> i64 {
        Utc::now().timestamp()
    }
}

#[async_trait]
impl BlobStorage for SqlBlobStorage {
    async fn put(
        &self,
        descriptor: &BlobDescriptor,
        content: &[u8],
        mut metadata: BlobMetadata,
    ) -> Result<BlobLocation> {
        if metadata.content_length.is_none() {
            metadata.content_length = Some(content.len() as u64);
        }

        if metadata.last_modified.is_none() {
            metadata.last_modified = Some(Utc::now());
        }

        let now = Self::now_timestamp();

        let scope = scope_from_descriptor(descriptor);
        self.blob_repo
            .upsert_blob(&scope, &descriptor.key, content, &metadata, now)
            .await?;

        Ok(BlobLocation::new(
            format!(
                "/api/workspaces/{}/blobs/{}",
                descriptor.workspace_id, descriptor.key
            ),
            None,
        ))
    }

    async fn get(
        &self,
        descriptor: &BlobDescriptor,
        _prefer_presigned: bool,
    ) -> Result<Option<BlobDownload>> {
        let scope = scope_from_descriptor(descriptor);
        let record = self.blob_repo.fetch_blob(&scope, &descriptor.key).await?;

        let Some(record) = record else {
            debug!(
                workspace_id = %descriptor.workspace_id,
                blob_key = %descriptor.key,
                "blob not found in sql blob storage"
            );
            return Ok(None);
        };

        Ok(Some(BlobDownload::from_bytes(
            record.metadata,
            record.bytes,
        )))
    }

    async fn delete(&self, descriptor: &BlobDescriptor, permanently: bool) -> Result<()> {
        let scope = scope_from_descriptor(descriptor);
        self.blob_repo
            .delete_blob(&scope, &descriptor.key, permanently, Self::now_timestamp())
            .await?;
        Ok(())
    }

    async fn presign_download(
        &self,
        descriptor: &BlobDescriptor,
        _ttl: Duration,
    ) -> Result<BlobLocation> {
        Ok(BlobLocation::new(
            format!(
                "/api/workspaces/{}/blobs/{}",
                descriptor.workspace_id, descriptor.key
            ),
            None,
        ))
    }

    async fn release_deleted(&self, workspace_id: &str) -> Result<()> {
        self.blob_repo
            .release_deleted(&workspace_scope(workspace_id))
            .await?;
        Ok(())
    }

    async fn list(&self, workspace_id: &str) -> Result<Vec<ListedBlobRecord>> {
        self.blob_repo
            .list_blobs(&workspace_scope(workspace_id))
            .await
    }

    async fn total_size(&self, workspace_id: &str) -> Result<i64> {
        self.blob_repo
            .total_size(&workspace_scope(workspace_id))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        blob::{AVATAR_STORAGE_NAMESPACE, BlobDescriptor, BlobMetadata},
        config::{AppConfig, BlobStoreBackend, DocDataBackend},
        db::{Database, user_repo::CreateUserParams, workspace_repo::CreateWorkspaceParams},
        ids::{UserId, WorkspaceId},
    };
    use tempfile::TempDir;

    async fn create_sql_store() -> (TempDir, Database, SqlBlobStorage) {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut config = AppConfig::default();
        config.database_path = temp_dir
            .path()
            .join("blobs.db")
            .to_string_lossy()
            .into_owned();
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

        let database = Database::connect(&config).await.expect("connect db");

        let now = Utc::now().timestamp();
        let repos = database.repositories();
        repos
            .user_repo()
            .create_user(CreateUserParams {
                id: "owner".to_string(),
                email: "owner@example.com".to_string(),
                password_hash: "hash".to_string(),
                name: None,
                created_at: now,
            })
            .await
            .expect("insert user");

        repos
            .workspace_repo()
            .create_workspace(CreateWorkspaceParams {
                id: WorkspaceId::from("workspace"),
                owner_id: UserId::from("owner"),
                name: "Workspace".to_string(),
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
            .expect("insert workspace");

        let store = SqlBlobStorage::new(&database);
        (temp_dir, database, store)
    }

    #[tokio::test]
    async fn sqlite_blob_storage_roundtrip() {
        let (_dir, _db, store) = create_sql_store().await;
        let descriptor = BlobDescriptor::new("workspace", "logo.png");
        let mut metadata = BlobMetadata::default();
        metadata.content_type = Some("image/png".into());
        metadata.last_modified = Some(Utc::now());

        let bytes = b"png-bytes".to_vec();
        store
            .put(&descriptor, &bytes, metadata.clone())
            .await
            .expect("store blob");

        let download = store
            .get(&descriptor, false)
            .await
            .expect("fetch blob")
            .expect("blob exists");
        assert_eq!(download.bytes.unwrap(), bytes);
        assert_eq!(
            download.metadata.unwrap().content_type,
            metadata.content_type
        );

        let listed = store.list("workspace").await.expect("list blobs");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].key, "logo.png");
    }

    #[tokio::test]
    async fn sqlite_blob_storage_soft_delete_and_release() {
        let (_dir, _db, store) = create_sql_store().await;
        let descriptor = BlobDescriptor::new("workspace", "doc.bin");
        let mut metadata = BlobMetadata::default();
        metadata.content_type = Some("application/octet-stream".into());

        store
            .put(&descriptor, b"bytes", metadata)
            .await
            .expect("store blob");

        store
            .delete(&descriptor, false)
            .await
            .expect("soft delete blob");
        assert!(
            store
                .get(&descriptor, false)
                .await
                .expect("fetch soft deleted")
                .is_none()
        );

        store
            .release_deleted("workspace")
            .await
            .expect("release deleted");

        let listed = store
            .list("workspace")
            .await
            .expect("list blobs after purge");
        assert!(listed.is_empty());
    }

    #[tokio::test]
    async fn sqlite_blob_storage_supports_namespace_storage() {
        let (_dir, _db, store) = create_sql_store().await;
        let descriptor = BlobDescriptor::new(AVATAR_STORAGE_NAMESPACE, "avatar.bin");
        let mut metadata = BlobMetadata::default();
        metadata.content_type = Some("image/png".into());

        let bytes = b"avatar-bytes".to_vec();
        store
            .put(&descriptor, &bytes, metadata.clone())
            .await
            .expect("store namespace blob");

        let download = store
            .get(&descriptor, false)
            .await
            .expect("fetch namespace blob")
            .expect("blob exists");
        assert_eq!(download.bytes.unwrap(), bytes);
        assert_eq!(
            download.metadata.unwrap().content_type,
            metadata.content_type
        );
    }
}
