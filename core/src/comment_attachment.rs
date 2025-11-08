use std::sync::Arc;

use anyhow::{Result, anyhow};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Row, Sqlite};

use crate::db::Database;

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
pub struct SqliteCommentAttachmentStore {
    pool: Arc<Pool<Sqlite>>,
}

impl SqliteCommentAttachmentStore {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: Arc::new(database.pool().clone()),
        }
    }

    pub async fn upsert(
        &self,
        input: CommentAttachmentUpsert<'_>,
    ) -> Result<CommentAttachmentRecord> {
        let CommentAttachmentUpsert {
            workspace_id,
            doc_id,
            key,
            name,
            mime,
            size,
            created_by,
        } = input;

        let created_at = Utc::now().timestamp();

        sqlx::query(
            "INSERT INTO comment_attachments (workspace_id, doc_id, key, name, mime, size, created_at, created_by)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(workspace_id, doc_id, key) DO UPDATE SET name = excluded.name, mime = excluded.mime, size = excluded.size",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(key)
        .bind(name)
        .bind(mime)
        .bind(size)
        .bind(created_at)
        .bind(created_by)
        .execute(&*self.pool)
        .await?;

        self.get(workspace_id, doc_id, key)
            .await?
            .ok_or_else(|| anyhow!("comment attachment not found after upsert"))
    }

    pub async fn get(
        &self,
        workspace_id: &str,
        doc_id: &str,
        key: &str,
    ) -> Result<Option<CommentAttachmentRecord>> {
        let row = sqlx::query(
            "SELECT workspace_id, doc_id, key, name, mime, size, created_at, created_by
             FROM comment_attachments
             WHERE workspace_id = ? AND doc_id = ? AND key = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(key)
        .fetch_optional(&*self.pool)
        .await?;

        Ok(row.map(map_record))
    }

    pub async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<CommentAttachmentRecord>> {
        let rows = sqlx::query(
            "SELECT workspace_id, doc_id, key, name, mime, size, created_at, created_by
             FROM comment_attachments
             WHERE workspace_id = ? AND doc_id = ?
             ORDER BY created_at ASC",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_all(&*self.pool)
        .await?;

        Ok(rows.into_iter().map(map_record).collect())
    }

    pub async fn delete(&self, workspace_id: &str, doc_id: &str, key: &str) -> Result<()> {
        sqlx::query(
            "DELETE FROM comment_attachments WHERE workspace_id = ? AND doc_id = ? AND key = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(key)
        .execute(&*self.pool)
        .await?;
        Ok(())
    }
}

fn map_record(row: sqlx::sqlite::SqliteRow) -> CommentAttachmentRecord {
    let timestamp = row.get::<i64, _>("created_at");
    let created_at = Utc
        .timestamp_opt(timestamp, 0)
        .single()
        .unwrap_or_else(Utc::now);

    CommentAttachmentRecord {
        workspace_id: row.get("workspace_id"),
        doc_id: row.get("doc_id"),
        key: row.get("key"),
        name: row.get("name"),
        mime: row.get("mime"),
        size: row.get("size"),
        created_at,
        created_by: row.get::<Option<String>, _>("created_by"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use chrono::Utc;
    use tempfile::TempDir;

    async fn setup_store() -> (TempDir, Database, SqliteCommentAttachmentStore) {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut config = AppConfig::default();
        let db_path = temp_dir.path().join("comment_attachments.db");
        config.database_path = db_path.to_string_lossy().into_owned();

        let database = Database::connect(&config).await.expect("connect database");
        sqlx::migrate!("../server/migrations")
            .run(database.pool())
            .await
            .expect("run migrations");

        let now = Utc::now().timestamp();
        sqlx::query("INSERT INTO users (id, email, password_hash, created_at) VALUES (?, ?, ?, ?)")
            .bind("user")
            .bind("user@example.com")
            .bind("password-hash")
            .bind(now)
            .execute(database.pool())
            .await
            .expect("seed user");

        sqlx::query("INSERT INTO workspaces (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)")
            .bind("workspace")
            .bind("Test Workspace")
            .bind("user")
            .bind(now)
            .execute(database.pool())
            .await
            .expect("seed workspace");

        sqlx::query(
            "INSERT INTO documents (id, workspace_id, snapshot, created_at, updated_at, default_role, public, mode, title, summary, creator_id, updater_id)
             VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)",
        )
        .bind("doc")
        .bind("workspace")
        .bind(Vec::<u8>::new())
        .bind(now)
        .bind(now)
        .bind("manager")
        .bind("page")
        .bind::<Option<String>>(None)
        .bind::<Option<String>>(None)
        .bind::<Option<String>>(None)
        .bind::<Option<String>>(None)
        .execute(database.pool())
        .await
        .expect("seed document");

        let store = SqliteCommentAttachmentStore::new(&database);
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
