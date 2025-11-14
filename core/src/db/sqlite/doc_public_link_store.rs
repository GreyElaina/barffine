use anyhow::Result;
use async_trait::async_trait;
use sqlx::Row;

use crate::db::doc_public_link_store::{DocPublicLinkRecord, DocPublicLinkStore};

use super::connection::SqlitePool;

pub struct SqliteDocPublicLinkStore {
    pool: SqlitePool,
}

impl SqliteDocPublicLinkStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DocPublicLinkStore for SqliteDocPublicLinkStore {
    async fn insert_link(
        &self,
        workspace_id: &str,
        doc_id: &str,
        token: &str,
        created_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO doc_public_links (workspace_id, doc_id, token, created_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(workspace_id, doc_id) DO NOTHING",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(token)
        .bind(created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_link(&self, workspace_id: &str, doc_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM doc_public_links WHERE workspace_id = ? AND doc_id = ?")
            .bind(workspace_id)
            .bind(doc_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_links_for_user(&self, user_id: &str) -> Result<Vec<DocPublicLinkRecord>> {
        let rows = sqlx::query(
            "SELECT l.workspace_id, l.doc_id, l.token, l.created_at
             FROM doc_public_links l
             JOIN workspace_members wm
               ON wm.workspace_id = l.workspace_id
             WHERE wm.user_id = ? AND wm.status = 'Accepted'",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| DocPublicLinkRecord {
                workspace_id: row.get::<String, _>("workspace_id"),
                doc_id: row.get::<String, _>("doc_id"),
                token: row.get::<String, _>("token"),
                created_at: row.get::<i64, _>("created_at"),
            })
            .collect())
    }
}
