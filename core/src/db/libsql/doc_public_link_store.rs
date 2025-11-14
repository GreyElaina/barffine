use anyhow::Result;
use async_trait::async_trait;

use crate::db::{
    doc_public_link_store::{DocPublicLinkRecord, DocPublicLinkStore},
    libsql::{pool::LibsqlPool, row_ext},
};

use libsql::params;

pub struct LibsqlDocPublicLinkStore {
    pool: LibsqlPool,
}

impl LibsqlDocPublicLinkStore {
    pub fn new(pool: LibsqlPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DocPublicLinkStore for LibsqlDocPublicLinkStore {
    async fn insert_link(
        &self,
        workspace_id: &str,
        doc_id: &str,
        token: &str,
        created_at: i64,
    ) -> Result<()> {
        let tx = self.pool.begin_transaction().await?;
        tx.execute(
            "INSERT INTO doc_public_links (workspace_id, doc_id, token, created_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(workspace_id, doc_id) DO NOTHING",
            params![workspace_id, doc_id, token, created_at],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn delete_link(&self, workspace_id: &str, doc_id: &str) -> Result<()> {
        let tx = self.pool.begin_transaction().await?;
        tx.execute(
            "DELETE FROM doc_public_links WHERE workspace_id = ? AND doc_id = ?",
            params![workspace_id, doc_id],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_links_for_user(&self, user_id: &str) -> Result<Vec<DocPublicLinkRecord>> {
        let conn = self.pool.pool().get().await?;
        let mut rows = conn
            .query(
                "SELECT l.workspace_id, l.doc_id, l.token, l.created_at
                 FROM doc_public_links l
                 JOIN workspace_members wm
                   ON wm.workspace_id = l.workspace_id
                 WHERE wm.user_id = ? AND wm.status = 'Accepted'",
                params![user_id],
            )
            .await?;

        let mut records = Vec::new();
        while let Some(row) = rows.next().await? {
            records.push(DocPublicLinkRecord {
                workspace_id: row_ext::get(&row, "workspace_id")?,
                doc_id: row_ext::get(&row, "doc_id")?,
                token: row_ext::get(&row, "token")?,
                created_at: row_ext::get(&row, "created_at")?,
            });
        }
        Ok(records)
    }
}
