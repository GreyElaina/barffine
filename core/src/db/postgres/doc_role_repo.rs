use anyhow::Result;
use async_trait::async_trait;
use sqlx::{Pool, Postgres, Row, postgres::PgRow};

use crate::{
    db::doc_role_repo::DocRoleRepository,
    doc_roles::{DocumentRoleCursor, DocumentRoleRecord},
};

pub struct PostgresDocRoleRepository {
    pool: Pool<Postgres>,
}

impl PostgresDocRoleRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    fn map_row(row: PgRow) -> DocumentRoleRecord {
        DocumentRoleRecord {
            workspace_id: row.get("workspace_id"),
            doc_id: row.get("doc_id"),
            user_id: row.get("user_id"),
            role: row.get("role"),
            created_at: row.get::<i64, _>("created_at"),
        }
    }
}

#[async_trait]
impl DocRoleRepository for PostgresDocRoleRepository {
    async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let rows = pg_query!(
            "SELECT workspace_id, doc_id, user_id, role, created_at
             FROM doc_user_roles
             WHERE workspace_id = ? AND doc_id = ?
             ORDER BY created_at DESC, user_id ASC",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_row).collect())
    }

    async fn find_for_user(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
    ) -> Result<Option<DocumentRoleRecord>> {
        let row = pg_query!(
            "SELECT workspace_id, doc_id, user_id, role, created_at
             FROM doc_user_roles
             WHERE workspace_id = ? AND doc_id = ? AND user_id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_row))
    }

    async fn paginate_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentRoleCursor>,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let rows = if let Some(cursor) = cursor {
            pg_query!(
                "SELECT workspace_id, doc_id, user_id, role, created_at
                 FROM doc_user_roles
                 WHERE workspace_id = ?
                   AND doc_id = ?
                   AND (created_at > ? OR (created_at = ? AND user_id > ?))
                 ORDER BY created_at ASC, user_id ASC
                 LIMIT ?",
            )
            .bind(workspace_id)
            .bind(doc_id)
            .bind(cursor.created_at)
            .bind(cursor.created_at)
            .bind(&cursor.user_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            pg_query!(
                "SELECT workspace_id, doc_id, user_id, role, created_at
                 FROM doc_user_roles
                 WHERE workspace_id = ? AND doc_id = ?
                 ORDER BY created_at ASC, user_id ASC
                 LIMIT ? OFFSET ?",
            )
            .bind(workspace_id)
            .bind(doc_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows.into_iter().map(Self::map_row).collect())
    }

    async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64> {
        let count: i64 = pg_query_scalar!(
            "SELECT COUNT(*) as count FROM doc_user_roles WHERE workspace_id = ? AND doc_id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    async fn owners_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let rows = pg_query!(
            "SELECT workspace_id, doc_id, user_id, role, created_at
             FROM doc_user_roles
             WHERE workspace_id = ? AND doc_id = ? AND LOWER(role) = 'owner'
             ORDER BY created_at DESC, user_id ASC",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_row).collect())
    }

    async fn upsert_roles(&self, roles: &[DocumentRoleRecord]) -> Result<()> {
        if roles.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        for role in roles {
            pg_query!(
                "INSERT INTO doc_user_roles (workspace_id, doc_id, user_id, role, created_at)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(workspace_id, doc_id, user_id)
                 DO UPDATE SET role = excluded.role, created_at = excluded.created_at",
            )
            .bind(&role.workspace_id)
            .bind(&role.doc_id)
            .bind(&role.user_id)
            .bind(&role.role)
            .bind(role.created_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn upsert_role(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
        role: &str,
        created_at: i64,
    ) -> Result<()> {
        pg_query!(
            "INSERT INTO doc_user_roles (workspace_id, doc_id, user_id, role, created_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(workspace_id, doc_id, user_id)
             DO UPDATE SET role = excluded.role, created_at = excluded.created_at",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(user_id)
        .bind(role)
        .bind(created_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn remove_role(&self, workspace_id: &str, doc_id: &str, user_id: &str) -> Result<()> {
        pg_query!(
            "DELETE FROM doc_user_roles WHERE workspace_id = ? AND doc_id = ? AND user_id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
