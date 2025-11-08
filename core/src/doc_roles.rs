use anyhow::Result;
use chrono::Utc;
use sqlx::{Pool, Row, Sqlite};

use crate::db::Database;

#[derive(Debug, Clone)]
pub struct DocumentRoleRecord {
    pub workspace_id: String,
    pub doc_id: String,
    pub user_id: String,
    pub role: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct DocumentRoleCursor {
    pub created_at: i64,
    pub user_id: String,
}

#[derive(Clone)]
pub struct DocumentRoleStore {
    pool: Pool<Sqlite>,
}

impl DocumentRoleStore {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: database.pool().clone(),
        }
    }

    pub async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let rows = sqlx::query(
            "SELECT workspace_id, doc_id, user_id, role, created_at
             FROM doc_user_roles
             WHERE workspace_id = ? AND doc_id = ?
             ORDER BY created_at DESC, user_id ASC",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| DocumentRoleRecord {
                workspace_id: row.get("workspace_id"),
                doc_id: row.get("doc_id"),
                user_id: row.get("user_id"),
                role: row.get("role"),
                created_at: row.get::<i64, _>("created_at"),
            })
            .collect())
    }

    pub async fn find_for_user(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
    ) -> Result<Option<DocumentRoleRecord>> {
        let row = sqlx::query(
            "SELECT workspace_id, doc_id, user_id, role, created_at
             FROM doc_user_roles
             WHERE workspace_id = ? AND doc_id = ? AND user_id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| DocumentRoleRecord {
            workspace_id: row.get("workspace_id"),
            doc_id: row.get("doc_id"),
            user_id: row.get("user_id"),
            role: row.get("role"),
            created_at: row.get::<i64, _>("created_at"),
        }))
    }

    pub async fn paginate_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentRoleCursor>,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let rows = if let Some(cursor) = cursor {
            sqlx::query(
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
            sqlx::query(
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

        Ok(rows
            .into_iter()
            .map(|row| DocumentRoleRecord {
                workspace_id: row.get("workspace_id"),
                doc_id: row.get("doc_id"),
                user_id: row.get("user_id"),
                role: row.get("role"),
                created_at: row.get::<i64, _>("created_at"),
            })
            .collect())
    }

    pub async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) as count FROM doc_user_roles WHERE workspace_id = ? AND doc_id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    pub async fn owners_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let rows = sqlx::query(
            "SELECT workspace_id, doc_id, user_id, role, created_at
             FROM doc_user_roles
             WHERE workspace_id = ? AND doc_id = ? AND LOWER(role) = 'owner'
             ORDER BY created_at DESC, user_id ASC",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| DocumentRoleRecord {
                workspace_id: row.get("workspace_id"),
                doc_id: row.get("doc_id"),
                user_id: row.get("user_id"),
                role: row.get("role"),
                created_at: row.get::<i64, _>("created_at"),
            })
            .collect())
    }

    pub async fn upsert(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
        role: &str,
    ) -> Result<()> {
        let created_at = Utc::now().timestamp();

        sqlx::query(
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

    pub async fn remove(&self, workspace_id: &str, doc_id: &str, user_id: &str) -> Result<()> {
        sqlx::query(
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
