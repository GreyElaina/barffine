use anyhow::Result;
use async_trait::async_trait;
use deadpool_libsql::{
    Pool,
    libsql::{self, params},
};

use crate::db::doc_role_repo::DocRoleRepository;
use crate::db::libsql::row_ext;
use crate::doc_roles::{DocumentRoleCursor, DocumentRoleRecord};

pub struct LibsqlDocRoleRepository {
    pool: Pool,
}

impl LibsqlDocRoleRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn map_row(row: libsql::Row) -> Result<DocumentRoleRecord> {
        Ok(DocumentRoleRecord {
            workspace_id: row_ext::get(&row, "workspace_id")?,
            doc_id: row_ext::get(&row, "doc_id")?,
            user_id: row_ext::get(&row, "user_id")?,
            role: row_ext::get(&row, "role")?,
            created_at: row_ext::get(&row, "created_at")?,
        })
    }
}

#[async_trait]
impl DocRoleRepository for LibsqlDocRoleRepository {
    async fn list_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT workspace_id, doc_id, user_id, role, created_at
                 FROM doc_user_roles
                 WHERE workspace_id = ? AND doc_id = ?
                 ORDER BY created_at DESC, user_id ASC",
                params![workspace_id, doc_id],
            )
            .await?;

        let mut roles = Vec::new();
        while let Some(row) = rows.next().await? {
            roles.push(Self::map_row(row)?);
        }
        Ok(roles)
    }

    async fn find_for_user(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
    ) -> Result<Option<DocumentRoleRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT workspace_id, doc_id, user_id, role, created_at
                 FROM doc_user_roles
                 WHERE workspace_id = ? AND doc_id = ? AND user_id = ?",
                params![workspace_id, doc_id, user_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::map_row(row)?))
        } else {
            Ok(None)
        }
    }

    async fn paginate_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentRoleCursor>,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = if let Some(cursor) = cursor {
            conn.query(
                "SELECT workspace_id, doc_id, user_id, role, created_at
                 FROM doc_user_roles
                 WHERE workspace_id = ?
                   AND doc_id = ?
                   AND (created_at > ? OR (created_at = ? AND user_id > ?))
                 ORDER BY created_at ASC, user_id ASC
                 LIMIT ?",
                params![
                    workspace_id,
                    doc_id,
                    cursor.created_at,
                    cursor.created_at,
                    cursor.user_id.as_str(),
                    limit
                ],
            )
            .await?
        } else {
            conn.query(
                "SELECT workspace_id, doc_id, user_id, role, created_at
                 FROM doc_user_roles
                 WHERE workspace_id = ? AND doc_id = ?
                 ORDER BY created_at ASC, user_id ASC
                 LIMIT ? OFFSET ?",
                params![workspace_id, doc_id, limit, offset],
            )
            .await?
        };

        let mut roles = Vec::new();
        while let Some(row) = rows.next().await? {
            roles.push(Self::map_row(row)?);
        }
        Ok(roles)
    }

    async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT COUNT(*) as count FROM doc_user_roles WHERE workspace_id = ? AND doc_id = ?",
                params![workspace_id, doc_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(row_ext::get(&row, "count")?)
        } else {
            Ok(0)
        }
    }

    async fn owners_for_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocumentRoleRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT workspace_id, doc_id, user_id, role, created_at
                 FROM doc_user_roles
                 WHERE workspace_id = ? AND doc_id = ? AND LOWER(role) = 'owner'
                 ORDER BY created_at DESC, user_id ASC",
                params![workspace_id, doc_id],
            )
            .await?;

        let mut owners = Vec::new();
        while let Some(row) = rows.next().await? {
            owners.push(Self::map_row(row)?);
        }
        Ok(owners)
    }

    async fn upsert_roles(&self, roles: &[DocumentRoleRecord]) -> Result<()> {
        if roles.is_empty() {
            return Ok(());
        }

        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        for role in roles {
            tx.execute(
                "INSERT INTO doc_user_roles (workspace_id, doc_id, user_id, role, created_at)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(workspace_id, doc_id, user_id)
                 DO UPDATE SET role = excluded.role, created_at = excluded.created_at",
                params![
                    role.workspace_id.as_str(),
                    role.doc_id.as_str(),
                    role.user_id.as_str(),
                    role.role.as_str(),
                    role.created_at
                ],
            )
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
        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO doc_user_roles (workspace_id, doc_id, user_id, role, created_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(workspace_id, doc_id, user_id)
             DO UPDATE SET role = excluded.role, created_at = excluded.created_at",
            params![workspace_id, doc_id, user_id, role, created_at],
        )
        .await?;
        Ok(())
    }

    async fn remove_role(&self, workspace_id: &str, doc_id: &str, user_id: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "DELETE FROM doc_user_roles WHERE workspace_id = ? AND doc_id = ? AND user_id = ?",
            params![workspace_id, doc_id, user_id],
        )
        .await?;
        Ok(())
    }
}
