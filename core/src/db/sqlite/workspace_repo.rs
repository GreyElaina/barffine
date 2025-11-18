use anyhow::Result;
use async_trait::async_trait;
use sqlx::{Pool, QueryBuilder, Row, Sqlite, sqlite::SqliteRow};

use crate::{
    db::workspace_repo::{CreateWorkspaceParams, UpdateWorkspaceParams, WorkspaceRepository},
    ids::{UserId, WorkspaceId},
    workspace::{
        UserWorkspaceMembership, WorkspaceInviteLinkRecord, WorkspaceMemberWithUser,
        WorkspaceRecord,
    },
    workspace_member::WorkspaceMemberRecord,
};

pub struct SqliteWorkspaceRepository {
    pool: Pool<Sqlite>,
}

impl SqliteWorkspaceRepository {
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self { pool }
    }

    fn map_workspace_row(row: SqliteRow) -> WorkspaceRecord {
        WorkspaceRecord {
            id: WorkspaceId::from(row.get::<String, _>("id")),
            name: row.get("name"),
            owner_id: UserId::from(row.get::<String, _>("owner_id")),
            created_at: row.get("created_at"),
            public: row.get::<i64, _>("public") != 0,
            enable_ai: row.get::<i64, _>("enable_ai") != 0,
            enable_doc_embedding: row.get::<i64, _>("enable_doc_embedding") != 0,
            enable_url_preview: row.get::<i64, _>("enable_url_preview") != 0,
            avatar_key: row.get("avatar_key"),
            indexed: row.get::<i64, _>("indexed") != 0,
            last_check_embeddings: row.get("last_check_embeddings"),
        }
    }

    fn map_member_row(row: SqliteRow) -> WorkspaceMemberWithUser {
        WorkspaceMemberWithUser {
            workspace_id: WorkspaceId::from(row.get::<String, _>("workspace_id")),
            user_id: UserId::from(row.get::<String, _>("user_id")),
            role: row.get("role"),
            status: row.get("status"),
            email: row.get("email"),
            name: row.get("name"),
            avatar_url: row.get("avatar_url"),
            email_verified_at: row.get("email_verified_at"),
            disabled: row.get::<i64, _>("disabled") != 0,
            has_password: row.get::<i64, _>("has_password") != 0,
            user_created_at: row.get("created_at"),
            inviter_id: row.get("inviter_id"),
            invited_at: row.get("invited_at"),
            source: row.get("source"),
        }
    }
}

#[async_trait]
impl WorkspaceRepository for SqliteWorkspaceRepository {
    async fn update_workspace(&self, params: UpdateWorkspaceParams) -> Result<bool> {
        let UpdateWorkspaceParams {
            id,
            name,
            public,
            enable_ai,
            enable_doc_embedding,
            enable_url_preview,
            avatar_key,
            indexed,
            last_check_embeddings,
        } = params;

        let mut builder = QueryBuilder::new("UPDATE workspaces SET ");
        let mut has_updates = false;

        if let Some(name) = name {
            builder.push("name = ");
            builder.push_bind(name);
            has_updates = true;
        }
        if let Some(public) = public {
            if has_updates {
                builder.push(", ");
            }
            builder.push("public = ");
            builder.push_bind(public);
            has_updates = true;
        }
        if let Some(enable_ai) = enable_ai {
            if has_updates {
                builder.push(", ");
            }
            builder.push("enable_ai = ");
            builder.push_bind(enable_ai);
            has_updates = true;
        }
        if let Some(enable_doc_embedding) = enable_doc_embedding {
            if has_updates {
                builder.push(", ");
            }
            builder.push("enable_doc_embedding = ");
            builder.push_bind(enable_doc_embedding);
            has_updates = true;
        }
        if let Some(enable_url_preview) = enable_url_preview {
            if has_updates {
                builder.push(", ");
            }
            builder.push("enable_url_preview = ");
            builder.push_bind(enable_url_preview);
            has_updates = true;
        }
        if let Some(avatar_key) = avatar_key {
            if has_updates {
                builder.push(", ");
            }
            builder.push("avatar_key = ");
            builder.push_bind(avatar_key);
            has_updates = true;
        }
        if let Some(indexed) = indexed {
            if has_updates {
                builder.push(", ");
            }
            builder.push("indexed = ");
            builder.push_bind(indexed);
            has_updates = true;
        }
        if let Some(last_check_embeddings) = last_check_embeddings {
            if has_updates {
                builder.push(", ");
            }
            builder.push("last_check_embeddings = ");
            builder.push_bind(last_check_embeddings);
            has_updates = true;
        }

        if !has_updates {
            return Ok(false);
        }

        builder.push(" WHERE id = ");
        builder.push_bind(id);

        let result = builder.build().execute(&self.pool).await?;
        Ok(result.rows_affected() > 0)
    }

    async fn delete_workspace(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM workspaces WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn create_workspace(&self, params: CreateWorkspaceParams) -> Result<WorkspaceRecord> {
        let CreateWorkspaceParams {
            id,
            owner_id,
            name,
            created_at,
            public,
            enable_ai,
            enable_doc_embedding,
            enable_url_preview,
            avatar_key,
            indexed,
            last_check_embeddings,
        } = params;

        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO workspaces (
                 id,
                 name,
                 owner_id,
                 created_at,
                 public,
                 enable_ai,
                 enable_doc_embedding,
                 enable_url_preview,
                 avatar_key,
                 indexed,
                 last_check_embeddings
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.as_str())
        .bind(&name)
        .bind(owner_id.as_str())
        .bind(created_at)
        .bind(public)
        .bind(enable_ai)
        .bind(enable_doc_embedding)
        .bind(enable_url_preview)
        .bind(avatar_key.as_ref())
        .bind(if indexed { 1 } else { 0 })
        .bind(last_check_embeddings)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO workspace_members (
                 workspace_id,
                 user_id,
                 role,
                 status,
                 inviter_id,
                 invited_at,
                 source
             ) VALUES (?, ?, ?, 'Accepted', NULL, ?, 'system')",
        )
        .bind(&id)
        .bind(&owner_id)
        .bind("owner")
        .bind(created_at)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(WorkspaceRecord {
            id,
            name,
            owner_id,
            created_at,
            public,
            enable_ai,
            enable_doc_embedding,
            enable_url_preview,
            avatar_key,
            indexed,
            last_check_embeddings,
        })
    }

    async fn fetch_workspace(&self, id: &str) -> Result<Option<WorkspaceRecord>> {
        let row = sqlx::query(
            "SELECT
                 id,
                 name,
                 owner_id,
                 created_at,
                 public,
                 enable_ai,
                 enable_doc_embedding,
                 enable_url_preview,
                 avatar_key,
                 indexed,
                 last_check_embeddings
             FROM workspaces
             WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Self::map_workspace_row))
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>> {
        let rows = sqlx::query(
            "SELECT
                 id,
                 name,
                 owner_id,
                 created_at,
                 public,
                 enable_ai,
                 enable_doc_embedding,
                 enable_url_preview,
                 avatar_key,
                 indexed,
                 last_check_embeddings
             FROM workspaces
             ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_workspace_row).collect())
    }

    async fn fetch_workspaces_by_ids(&self, ids: &[String]) -> Result<Vec<WorkspaceRecord>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut builder = QueryBuilder::new(
            "SELECT
                 id,
                 name,
                 owner_id,
                 created_at,
                 public,
                 enable_ai,
                 enable_doc_embedding,
                 enable_url_preview,
                 avatar_key,
                 indexed,
                 last_check_embeddings
             FROM workspaces
             WHERE id IN (",
        );

        {
            let mut separated = builder.separated(", ");
            for id in ids {
                separated.push_bind(id);
            }
        }

        builder.push(")");

        let rows = builder.build().fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(Self::map_workspace_row).collect())
    }

    async fn list_members_with_users(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceMemberWithUser>> {
        let rows = sqlx::query(
            "SELECT
                 wm.workspace_id,
                 wm.user_id,
                 wm.role,
                 wm.status,
                 wm.inviter_id,
                 wm.invited_at,
                 wm.source,
                 u.email,
                 u.name,
                 u.avatar_url,
                 u.email_verified_at,
                 u.disabled,
                 (CASE WHEN length(u.password_hash) > 0 THEN 1 ELSE 0 END) AS has_password,
                 u.created_at
             FROM workspace_members wm
             JOIN users u ON u.id = wm.user_id
             WHERE wm.workspace_id = ?
             ORDER BY u.created_at ASC",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_member_row).collect())
    }

    async fn search_members_with_users(
        &self,
        workspace_id: &str,
        pattern: &str,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<WorkspaceMemberWithUser>> {
        let rows = sqlx::query(
            "SELECT
                 wm.workspace_id,
                 wm.user_id,
                 wm.role,
                 wm.status,
                 wm.inviter_id,
                 wm.invited_at,
                 wm.source,
                 u.email,
                 u.name,
                 u.avatar_url,
                 u.email_verified_at,
                 u.disabled,
                 (CASE WHEN length(u.password_hash) > 0 THEN 1 ELSE 0 END) AS has_password,
                 u.created_at
             FROM workspace_members wm
             JOIN users u ON u.id = wm.user_id
             WHERE wm.workspace_id = ?
               AND wm.status IN (
                   'Accepted',
                   'Pending',
                   'UnderReview',
                   'AllocatingSeat',
                   'NeedMoreSeat',
                   'NeedMoreSeatAndReview'
               )
               AND (
                   LOWER(u.email) LIKE ?
                   OR LOWER(COALESCE(u.name, '')) LIKE ?
               )
             ORDER BY u.created_at ASC
             LIMIT ? OFFSET ?",
        )
        .bind(workspace_id)
        .bind(pattern)
        .bind(pattern)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_member_row).collect())
    }

    async fn list_memberships_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<UserWorkspaceMembership>> {
        let rows = sqlx::query(
            "SELECT w.id AS workspace_id, w.name AS workspace_name, w.owner_id AS workspace_owner_id, w.created_at AS workspace_created_at, wm.role
             FROM workspace_members wm
             JOIN workspaces w ON w.id = wm.workspace_id
             WHERE wm.user_id = ?
               AND wm.status = 'Accepted'
             ORDER BY w.created_at ASC, w.id ASC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| UserWorkspaceMembership {
                workspace_id: row.get("workspace_id"),
                workspace_name: row.get("workspace_name"),
                workspace_owner_id: row.get("workspace_owner_id"),
                workspace_created_at: row.get("workspace_created_at"),
                role: row.get("role"),
            })
            .collect())
    }

    async fn find_member_role(&self, workspace_id: &str, user_id: &str) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT role
             FROM workspace_members
             WHERE workspace_id = ? AND user_id = ?",
        )
        .bind(workspace_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| row.get("role")))
    }

    async fn find_active_member_role(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT role
             FROM workspace_members
             WHERE workspace_id = ? AND user_id = ? AND status = 'Accepted'",
        )
        .bind(workspace_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| row.get("role")))
    }

    async fn get_member(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<WorkspaceMemberRecord>> {
        let row = sqlx::query(
            "SELECT workspace_id, user_id, role, status, inviter_id, invited_at, source
             FROM workspace_members
             WHERE workspace_id = ? AND user_id = ?",
        )
        .bind(workspace_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| WorkspaceMemberRecord {
            workspace_id: WorkspaceId::from(row.get::<String, _>("workspace_id")),
            user_id: UserId::from(row.get::<String, _>("user_id")),
            role: row.get("role"),
            status: row.get("status"),
            inviter_id: row.get("inviter_id"),
            invited_at: row.get("invited_at"),
            source: row.get("source"),
        }))
    }

    async fn set_member_status(
        &self,
        workspace_id: &str,
        user_id: &str,
        status: &str,
    ) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE workspace_members SET status = ? WHERE workspace_id = ? AND user_id = ?",
        )
        .bind(status)
        .bind(workspace_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn set_member_role_with_status(
        &self,
        workspace_id: &str,
        user_id: &str,
        role: &str,
        status: &str,
        inviter_id: Option<&str>,
        source: Option<&str>,
        invited_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO workspace_members (
                 workspace_id,
                 user_id,
                 role,
                 status,
                 inviter_id,
                 invited_at,
                 source
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(workspace_id, user_id) DO UPDATE SET
                 role = excluded.role,
                 status = excluded.status,
                 inviter_id = excluded.inviter_id,
                 invited_at = excluded.invited_at,
                 source = excluded.source",
        )
        .bind(workspace_id)
        .bind(user_id)
        .bind(role)
        .bind(status)
        .bind(inviter_id)
        .bind(invited_at)
        .bind(source)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete_member(&self, workspace_id: &str, user_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM workspace_members WHERE workspace_id = ? AND user_id = ?")
            .bind(workspace_id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn normalize_member_status_alias(&self, canonical: &str, alias: &str) -> Result<()> {
        sqlx::query(
            "UPDATE workspace_members SET status = ?
             WHERE LOWER(REPLACE(REPLACE(status, '_', ''), ' ', '')) = ?",
        )
        .bind(canonical)
        .bind(alias)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn upsert_invite_link(
        &self,
        workspace_id: &str,
        token: &str,
        expires_at: i64,
        inviter_id: &str,
        created_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO workspace_invite_links (
                 workspace_id,
                 token,
                 expires_at,
                 inviter_id,
                 created_at
             ) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(workspace_id) DO UPDATE SET
                 token = excluded.token,
                 expires_at = excluded.expires_at,
                 inviter_id = excluded.inviter_id,
                 created_at = excluded.created_at",
        )
        .bind(workspace_id)
        .bind(token)
        .bind(expires_at)
        .bind(inviter_id)
        .bind(created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_invite_link(&self, workspace_id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM workspace_invite_links WHERE workspace_id = ?")
            .bind(workspace_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn find_invite_link(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceInviteLinkRecord>> {
        let row = sqlx::query(
            "SELECT workspace_id, token, expires_at, inviter_id, created_at
             FROM workspace_invite_links
             WHERE workspace_id = ?",
        )
        .bind(workspace_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| WorkspaceInviteLinkRecord {
            workspace_id: row.get("workspace_id"),
            token: row.get("token"),
            expires_at: row.get("expires_at"),
            inviter_id: row.get("inviter_id"),
            created_at: row.get("created_at"),
        }))
    }

    async fn find_invite_link_by_token(
        &self,
        token: &str,
    ) -> Result<Option<WorkspaceInviteLinkRecord>> {
        let row = sqlx::query(
            "SELECT workspace_id, token, expires_at, inviter_id, created_at
             FROM workspace_invite_links
             WHERE token = ?",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| WorkspaceInviteLinkRecord {
            workspace_id: row.get("workspace_id"),
            token: row.get("token"),
            expires_at: row.get("expires_at"),
            inviter_id: row.get("inviter_id"),
            created_at: row.get("created_at"),
        }))
    }
}
