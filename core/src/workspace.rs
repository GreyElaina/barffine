use anyhow::Result;
use chrono::Utc;
use sqlx::{Pool, QueryBuilder, Row, Sqlite, sqlite::SqliteRow};
use uuid::Uuid;

use crate::{db::Database, workspace_member::WorkspaceMemberRecord};

pub const DEFAULT_WORKSPACE_NAME: &str = "Untitled Workspace";

const WORKSPACE_MEMBER_STATUS_NORMALIZATION: &[(&str, &str)] = &[
    ("Accepted", "accepted"),
    ("Pending", "pending"),
    ("UnderReview", "underreview"),
    ("AllocatingSeat", "allocatingseat"),
    ("NeedMoreSeat", "needmoreseat"),
    ("NeedMoreSeatAndReview", "needmoreseatandreview"),
];

#[derive(Debug, Clone)]
pub struct WorkspaceRecord {
    pub id: String,
    pub name: String,
    pub owner_id: String,
    pub created_at: i64,
    pub public: bool,
    pub enable_ai: bool,
    pub enable_doc_embedding: bool,
    pub enable_url_preview: bool,
    pub avatar_key: Option<String>,
    pub indexed: bool,
    pub last_check_embeddings: i64,
}

#[derive(Debug, Clone)]
pub struct WorkspaceMemberWithUser {
    pub workspace_id: String,
    pub user_id: String,
    pub role: String,
    pub status: String,
    pub email: String,
    pub name: Option<String>,
    pub avatar_url: Option<String>,
    pub email_verified_at: Option<i64>,
    pub disabled: bool,
    pub has_password: bool,
    pub user_created_at: i64,
    pub inviter_id: Option<String>,
    pub invited_at: Option<i64>,
    pub source: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkspaceInviteLinkRecord {
    pub workspace_id: String,
    pub token: String,
    pub expires_at: i64,
    pub inviter_id: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct UserWorkspaceMembership {
    pub workspace_id: String,
    pub workspace_name: String,
    pub workspace_owner_id: String,
    pub workspace_created_at: i64,
    pub role: String,
}

#[derive(Clone)]
pub struct WorkspaceStore {
    pool: Pool<Sqlite>,
}

impl WorkspaceStore {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: database.pool().clone(),
        }
    }

    pub fn canonical_member_status(status: &str) -> Option<&'static str> {
        if status.trim().is_empty() {
            return None;
        }

        let mut simplified = String::with_capacity(status.len());
        for ch in status.trim().chars() {
            if ch != '_' && !ch.is_ascii_whitespace() {
                simplified.push(ch.to_ascii_lowercase());
            }
        }

        WORKSPACE_MEMBER_STATUS_NORMALIZATION
            .iter()
            .find_map(|(canonical, alias)| {
                if simplified == *alias {
                    Some(*canonical)
                } else {
                    None
                }
            })
    }

    pub async fn create(
        &self,
        owner_id: &str,
        name: Option<&str>,
        public: Option<bool>,
        enable_ai: Option<bool>,
        enable_doc_embedding: Option<bool>,
        enable_url_preview: Option<bool>,
    ) -> Result<WorkspaceRecord> {
        let id = Uuid::new_v4().to_string();
        let created_at = Utc::now().timestamp();
        let public = public.unwrap_or(false);
        let enable_ai = enable_ai.unwrap_or(true);
        let enable_doc_embedding = enable_doc_embedding.unwrap_or(true);
        let enable_url_preview = enable_url_preview.unwrap_or(false);
        let avatar_key: Option<String> = None;
        let indexed = false;
        let last_check_embeddings = 0_i64;
        let resolved_name = name
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| DEFAULT_WORKSPACE_NAME.to_string());

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
        .bind(&id)
        .bind(resolved_name.as_str())
        .bind(owner_id)
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
        .bind(owner_id)
        .bind("owner")
        .bind(created_at)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(WorkspaceRecord {
            id,
            name: resolved_name,
            owner_id: owner_id.to_owned(),
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

    pub async fn find_by_id(&self, id: &str) -> Result<Option<WorkspaceRecord>> {
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

    pub async fn list_all(&self) -> Result<Vec<WorkspaceRecord>> {
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

    fn map_workspace_row(row: SqliteRow) -> WorkspaceRecord {
        WorkspaceRecord {
            id: row.get("id"),
            name: row.get("name"),
            owner_id: row.get("owner_id"),
            created_at: row.get::<i64, _>("created_at"),
            public: row.get::<i64, _>("public") != 0,
            enable_ai: row.get::<i64, _>("enable_ai") != 0,
            enable_doc_embedding: row.get::<i64, _>("enable_doc_embedding") != 0,
            enable_url_preview: row.get::<i64, _>("enable_url_preview") != 0,
            avatar_key: row.get::<Option<String>, _>("avatar_key"),
            indexed: row.get::<i64, _>("indexed") != 0,
            last_check_embeddings: row.get::<i64, _>("last_check_embeddings"),
        }
    }

    pub async fn find_by_ids(&self, ids: &[String]) -> Result<Vec<WorkspaceRecord>> {
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

    fn map_member_row(row: SqliteRow) -> WorkspaceMemberWithUser {
        WorkspaceMemberWithUser {
            workspace_id: row.get("workspace_id"),
            user_id: row.get("user_id"),
            role: row.get("role"),
            status: row.get("status"),
            email: row.get("email"),
            name: row.get::<Option<String>, _>("name"),
            avatar_url: row.get::<Option<String>, _>("avatar_url"),
            email_verified_at: row.get::<Option<i64>, _>("email_verified_at"),
            disabled: row.get::<i64, _>("disabled") != 0,
            has_password: row.get::<i64, _>("has_password") != 0,
            user_created_at: row.get::<i64, _>("created_at"),
            inviter_id: row.get::<Option<String>, _>("inviter_id"),
            invited_at: row.get::<Option<i64>, _>("invited_at"),
            source: row.get::<Option<String>, _>("source"),
        }
    }

    pub async fn list_members_with_users(
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

    pub async fn search_members_with_users(
        &self,
        workspace_id: &str,
        query: &str,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<WorkspaceMemberWithUser>> {
        let lowered = query.to_ascii_lowercase();
        let pattern = format!("%{}%", lowered);
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
        .bind(&pattern)
        .bind(&pattern)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Self::map_member_row).collect())
    }

    pub async fn list_memberships_for_user(
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
                workspace_created_at: row.get::<i64, _>("workspace_created_at"),
                role: row.get("role"),
            })
            .collect())
    }

    pub async fn find_member_role(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<String>> {
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

    pub async fn find_active_member_role(
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

    pub async fn get_member(
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
            workspace_id: row.get("workspace_id"),
            user_id: row.get("user_id"),
            role: row.get("role"),
            status: row.get("status"),
            inviter_id: row.get::<Option<String>, _>("inviter_id"),
            invited_at: row.get::<Option<i64>, _>("invited_at"),
            source: row.get::<Option<String>, _>("source"),
        }))
    }

    pub async fn set_member_status(
        &self,
        workspace_id: &str,
        user_id: &str,
        status: &str,
    ) -> Result<bool> {
        let canonical = Self::canonical_member_status(status)
            .map(|value| value.to_string())
            .unwrap_or_else(|| status.trim().to_string());
        let result = sqlx::query(
            "UPDATE workspace_members SET status = ? WHERE workspace_id = ? AND user_id = ?",
        )
        .bind(&canonical)
        .bind(workspace_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn set_member_role_with_status(
        &self,
        workspace_id: &str,
        user_id: &str,
        role: &str,
        status: &str,
        inviter_id: Option<&str>,
        source: Option<&str>,
        invited_at: Option<i64>,
    ) -> Result<()> {
        let invited_timestamp = invited_at.unwrap_or_else(|| Utc::now().timestamp());
        let canonical_status = Self::canonical_member_status(status)
            .map(|value| value.to_string())
            .unwrap_or_else(|| status.trim().to_string());
        sqlx::query(
            "INSERT INTO workspace_members (
                 workspace_id,
                 user_id,
                 role,
                 status,
                 inviter_id,
                 source,
                 invited_at
             )
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(workspace_id, user_id) DO UPDATE SET
                 role = excluded.role,
                 status = excluded.status,
                 inviter_id = COALESCE(excluded.inviter_id, workspace_members.inviter_id),
                 source = COALESCE(excluded.source, workspace_members.source),
                 invited_at = COALESCE(workspace_members.invited_at, excluded.invited_at)",
        )
        .bind(workspace_id)
        .bind(user_id)
        .bind(role)
        .bind(&canonical_status)
        .bind(inviter_id)
        .bind(source)
        .bind(invited_timestamp)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn update(
        &self,
        id: &str,
        name: Option<&str>,
        public: Option<bool>,
        enable_ai: Option<bool>,
        enable_doc_embedding: Option<bool>,
        enable_url_preview: Option<bool>,
        avatar_key: Option<Option<&str>>,
        indexed: Option<bool>,
        last_check_embeddings: Option<i64>,
    ) -> Result<Option<WorkspaceRecord>> {
        let mut builder = QueryBuilder::new("UPDATE workspaces SET ");
        let mut has_updates = false;

        if let Some(name) = name {
            let trimmed = name.trim();
            if !trimmed.is_empty() {
                if has_updates {
                    builder.push(", ");
                }
                builder.push("name = ");
                builder.push_bind(trimmed);
                has_updates = true;
            }
        }

        if let Some(value) = public {
            if has_updates {
                builder.push(", ");
            }
            builder.push("public = ");
            builder.push_bind(if value { 1 } else { 0 });
            has_updates = true;
        }

        if let Some(value) = enable_ai {
            if has_updates {
                builder.push(", ");
            }
            builder.push("enable_ai = ");
            builder.push_bind(if value { 1 } else { 0 });
            has_updates = true;
        }

        if let Some(value) = enable_doc_embedding {
            if has_updates {
                builder.push(", ");
            }
            builder.push("enable_doc_embedding = ");
            builder.push_bind(if value { 1 } else { 0 });
            has_updates = true;
        }

        if let Some(value) = enable_url_preview {
            if has_updates {
                builder.push(", ");
            }
            builder.push("enable_url_preview = ");
            builder.push_bind(if value { 1 } else { 0 });
            has_updates = true;
        }

        if let Some(value) = avatar_key {
            if has_updates {
                builder.push(", ");
            }
            builder.push("avatar_key = ");
            match value {
                Some(key) => {
                    builder.push_bind(key);
                }
                None => {
                    builder.push("NULL");
                }
            }
            has_updates = true;
        }

        if let Some(value) = indexed {
            if has_updates {
                builder.push(", ");
            }
            builder.push("indexed = ");
            builder.push_bind(if value { 1 } else { 0 });
            has_updates = true;
        }

        if let Some(value) = last_check_embeddings {
            if has_updates {
                builder.push(", ");
            }
            builder.push("last_check_embeddings = ");
            builder.push_bind(value);
            has_updates = true;
        }

        if !has_updates {
            return self.find_by_id(id).await;
        }

        builder.push(" WHERE id = ");
        builder.push_bind(id);

        let result = builder.build().execute(&self.pool).await?;
        if result.rows_affected() == 0 {
            return Ok(None);
        }

        self.find_by_id(id).await
    }

    pub async fn delete(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM workspaces WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn set_member_role(
        &self,
        workspace_id: &str,
        user_id: &str,
        role: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role)
             VALUES (?, ?, ?)
             ON CONFLICT(workspace_id, user_id) DO UPDATE SET role = excluded.role",
        )
        .bind(workspace_id)
        .bind(user_id)
        .bind(role)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn remove_member(&self, workspace_id: &str, user_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM workspace_members WHERE workspace_id = ? AND user_id = ?")
            .bind(workspace_id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn normalize_member_statuses(&self) -> Result<()> {
        for (canonical, alias) in WORKSPACE_MEMBER_STATUS_NORMALIZATION {
            sqlx::query(
                "UPDATE workspace_members SET status = ?
                 WHERE LOWER(REPLACE(REPLACE(status, '_', ''), ' ', '')) = ?",
            )
            .bind(*canonical)
            .bind(*alias)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    pub async fn upsert_invite_link(
        &self,
        workspace_id: &str,
        token: &str,
        expires_at: i64,
        inviter_id: &str,
    ) -> Result<()> {
        let created_at = Utc::now().timestamp();
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

    pub async fn delete_invite_link(&self, workspace_id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM workspace_invite_links WHERE workspace_id = ?")
            .bind(workspace_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn find_invite_link(
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

    pub async fn find_invite_link_by_token(
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
