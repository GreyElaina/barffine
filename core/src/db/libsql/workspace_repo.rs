use anyhow::Result;
use async_trait::async_trait;
use deadpool_libsql::{
    Pool,
    libsql::{self, params, params_from_iter},
};

use crate::db::{
    libsql::row_ext,
    workspace_repo::{CreateWorkspaceParams, UpdateWorkspaceParams, WorkspaceRepository},
};
use crate::workspace::{
    UserWorkspaceMembership, WorkspaceInviteLinkRecord, WorkspaceMemberWithUser, WorkspaceRecord,
};
use crate::workspace_member::WorkspaceMemberRecord;

fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn like_pattern(input: &str) -> String {
    format!("%{}%", input.to_ascii_lowercase())
}

pub struct LibsqlWorkspaceRepository {
    pool: Pool,
}

impl LibsqlWorkspaceRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn map_workspace_row(row: &libsql::Row) -> Result<WorkspaceRecord> {
        Ok(WorkspaceRecord {
            id: row_ext::get(row, "id")?,
            name: row_ext::get(row, "name")?,
            owner_id: row_ext::get(row, "owner_id")?,
            created_at: row_ext::get(row, "created_at")?,
            public: row_ext::get::<Option<i64>>(row, "public")?.unwrap_or(0) != 0,
            enable_ai: row_ext::get::<Option<i64>>(row, "enable_ai")?.unwrap_or(0) != 0,
            enable_doc_embedding: row_ext::get::<Option<i64>>(row, "enable_doc_embedding")?
                .unwrap_or(0)
                != 0,
            enable_url_preview: row_ext::get::<Option<i64>>(row, "enable_url_preview")?
                .unwrap_or(0)
                != 0,
            avatar_key: row_ext::get(row, "avatar_key")?,
            indexed: row_ext::get::<Option<i64>>(row, "indexed")?.unwrap_or(0) != 0,
            last_check_embeddings: row_ext::get(row, "last_check_embeddings")?,
        })
    }

    fn map_member_row(row: &libsql::Row) -> Result<WorkspaceMemberWithUser> {
        Ok(WorkspaceMemberWithUser {
            workspace_id: row_ext::get(row, "workspace_id")?,
            user_id: row_ext::get(row, "user_id")?,
            role: row_ext::get(row, "role")?,
            status: row_ext::get(row, "status")?,
            email: row_ext::get(row, "email")?,
            name: row_ext::get(row, "name")?,
            avatar_url: row_ext::get(row, "avatar_url")?,
            email_verified_at: row_ext::get(row, "email_verified_at")?,
            disabled: row_ext::get::<i64>(row, "disabled")? != 0,
            has_password: row_ext::get::<i64>(row, "has_password")? != 0,
            user_created_at: row_ext::get(row, "created_at")?,
            inviter_id: row_ext::get(row, "inviter_id")?,
            invited_at: row_ext::get(row, "invited_at")?,
            source: row_ext::get(row, "source")?,
        })
    }
}

#[async_trait]
impl WorkspaceRepository for LibsqlWorkspaceRepository {
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
        let id_ref = id.as_str();
        let owner_id_ref = owner_id.as_str();
        let name_ref = name.as_str();

        let mut conn = self.pool.get().await?;
        let mut tx = conn.transaction().await?;
        tx.execute(
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
            params![
                id_ref,
                name_ref,
                owner_id_ref,
                created_at,
                bool_to_i64(public),
                bool_to_i64(enable_ai),
                bool_to_i64(enable_doc_embedding),
                bool_to_i64(enable_url_preview),
                avatar_key.as_deref(),
                bool_to_i64(indexed),
                last_check_embeddings
            ],
        )
        .await?;

        tx.execute(
            "INSERT INTO workspace_members (
                 workspace_id,
                 user_id,
                 role,
                 status,
                 inviter_id,
                 invited_at,
                 source
             ) VALUES (?, ?, 'owner', 'Accepted', NULL, ?, 'system')",
            params![id_ref, owner_id_ref, created_at],
        )
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
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
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
                params![id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::map_workspace_row(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
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
                params![],
            )
            .await?;

        let mut workspaces = Vec::new();
        while let Some(row) = rows.next().await? {
            workspaces.push(Self::map_workspace_row(&row)?);
        }
        Ok(workspaces)
    }

    async fn fetch_workspaces_by_ids(&self, ids: &[String]) -> Result<Vec<WorkspaceRecord>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut sql = String::from(
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
        for (idx, _) in ids.iter().enumerate() {
            if idx > 0 {
                sql.push_str(", ");
            }
            sql.push('?');
        }
        sql.push(')');

        let bindings = ids.iter().cloned().collect::<Vec<_>>();
        let conn = self.pool.get().await?;
        let mut rows = conn.query(&sql, params_from_iter(bindings)).await?;

        let mut workspaces = Vec::new();
        while let Some(row) = rows.next().await? {
            workspaces.push(Self::map_workspace_row(&row)?);
        }
        Ok(workspaces)
    }

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
        let id_ref = id.as_str();

        let name_ref = name.as_deref();
        let public_val = public.map(bool_to_i64);
        let enable_ai_val = enable_ai.map(bool_to_i64);
        let enable_doc_embedding_val = enable_doc_embedding.map(bool_to_i64);
        let enable_url_preview_val = enable_url_preview.map(bool_to_i64);
        let avatar_ref = avatar_key.as_ref().and_then(|value| value.as_deref());
        let indexed_val = indexed.map(bool_to_i64);

        let flags = [
            name_ref.is_some(),
            public_val.is_some(),
            enable_ai_val.is_some(),
            enable_doc_embedding_val.is_some(),
            enable_url_preview_val.is_some(),
            avatar_key.is_some(),
            indexed_val.is_some(),
            last_check_embeddings.is_some(),
        ];

        if !flags.iter().any(|flag| *flag) {
            return Ok(false);
        }

        let conn = self.pool.get().await?;
        let changed = conn
            .execute(
                "UPDATE workspaces SET
                     name = CASE WHEN ?1 THEN ?2 ELSE name END,
                     public = CASE WHEN ?3 THEN ?4 ELSE public END,
                     enable_ai = CASE WHEN ?5 THEN ?6 ELSE enable_ai END,
                     enable_doc_embedding = CASE WHEN ?7 THEN ?8 ELSE enable_doc_embedding END,
                     enable_url_preview = CASE WHEN ?9 THEN ?10 ELSE enable_url_preview END,
                     avatar_key = CASE WHEN ?11 THEN ?12 ELSE avatar_key END,
                     indexed = CASE WHEN ?13 THEN ?14 ELSE indexed END,
                     last_check_embeddings = CASE WHEN ?15 THEN ?16 ELSE last_check_embeddings END
                 WHERE id = ?17",
                params![
                    bool_to_i64(name_ref.is_some()),
                    name_ref,
                    bool_to_i64(public_val.is_some()),
                    public_val,
                    bool_to_i64(enable_ai_val.is_some()),
                    enable_ai_val,
                    bool_to_i64(enable_doc_embedding_val.is_some()),
                    enable_doc_embedding_val,
                    bool_to_i64(enable_url_preview_val.is_some()),
                    enable_url_preview_val,
                    bool_to_i64(avatar_key.is_some()),
                    avatar_ref,
                    bool_to_i64(indexed_val.is_some()),
                    indexed_val,
                    bool_to_i64(last_check_embeddings.is_some()),
                    last_check_embeddings,
                    id_ref
                ],
            )
            .await?;

        Ok(changed > 0)
    }

    async fn delete_workspace(&self, id: &str) -> Result<bool> {
        let conn = self.pool.get().await?;
        let changed = conn
            .execute("DELETE FROM workspaces WHERE id = ?", params![id])
            .await?;
        Ok(changed > 0)
    }

    async fn list_members_with_users(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceMemberWithUser>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
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
                params![workspace_id],
            )
            .await?;

        let mut members = Vec::new();
        while let Some(row) = rows.next().await? {
            members.push(Self::map_member_row(&row)?);
        }
        Ok(members)
    }

    async fn search_members_with_users(
        &self,
        workspace_id: &str,
        pattern: &str,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<WorkspaceMemberWithUser>> {
        let like = like_pattern(pattern);
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
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
                params![workspace_id, like.clone(), like, limit, offset],
            )
            .await?;

        let mut members = Vec::new();
        while let Some(row) = rows.next().await? {
            members.push(Self::map_member_row(&row)?);
        }
        Ok(members)
    }

    async fn list_memberships_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<UserWorkspaceMembership>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT w.id AS workspace_id,
                        w.name AS workspace_name,
                        w.owner_id AS workspace_owner_id,
                        w.created_at AS workspace_created_at,
                        wm.role
                 FROM workspace_members wm
                 JOIN workspaces w ON w.id = wm.workspace_id
                 WHERE wm.user_id = ?
                   AND wm.status = 'Accepted'
                 ORDER BY w.created_at ASC, w.id ASC",
                params![user_id],
            )
            .await?;

        let mut memberships = Vec::new();
        while let Some(row) = rows.next().await? {
            memberships.push(UserWorkspaceMembership {
                workspace_id: row_ext::get(&row, "workspace_id")?,
                workspace_name: row_ext::get(&row, "workspace_name")?,
                workspace_owner_id: row_ext::get(&row, "workspace_owner_id")?,
                workspace_created_at: row_ext::get(&row, "workspace_created_at")?,
                role: row_ext::get(&row, "role")?,
            });
        }
        Ok(memberships)
    }

    async fn find_member_role(&self, workspace_id: &str, user_id: &str) -> Result<Option<String>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT role
                 FROM workspace_members
                 WHERE workspace_id = ? AND user_id = ?",
                params![workspace_id, user_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row_ext::get(&row, "role")?))
        } else {
            Ok(None)
        }
    }

    async fn find_active_member_role(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<String>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT role
                 FROM workspace_members
                 WHERE workspace_id = ? AND user_id = ? AND status = 'Accepted'",
                params![workspace_id, user_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(row_ext::get(&row, "role")?))
        } else {
            Ok(None)
        }
    }

    async fn get_member(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<WorkspaceMemberRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT workspace_id, user_id, role, status, inviter_id, invited_at, source
                 FROM workspace_members
                 WHERE workspace_id = ? AND user_id = ?",
                params![workspace_id, user_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(WorkspaceMemberRecord {
                workspace_id: row_ext::get(&row, "workspace_id")?,
                user_id: row_ext::get(&row, "user_id")?,
                role: row_ext::get(&row, "role")?,
                status: row_ext::get(&row, "status")?,
                inviter_id: row_ext::get(&row, "inviter_id")?,
                invited_at: row_ext::get(&row, "invited_at")?,
                source: row_ext::get(&row, "source")?,
            }))
        } else {
            Ok(None)
        }
    }

    async fn set_member_status(
        &self,
        workspace_id: &str,
        user_id: &str,
        status: &str,
    ) -> Result<bool> {
        let conn = self.pool.get().await?;
        let changed = conn
            .execute(
                "UPDATE workspace_members SET status = ? WHERE workspace_id = ? AND user_id = ?",
                params![status, workspace_id, user_id],
            )
            .await?;
        Ok(changed > 0)
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
        let conn = self.pool.get().await?;
        conn.execute(
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
            params![
                workspace_id,
                user_id,
                role,
                status,
                inviter_id,
                invited_at,
                source
            ],
        )
        .await?;
        Ok(())
    }

    async fn delete_member(&self, workspace_id: &str, user_id: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "DELETE FROM workspace_members WHERE workspace_id = ? AND user_id = ?",
            params![workspace_id, user_id],
        )
        .await?;
        Ok(())
    }

    async fn normalize_member_status_alias(&self, canonical: &str, alias: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "UPDATE workspace_members SET status = ?
             WHERE LOWER(REPLACE(REPLACE(status, '_', ''), ' ', '')) = ?",
            params![canonical, alias],
        )
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
        let conn = self.pool.get().await?;
        conn.execute(
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
            params![workspace_id, token, expires_at, inviter_id, created_at],
        )
        .await?;
        Ok(())
    }

    async fn delete_invite_link(&self, workspace_id: &str) -> Result<bool> {
        let conn = self.pool.get().await?;
        let changed = conn
            .execute(
                "DELETE FROM workspace_invite_links WHERE workspace_id = ?",
                params![workspace_id],
            )
            .await?;
        Ok(changed > 0)
    }

    async fn find_invite_link(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceInviteLinkRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT workspace_id, token, expires_at, inviter_id, created_at
                 FROM workspace_invite_links
                 WHERE workspace_id = ?",
                params![workspace_id],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(WorkspaceInviteLinkRecord {
                workspace_id: row_ext::get(&row, "workspace_id")?,
                token: row_ext::get(&row, "token")?,
                expires_at: row_ext::get(&row, "expires_at")?,
                inviter_id: row_ext::get(&row, "inviter_id")?,
                created_at: row_ext::get(&row, "created_at")?,
            }))
        } else {
            Ok(None)
        }
    }

    async fn find_invite_link_by_token(
        &self,
        token: &str,
    ) -> Result<Option<WorkspaceInviteLinkRecord>> {
        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT workspace_id, token, expires_at, inviter_id, created_at
                 FROM workspace_invite_links
                 WHERE token = ?",
                params![token],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(WorkspaceInviteLinkRecord {
                workspace_id: row_ext::get(&row, "workspace_id")?,
                token: row_ext::get(&row, "token")?,
                expires_at: row_ext::get(&row, "expires_at")?,
                inviter_id: row_ext::get(&row, "inviter_id")?,
                created_at: row_ext::get(&row, "created_at")?,
            }))
        } else {
            Ok(None)
        }
    }
}
