use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

use crate::{
    db::{
        Database,
        workspace_repo::{CreateWorkspaceParams, UpdateWorkspaceParams, WorkspaceRepositoryRef},
    },
    workspace_member::WorkspaceMemberRecord,
};

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
    workspace_repo: WorkspaceRepositoryRef,
}

impl WorkspaceStore {
    pub fn new(database: &Database) -> Self {
        Self {
            workspace_repo: database.repositories().workspace_repo(),
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

        self.workspace_repo
            .create_workspace(CreateWorkspaceParams {
                id,
                owner_id: owner_id.to_owned(),
                name: resolved_name,
                created_at,
                public,
                enable_ai,
                enable_doc_embedding,
                enable_url_preview,
                avatar_key,
                indexed,
                last_check_embeddings,
            })
            .await
    }

    pub async fn find_by_id(&self, id: &str) -> Result<Option<WorkspaceRecord>> {
        self.workspace_repo.fetch_workspace(id).await
    }

    pub async fn list_all(&self) -> Result<Vec<WorkspaceRecord>> {
        self.workspace_repo.list_workspaces().await
    }

    pub async fn find_by_ids(&self, ids: &[String]) -> Result<Vec<WorkspaceRecord>> {
        self.workspace_repo.fetch_workspaces_by_ids(ids).await
    }

    pub async fn list_members_with_users(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceMemberWithUser>> {
        self.workspace_repo
            .list_members_with_users(workspace_id)
            .await
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
        self.workspace_repo
            .search_members_with_users(workspace_id, &pattern, offset, limit)
            .await
    }

    pub async fn list_memberships_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<UserWorkspaceMembership>> {
        self.workspace_repo.list_memberships_for_user(user_id).await
    }

    pub async fn find_member_role(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<String>> {
        self.workspace_repo
            .find_member_role(workspace_id, user_id)
            .await
    }

    pub async fn find_active_member_role(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<String>> {
        self.workspace_repo
            .find_active_member_role(workspace_id, user_id)
            .await
    }

    pub async fn get_member(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<WorkspaceMemberRecord>> {
        self.workspace_repo.get_member(workspace_id, user_id).await
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
        self.workspace_repo
            .set_member_status(workspace_id, user_id, &canonical)
            .await
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
        self.workspace_repo
            .set_member_role_with_status(
                workspace_id,
                user_id,
                role,
                &canonical_status,
                inviter_id,
                source,
                invited_timestamp,
            )
            .await
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
        let normalized_name = name
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_owned());
        let normalized_avatar = avatar_key.map(|value| value.map(|inner| inner.to_owned()));

        let has_updates = normalized_name.is_some()
            || public.is_some()
            || enable_ai.is_some()
            || enable_doc_embedding.is_some()
            || enable_url_preview.is_some()
            || normalized_avatar.is_some()
            || indexed.is_some()
            || last_check_embeddings.is_some();

        if !has_updates {
            return self.find_by_id(id).await;
        }

        let updated = self
            .workspace_repo
            .update_workspace(UpdateWorkspaceParams {
                id: id.to_owned(),
                name: normalized_name,
                public,
                enable_ai,
                enable_doc_embedding,
                enable_url_preview,
                avatar_key: normalized_avatar,
                indexed,
                last_check_embeddings,
            })
            .await?;

        if !updated {
            return Ok(None);
        }

        self.find_by_id(id).await
    }

    pub async fn delete(&self, id: &str) -> Result<bool> {
        self.workspace_repo.delete_workspace(id).await
    }

    pub async fn set_member_role(
        &self,
        workspace_id: &str,
        user_id: &str,
        role: &str,
    ) -> Result<()> {
        self.workspace_repo
            .set_member_role_with_status(
                workspace_id,
                user_id,
                role,
                "Accepted",
                None,
                Some("system"),
                Utc::now().timestamp(),
            )
            .await
    }

    pub async fn remove_member(&self, workspace_id: &str, user_id: &str) -> Result<()> {
        self.workspace_repo
            .delete_member(workspace_id, user_id)
            .await
    }

    pub async fn normalize_member_statuses(&self) -> Result<()> {
        for (canonical, alias) in WORKSPACE_MEMBER_STATUS_NORMALIZATION {
            self.workspace_repo
                .normalize_member_status_alias(canonical, alias)
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
        self.workspace_repo
            .upsert_invite_link(
                workspace_id,
                token,
                expires_at,
                inviter_id,
                Utc::now().timestamp(),
            )
            .await
    }

    pub async fn delete_invite_link(&self, workspace_id: &str) -> Result<bool> {
        self.workspace_repo.delete_invite_link(workspace_id).await
    }

    pub async fn find_invite_link(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceInviteLinkRecord>> {
        self.workspace_repo.find_invite_link(workspace_id).await
    }

    pub async fn find_invite_link_by_token(
        &self,
        token: &str,
    ) -> Result<Option<WorkspaceInviteLinkRecord>> {
        self.workspace_repo.find_invite_link_by_token(token).await
    }
}
