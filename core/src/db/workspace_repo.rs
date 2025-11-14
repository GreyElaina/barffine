use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    workspace::{
        UserWorkspaceMembership, WorkspaceInviteLinkRecord, WorkspaceMemberWithUser,
        WorkspaceRecord,
    },
    workspace_member::WorkspaceMemberRecord,
};

#[derive(Debug, Clone)]
pub struct CreateWorkspaceParams {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub created_at: i64,
    pub public: bool,
    pub enable_ai: bool,
    pub enable_doc_embedding: bool,
    pub enable_url_preview: bool,
    pub avatar_key: Option<String>,
    pub indexed: bool,
    pub last_check_embeddings: i64,
}

#[async_trait]
pub trait WorkspaceRepository: Send + Sync {
    async fn create_workspace(&self, params: CreateWorkspaceParams) -> Result<WorkspaceRecord>;

    async fn fetch_workspace(&self, id: &str) -> Result<Option<WorkspaceRecord>>;

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>>;

    async fn fetch_workspaces_by_ids(&self, ids: &[String]) -> Result<Vec<WorkspaceRecord>>;

    async fn update_workspace(&self, params: UpdateWorkspaceParams) -> Result<bool>;

    async fn delete_workspace(&self, id: &str) -> Result<bool>;

    async fn list_members_with_users(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceMemberWithUser>>;

    async fn search_members_with_users(
        &self,
        workspace_id: &str,
        pattern: &str,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<WorkspaceMemberWithUser>>;

    async fn list_memberships_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<UserWorkspaceMembership>>;

    async fn find_member_role(&self, workspace_id: &str, user_id: &str) -> Result<Option<String>>;

    async fn find_active_member_role(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<String>>;

    async fn get_member(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<WorkspaceMemberRecord>>;

    async fn set_member_status(
        &self,
        workspace_id: &str,
        user_id: &str,
        status: &str,
    ) -> Result<bool>;

    async fn set_member_role_with_status(
        &self,
        workspace_id: &str,
        user_id: &str,
        role: &str,
        status: &str,
        inviter_id: Option<&str>,
        source: Option<&str>,
        invited_at: i64,
    ) -> Result<()>;

    async fn delete_member(&self, workspace_id: &str, user_id: &str) -> Result<()>;

    async fn normalize_member_status_alias(&self, canonical: &str, alias: &str) -> Result<()>;

    async fn upsert_invite_link(
        &self,
        workspace_id: &str,
        token: &str,
        expires_at: i64,
        inviter_id: &str,
        created_at: i64,
    ) -> Result<()>;

    async fn delete_invite_link(&self, workspace_id: &str) -> Result<bool>;

    async fn find_invite_link(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceInviteLinkRecord>>;

    async fn find_invite_link_by_token(
        &self,
        token: &str,
    ) -> Result<Option<WorkspaceInviteLinkRecord>>;
}

pub type WorkspaceRepositoryRef = Arc<dyn WorkspaceRepository>;
#[derive(Debug, Clone)]
pub struct UpdateWorkspaceParams {
    pub id: String,
    pub name: Option<String>,
    pub public: Option<bool>,
    pub enable_ai: Option<bool>,
    pub enable_doc_embedding: Option<bool>,
    pub enable_url_preview: Option<bool>,
    pub avatar_key: Option<Option<String>>,
    pub indexed: Option<bool>,
    pub last_check_embeddings: Option<i64>,
}
