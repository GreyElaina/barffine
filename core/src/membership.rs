use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{ids::WorkspaceId, workspace_member::WorkspaceMemberRecord};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkspaceRole {
    Owner,
    Admin,
    Member,
    Viewer,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum InvitationStatus {
    Pending,
    Accepted,
    Declined,
    Revoked,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceInvitation {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub email: String,
    pub role: WorkspaceRole,
    pub status: InvitationStatus,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait MembershipStore: Send + Sync {
    async fn list_members(&self, workspace_id: &str) -> Result<Vec<WorkspaceMemberRecord>>;
    async fn upsert_member(&self, member: WorkspaceMemberRecord) -> Result<()>;
    async fn remove_member(&self, workspace_id: &str, user_id: &str) -> Result<()>;
    async fn create_invitation(&self, invitation: WorkspaceInvitation) -> Result<()>;
    async fn resolve_invitation(
        &self,
        invitation_id: &str,
        status: InvitationStatus,
    ) -> Result<Option<WorkspaceInvitation>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invitation_defaults() {
        let invitation = WorkspaceInvitation {
            id: "inv".into(),
            workspace_id: WorkspaceId::from("ws"),
            email: "user@example.com".into(),
            role: WorkspaceRole::Member,
            status: InvitationStatus::Pending,
            created_at: Utc::now(),
            expires_at: None,
        };

        assert_eq!(invitation.status, InvitationStatus::Pending);
    }
}
