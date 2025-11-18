use crate::ids::{UserId, WorkspaceId};

#[derive(Debug, Clone)]
pub struct WorkspaceMemberRecord {
    pub workspace_id: WorkspaceId,
    pub user_id: UserId,
    pub role: String,
    pub status: String,
    pub inviter_id: Option<String>,
    pub invited_at: Option<i64>,
    pub source: Option<String>,
}
