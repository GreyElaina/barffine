use async_graphql::{ID, InputObject, SimpleObject};
use chrono::{DateTime, Utc};

use crate::graphql::timestamp_to_datetime;
use barffine_core::{doc_store::UserShareTokenRecord, user, workspace::UserWorkspaceMembership};

#[derive(Clone)]
pub struct UserType {
    pub record: user::UserRecord,
}

impl From<user::UserRecord> for UserType {
    fn from(record: user::UserRecord) -> Self {
        Self { record }
    }
}

#[derive(InputObject, Clone)]
#[graphql(name = "CreateUserInput")]
pub struct CreateUserInput {
    pub email: String,
    pub password: String,
    pub name: Option<String>,
}

#[derive(InputObject, Clone)]
#[graphql(name = "CreateSessionInput")]
pub struct CreateSessionInput {
    pub email: String,
    pub password: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "UserMembership")]
pub(crate) struct UserMembershipType {
    #[graphql(name = "workspaceId")]
    pub workspace_id: ID,
    #[graphql(name = "workspaceName")]
    pub workspace_name: String,
    #[graphql(name = "isOwner")]
    pub is_owner: bool,
    pub role: crate::graphql::Permission,
}

impl UserMembershipType {
    pub fn from_membership(
        record: UserWorkspaceMembership,
        user_id: &str,
        role: crate::graphql::Permission,
    ) -> Self {
        let is_owner =
            role == crate::graphql::Permission::Owner || record.workspace_owner_id == user_id;
        Self {
            workspace_id: ID(record.workspace_id),
            workspace_name: record.workspace_name,
            is_owner,
            role,
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "UserShareToken")]
pub(crate) struct UserShareTokenType {
    #[graphql(name = "workspaceId")]
    pub workspace_id: ID,
    #[graphql(name = "docId")]
    pub doc_id: ID,
    pub token: String,
    #[graphql(name = "createdAt")]
    pub created_at: DateTime<Utc>,
}

impl From<UserShareTokenRecord> for UserShareTokenType {
    fn from(record: UserShareTokenRecord) -> Self {
        Self {
            workspace_id: ID(record.workspace_id),
            doc_id: ID(record.doc_id),
            token: record.token,
            created_at: timestamp_to_datetime(record.created_at),
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "SessionObject")]
pub struct SessionObject {
    #[graphql(name = "id")]
    pub id: ID,
    #[graphql(name = "userId")]
    pub user_id: ID,
    #[graphql(name = "createdAt")]
    pub created_at: DateTime<Utc>,
    #[graphql(name = "expiresAt")]
    pub expires_at: DateTime<Utc>,
}

impl From<user::SessionRecord> for SessionObject {
    fn from(record: user::SessionRecord) -> Self {
        Self {
            id: ID(record.id),
            user_id: ID(record.user_id),
            created_at: timestamp_to_datetime(record.created_at),
            expires_at: timestamp_to_datetime(record.expires_at),
        }
    }
}

#[derive(InputObject, Default, Clone)]
#[graphql(name = "UpdateUserInput")]
pub(crate) struct UpdateProfileInput {
    pub name: Option<String>,
    #[graphql(name = "avatarUrl")]
    pub avatar_url: Option<String>,
    #[graphql(name = "removeAvatar")]
    pub remove_avatar: Option<bool>,
}

#[derive(InputObject, Default, Clone)]
#[graphql(name = "ManageUserInput")]
pub struct ManageUserInput {
    pub email: Option<String>,
    pub name: Option<Option<String>>,
}

#[derive(InputObject, Default, Clone)]
#[graphql(name = "UpdateUserSettingsInput")]
pub struct UpdateUserSettingsInput {
    #[graphql(name = "receiveCommentEmail")]
    pub receive_comment_email: Option<bool>,
    #[graphql(name = "receiveInvitationEmail")]
    pub receive_invitation_email: Option<bool>,
    #[graphql(name = "receiveMentionEmail")]
    pub receive_mention_email: Option<bool>,
    #[graphql(name = "receiveDocActivityNotification")]
    pub receive_doc_activity_notification: Option<bool>,
    #[graphql(name = "receivePermissionChangeNotification")]
    pub receive_permission_change_notification: Option<bool>,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "RemoveAvatar")]
pub struct RemoveAvatarType {
    pub success: bool,
}
