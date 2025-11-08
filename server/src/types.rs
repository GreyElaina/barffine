// Request and response types for REST API handlers

use barffine_core::{
    doc_store::{DocumentHistoryRecord, DocumentMetadata},
    user,
    workspace::WorkspaceRecord,
};
use serde::{Deserialize, Serialize};

use crate::graphql::{DocRole, Permission};
use crate::{
    doc::{history::EncodedDocHistory, metadata},
    utils::users::display_name_for_user,
};
// ========== Authentication Types ==========

pub(crate) struct AuthenticatedRestSession {
    pub(crate) user: user::UserRecord,
    pub(crate) set_cookies: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct RestDocAccess {
    pub(crate) metadata: DocumentMetadata,
    pub(crate) user: Option<user::UserRecord>,
    pub(crate) set_cookies: Vec<String>,
    pub(crate) workspace_role: Option<Permission>,
    pub(crate) doc_role: Option<DocRole>,
}

#[derive(Clone)]
pub(crate) struct WorkspaceAccess {
    pub(crate) workspace: WorkspaceRecord,
    pub(crate) user: Option<user::UserRecord>,
    pub(crate) set_cookies: Vec<String>,
    pub(crate) workspace_role: Option<Permission>,
}

pub(crate) struct SessionLookup {
    pub(crate) user: Option<SessionUser>,
    pub(crate) cookies: Vec<String>,
}

// ========== Request Types ==========

#[derive(Deserialize)]
pub(crate) struct CreateUserRequest {
    pub(crate) email: String,
    pub(crate) password: String,
    #[serde(default)]
    pub(crate) name: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct CreateAdminUserRequest {
    pub(crate) email: String,
    pub(crate) password: String,
}

#[derive(Deserialize)]
pub(crate) struct CreateSessionRequest {
    pub(crate) email: String,
    pub(crate) password: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SignInRequest {
    pub(crate) email: String,
    #[serde(default)]
    pub(crate) password: Option<String>,
    #[serde(default)]
    pub(crate) callback_url: Option<String>,
    #[serde(default)]
    pub(crate) client_nonce: Option<String>,
    #[serde(default)]
    pub(crate) redirect_uri: Option<String>,
    #[serde(default)]
    pub(crate) provider: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct PreflightRequest {
    pub(crate) email: String,
}

#[derive(Deserialize)]
pub(crate) struct DocContentQuery {
    #[serde(default)]
    pub(crate) full: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct BlobDownloadQuery {
    #[serde(default)]
    pub(crate) redirect: Option<String>,
}

#[derive(Deserialize, Default)]
pub(crate) struct DeleteSessionRequest {
    #[serde(default)]
    pub(crate) session_id: Option<String>,
    #[serde(default)]
    pub(crate) user_id: Option<String>,
}

#[derive(Deserialize, Default)]
pub(crate) struct RefreshSessionRequest {
    #[serde(default)]
    pub(crate) session_id: Option<String>,
    #[serde(default)]
    pub(crate) user_id: Option<String>,
}

#[derive(Deserialize, Default)]
pub(crate) struct ListUsersQuery {
    #[serde(default)]
    pub(crate) skip: Option<i64>,
    #[serde(default)]
    pub(crate) first: Option<i64>,
    #[serde(default)]
    pub(crate) query: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct CreateWorkspaceRequest {
    #[serde(default)]
    pub(crate) owner_id: Option<String>,
    pub(crate) name: String,
}

#[derive(Deserialize)]
pub(crate) struct PublishDocRequest {
    #[serde(default)]
    pub(crate) mode: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct HistoryQuery {
    pub(crate) limit: Option<i64>,
}

// ========== Response Types ==========

#[derive(Serialize)]
pub(crate) struct CreateUserResponse {
    pub(crate) id: String,
    pub(crate) email: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct PresignedUrlResponse {
    pub(crate) url: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PreflightResponse {
    pub(crate) registered: bool,
    pub(crate) has_password: bool,
}

#[derive(Serialize)]
pub(crate) struct DocContentResponse {
    pub(crate) title: String,
    pub(crate) summary: String,
}

#[derive(Serialize)]
pub(crate) struct DocMarkdownResponse {
    pub(crate) title: String,
    pub(crate) markdown: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkspaceContentResponse {
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) avatar_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) avatar_url: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct CreateSessionResponse {
    pub(crate) session_id: String,
    pub(crate) user_id: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionUser {
    pub(crate) id: String,
    pub(crate) email: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) avatar_url: Option<String>,
    pub(crate) disabled: bool,
    pub(crate) email_verified: bool,
    pub(crate) has_password: bool,
}

impl From<&user::UserRecord> for SessionUser {
    fn from(record: &user::UserRecord) -> Self {
        Self {
            id: record.id.clone(),
            email: record.email.clone(),
            name: record.name.clone(),
            avatar_url: record.avatar_url.clone(),
            disabled: record.disabled,
            email_verified: record.email_verified_at.is_some(),
            has_password: !record.password_hash.trim().is_empty(),
        }
    }
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionUserPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) user: Option<SessionUser>,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionUsersPayload {
    pub(crate) users: Vec<SessionUser>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct UserResponse {
    pub(crate) id: String,
    pub(crate) email: String,
    pub(crate) name: String,
    #[serde(rename = "avatarUrl", skip_serializing_if = "Option::is_none")]
    pub(crate) avatar_url: Option<String>,
    #[serde(rename = "emailVerified")]
    pub(crate) email_verified: bool,
    pub(crate) disabled: bool,
}

impl From<user::UserRecord> for UserResponse {
    fn from(record: user::UserRecord) -> Self {
        Self {
            id: record.id.clone(),
            email: record.email.clone(),
            name: display_name_for_user(&record),
            avatar_url: record.avatar_url.clone(),
            email_verified: record.email_verified_at.is_some(),
            disabled: record.disabled,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ListUsersResponse {
    pub(crate) users: Vec<UserResponse>,
    pub(crate) total: i64,
    pub(crate) skip: i64,
    pub(crate) first: i64,
}

#[derive(Serialize)]
pub(crate) struct CreateWorkspaceResponse {
    pub(crate) id: String,
    pub(crate) name: String,
}

#[derive(Serialize)]
pub(crate) struct DocumentHistoryItem {
    pub(crate) snapshot: String,
    pub(crate) created_at: i64,
}

impl From<EncodedDocHistory> for DocumentHistoryItem {
    fn from(history: EncodedDocHistory) -> Self {
        Self {
            snapshot: history.snapshot,
            created_at: history.created_at,
        }
    }
}

impl From<DocumentHistoryRecord> for DocumentHistoryItem {
    fn from(record: DocumentHistoryRecord) -> Self {
        EncodedDocHistory::from(record).into()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DocumentMetadataResponse {
    pub(crate) id: String,
    pub(crate) workspace_id: String,
    pub(crate) created_at: i64,
    pub(crate) updated_at: i64,
    pub(crate) default_role: String,
    pub(crate) public: bool,
    pub(crate) blocked: bool,
    pub(crate) mode: String,
    pub(crate) title: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) creator_id: Option<String>,
    pub(crate) updater_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) share_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) share_token: Option<String>,
}

impl From<DocumentMetadata> for DocumentMetadataResponse {
    fn from(metadata: DocumentMetadata) -> Self {
        let share_url = metadata::public_share_url(&metadata);
        let DocumentMetadata {
            id,
            workspace_id,
            created_at,
            updated_at,
            default_role,
            public,
            blocked,
            mode,
            title,
            summary,
            creator_id,
            updater_id,
            share_token,
            ..
        } = metadata;

        Self {
            id,
            workspace_id,
            created_at,
            updated_at,
            default_role,
            public,
            blocked,
            mode,
            title,
            summary,
            creator_id,
            updater_id,
            share_url,
            share_token,
        }
    }
}

#[derive(Serialize)]
pub(crate) struct HealthResponse {
    pub(crate) status: &'static str,
}
