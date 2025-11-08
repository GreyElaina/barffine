use async_graphql::{ID, InputObject};

use crate::graphql::doc::DocRole;

use barffine_core::doc_store::DocumentMetadata;

#[derive(Clone)]
pub struct DocType {
    pub metadata: DocumentMetadata,
}

#[derive(InputObject, Clone)]
#[graphql(name = "ApplyDocUpdatesInput")]
pub struct ApplyDocUpdatesInput {
    #[graphql(name = "workspaceId")]
    pub workspace_id: String,
    #[graphql(name = "docId")]
    pub doc_id: String,
    pub updates: Vec<String>,
}

#[derive(InputObject, Clone)]
#[graphql(name = "GrantDocUserRolesInput")]
pub struct GrantDocUserRolesInput {
    #[graphql(name = "workspaceId")]
    pub workspace_id: String,
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "userIds")]
    pub user_ids: Vec<String>,
    pub role: DocRole,
}

#[derive(InputObject, Clone)]
#[graphql(name = "RevokeDocUserRoleInput")]
pub struct RevokeDocUserRoleInput {
    #[graphql(name = "workspaceId")]
    pub workspace_id: String,
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "userId")]
    pub user_id: String,
}

#[derive(InputObject, Clone)]
#[graphql(name = "UpdateDocUserRoleInput")]
pub struct UpdateDocUserRoleInput {
    #[graphql(name = "workspaceId")]
    pub workspace_id: String,
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "userId")]
    pub user_id: String,
    pub role: DocRole,
}

#[derive(InputObject, Clone)]
#[graphql(name = "UpdateDocDefaultRoleInput")]
pub struct UpdateDocDefaultRoleInput {
    #[graphql(name = "workspaceId")]
    pub workspace_id: String,
    #[graphql(name = "docId")]
    pub doc_id: String,
    pub role: DocRole,
}

#[derive(InputObject, Clone)]
#[graphql(name = "DuplicateDocInput")]
pub struct DuplicateDocInput {
    #[graphql(name = "workspaceId")]
    pub workspace_id: String,
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "targetWorkspaceId")]
    pub target_workspace_id: Option<String>,
    #[graphql(name = "targetDocId")]
    pub target_doc_id: Option<String>,
    pub title: Option<String>,
}

#[derive(InputObject, Clone)]
#[graphql(name = "RestoreDocHistoryInput")]
pub struct RestoreDocHistoryInput {
    #[graphql(name = "workspaceId")]
    pub workspace_id: String,
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "historyId")]
    pub history_id: ID,
}
