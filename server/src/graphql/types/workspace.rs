use async_graphql::{ID, InputObject};
use chrono::{DateTime, Utc};

#[derive(InputObject, Clone, Default)]
#[graphql(name = "CreateWorkspaceInput")]
pub struct CreateWorkspaceInput {
    #[graphql(name = "ownerId")]
    pub owner_id: Option<String>,
    pub name: Option<String>,
    pub public: Option<bool>,
    #[graphql(name = "enableAi")]
    pub enable_ai: Option<bool>,
    #[graphql(name = "enableDocEmbedding")]
    pub enable_doc_embedding: Option<bool>,
    #[graphql(name = "enableUrlPreview")]
    pub enable_url_preview: Option<bool>,
    #[graphql(name = "inviteUserIds")]
    pub invite_user_ids: Option<Vec<String>>,
}

#[derive(InputObject, Clone)]
#[graphql(name = "UpdateWorkspaceInput")]
pub struct UpdateWorkspaceInput {
    pub id: ID,
    pub name: Option<String>,
    pub public: Option<bool>,
    #[graphql(name = "enableAi")]
    pub enable_ai: Option<bool>,
    #[graphql(name = "enableDocEmbedding")]
    pub enable_doc_embedding: Option<bool>,
    #[graphql(name = "enableUrlPreview")]
    pub enable_url_preview: Option<bool>,
    #[graphql(name = "avatarKey")]
    pub avatar_key: Option<Option<String>>,
    pub indexed: Option<bool>,
    #[graphql(name = "lastCheckEmbeddings")]
    pub last_check_embeddings: Option<DateTime<Utc>>,
}
