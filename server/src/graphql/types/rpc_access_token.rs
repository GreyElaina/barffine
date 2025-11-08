use async_graphql::{ID, InputObject, SimpleObject};

#[derive(SimpleObject, Clone)]
#[graphql(name = "RpcAccessToken")]
pub struct RpcAccessTokenObject {
    #[graphql(name = "workspaceId")]
    pub workspace_id: ID,
    #[graphql(name = "docId")]
    pub doc_id: Option<ID>,
    pub token: String,
}

impl RpcAccessTokenObject {
    pub fn new(workspace_id: impl Into<ID>, doc_id: Option<ID>, token: String) -> Self {
        Self {
            workspace_id: workspace_id.into(),
            doc_id,
            token,
        }
    }
}

#[derive(InputObject)]
#[graphql(name = "GenerateRpcAccessTokenInput")]
pub struct GenerateRpcAccessTokenInput {
    #[graphql(name = "workspaceId")]
    pub workspace_id: ID,
    #[graphql(name = "docId")]
    pub doc_id: Option<ID>,
}
