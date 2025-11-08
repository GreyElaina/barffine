use async_graphql::{Context, Enum, ID, InputObject, Json, Object, Result as GraphQLResult};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use barffine_core::notification::{
    CommentChangeAction as CoreCommentChangeAction, CommentChangeRecord, CommentRecord,
    CommentReplyRecord, CommentVisibility,
};

use crate::graphql::PublicUserType;
use crate::{AppState, comment::fetch as comment_fetch, graphql::helpers::map_app_error};

use super::common::PageInfo;

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(name = "CommentChangeAction")]
pub enum CommentChangeAction {
    #[graphql(name = "update")]
    Update,
    #[graphql(name = "delete")]
    Delete,
}

impl From<CoreCommentChangeAction> for CommentChangeAction {
    fn from(value: CoreCommentChangeAction) -> Self {
        match value {
            CoreCommentChangeAction::Update => CommentChangeAction::Update,
            CoreCommentChangeAction::Delete => CommentChangeAction::Delete,
        }
    }
}

#[derive(Clone)]
pub struct CommentObjectTypeEdge {
    pub cursor: String,
    pub node: CommentObjectType,
}

#[Object(name = "CommentObjectTypeEdge")]
impl CommentObjectTypeEdge {
    async fn cursor(&self) -> &str {
        &self.cursor
    }

    async fn node(&self) -> &CommentObjectType {
        &self.node
    }
}

#[derive(Clone)]
pub struct PaginatedCommentObjectType {
    pub edges: Vec<CommentObjectTypeEdge>,
    pub page_info: PageInfo,
    pub total_count: i32,
}

#[Object(name = "PaginatedCommentObjectType")]
impl PaginatedCommentObjectType {
    async fn edges(&self) -> &[CommentObjectTypeEdge] {
        &self.edges
    }

    #[graphql(name = "pageInfo")]
    async fn page_info(&self) -> &PageInfo {
        &self.page_info
    }

    #[graphql(name = "totalCount")]
    async fn total_count(&self) -> i32 {
        self.total_count
    }
}

#[derive(Clone)]
pub struct CommentChangeObjectTypeEdge {
    pub cursor: String,
    pub node: CommentChangeObjectType,
}

#[Object(name = "CommentChangeObjectTypeEdge")]
impl CommentChangeObjectTypeEdge {
    async fn cursor(&self) -> &str {
        &self.cursor
    }

    async fn node(&self) -> &CommentChangeObjectType {
        &self.node
    }
}

#[derive(Clone)]
pub struct PaginatedCommentChangeObjectType {
    pub edges: Vec<CommentChangeObjectTypeEdge>,
    pub page_info: PageInfo,
    pub total_count: i32,
}

#[Object(name = "PaginatedCommentChangeObjectType")]
impl PaginatedCommentChangeObjectType {
    async fn edges(&self) -> &[CommentChangeObjectTypeEdge] {
        &self.edges
    }

    #[graphql(name = "pageInfo")]
    async fn page_info(&self) -> &PageInfo {
        &self.page_info
    }

    #[graphql(name = "totalCount")]
    async fn total_count(&self) -> i32 {
        self.total_count
    }
}

#[derive(Clone)]
pub struct CommentChangeObjectType {
    pub(crate) action: CommentChangeAction,
    pub(crate) id: String,
    pub(crate) comment_id: Option<String>,
    pub(crate) item: JsonValue,
    pub(crate) updated_at: DateTime<Utc>,
}

impl From<CommentChangeRecord> for CommentChangeObjectType {
    fn from(record: CommentChangeRecord) -> Self {
        Self {
            action: CommentChangeAction::from(record.action),
            id: record.id,
            comment_id: record.comment_id,
            item: record.item,
            updated_at: record.updated_at,
        }
    }
}

#[Object(name = "CommentChangeObjectType")]
impl CommentChangeObjectType {
    async fn action(&self) -> CommentChangeAction {
        self.action
    }

    async fn id(&self) -> &str {
        &self.id
    }

    #[graphql(name = "commentId")]
    async fn comment_id(&self) -> Option<&str> {
        self.comment_id.as_deref()
    }

    async fn item(&self) -> Json<JsonValue> {
        Json(self.item.clone())
    }

    #[graphql(name = "updatedAt")]
    async fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

#[derive(InputObject, Clone)]
#[graphql(name = "CommentCreateInput")]
pub struct CommentCreateInput {
    pub content: JsonValue,
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "docMode")]
    pub doc_mode: Option<String>,
    #[graphql(name = "docTitle")]
    pub doc_title: Option<String>,
    pub mentions: Option<Vec<String>>,
    #[graphql(name = "workspaceId")]
    pub workspace_id: String,
}

#[derive(InputObject, Clone)]
#[graphql(name = "CommentResolveInput")]
pub struct CommentResolveInput {
    pub id: ID,
    pub resolved: bool,
}

#[derive(InputObject, Clone)]
#[graphql(name = "CommentUpdateInput")]
pub struct CommentUpdateInput {
    pub content: JsonValue,
    pub id: ID,
}

#[derive(InputObject, Clone)]
#[graphql(name = "ReplyCreateInput")]
pub struct ReplyCreateInput {
    #[graphql(name = "commentId")]
    pub comment_id: ID,
    pub content: JsonValue,
    #[graphql(name = "docMode")]
    pub doc_mode: Option<String>,
    #[graphql(name = "docTitle")]
    pub doc_title: Option<String>,
    pub mentions: Option<Vec<String>>,
}

#[derive(InputObject, Clone)]
#[graphql(name = "ReplyUpdateInput")]
pub struct ReplyUpdateInput {
    pub content: JsonValue,
    pub id: ID,
}

#[derive(Clone)]
pub struct ReplyObjectType {
    pub(crate) record: CommentReplyRecord,
}

impl ReplyObjectType {
    pub fn from_record(record: CommentReplyRecord) -> Self {
        Self { record }
    }

    fn body_json(&self) -> JsonValue {
        serde_json::from_str(&self.record.body).unwrap_or(JsonValue::Null)
    }
}

#[Object(name = "ReplyObjectType")]
impl ReplyObjectType {
    #[graphql(name = "commentId")]
    async fn comment_id(&self) -> ID {
        ID(self.record.comment_id.clone())
    }

    async fn content(&self) -> Json<JsonValue> {
        Json(self.body_json())
    }

    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> DateTime<Utc> {
        self.record.created_at
    }

    async fn id(&self) -> ID {
        ID(self.record.id.clone())
    }

    #[graphql(name = "updatedAt")]
    async fn updated_at(&self) -> DateTime<Utc> {
        self.record.updated_at
    }

    async fn user(&self, ctx: &Context<'_>) -> GraphQLResult<PublicUserType> {
        let state = ctx.data::<AppState>()?;
        let user = state
            .user_service
            .fetch_user(&self.record.author_id)
            .await
            .map_err(map_app_error)?;
        Ok(PublicUserType::from_user(&user))
    }
}

#[derive(Clone)]
pub struct CommentObjectType {
    pub(crate) record: CommentRecord,
    replies: Option<Vec<CommentReplyRecord>>,
}

impl CommentObjectType {
    pub fn from_record(record: CommentRecord) -> Self {
        Self {
            record,
            replies: None,
        }
    }

    pub fn from_record_with_replies(
        record: CommentRecord,
        replies: Vec<CommentReplyRecord>,
    ) -> Self {
        Self {
            record,
            replies: Some(replies),
        }
    }

    fn body_json(&self) -> JsonValue {
        serde_json::from_str(&self.record.body).unwrap_or(JsonValue::Null)
    }
}

#[Object(name = "CommentObjectType")]
impl CommentObjectType {
    async fn content(&self) -> Json<JsonValue> {
        Json(self.body_json())
    }

    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> DateTime<Utc> {
        self.record.created_at
    }

    async fn id(&self) -> ID {
        ID(self.record.id.clone())
    }

    #[graphql(name = "resolved")]
    async fn resolved(&self) -> bool {
        self.record.resolved
    }

    #[graphql(name = "docId")]
    async fn doc_id(&self) -> &str {
        &self.record.doc_id
    }

    async fn user(&self, ctx: &Context<'_>) -> GraphQLResult<PublicUserType> {
        let state = ctx.data::<AppState>()?;
        let user = state
            .user_service
            .fetch_user(&self.record.author_id)
            .await
            .map_err(map_app_error)?;
        Ok(PublicUserType::from_user(&user))
    }

    async fn replies(&self, ctx: &Context<'_>) -> GraphQLResult<Vec<ReplyObjectType>> {
        if let Some(cached) = &self.replies {
            return Ok(cached
                .iter()
                .cloned()
                .map(ReplyObjectType::from_record)
                .collect());
        }

        let state = ctx.data::<AppState>()?;
        let replies = comment_fetch::fetch_replies_for_comment(state, &self.record.id)
            .await
            .map_err(map_app_error)?;
        Ok(replies
            .into_iter()
            .map(ReplyObjectType::from_record)
            .collect())
    }

    #[graphql(name = "updatedAt")]
    async fn updated_at(&self) -> DateTime<Utc> {
        self.record.updated_at
    }
}

pub fn build_comment_record(
    id: String,
    workspace_id: String,
    doc_id: String,
    author_id: String,
    content: JsonValue,
    metadata: JsonValue,
) -> CommentRecord {
    CommentRecord {
        id,
        workspace_id,
        doc_id,
        author_id,
        body: serde_json::to_string(&content).unwrap_or_else(|_| "null".to_string()),
        visibility: CommentVisibility::Workspace,
        metadata,
        resolved: false,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

pub fn build_reply_record(
    id: String,
    comment_id: String,
    author_id: String,
    content: JsonValue,
    metadata: JsonValue,
) -> CommentReplyRecord {
    CommentReplyRecord {
        id,
        comment_id,
        author_id,
        body: serde_json::to_string(&content).unwrap_or_else(|_| "null".to_string()),
        metadata,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}
