use std::sync::Arc;

use async_graphql::{
    Context, Enum, ID, InputObject, Json, Object, Result as GraphQLResult, SimpleObject, Upload,
};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use crate::AppState;

use super::{
    helpers::map_app_error,
    types::{PageInfo, PaginationInput},
};

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "CopilotPromptMessageRole")]
pub(crate) enum CopilotPromptMessageRole {
    #[graphql(name = "assistant")]
    Assistant,
    #[graphql(name = "system")]
    System,
    #[graphql(name = "user")]
    User,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotPromptConfigType")]
pub(crate) struct CopilotPromptConfigType {
    #[graphql(name = "frequencyPenalty")]
    pub(crate) frequency_penalty: Option<f64>,
    #[graphql(name = "presencePenalty")]
    pub(crate) presence_penalty: Option<f64>,
    pub(crate) temperature: Option<f64>,
    #[graphql(name = "topP")]
    pub(crate) top_p: Option<f64>,
}

impl CopilotPromptConfigType {
    pub(crate) fn new(
        frequency_penalty: Option<f64>,
        presence_penalty: Option<f64>,
        temperature: Option<f64>,
        top_p: Option<f64>,
    ) -> Self {
        Self {
            frequency_penalty,
            presence_penalty,
            temperature,
            top_p,
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotPromptMessageType")]
pub(crate) struct CopilotPromptMessageType {
    pub(crate) content: String,
    pub(crate) params: Option<Json<JsonValue>>,
    pub(crate) role: CopilotPromptMessageRole,
}

impl CopilotPromptMessageType {
    pub(crate) fn new(
        role: CopilotPromptMessageRole,
        content: impl Into<String>,
        params: Option<JsonValue>,
    ) -> Self {
        Self {
            content: content.into(),
            params: params.map(Json),
            role,
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotPromptType")]
pub(crate) struct CopilotPromptType {
    pub(crate) action: Option<String>,
    pub(crate) config: Option<CopilotPromptConfigType>,
    pub(crate) messages: Vec<CopilotPromptMessageType>,
    pub(crate) model: String,
    pub(crate) name: String,
}

impl CopilotPromptType {
    pub(crate) fn new(
        name: impl Into<String>,
        model: impl Into<String>,
        action: Option<String>,
        config: Option<CopilotPromptConfigType>,
        messages: Vec<CopilotPromptMessageType>,
    ) -> Self {
        Self {
            action,
            config,
            messages,
            model: model.into(),
            name: name.into(),
        }
    }
}

pub(crate) fn default_copilot_prompts() -> Vec<CopilotPromptType> {
    vec![
        CopilotPromptType::new(
            "doc-editor",
            "morph-v3-large",
            Some("doc.edit".to_string()),
            Some(CopilotPromptConfigType::new(
                Some(0.0),
                Some(0.0),
                Some(0.3),
                Some(0.9),
            )),
            vec![CopilotPromptMessageType::new(
                CopilotPromptMessageRole::System,
                "You are a helpful writing assistant specialised in AFFiNE doc updates.",
                None,
            )],
        ),
        CopilotPromptType::new(
            "doc-summary",
            "morph-v3-large",
            Some("doc.summary".to_string()),
            Some(CopilotPromptConfigType::new(
                Some(0.0),
                Some(0.0),
                Some(0.4),
                Some(0.9),
            )),
            vec![CopilotPromptMessageType::new(
                CopilotPromptMessageRole::System,
                "Summarise the given AFFiNE document content in a concise paragraph.",
                None,
            )],
        ),
    ]
}

#[derive(Clone)]
pub(crate) struct CopilotType {
    pub(super) user_id: String,
    pub(super) workspace_id: Option<String>,
}

impl CopilotType {
    pub(crate) fn new(user_id: String, workspace_id: Option<String>) -> Self {
        Self {
            user_id,
            workspace_id,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CopilotMessageRecord {
    pub id: String,
    pub session_id: String,
    pub role: String,
    pub content: Option<String>,
    pub attachments: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub(crate) struct CopilotSessionRecord {
    pub id: String,
    pub workspace_id: String,
    pub doc_id: Option<String>,
    pub user_id: String,
    pub prompt_name: String,
    pub pinned: bool,
    pub parent_session_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub model: String,
    pub messages: Vec<CopilotMessageRecord>,
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value
        .map(|input| input.trim().to_string())
        .filter(|trimmed| !trimmed.is_empty())
}

fn collect_sessions_for_user(
    state: &AppState,
    user_id: &str,
    workspace_id: &str,
) -> Vec<Arc<CopilotSessionRecord>> {
    let mut sessions = Vec::new();
    for entry in state.copilot_sessions.iter() {
        let record = entry.value();
        if record.user_id == user_id && record.workspace_id == workspace_id {
            sessions.push(Arc::new(record.clone()));
        }
    }
    sessions
}

#[Object(name = "Copilot")]
impl CopilotType {
    async fn workspace_id(&self) -> Option<ID> {
        self.workspace_id.clone().map(ID::from)
    }

    async fn quota(&self) -> CopilotQuotaType {
        CopilotQuotaType::default()
    }

    async fn models(
        &self,
        #[graphql(name = "promptName")] _prompt_name: String,
    ) -> CopilotModelsType {
        CopilotModelsType::default()
    }

    async fn contexts(
        &self,
        #[graphql(name = "contextId")] _context_id: Option<String>,
        #[graphql(name = "sessionId")] _session_id: Option<String>,
    ) -> GraphQLResult<Vec<CopilotContextType>> {
        let _ = &self.user_id;
        Ok(Vec::new())
    }

    async fn chats(
        &self,
        ctx: &Context<'_>,
        pagination: PaginationInput,
        #[graphql(name = "docId")] doc_id: Option<String>,
        #[graphql(name = "options")] options: Option<QueryChatHistoriesInput>,
    ) -> GraphQLResult<PaginatedCopilotHistoriesType> {
        let Some(workspace_id) = self.workspace_id.as_ref() else {
            return Ok(PaginatedCopilotHistoriesType::empty());
        };

        let (limit, offset, _) = pagination.normalize().map_err(map_app_error)?;
        let offset = offset as usize;
        let limit = limit as usize;

        let state = ctx.data::<AppState>()?;
        let mut records = collect_sessions_for_user(state, &self.user_id, workspace_id);

        if let Some(doc_filter) = normalize_optional(doc_id) {
            records.retain(|record| record.doc_id.as_deref() == Some(doc_filter.as_str()));
        }

        let opts = options.unwrap_or_default();

        if let Some(ref session_id) = opts.session_id {
            let normalized = session_id.trim();
            if !normalized.is_empty() {
                records.retain(|record| record.id == normalized);
            }
        }

        if let Some(pinned) = opts.pinned {
            records.retain(|record| record.pinned == pinned);
        }

        if let Some(fork_only) = opts.fork {
            records.retain(|record| record.parent_session_id.is_some() == fork_only);
        }

        let session_order = opts.session_order.unwrap_or(ChatHistoryOrder::Desc);
        records.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));
        if matches!(session_order, ChatHistoryOrder::Desc) {
            records.reverse();
        }

        let total_count = records.len();
        let slice = records
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();

        let include_messages = opts.with_messages.unwrap_or(false);
        let message_order = opts.message_order.unwrap_or(ChatHistoryOrder::Desc);

        let nodes = slice
            .into_iter()
            .map(|record| {
                let messages = if include_messages {
                    let mut items: Vec<_> = record
                        .messages
                        .iter()
                        .cloned()
                        .map(|msg| Arc::new(msg))
                        .collect();
                    items.sort_by(|a, b| a.created_at.cmp(&b.created_at));
                    if matches!(message_order, ChatHistoryOrder::Desc) {
                        items.reverse();
                    }
                    items
                } else {
                    Vec::new()
                };
                (record.clone(), messages)
            })
            .collect();

        Ok(PaginatedCopilotHistoriesType::from_records(
            nodes,
            offset,
            total_count,
        ))
    }

    async fn session(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "sessionId")] session_id: String,
    ) -> GraphQLResult<Option<CopilotSessionType>> {
        let trimmed = session_id.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        let state = ctx.data::<AppState>()?;
        let Some(entry) = state.copilot_sessions.get(trimmed) else {
            return Ok(None);
        };
        let record = entry.value();
        if record.user_id != self.user_id {
            return Ok(None);
        }
        if let Some(workspace_id) = &self.workspace_id {
            if &record.workspace_id != workspace_id {
                return Ok(None);
            }
        }
        Ok(Some(CopilotSessionType::from_record(Arc::new(
            record.clone(),
        ))))
    }

    async fn sessions(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "docId")] doc_id: Option<String>,
        #[graphql(name = "options")] options: Option<QueryChatSessionsInput>,
    ) -> GraphQLResult<Vec<CopilotSessionType>> {
        let Some(workspace_id) = self.workspace_id.as_ref() else {
            return Ok(Vec::new());
        };

        let state = ctx.data::<AppState>()?;
        let mut records = collect_sessions_for_user(state, &self.user_id, workspace_id);

        if let Some(doc_filter) = normalize_optional(doc_id) {
            records.retain(|record| record.doc_id.as_deref() == Some(doc_filter.as_str()));
        }

        let opts = options.unwrap_or_default();
        if let Some(pinned) = opts.pinned {
            records.retain(|record| record.pinned == pinned);
        }
        if let Some(fork_only) = opts.fork {
            records.retain(|record| record.parent_session_id.is_some() == fork_only);
        }

        records.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));
        records.reverse();

        let skip = opts.skip.unwrap_or(0).max(0) as usize;
        let limit = opts.limit.unwrap_or(20).clamp(1, 100) as usize;

        let slice = records
            .into_iter()
            .skip(skip)
            .take(limit)
            .map(CopilotSessionType::from_record)
            .collect();

        Ok(slice)
    }

    async fn histories(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "docId")] doc_id: Option<String>,
        #[graphql(name = "options")] options: Option<QueryChatHistoriesInput>,
    ) -> GraphQLResult<Vec<CopilotHistories>> {
        let Some(workspace_id) = self.workspace_id.as_ref() else {
            return Ok(Vec::new());
        };

        let state = ctx.data::<AppState>()?;
        let mut records = collect_sessions_for_user(state, &self.user_id, workspace_id);

        if let Some(doc_filter) = normalize_optional(doc_id) {
            records.retain(|record| record.doc_id.as_deref() == Some(doc_filter.as_str()));
        }

        let opts = options.unwrap_or_default();
        if let Some(ref session_id) = opts.session_id {
            let normalized = session_id.trim();
            if !normalized.is_empty() {
                records.retain(|record| record.id == normalized);
            }
        }
        if let Some(pinned) = opts.pinned {
            records.retain(|record| record.pinned == pinned);
        }
        if let Some(fork_only) = opts.fork {
            records.retain(|record| record.parent_session_id.is_some() == fork_only);
        }

        records.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));
        if matches!(
            opts.session_order.unwrap_or(ChatHistoryOrder::Desc),
            ChatHistoryOrder::Desc
        ) {
            records.reverse();
        }

        let skip = opts.skip.unwrap_or(0).max(0) as usize;
        let limit = opts.limit.unwrap_or(20).clamp(1, 100) as usize;

        let include_messages = opts.with_messages.unwrap_or(false);
        let message_order = opts.message_order.unwrap_or(ChatHistoryOrder::Desc);

        let slice = records
            .into_iter()
            .skip(skip)
            .take(limit)
            .map(|record| {
                let messages = if include_messages {
                    let mut items: Vec<_> = record
                        .messages
                        .iter()
                        .cloned()
                        .map(|m| Arc::new(m))
                        .collect();
                    items.sort_by(|a, b| a.created_at.cmp(&b.created_at));
                    if matches!(message_order, ChatHistoryOrder::Desc) {
                        items.reverse();
                    }
                    items
                } else {
                    Vec::new()
                };
                CopilotHistories::from_record(record, messages)
            })
            .collect();

        Ok(slice)
    }

    async fn audio_transcription(
        &self,
        #[graphql(name = "blobId")] _blob_id: Option<String>,
        #[graphql(name = "jobId")] _job_id: Option<String>,
    ) -> GraphQLResult<Option<TranscriptionResultType>> {
        let _ = &self.user_id;
        Ok(None)
    }
}

#[derive(InputObject, Clone)]
#[graphql(name = "CreateChatSessionInput")]
pub(crate) struct CreateChatSessionInput {
    #[graphql(name = "workspaceId")]
    pub(crate) workspace_id: String,
    #[graphql(name = "docId")]
    pub(crate) doc_id: Option<String>,
    #[graphql(name = "promptName")]
    pub(crate) prompt_name: String,
    pub(crate) pinned: Option<bool>,
    #[graphql(name = "reuseLatestChat")]
    pub(crate) reuse_latest_chat: Option<bool>,
}

#[derive(InputObject, Clone)]
#[graphql(name = "UpdateChatSessionInput")]
pub(crate) struct UpdateChatSessionInput {
    #[graphql(name = "sessionId")]
    pub(crate) session_id: String,
    #[graphql(name = "docId")]
    pub(crate) doc_id: Option<String>,
    pub(crate) pinned: Option<bool>,
    #[graphql(name = "promptName")]
    pub(crate) prompt_name: Option<String>,
}

#[derive(InputObject, Clone)]
#[graphql(name = "ForkChatSessionInput")]
pub(crate) struct ForkChatSessionInput {
    #[graphql(name = "workspaceId")]
    pub(crate) workspace_id: String,
    #[graphql(name = "docId")]
    pub(crate) doc_id: String,
    #[graphql(name = "sessionId")]
    pub(crate) session_id: String,
    #[graphql(name = "latestMessageId")]
    pub(crate) latest_message_id: Option<String>,
}

#[derive(InputObject, Clone)]
#[graphql(name = "DeleteSessionInput")]
pub(crate) struct DeleteSessionInput {
    #[graphql(name = "workspaceId")]
    pub(crate) workspace_id: String,
    #[graphql(name = "docId")]
    pub(crate) doc_id: Option<String>,
    #[graphql(name = "sessionIds")]
    pub(crate) session_ids: Vec<String>,
}

#[derive(InputObject)]
#[graphql(name = "CreateChatMessageInput")]
pub(crate) struct CreateChatMessageInput {
    #[graphql(name = "sessionId")]
    pub(crate) session_id: String,
    pub(crate) content: Option<String>,
    pub(crate) attachments: Option<Vec<String>>,
    pub(crate) blob: Option<Upload>,
    pub(crate) blobs: Option<Vec<Upload>>,
    pub(crate) params: Option<JsonValue>,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotQuota")]
pub(crate) struct CopilotQuotaType {
    limit: Option<i64>,
    used: i64,
}

impl Default for CopilotQuotaType {
    fn default() -> Self {
        Self {
            limit: None,
            used: 0,
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotModelType")]
pub(crate) struct CopilotModelType {
    id: String,
    name: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotModelsType")]
pub(crate) struct CopilotModelsType {
    #[graphql(name = "defaultModel")]
    default_model: String,
    #[graphql(name = "optionalModels")]
    optional_models: Vec<CopilotModelType>,
    #[graphql(name = "proModels")]
    pro_models: Vec<CopilotModelType>,
}

impl Default for CopilotModelsType {
    fn default() -> Self {
        Self {
            default_model: "disabled".to_string(),
            optional_models: Vec::new(),
            pro_models: Vec::new(),
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotWorkspaceFile")]
pub(crate) struct CopilotWorkspaceFileType {
    #[graphql(name = "workspaceId")]
    workspace_id: ID,
    #[graphql(name = "fileId")]
    file_id: ID,
    #[graphql(name = "fileName")]
    file_name: String,
    #[graphql(name = "blobId")]
    blob_id: String,
}

impl CopilotWorkspaceFileType {
    pub fn new(workspace_id: &str, file_id: &str, file_name: &str, blob_id: &str) -> Self {
        Self {
            workspace_id: ID::from(workspace_id.to_owned()),
            file_id: ID::from(file_id.to_owned()),
            file_name: file_name.to_owned(),
            blob_id: blob_id.to_owned(),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct CopilotContextType {
    id: Option<String>,
    workspace_id: Option<String>,
}

#[Object(name = "CopilotContext")]
impl CopilotContextType {
    async fn id(&self) -> Option<ID> {
        self.id.clone().map(ID::from)
    }

    #[graphql(name = "workspaceId")]
    async fn workspace_id(&self) -> Option<String> {
        self.workspace_id.clone()
    }

    async fn blobs(&self) -> Vec<CopilotContextBlob> {
        Vec::new()
    }

    async fn collections(&self) -> Vec<CopilotContextCategory> {
        Vec::new()
    }

    async fn docs(&self) -> Vec<CopilotContextDoc> {
        Vec::new()
    }

    async fn files(&self) -> Vec<CopilotContextFile> {
        Vec::new()
    }

    async fn tags(&self) -> Vec<CopilotContextCategory> {
        Vec::new()
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "ContextCategories")]
pub(crate) enum ContextCategoriesEnum {
    Collection,
    Tag,
}

#[derive(InputObject, Clone)]
#[graphql(name = "AddContextCategoryInput")]
pub(crate) struct AddContextCategoryInput {
    #[graphql(name = "contextId")]
    pub(crate) context_id: String,
    #[graphql(name = "type")]
    pub(crate) context_type: ContextCategoriesEnum,
    #[graphql(name = "categoryId")]
    pub(crate) category_id: String,
    pub(crate) docs: Option<Vec<String>>,
}

#[derive(InputObject, Clone)]
#[graphql(name = "RemoveContextCategoryInput")]
pub(crate) struct RemoveContextCategoryInput {
    #[graphql(name = "contextId")]
    pub(crate) context_id: String,
    #[graphql(name = "type")]
    pub(crate) context_type: ContextCategoriesEnum,
    #[graphql(name = "categoryId")]
    pub(crate) category_id: String,
}

#[derive(InputObject, Clone)]
#[graphql(name = "AddContextDocInput")]
pub(crate) struct AddContextDocInput {
    #[graphql(name = "contextId")]
    pub(crate) context_id: String,
    #[graphql(name = "docId")]
    pub(crate) doc_id: String,
}

#[derive(InputObject, Clone)]
#[graphql(name = "RemoveContextDocInput")]
pub(crate) struct RemoveContextDocInput {
    #[graphql(name = "contextId")]
    pub(crate) context_id: String,
    #[graphql(name = "docId")]
    pub(crate) doc_id: String,
}

#[derive(InputObject, Clone)]
#[graphql(name = "AddContextFileInput")]
pub(crate) struct AddContextFileInput {
    #[graphql(name = "contextId")]
    pub(crate) context_id: String,
    #[graphql(name = "blobId")]
    pub(crate) blob_id: Option<String>,
}

#[derive(InputObject, Clone)]
#[graphql(name = "RemoveContextFileInput")]
pub(crate) struct RemoveContextFileInput {
    #[graphql(name = "contextId")]
    pub(crate) context_id: String,
    #[graphql(name = "fileId")]
    pub(crate) file_id: String,
}

#[derive(InputObject, Clone)]
#[graphql(name = "AddContextBlobInput")]
pub(crate) struct AddContextBlobInput {
    #[graphql(name = "contextId")]
    pub(crate) context_id: String,
    #[graphql(name = "blobId")]
    pub(crate) blob_id: String,
}

#[derive(InputObject, Clone)]
#[graphql(name = "RemoveContextBlobInput")]
pub(crate) struct RemoveContextBlobInput {
    #[graphql(name = "contextId")]
    pub(crate) context_id: String,
    #[graphql(name = "blobId")]
    pub(crate) blob_id: String,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "ContextEmbedStatus")]
pub(crate) enum ContextEmbedStatusEnum {
    Failed,
    Finished,
    Processing,
}

#[derive(Clone, Default)]
pub(crate) struct CopilotContextBlob;

#[Object(name = "CopilotContextBlob")]
impl CopilotContextBlob {
    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> i64 {
        0
    }

    async fn id(&self) -> ID {
        ID::from("blob")
    }

    async fn status(&self) -> ContextEmbedStatusEnum {
        ContextEmbedStatusEnum::Processing
    }
}

#[derive(Clone, Default)]
pub(crate) struct CopilotContextCategory;

#[Object(name = "CopilotContextCategory")]
impl CopilotContextCategory {
    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> i64 {
        0
    }

    async fn docs(&self) -> Vec<CopilotContextDoc> {
        Vec::new()
    }

    async fn id(&self) -> ID {
        ID::from("category")
    }

    #[graphql(name = "type")]
    async fn category_type(&self) -> ContextCategoriesEnum {
        ContextCategoriesEnum::Collection
    }
}

#[derive(Clone, Default)]
pub(crate) struct CopilotContextDoc;

#[Object(name = "CopilotContextDoc")]
impl CopilotContextDoc {
    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> i64 {
        0
    }

    async fn id(&self) -> ID {
        ID::from("doc")
    }

    async fn status(&self) -> ContextEmbedStatusEnum {
        ContextEmbedStatusEnum::Processing
    }
}

#[derive(Clone, Default)]
pub(crate) struct CopilotContextFile;

#[Object(name = "CopilotContextFile")]
impl CopilotContextFile {
    #[graphql(name = "blobId")]
    async fn blob_id(&self) -> String {
        String::new()
    }

    #[graphql(name = "chunkSize")]
    async fn chunk_size(&self) -> i64 {
        0
    }

    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> i64 {
        0
    }

    async fn error(&self) -> Option<String> {
        None
    }

    async fn id(&self) -> ID {
        ID::from("file")
    }

    #[graphql(name = "mimeType")]
    async fn mime_type(&self) -> String {
        String::new()
    }

    async fn name(&self) -> String {
        String::new()
    }

    async fn status(&self) -> ContextEmbedStatusEnum {
        ContextEmbedStatusEnum::Processing
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "ChatHistoryOrder")]
pub(crate) enum ChatHistoryOrder {
    #[graphql(name = "asc")]
    Asc,
    #[graphql(name = "desc")]
    Desc,
}

#[derive(InputObject, Default, Clone)]
pub(crate) struct QueryChatHistoriesInput {
    pub(crate) action: Option<bool>,
    pub(crate) fork: Option<bool>,
    pub(crate) limit: Option<i32>,
    #[graphql(name = "messageOrder")]
    pub(crate) message_order: Option<ChatHistoryOrder>,
    pub(crate) pinned: Option<bool>,
    #[graphql(name = "sessionId")]
    pub(crate) session_id: Option<String>,
    #[graphql(name = "sessionOrder")]
    pub(crate) session_order: Option<ChatHistoryOrder>,
    pub(crate) skip: Option<i32>,
    #[graphql(name = "withMessages")]
    pub(crate) with_messages: Option<bool>,
    #[graphql(name = "withPrompt")]
    pub(crate) with_prompt: Option<bool>,
}

#[derive(InputObject, Default, Clone)]
pub(crate) struct QueryChatSessionsInput {
    pub(crate) action: Option<bool>,
    pub(crate) fork: Option<bool>,
    pub(crate) limit: Option<i32>,
    pub(crate) pinned: Option<bool>,
    pub(crate) skip: Option<i32>,
}

#[derive(Clone)]
pub(crate) struct CopilotSessionType {
    record: Arc<CopilotSessionRecord>,
}

impl CopilotSessionType {
    fn from_record(record: Arc<CopilotSessionRecord>) -> Self {
        Self { record }
    }
}

#[Object(name = "CopilotSessionType")]
impl CopilotSessionType {
    #[graphql(name = "docId")]
    async fn doc_id(&self) -> Option<String> {
        self.record.doc_id.clone()
    }

    async fn id(&self) -> ID {
        ID::from(self.record.id.clone())
    }

    async fn model(&self) -> String {
        self.record.model.clone()
    }

    #[graphql(name = "optionalModels")]
    async fn optional_models(&self) -> Vec<String> {
        Vec::new()
    }

    #[graphql(name = "parentSessionId")]
    async fn parent_session_id(&self) -> Option<ID> {
        self.record
            .parent_session_id
            .as_ref()
            .map(|id| ID::from(id.clone()))
    }

    async fn pinned(&self) -> bool {
        self.record.pinned
    }

    #[graphql(name = "promptName")]
    async fn prompt_name(&self) -> Option<String> {
        Some(self.record.prompt_name.clone())
    }

    async fn title(&self) -> Option<String> {
        None
    }
}

#[derive(Clone)]
pub(crate) struct CopilotHistories {
    record: Arc<CopilotSessionRecord>,
    messages: Vec<Arc<CopilotMessageRecord>>,
}

impl CopilotHistories {
    fn from_record(
        record: Arc<CopilotSessionRecord>,
        messages: Vec<Arc<CopilotMessageRecord>>,
    ) -> Self {
        Self { record, messages }
    }
}

#[Object(name = "CopilotHistories")]
impl CopilotHistories {
    async fn action(&self) -> Option<String> {
        None
    }

    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> DateTime<Utc> {
        self.record.created_at
    }

    #[graphql(name = "docId")]
    async fn doc_id(&self) -> Option<String> {
        self.record.doc_id.clone()
    }

    async fn messages(&self) -> Vec<ChatMessageType> {
        self.messages
            .iter()
            .cloned()
            .map(ChatMessageType::from_record)
            .collect()
    }

    async fn model(&self) -> String {
        self.record.model.clone()
    }

    #[graphql(name = "optionalModels")]
    async fn optional_models(&self) -> Vec<String> {
        Vec::new()
    }

    #[graphql(name = "parentSessionId")]
    async fn parent_session_id(&self) -> Option<String> {
        self.record.parent_session_id.clone()
    }

    async fn pinned(&self) -> bool {
        self.record.pinned
    }

    #[graphql(name = "promptName")]
    async fn prompt_name(&self) -> String {
        self.record.prompt_name.clone()
    }

    #[graphql(name = "sessionId")]
    async fn session_id(&self) -> String {
        self.record.id.clone()
    }

    async fn title(&self) -> Option<String> {
        None
    }

    #[graphql(name = "tokens")]
    async fn tokens(&self) -> i32 {
        0
    }

    #[graphql(name = "updatedAt")]
    async fn updated_at(&self) -> DateTime<Utc> {
        self.record.updated_at
    }

    #[graphql(name = "workspaceId")]
    async fn workspace_id(&self) -> Option<String> {
        Some(self.record.workspace_id.clone())
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotHistoriesTypeEdge")]
pub(crate) struct CopilotHistoriesTypeEdge {
    cursor: String,
    node: CopilotHistories,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "PaginatedCopilotHistoriesType")]
pub(crate) struct PaginatedCopilotHistoriesType {
    edges: Vec<CopilotHistoriesTypeEdge>,
    #[graphql(name = "pageInfo")]
    page_info: PageInfo,
    #[graphql(name = "totalCount")]
    total_count: i32,
}

impl PaginatedCopilotHistoriesType {
    pub(crate) fn empty() -> Self {
        Self {
            edges: Vec::new(),
            page_info: PageInfo::default(),
            total_count: 0,
        }
    }

    pub(crate) fn from_records(
        records: Vec<(Arc<CopilotSessionRecord>, Vec<Arc<CopilotMessageRecord>>)>,
        offset: usize,
        total_count: usize,
    ) -> Self {
        if records.is_empty() {
            return Self {
                edges: Vec::new(),
                page_info: PageInfo::default(),
                total_count: total_count as i32,
            };
        }

        let mut edges = Vec::with_capacity(records.len());
        for (index, (session, messages)) in records.into_iter().enumerate() {
            let cursor_index = offset + index;
            edges.push(CopilotHistoriesTypeEdge {
                cursor: cursor_index.to_string(),
                node: CopilotHistories::from_record(session, messages),
            });
        }

        let start_cursor = edges.first().map(|edge| edge.cursor.clone());
        let end_cursor = edges.last().map(|edge| edge.cursor.clone());
        let has_next_page = offset + edges.len() < total_count;
        let has_previous_page = offset > 0;

        Self {
            edges,
            page_info: PageInfo {
                start_cursor,
                end_cursor,
                has_next_page,
                has_previous_page,
            },
            total_count: total_count as i32,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ChatMessageType {
    record: Arc<CopilotMessageRecord>,
}

impl ChatMessageType {
    fn from_record(record: Arc<CopilotMessageRecord>) -> Self {
        Self { record }
    }
}

#[Object(name = "ChatMessage")]
impl ChatMessageType {
    async fn attachments(&self) -> Vec<String> {
        self.record.attachments.clone()
    }

    async fn content(&self) -> String {
        self.record.content.clone().unwrap_or_default()
    }

    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> DateTime<Utc> {
        self.record.created_at
    }

    async fn id(&self) -> Option<ID> {
        Some(ID::from(self.record.id.clone()))
    }

    async fn params(&self) -> Option<Json<JsonValue>> {
        None
    }

    async fn role(&self) -> String {
        self.record.role.clone()
    }

    #[graphql(name = "streamObjects")]
    async fn stream_objects(&self) -> Vec<StreamObjectType> {
        Vec::new()
    }
}

#[derive(Clone, Default)]
pub(crate) struct StreamObjectType;

#[Object(name = "StreamObject")]
impl StreamObjectType {
    async fn args(&self) -> Option<Json<JsonValue>> {
        None
    }

    async fn result(&self) -> Option<Json<JsonValue>> {
        None
    }

    #[graphql(name = "textDelta")]
    async fn text_delta(&self) -> Option<String> {
        None
    }

    #[graphql(name = "toolCallId")]
    async fn tool_call_id(&self) -> Option<String> {
        None
    }

    #[graphql(name = "toolName")]
    async fn tool_name(&self) -> Option<String> {
        None
    }

    #[graphql(name = "type")]
    async fn object_type(&self) -> String {
        "text".to_string()
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "TranscriptionItemType")]
pub(crate) struct TranscriptionItemType {
    end: String,
    speaker: String,
    start: String,
    transcription: String,
}

impl Default for TranscriptionItemType {
    fn default() -> Self {
        Self {
            end: String::new(),
            speaker: String::new(),
            start: String::new(),
            transcription: String::new(),
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "TranscriptionResultType")]
pub(crate) struct TranscriptionResultType {
    actions: Option<String>,
    id: ID,
    status: AiJobStatus,
    summary: Option<String>,
    title: Option<String>,
    transcription: Vec<TranscriptionItemType>,
}

impl Default for TranscriptionResultType {
    fn default() -> Self {
        Self {
            actions: None,
            id: ID::from("transcription"),
            status: AiJobStatus::Pending,
            summary: None,
            title: None,
            transcription: Vec::new(),
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "AiJobStatus")]
pub(crate) enum AiJobStatus {
    Claimed,
    Failed,
    Finished,
    Pending,
    Running,
}

#[cfg(test)]
mod tests {
    use async_graphql::Request as GraphQLRequest;
    use uuid::Uuid;

    use crate::{
        graphql::{self, RequestUser},
        testing::{insert_document, seed_workspace, setup_state},
    };

    #[tokio::test]
    async fn graphql_copilot_session_lifecycle() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let schema = graphql::build_schema(state.clone());
        let create_mutation = format!(
            r#"
			mutation {{
				createCopilotSession(options: {{ workspaceId: "{workspace_id}", docId: "{doc_id}", promptName: "default" }})
			}}
			"#,
        );

        let response = schema
            .execute(GraphQLRequest::new(create_mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        let session_id = data["createCopilotSession"]
            .as_str()
            .expect("session id")
            .to_string();

        let update_mutation = format!(
            r#"
			mutation {{
				updateCopilotSession(options: {{ sessionId: "{session_id}", docId: "{doc_id}", pinned: true, promptName: "brainstorm" }})
			}}
			"#,
        );

        let response = schema
            .execute(GraphQLRequest::new(update_mutation).data(RequestUser::new(owner_id.clone())))
            .await;
        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let message_mutation = format!(
            r#"
			mutation {{
				createCopilotMessage(options: {{ sessionId: "{session_id}", content: "Hello world" }})
			}}
			"#,
        );

        let response = schema
            .execute(GraphQLRequest::new(message_mutation).data(RequestUser::new(owner_id.clone())))
            .await;
        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let query = format!(
            r#"
			{{
				currentUser {{
					copilot(workspaceId: "{workspace_id}") {{
						sessions(docId: "{doc_id}", options: {{ pinned: true }}) {{
							id
							docId
							promptName
							pinned
						}}
						chats(pagination: {{ first: 10 }}, docId: "{doc_id}", options: {{ withMessages: true }}) {{
							totalCount
							edges {{
								node {{
									sessionId
									docId
									promptName
									pinned
									messages {{ content }}
								}}
							}}
						}}
					}}
				}}
			}}
			"#,
        );

        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        let copilot = &data["currentUser"]["copilot"];
        let sessions = copilot["sessions"].as_array().expect("sessions array");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0]["id"], session_id);
        assert_eq!(sessions[0]["docId"], doc_id);
        assert_eq!(sessions[0]["promptName"], "brainstorm");
        assert!(sessions[0]["pinned"].as_bool().unwrap());

        let chats = copilot["chats"].clone();
        assert_eq!(chats["totalCount"], 1);
        let edges = chats["edges"].as_array().expect("chat edges");
        assert_eq!(edges.len(), 1);
        let chat_node = &edges[0]["node"];
        assert_eq!(chat_node["sessionId"], session_id);
        let messages = chat_node["messages"].as_array().expect("messages");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["content"], "Hello world");

        let cleanup_mutation = format!(
            r#"
			mutation {{
				cleanupCopilotSession(options: {{ workspaceId: "{workspace_id}", docId: "{doc_id}", sessionIds: ["{session_id}"] }})
			}}
			"#,
        );

        let response = schema
            .execute(GraphQLRequest::new(cleanup_mutation).data(RequestUser::new(owner_id.clone())))
            .await;
        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        let removed = data["cleanupCopilotSession"].as_array().expect("removed");
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].as_str(), Some(session_id.as_str()));

        let requery = format!(
            r#"
			{{
				currentUser {{
					copilot(workspaceId: "{workspace_id}") {{
						sessions(docId: "{doc_id}") {{ id }}
						chats(pagination: {{ first: 10 }}, docId: "{doc_id}") {{ totalCount }}
					}}
				}}
			}}
			"#,
        );

        let response = schema
            .execute(GraphQLRequest::new(requery).data(RequestUser::new(owner_id)))
            .await;
        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        let copilot = &data["currentUser"]["copilot"];
        assert!(copilot["sessions"].as_array().unwrap().is_empty());
        assert_eq!(copilot["chats"]["totalCount"], 0);
    }

    #[tokio::test]
    async fn graphql_copilot_resolver_returns_empty_defaults() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, user_id) = seed_workspace(&state).await;

        let schema = graphql::build_schema(state.clone());
        let query = format!(
            "{{ currentUser {{ copilot(workspaceId: \"{}\") {{ workspaceId quota {{ limit used }} models(promptName: \"default\") {{ defaultModel optionalModels {{ id name }} proModels {{ id name }} }} contexts(sessionId: \"s\") {{ id workspaceId }} chats(pagination: {{ first: 5 }}) {{ totalCount edges {{ cursor node {{ sessionId }} }} pageInfo {{ hasNextPage hasPreviousPage startCursor endCursor }} }} }} }} }}",
            workspace_id
        );

        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(user_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("valid json");
        let copilot = data["currentUser"]["copilot"].clone();
        assert_eq!(copilot["workspaceId"], workspace_id);

        let quota = copilot["quota"].clone();
        assert!(quota["limit"].is_null());
        assert_eq!(quota["used"], 0);

        let models = copilot["models"].clone();
        assert_eq!(models["defaultModel"], "disabled");
        assert_eq!(models["optionalModels"].as_array().unwrap().len(), 0);
        assert_eq!(models["proModels"].as_array().unwrap().len(), 0);

        assert_eq!(copilot["contexts"].as_array().unwrap().len(), 0);

        let chats = copilot["chats"].clone();
        assert_eq!(chats["totalCount"], 0);
        assert_eq!(chats["edges"].as_array().unwrap().len(), 0);
        let page_info = chats["pageInfo"].clone();
        assert!(!page_info["hasNextPage"].as_bool().unwrap());
        assert!(!page_info["hasPreviousPage"].as_bool().unwrap());
    }
}
