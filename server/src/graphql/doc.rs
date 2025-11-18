use std::collections::HashMap;

use async_graphql::{
    Context, Enum, ID, InputObject, Object, Result as GraphQLResult, SimpleObject,
};
use chrono::{DateTime, Utc};

use barffine_core::{
    doc_roles::{DocumentRoleCursor, DocumentRoleRecord},
    doc_store::{DocumentCursor, DocumentHistoryRecord, DocumentMetadata},
    user, workspace as core_workspace,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

use super::{
    RequestUser,
    cursors::{decode_cursor_payload, parse_timestamp_cursor},
    helpers::{
        doc_permissions_for_user_with_metadata, fetch_workspace_record, map_anyhow, map_app_error,
        require_request_user, timestamp_to_datetime,
    },
    types::{PageInfo, PaginationInput},
    workspace::Permission,
};
use crate::{
    AppError, AppState,
    doc::{history::EncodedDocHistory, metadata, mode::DocPublishMode},
    user::helpers::{display_name_for_user, normalize_user_list_params},
};

use super::types::doc::DocType;

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "DocRole")]
pub(crate) enum DocRole {
    Commenter,
    Editor,
    External,
    Manager,
    None,
    Owner,
    Reader,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "PublicDocMode")]
pub(crate) enum PublicDocMode {
    #[graphql(name = "Page")]
    Page,
    #[graphql(name = "Edgeless")]
    Edgeless,
}

impl From<DocPublishMode> for PublicDocMode {
    fn from(mode: DocPublishMode) -> Self {
        match mode {
            DocPublishMode::Page => PublicDocMode::Page,
            DocPublishMode::Edgeless => PublicDocMode::Edgeless,
        }
    }
}

impl From<PublicDocMode> for DocPublishMode {
    fn from(mode: PublicDocMode) -> Self {
        match mode {
            PublicDocMode::Page => DocPublishMode::Page,
            PublicDocMode::Edgeless => DocPublishMode::Edgeless,
        }
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "DocPermissions")]
pub(crate) struct DocPermissions {
    #[graphql(name = "Doc_Comments_Create")]
    doc_comments_create: bool,
    #[graphql(name = "Doc_Comments_Delete")]
    doc_comments_delete: bool,
    #[graphql(name = "Doc_Comments_Read")]
    doc_comments_read: bool,
    #[graphql(name = "Doc_Comments_Resolve")]
    doc_comments_resolve: bool,
    #[graphql(name = "Doc_Comments_Update")]
    doc_comments_update: bool,
    #[graphql(name = "Doc_Copy")]
    doc_copy: bool,
    #[graphql(name = "Doc_Delete")]
    doc_delete: bool,
    #[graphql(name = "Doc_Duplicate")]
    doc_duplicate: bool,
    #[graphql(name = "Doc_Properties_Read")]
    doc_properties_read: bool,
    #[graphql(name = "Doc_Properties_Update")]
    doc_properties_update: bool,
    #[graphql(name = "Doc_Publish")]
    doc_publish: bool,
    #[graphql(name = "Doc_Read")]
    doc_read: bool,
    #[graphql(name = "Doc_Restore")]
    doc_restore: bool,
    #[graphql(name = "Doc_TransferOwner")]
    doc_transfer_owner: bool,
    #[graphql(name = "Doc_Trash")]
    doc_trash: bool,
    #[graphql(name = "Doc_Update")]
    doc_update: bool,
    #[graphql(name = "Doc_Users_Manage")]
    doc_users_manage: bool,
    #[graphql(name = "Doc_Users_Read")]
    doc_users_read: bool,
}

impl Default for DocPermissions {
    fn default() -> Self {
        Self {
            doc_comments_create: false,
            doc_comments_delete: false,
            doc_comments_read: false,
            doc_comments_resolve: false,
            doc_comments_update: false,
            doc_copy: false,
            doc_delete: false,
            doc_duplicate: false,
            doc_properties_read: false,
            doc_properties_update: false,
            doc_publish: false,
            doc_read: false,
            doc_restore: false,
            doc_transfer_owner: false,
            doc_trash: false,
            doc_update: false,
            doc_users_manage: false,
            doc_users_read: false,
        }
    }
}

impl DocPermissions {
    pub(crate) fn can_create_comments(&self) -> bool {
        self.doc_comments_create
    }

    pub(crate) fn can_delete_comments(&self) -> bool {
        self.doc_comments_delete
    }

    pub(crate) fn can_read_comments(&self) -> bool {
        self.doc_comments_read
    }

    pub(crate) fn can_resolve_comments(&self) -> bool {
        self.doc_comments_resolve
    }

    pub(crate) fn can_update_comments(&self) -> bool {
        self.doc_comments_update
    }

    pub(crate) fn can_read_doc(&self) -> bool {
        self.doc_read
    }

    pub(crate) fn can_duplicate_doc(&self) -> bool {
        self.doc_duplicate
    }

    pub(crate) fn can_publish_doc(&self) -> bool {
        self.doc_publish
    }

    pub(crate) fn can_delete_doc(&self) -> bool {
        self.doc_delete
    }

    pub(crate) fn can_trash_doc(&self) -> bool {
        self.doc_trash
    }

    pub(crate) fn can_restore_doc(&self) -> bool {
        self.doc_restore
    }

    pub(crate) fn can_update_doc(&self) -> bool {
        self.doc_update
    }

    pub(crate) fn can_manage_doc_users(&self) -> bool {
        self.doc_users_manage
    }

    pub(crate) fn can_read_doc_users(&self) -> bool {
        self.doc_users_read
    }

    pub(crate) fn can_transfer_doc_owner(&self) -> bool {
        self.doc_transfer_owner
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "WorkspaceDocMeta")]
pub(crate) struct WorkspaceDocMeta {
    #[graphql(name = "createdAt")]
    created_at: DateTime<Utc>,
    #[graphql(name = "createdBy")]
    created_by: Option<EditorType>,
    #[graphql(name = "updatedAt")]
    updated_at: DateTime<Utc>,
    #[graphql(name = "updatedBy")]
    updated_by: Option<EditorType>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "PublicUserType")]
pub(crate) struct PublicUserType {
    #[graphql(name = "avatarUrl")]
    avatar_url: Option<String>,
    id: String,
    name: String,
}

impl PublicUserType {
    pub(crate) fn from_user(record: &user::UserRecord) -> Self {
        Self {
            avatar_url: record.avatar_url.clone(),
            id: record.id.clone(),
            name: display_name_for_user(record),
        }
    }
}

#[derive(InputObject, Default, Clone)]
pub(crate) struct ListUserInput {
    skip: Option<i32>,
    first: Option<i32>,
    query: Option<String>,
    // after: Option<String>,
}

impl ListUserInput {
    pub(crate) fn normalize(
        &self,
    ) -> Result<(i64, i64, Option<String>, Option<user::UserCursor>), AppError> {
        let (limit, skip, keyword) = normalize_user_list_params(
            self.skip.map(|value| value as i64),
            self.first.map(|value| value as i64),
            self.query.as_deref(),
        )?;

        Ok((limit, skip, keyword, None))
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        skip: Option<i32>,
        first: Option<i32>,
        query: Option<String>,
        _after: Option<String>,
    ) -> Self {
        Self { skip, first, query }
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "WorkspaceUserType")]
pub(crate) struct WorkspaceUserGraph {
    #[graphql(name = "avatarUrl")]
    pub(crate) avatar_url: Option<String>,
    pub(crate) email: Option<String>,
    pub(crate) id: String,
    pub(crate) name: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "GrantedDocUserType")]
pub(crate) struct GrantedDocUserType {
    role: DocRole,
    user: WorkspaceUserGraph,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "GrantedDocUserTypeEdge")]
pub(crate) struct GrantedDocUserTypeEdge {
    cursor: String,
    node: GrantedDocUserType,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "PaginatedGrantedDocUserType")]
pub(crate) struct PaginatedGrantedDocUserType {
    edges: Vec<GrantedDocUserTypeEdge>,
    #[graphql(name = "pageInfo")]
    page_info: PageInfo,
    #[graphql(name = "totalCount")]
    total_count: i32,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "DocTypeEdge")]
pub(crate) struct DocTypeEdge {
    pub(crate) cursor: String,
    pub(crate) node: DocType,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "PaginatedDocType")]
pub(crate) struct PaginatedDocType {
    pub(crate) edges: Vec<DocTypeEdge>,
    #[graphql(name = "pageInfo")]
    pub(crate) page_info: PageInfo,
    #[graphql(name = "totalCount")]
    pub(crate) total_count: i32,
}

#[derive(Clone)]
pub(crate) struct DocHistoryType {
    id: String,
    workspace_id: String,
    timestamp: DateTime<Utc>,
}

impl DocHistoryType {
    pub(crate) fn from_record(record: DocumentHistoryRecord, workspace_id: String) -> Self {
        Self {
            id: record.id.to_string(),
            workspace_id,
            timestamp: timestamp_to_datetime(record.created_at),
        }
    }
}

#[Object(name = "DocHistoryType")]
impl DocHistoryType {
    async fn id(&self) -> &str {
        &self.id
    }

    async fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    #[graphql(name = "workspaceId")]
    async fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    async fn editor(&self) -> Option<EditorType> {
        None
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "EditorType")]
#[derive(Debug)]
pub(crate) struct EditorType {
    #[graphql(name = "avatarUrl")]
    avatar_url: Option<String>,
    name: String,
}

impl EditorType {
    fn from_user(record: &user::UserRecord) -> Self {
        Self {
            avatar_url: record.avatar_url.clone(),
            name: display_name_for_user(record),
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct DocumentSnapshotObject {
    pub snapshot: String,
}

#[derive(SimpleObject)]
pub(crate) struct DocumentHistoryObject {
    pub snapshot: String,
    #[graphql(name = "createdAt")]
    pub created_at: DateTime<Utc>,
}

impl From<EncodedDocHistory> for DocumentHistoryObject {
    fn from(history: EncodedDocHistory) -> Self {
        Self {
            snapshot: history.snapshot,
            created_at: timestamp_to_datetime(history.created_at),
        }
    }
}

impl From<DocumentHistoryRecord> for DocumentHistoryObject {
    fn from(record: DocumentHistoryRecord) -> Self {
        EncodedDocHistory::from(record).into()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum DocCursorKind {
    CreatedAt,
    UpdatedAt,
}

pub(crate) fn encode_doc_cursor(metadata: &DocumentMetadata, kind: DocCursorKind) -> String {
    let timestamp = match kind {
        DocCursorKind::CreatedAt => metadata.created_at,
        DocCursorKind::UpdatedAt => metadata.updated_at,
    };
    let payload = format!("{timestamp}:{}", metadata.id.as_str());
    BASE64.encode(payload)
}

pub(crate) fn decode_doc_cursor(cursor: &str) -> Result<DocumentCursor, AppError> {
    let decoded = decode_cursor_payload(cursor)?;

    let (timestamp, id) = parse_timestamp_cursor(&decoded)?;

    Ok(DocumentCursor {
        timestamp,
        id: id.into(),
    })
}

pub(crate) fn encode_granted_user_cursor(record: &DocumentRoleRecord) -> String {
    let payload = format!("{}:{}", record.created_at, record.user_id.as_str());
    BASE64.encode(payload)
}

pub(crate) fn decode_granted_user_cursor(cursor: &str) -> Result<DocumentRoleCursor, AppError> {
    let decoded = decode_cursor_payload(cursor)?;

    let (created_at, user_id) = parse_timestamp_cursor(&decoded)?;

    Ok(DocumentRoleCursor {
        created_at,
        user_id: user_id.into(),
    })
}

/*
pub(crate) fn encode_user_cursor(record: &user::UserRecord) -> String {
    let payload = format!("{}:{}", record.created_at, record.id);
    BASE64.encode(payload)
}

pub(crate) fn decode_user_cursor(cursor: &str) -> Result<user::UserCursor, AppError> {
    let bytes = BASE64
        .decode(cursor)
        .map_err(|_| AppError::bad_request("invalid cursor encoding"))?;
    let decoded =
        String::from_utf8(bytes).map_err(|_| AppError::bad_request("invalid cursor payload"))?;

    let mut parts = decoded.splitn(2, ':');
    let timestamp = parts
        .next()
        .ok_or_else(|| AppError::bad_request("invalid cursor payload"))?;
    let id = parts
        .next()
        .ok_or_else(|| AppError::bad_request("invalid cursor payload"))?;

    if timestamp.is_empty() || id.is_empty() {
        return Err(AppError::bad_request("invalid cursor payload"));
    }

    let timestamp = timestamp
        .parse::<i64>()
        .map_err(|_| AppError::bad_request("invalid cursor payload"))?;

    Ok(user::UserCursor {
        created_at: timestamp,
        id: id.to_string(),
    })
}
*/

pub(crate) fn doc_role_from_str(role: &str) -> DocRole {
    match role.to_ascii_lowercase().as_str() {
        "owner" => DocRole::Owner,
        "manager" => DocRole::Manager,
        "admin" => DocRole::Manager,
        "commenter" => DocRole::Commenter,
        "viewer" | "reader" => DocRole::Reader,
        "editor" => DocRole::Editor,
        "external" => DocRole::External,
        "none" => DocRole::None,
        _ => DocRole::Editor,
    }
}

pub(crate) fn doc_role_to_str(role: DocRole) -> &'static str {
    match role {
        DocRole::Owner => "owner",
        DocRole::Manager => "manager",
        DocRole::Editor => "editor",
        DocRole::Commenter => "commenter",
        DocRole::Reader => "reader",
        DocRole::External => "external",
        DocRole::None => "none",
    }
}

fn doc_role_rank(role: DocRole) -> i32 {
    match role {
        DocRole::None => 0,
        DocRole::External => 1,
        DocRole::Reader => 2,
        DocRole::Commenter => 3,
        DocRole::Editor => 4,
        DocRole::Manager => 5,
        DocRole::Owner => 6,
    }
}

fn doc_role_min(a: DocRole, b: DocRole) -> DocRole {
    if doc_role_rank(a) <= doc_role_rank(b) {
        a
    } else {
        b
    }
}

fn doc_role_max(a: DocRole, b: DocRole) -> DocRole {
    if doc_role_rank(a) >= doc_role_rank(b) {
        a
    } else {
        b
    }
}

pub(crate) fn fixup_doc_role(
    workspace_role: Option<Permission>,
    doc_role: Option<DocRole>,
) -> Option<DocRole> {
    if workspace_role.is_none() && matches!(doc_role, None | Some(DocRole::None)) {
        return None;
    }

    let workspace_role = workspace_role.unwrap_or(Permission::External);
    let doc_role = doc_role.unwrap_or(DocRole::External);

    match workspace_role {
        Permission::External => Some(doc_role_min(DocRole::Editor, doc_role)),
        Permission::Owner => Some(DocRole::Owner),
        Permission::Admin => Some(doc_role_max(DocRole::Manager, doc_role)),
        Permission::Collaborator => Some(doc_role),
    }
}

pub(crate) fn doc_permissions_for_role(role: DocRole) -> DocPermissions {
    let mut perms = DocPermissions::default();

    match role {
        DocRole::Owner => {
            set_all_doc_permissions(&mut perms, true);
        }
        DocRole::Manager => {
            set_all_doc_permissions(&mut perms, true);
            perms.doc_transfer_owner = false;
        }
        DocRole::Editor => {
            perms.doc_comments_create = true;
            perms.doc_comments_delete = true;
            perms.doc_comments_read = true;
            perms.doc_comments_resolve = true;
            perms.doc_comments_update = true;
            perms.doc_copy = true;
            perms.doc_delete = true;
            perms.doc_duplicate = true;
            perms.doc_properties_read = true;
            perms.doc_properties_update = true;
            perms.doc_read = true;
            perms.doc_update = true;
            perms.doc_publish = false;
            perms.doc_restore = true;
            perms.doc_trash = true;
            perms.doc_users_manage = false;
            perms.doc_users_read = true;
        }
        DocRole::Commenter => {
            perms.doc_comments_create = true;
            perms.doc_comments_read = true;
            perms.doc_copy = true;
            perms.doc_duplicate = true;
            perms.doc_read = true;
            perms.doc_users_read = true;
        }
        DocRole::Reader => {
            perms.doc_comments_read = true;
            perms.doc_copy = true;
            perms.doc_duplicate = true;
            perms.doc_properties_read = true;
            perms.doc_read = true;
            perms.doc_users_read = true;
        }
        DocRole::External => {
            perms.doc_comments_read = true;
            perms.doc_copy = true;
            perms.doc_properties_read = true;
            perms.doc_read = true;
        }
        DocRole::None => {}
    }

    perms
}

fn set_all_doc_permissions(perms: &mut DocPermissions, value: bool) {
    perms.doc_comments_create = value;
    perms.doc_comments_delete = value;
    perms.doc_comments_read = value;
    perms.doc_comments_resolve = value;
    perms.doc_comments_update = value;
    perms.doc_copy = value;
    perms.doc_delete = value;
    perms.doc_duplicate = value;
    perms.doc_properties_read = value;
    perms.doc_properties_update = value;
    perms.doc_publish = value;
    perms.doc_read = value;
    perms.doc_restore = value;
    perms.doc_transfer_owner = value;
    perms.doc_trash = value;
    perms.doc_update = value;
    perms.doc_users_manage = value;
    perms.doc_users_read = value;
}

pub(crate) async fn ensure_doc_owner_role(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
) -> GraphQLResult<()> {
    let owners = state
        .doc_role_store
        .owners_for_doc(workspace_id, doc_id)
        .await
        .map_err(map_anyhow)?;

    if !owners.is_empty() {
        return Ok(());
    }

    let workspace = match state.workspace_service.fetch_workspace(workspace_id).await {
        Ok(record) => record,
        Err(_) => return Ok(()),
    };

    let creator_id = state
        .document_store
        .find_metadata(workspace_id, doc_id)
        .await
        .map_err(map_anyhow)?
        .and_then(|metadata| metadata.creator_id);

    let owner_id = if let Some(candidate) = creator_id {
        let user_exists = state
            .user_store
            .find_by_id(&candidate)
            .await
            .map_err(map_anyhow)?
            .is_some();

        if user_exists {
            candidate
        } else {
            workspace.owner_id.to_string()
        }
    } else {
        workspace.owner_id.to_string()
    };

    state
        .doc_role_store
        .upsert(workspace_id, doc_id, &owner_id, "owner")
        .await
        .map_err(map_anyhow)?;

    Ok(())
}

impl DocType {
    pub(crate) fn from_metadata(metadata: DocumentMetadata) -> Self {
        Self {
            metadata,
            cached_permissions: None,
        }
    }

    pub(crate) fn from_metadata_with_permissions(
        metadata: DocumentMetadata,
        permissions: DocPermissions,
    ) -> Self {
        Self {
            metadata,
            cached_permissions: Some(permissions),
        }
    }

    async fn workspace_record(
        &self,
        ctx: &Context<'_>,
    ) -> GraphQLResult<core_workspace::WorkspaceRecord> {
        let state = ctx.data::<AppState>()?;
        fetch_workspace_record(state, &self.metadata.workspace_id).await
    }

    async fn workspace_owner_record(
        &self,
        ctx: &Context<'_>,
    ) -> GraphQLResult<Option<user::UserRecord>> {
        let workspace = self.workspace_record(ctx).await?;
        let state = ctx.data::<AppState>()?;
        state
            .user_service
            .maybe_find_user(&workspace.owner_id)
            .await
            .map_err(map_app_error)
    }

    async fn metadata_user_record(
        &self,
        ctx: &Context<'_>,
        user_id: Option<&String>,
    ) -> GraphQLResult<Option<user::UserRecord>> {
        if let Some(id) = user_id {
            let state = ctx.data::<AppState>()?;
            state
                .user_service
                .maybe_find_user(id)
                .await
                .map_err(map_app_error)
        } else {
            Ok(None)
        }
    }
}

#[Object(name = "DocType")]
impl DocType {
    async fn id(&self) -> &str {
        &self.metadata.id
    }

    #[graphql(name = "workspaceId")]
    async fn workspace_id(&self) -> &str {
        &self.metadata.workspace_id
    }

    #[graphql(name = "defaultRole")]
    async fn default_role(&self) -> DocRole {
        doc_role_from_str(&self.metadata.default_role)
    }

    async fn permissions(&self, ctx: &Context<'_>) -> GraphQLResult<DocPermissions> {
        if let Some(perms) = self.cached_permissions.as_ref() {
            return Ok(perms.clone());
        }

        if let Some(request_user) = ctx.data_opt::<RequestUser>() {
            let workspace = self.workspace_record(ctx).await?;
            let state = ctx.data::<AppState>()?;
            if let Some(perms) = doc_permissions_for_user_with_metadata(
                state,
                &workspace,
                &self.metadata,
                &request_user.user_id,
            )
            .await?
            {
                return Ok(perms);
            }
        }

        if self.metadata.public {
            Ok(doc_permissions_for_role(DocRole::Reader))
        } else {
            Ok(DocPermissions::default())
        }
    }

    async fn meta(&self, ctx: &Context<'_>) -> GraphQLResult<WorkspaceDocMeta> {
        let created_at = timestamp_to_datetime(self.metadata.created_at);
        let updated_at = timestamp_to_datetime(self.metadata.updated_at);
        let owner = self.workspace_owner_record(ctx).await?;
        let created_user = self
            .metadata_user_record(ctx, self.metadata.creator_id.as_ref())
            .await?;
        let updated_user = self
            .metadata_user_record(ctx, self.metadata.updater_id.as_ref())
            .await?;

        let created_editor = created_user
            .as_ref()
            .map(EditorType::from_user)
            .or_else(|| owner.as_ref().map(EditorType::from_user));
        let updated_editor = updated_user
            .as_ref()
            .map(EditorType::from_user)
            .or_else(|| owner.as_ref().map(EditorType::from_user));

        Ok(WorkspaceDocMeta {
            created_at,
            created_by: created_editor,
            updated_at,
            updated_by: updated_editor,
        })
    }

    async fn mode(&self) -> PublicDocMode {
        DocPublishMode::from_str(&self.metadata.mode)
            .unwrap_or_default()
            .into()
    }

    async fn public(&self) -> bool {
        self.metadata.public
    }

    async fn blocked(&self) -> bool {
        self.metadata.blocked
    }

    #[graphql(name = "publicShareUrl")]
    async fn public_share_url(&self) -> Option<String> {
        metadata::public_share_url(&self.metadata)
    }

    #[graphql(name = "publicShareToken")]
    async fn public_share_token(&self) -> Option<String> {
        self.metadata.share_token.clone()
    }

    async fn summary(&self) -> Option<String> {
        self.metadata.summary.clone()
    }

    async fn title(&self) -> Option<String> {
        self.metadata.title.clone()
    }

    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> DateTime<Utc> {
        timestamp_to_datetime(self.metadata.created_at)
    }

    #[graphql(name = "updatedAt")]
    async fn updated_at(&self) -> DateTime<Utc> {
        timestamp_to_datetime(self.metadata.updated_at)
    }

    #[graphql(name = "creatorId")]
    async fn creator_id(&self, ctx: &Context<'_>) -> GraphQLResult<Option<ID>> {
        if let Some(id) = &self.metadata.creator_id {
            return Ok(Some(ID(id.clone())));
        }
        let owner = self.workspace_owner_record(ctx).await?;
        Ok(owner.map(|user| ID(user.id)))
    }

    #[graphql(name = "lastUpdaterId")]
    async fn last_updater_id(&self, ctx: &Context<'_>) -> GraphQLResult<Option<ID>> {
        if let Some(id) = &self.metadata.updater_id {
            return Ok(Some(ID(id.clone())));
        }
        let owner = self.workspace_owner_record(ctx).await?;
        Ok(owner.map(|user| ID(user.id)))
    }

    #[graphql(name = "createdBy")]
    async fn created_by(&self, ctx: &Context<'_>) -> GraphQLResult<Option<PublicUserType>> {
        if let Some(user) = self
            .metadata_user_record(ctx, self.metadata.creator_id.as_ref())
            .await?
        {
            return Ok(Some(PublicUserType::from_user(&user)));
        }
        let owner = self.workspace_owner_record(ctx).await?;
        Ok(owner.as_ref().map(PublicUserType::from_user))
    }

    #[graphql(name = "lastUpdatedBy")]
    async fn last_updated_by(&self, ctx: &Context<'_>) -> GraphQLResult<Option<PublicUserType>> {
        if let Some(user) = self
            .metadata_user_record(ctx, self.metadata.updater_id.as_ref())
            .await?
        {
            return Ok(Some(PublicUserType::from_user(&user)));
        }
        let owner = self.workspace_owner_record(ctx).await?;
        Ok(owner.as_ref().map(PublicUserType::from_user))
    }

    #[graphql(name = "grantedUsersList")]
    async fn granted_users_list(
        &self,
        ctx: &Context<'_>,
        pagination: PaginationInput,
    ) -> GraphQLResult<PaginatedGrantedDocUserType> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;
        let workspace = self.workspace_record(ctx).await?;

        let permissions = doc_permissions_for_user_with_metadata(
            state,
            &workspace,
            &self.metadata,
            &request_user.user_id,
        )
        .await?
        .unwrap_or_default();

        if !permissions.can_read_doc_users() {
            return Err(map_app_error(AppError::forbidden(
                "Doc.Users.Read permission required",
            )));
        }

        ensure_doc_owner_role(state, &self.metadata.workspace_id, &self.metadata.id).await?;

        let (limit, offset, after) = pagination.normalize().map_err(map_app_error)?;
        let cursor = after
            .as_deref()
            .map(decode_granted_user_cursor)
            .transpose()
            .map_err(map_app_error)?;

        let roles = state
            .doc_role_store
            .paginate_for_doc(
                &self.metadata.workspace_id,
                &self.metadata.id,
                limit + 1,
                offset,
                cursor.as_ref(),
            )
            .await
            .map_err(map_anyhow)?;

        let total_count = state
            .doc_role_store
            .count_for_doc(&self.metadata.workspace_id, &self.metadata.id)
            .await
            .map_err(map_anyhow)?;

        let has_next_page = (roles.len() as i64) > limit;
        let visible = if has_next_page {
            &roles[..roles.len() - 1]
        } else {
            &roles[..]
        };

        let user_ids: Vec<String> = visible
            .iter()
            .map(|record| record.user_id.to_string())
            .collect();
        let users = state
            .user_store
            .find_by_ids(&user_ids)
            .await
            .map_err(map_anyhow)?;
        let user_map: HashMap<_, _> = users
            .into_iter()
            .map(|user| (user.id.clone(), user))
            .collect();

        let edges = visible
            .into_iter()
            .filter_map(|record| {
                user_map.get(record.user_id.as_str()).map(|user| {
                    let cursor = encode_granted_user_cursor(record);
                    let node = GrantedDocUserType {
                        role: doc_role_from_str(&record.role),
                        user: WorkspaceUserGraph {
                            avatar_url: user.avatar_url.clone(),
                            email: Some(user.email.clone()),
                            id: user.id.clone(),
                            name: display_name_for_user(user),
                        },
                    };

                    GrantedDocUserTypeEdge { cursor, node }
                })
            })
            .collect::<Vec<_>>();

        let page_info = PageInfo {
            end_cursor: edges.last().map(|edge| edge.cursor.clone()),
            has_next_page,
            has_previous_page: cursor.is_some() || offset > 0,
            start_cursor: edges.first().map(|edge| edge.cursor.clone()),
        };

        Ok(PaginatedGrantedDocUserType {
            edges,
            page_info,
            total_count: total_count as i32,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        auth::generate_password_hash,
        graphql::{self, RequestUser},
        testing::{insert_document, seed_workspace, setup_state, setup_state_with_rocks_doc_store},
    };
    use async_graphql::Request as GraphQLRequest;
    use serde_json::to_value;
    use uuid::Uuid;

    #[test]
    fn doc_permissions_match_affine_contract() {
        let none = doc_permissions_for_role(DocRole::None);
        assert!(!none.doc_read);
        assert!(!none.doc_users_read);
        assert!(!none.doc_comments_read);
        assert!(!none.doc_comments_update);

        let external = doc_permissions_for_role(DocRole::External);
        assert!(external.doc_read);
        assert!(external.doc_comments_read);
        assert!(external.doc_copy);
        assert!(external.doc_properties_read);
        assert!(!external.doc_users_read);
        assert!(!external.doc_comments_update);

        let reader = doc_permissions_for_role(DocRole::Reader);
        assert!(reader.doc_read);
        assert!(reader.doc_comments_read);
        assert!(reader.doc_copy);
        assert!(reader.doc_duplicate);
        assert!(reader.doc_properties_read);
        assert!(reader.doc_users_read);
        assert!(!reader.doc_comments_create);
        assert!(!reader.doc_comments_resolve);
        assert!(!reader.doc_comments_update);

        let commenter = doc_permissions_for_role(DocRole::Commenter);
        assert!(commenter.doc_read);
        assert!(commenter.doc_comments_create);
        assert!(commenter.doc_comments_read);
        assert!(!commenter.doc_comments_resolve);
        assert!(!commenter.doc_comments_update);
        assert!(commenter.doc_copy);
        assert!(commenter.doc_duplicate);
        assert!(commenter.doc_users_read);

        let editor = doc_permissions_for_role(DocRole::Editor);
        assert!(editor.doc_read);
        assert!(editor.doc_update);
        assert!(editor.doc_delete);
        assert!(editor.doc_restore);
        assert!(editor.doc_trash);
        assert!(editor.doc_comments_delete);
        assert!(editor.doc_comments_resolve);
        assert!(editor.doc_comments_update);
        assert!(editor.doc_users_read);
        assert!(!editor.doc_publish);
        assert!(!editor.doc_transfer_owner);
        assert!(!editor.doc_users_manage);

        let manager = doc_permissions_for_role(DocRole::Manager);
        assert!(manager.doc_publish);
        assert!(manager.doc_users_manage);
        assert!(manager.doc_users_read);
        assert!(!manager.doc_transfer_owner);

        let owner = doc_permissions_for_role(DocRole::Owner);
        assert!(owner.doc_transfer_owner);
        assert!(owner.doc_users_manage);
        assert!(owner.doc_users_read);
    }

    #[tokio::test]
    async fn graphql_publish_doc_marks_doc_public() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            r#"
			mutation {{
				publishDoc(workspaceId: "{workspace_id}", docId: "{doc_id}") {{
					id
					public
					mode
				}}
			}}
			"#
        );

        let response = schema
            .execute(GraphQLRequest::new(mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("valid json");
        let payload = data["publishDoc"].clone();
        assert_eq!(payload["id"], doc_id);
        let mode = payload["mode"].as_str().expect("mode string");
        assert_eq!(mode.to_ascii_uppercase(), "PAGE");
        assert!(payload["public"].as_bool().unwrap());

        let metadata = state
            .document_store
            .find_metadata(&workspace_id, &doc_id)
            .await
            .expect("query metadata")
            .expect("metadata present");
        assert!(metadata.public, "document should be marked public");
        assert_eq!(metadata.mode, "page");
        assert!(
            metadata.share_token.is_some(),
            "share token should be provisioned"
        );
    }

    #[tokio::test]
    async fn graphql_revoke_public_doc_unsets_public_flag() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        state
            .document_store
            .publish_doc(&workspace_id, &doc_id, "page")
            .await
            .expect("publish doc");

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            r#"
			mutation {{
				revokePublicDoc(workspaceId: "{workspace_id}", docId: "{doc_id}") {{
					id
					public
				}}
			}}
			"#
        );

        let response = schema
            .execute(GraphQLRequest::new(mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("valid json");
        let payload = data["revokePublicDoc"].clone();
        assert_eq!(payload["id"], doc_id);
        assert!(!payload["public"].as_bool().unwrap());

        let metadata = state
            .document_store
            .find_metadata(&workspace_id, &doc_id)
            .await
            .expect("query metadata")
            .expect("metadata present");
        assert!(!metadata.public, "document should no longer be public");
    }

    #[tokio::test]
    async fn graphql_doc_role_mutations_manage_roles() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let password_hash = generate_password_hash("secret").expect("hash password");
        let collaborator = state
            .user_store
            .create("doc-user@example.com", &password_hash, Some("Doc User"))
            .await
            .expect("create collaborator");

        let schema = graphql::build_schema(state.clone());
        let grant_mutation = format!(
            r#"
			mutation {{
				grantDocUserRoles(input: {{
					workspaceId: "{workspace_id}",
					docId: "{doc_id}",
					userIds: ["{user_id}"],
					role: EDITOR
				}})
			}}
			"#,
            workspace_id = workspace_id,
            doc_id = doc_id,
            user_id = collaborator.id
        );

        let grant_response = schema
            .execute(GraphQLRequest::new(grant_mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            grant_response.errors.is_empty(),
            "unexpected errors: {:?}",
            grant_response.errors
        );

        let record = state
            .doc_role_store
            .find_for_user(&workspace_id, &doc_id, &collaborator.id)
            .await
            .expect("query doc role")
            .expect("doc role exists");
        assert_eq!(record.role, "editor");

        let update_mutation = format!(
            r#"
			mutation {{
				updateDocUserRole(input: {{
					workspaceId: "{workspace_id}",
					docId: "{doc_id}",
					userId: "{user_id}",
					role: COMMENTER
				}})
			}}
			"#,
            workspace_id = workspace_id,
            doc_id = doc_id,
            user_id = collaborator.id
        );

        let update_response = schema
            .execute(GraphQLRequest::new(update_mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            update_response.errors.is_empty(),
            "unexpected errors: {:?}",
            update_response.errors
        );

        let record = state
            .doc_role_store
            .find_for_user(&workspace_id, &doc_id, &collaborator.id)
            .await
            .expect("query doc role")
            .expect("doc role exists");
        assert_eq!(record.role, "commenter");

        let revoke_mutation = format!(
            r#"
			mutation {{
				revokeDocUserRoles(input: {{
					workspaceId: "{workspace_id}",
					docId: "{doc_id}",
					userId: "{user_id}"
				}})
			}}
			"#,
            workspace_id = workspace_id,
            doc_id = doc_id,
            user_id = collaborator.id
        );

        let revoke_response = schema
            .execute(GraphQLRequest::new(revoke_mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            revoke_response.errors.is_empty(),
            "unexpected errors: {:?}",
            revoke_response.errors
        );

        let record = state
            .doc_role_store
            .find_for_user(&workspace_id, &doc_id, &collaborator.id)
            .await
            .expect("query doc role");
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn graphql_duplicate_doc_in_rocks_mode_matches_sql_semantics() {
        let (_temp_dir, database, state) = setup_state_with_rocks_doc_store().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;

        // 在 Rocks doc store 模式下插入一个源文档
        let source_doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &source_doc_id, false, "page").await;

        let schema = graphql::build_schema(state.clone());
        let new_title = "Rocks duplicated";
        let mutation = r#"
		mutation DuplicateDoc($input: DuplicateDocInput!) {
			duplicateDoc(input: $input) {
				id
				workspaceId
				title
			}
		}
		"#;

        let vars = async_graphql::Variables::from_json(serde_json::json!({
            "input": {
                "workspaceId": workspace_id,
                "docId": source_doc_id,
                "title": new_title,
            }
        }));

        let response = schema
            .execute(
                async_graphql::Request::new(mutation)
                    .variables(vars)
                    .data(RequestUser::new(owner_id.clone())),
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("valid json");
        let duplicated = &data["duplicateDoc"];

        let duplicated_id = duplicated["id"].as_str().expect("duplicated id");
        assert_ne!(duplicated_id, source_doc_id);
        assert_eq!(
            duplicated["workspaceId"].as_str(),
            Some(workspace_id.as_str())
        );
        assert_eq!(duplicated["title"].as_str(), Some(new_title));

        // 通过 DocumentStore 读取，确认 Rocks 模式下元数据完整
        let doc_store = barffine_core::doc_store::DocumentStore::new(&database);
        let metadata = doc_store
            .find_metadata(&workspace_id, duplicated_id)
            .await
            .expect("fetch metadata")
            .expect("duplicated doc metadata");
        assert_eq!(metadata.id, duplicated_id.into());
        assert_eq!(metadata.workspace_id, workspace_id.clone().into());
        assert_eq!(metadata.title.as_deref(), Some(new_title));
    }

    #[tokio::test]
    async fn graphql_update_doc_user_role_transfers_owner() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        state
            .doc_role_store
            .upsert(&workspace_id, &doc_id, &owner_id, "owner")
            .await
            .expect("seed owner role");

        let password_hash = generate_password_hash("secret").expect("hash password");
        let collaborator = state
            .user_store
            .create("doc-owner@example.com", &password_hash, Some("Doc Owner"))
            .await
            .expect("create collaborator");

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            r#"
			mutation {{
				updateDocUserRole(input: {{
					workspaceId: "{workspace_id}",
					docId: "{doc_id}",
					userId: "{user_id}",
					role: OWNER
				}})
			}}
			"#,
            workspace_id = workspace_id,
            doc_id = doc_id,
            user_id = collaborator.id
        );

        let response = schema
            .execute(GraphQLRequest::new(mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let new_owner_role = state
            .doc_role_store
            .find_for_user(&workspace_id, &doc_id, &collaborator.id)
            .await
            .expect("query doc role")
            .expect("new owner role exists");
        assert_eq!(new_owner_role.role, "owner");

        let previous_owner_role = state
            .doc_role_store
            .find_for_user(&workspace_id, &doc_id, &owner_id)
            .await
            .expect("query previous owner role")
            .expect("previous owner role exists");
        assert_eq!(previous_owner_role.role, "manager");
    }

    #[tokio::test]
    async fn graphql_update_doc_user_role_requires_transfer_permission() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let password_hash = generate_password_hash("secret").expect("hash password");
        let collaborator = state
            .user_store
            .create(
                "transfer-guard@example.com",
                &password_hash,
                Some("Transfer Guard"),
            )
            .await
            .expect("create collaborator");

        let schema = graphql::build_schema(state.clone());
        let grant_mutation = format!(
            r#"
			mutation {{
				grantDocUserRoles(input: {{
					workspaceId: "{workspace_id}",
					docId: "{doc_id}",
					userIds: ["{user_id}"],
					role: MANAGER
				}})
			}}
			"#,
            workspace_id = workspace_id,
            doc_id = doc_id,
            user_id = collaborator.id
        );

        let grant_response = schema
            .execute(GraphQLRequest::new(grant_mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            grant_response.errors.is_empty(),
            "unexpected errors: {:?}",
            grant_response.errors
        );

        let mutation = format!(
            r#"
			mutation {{
				updateDocUserRole(input: {{
					workspaceId: "{workspace_id}",
					docId: "{doc_id}",
					userId: "{user_id}",
					role: OWNER
				}})
			}}
			"#,
            workspace_id = workspace_id,
            doc_id = doc_id,
            user_id = collaborator.id
        );

        let response = schema
            .execute(GraphQLRequest::new(mutation).data(RequestUser::new(collaborator.id.clone())))
            .await;

        assert!(!response.errors.is_empty(), "expected permission error");
        assert_eq!(
            response.errors[0].message,
            "Doc.TransferOwner permission required"
        );
    }

    #[tokio::test]
    async fn graphql_update_doc_default_role_updates_metadata() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            r#"
			mutation {{
				updateDocDefaultRole(input: {{
					workspaceId: "{workspace_id}",
					docId: "{doc_id}",
					role: READER
				}})
			}}
			"#,
            workspace_id = workspace_id,
            doc_id = doc_id
        );

        let response = schema
            .execute(GraphQLRequest::new(mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let metadata = state
            .document_store
            .find_metadata(&workspace_id, &doc_id)
            .await
            .expect("query metadata")
            .expect("metadata exists");
        assert_eq!(metadata.default_role, "reader");
    }

    #[tokio::test]
    async fn graphql_doc_permission_notifications_respect_preferences() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let password_hash = generate_password_hash("secret").expect("hash password");
        let collaborator = state
            .user_store
            .create(
                "collaborator@example.com",
                &password_hash,
                Some("Collaborator"),
            )
            .await
            .expect("create collaborator");

        state
            .workspace_store
            .set_member_role_with_status(
                &workspace_id,
                &collaborator.id,
                "member",
                "Accepted",
                Some(&owner_id),
                Some("test"),
                None,
            )
            .await
            .expect("add workspace member");

        state
            .doc_role_store
            .upsert(&workspace_id, &doc_id, &collaborator.id, "manager")
            .await
            .expect("grant doc role");

        let schema = graphql::build_schema(state.clone());

        let grant_mutation = format!(
            "mutation {{
                grantDocUserRoles(input: {{
                    workspaceId: \"{workspace_id}\",
                    docId: \"{doc_id}\",
                    userIds: [\"{collaborator_id}\"],
                    role: EDITOR
                }})
            }}",
            workspace_id = workspace_id,
            doc_id = doc_id,
            collaborator_id = collaborator.id
        );

        let grant_response = schema
            .execute(GraphQLRequest::new(grant_mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            grant_response.errors.is_empty(),
            "grant errors: {:?}",
            grant_response.errors
        );

        let notification_query = r#"
            {
                currentUser {
                    notificationCount
                    notifications(pagination: { first: 10 }) {
                        edges {
                            node {
                                type
                                body
                            }
                        }
                    }
                }
            }
        "#;

        let collaborator_before = schema
            .execute(
                GraphQLRequest::new(notification_query)
                    .data(RequestUser::new(collaborator.id.clone())),
            )
            .await;

        assert!(
            collaborator_before.errors.is_empty(),
            "notification errors: {:?}",
            collaborator_before.errors
        );
        let before_json = collaborator_before.data.into_json().expect("valid json");
        assert_eq!(before_json["currentUser"]["notificationCount"], 1);
        let node = &before_json["currentUser"]["notifications"]["edges"][0]["node"];
        assert_eq!(node["type"], "Comment");
        assert_eq!(node["body"]["event"], "doc.activity.role.granted");
        assert_eq!(node["body"]["changedByUserId"], owner_id);

        let settings_mutation = r#"
            mutation {
                updateUserSettings(input: {
                    receivePermissionChangeNotification: false
                    receiveDocActivityNotification: false
                }) {
                    receivePermissionChangeNotification
                    receiveDocActivityNotification
                }
            }
        "#;

        let settings_response = schema
            .execute(
                GraphQLRequest::new(settings_mutation)
                    .data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            settings_response.errors.is_empty(),
            "settings errors: {:?}",
            settings_response.errors
        );

        let revoke_mutation = format!(
            "mutation {{
                revokeDocUserRoles(input: {{
                    workspaceId: \"{workspace_id}\",
                    docId: \"{doc_id}\",
                    userId: \"{collaborator_id}\"
                }})
            }}",
            workspace_id = workspace_id,
            doc_id = doc_id,
            collaborator_id = collaborator.id
        );

        let revoke_response = schema
            .execute(GraphQLRequest::new(revoke_mutation).data(RequestUser::new(owner_id.clone())))
            .await;
        assert!(
            revoke_response.errors.is_empty(),
            "revoke errors: {:?}",
            revoke_response.errors
        );

        let collaborator_after = schema
            .execute(
                GraphQLRequest::new(notification_query)
                    .data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            collaborator_after.errors.is_empty(),
            "post-revoke notification errors: {:?}",
            collaborator_after.errors
        );
        let after_json = collaborator_after.data.into_json().expect("valid json");
        assert_eq!(
            after_json["currentUser"]["notificationCount"], 1,
            "no additional notifications should be created once preference is disabled"
        );
    }

    #[tokio::test]
    async fn graphql_doc_role_permissions_enforced_for_managers() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let password_hash = generate_password_hash("secret").expect("hash password");
        let manager = state
            .user_store
            .create("manager@example.com", &password_hash, Some("Manager"))
            .await
            .expect("create manager");
        let target = state
            .user_store
            .create("target@example.com", &password_hash, Some("Target"))
            .await
            .expect("create target");
        let observer = state
            .user_store
            .create("observer@example.com", &password_hash, Some("Observer"))
            .await
            .expect("create observer");

        for user in [&manager, &target, &observer] {
            state
                .workspace_store
                .set_member_role(&workspace_id, &user.id, "member")
                .await
                .expect("add workspace membership");
        }

        state
            .doc_role_store
            .upsert(&workspace_id, &doc_id, &manager.id, "manager")
            .await
            .expect("promote manager to doc manager");
        state
            .doc_role_store
            .upsert(&workspace_id, &doc_id, &observer.id, "reader")
            .await
            .expect("explicit doc role for observer");

        let observer_permissions =
            graphql::doc_permissions_for_user(&state, &workspace_id, &doc_id, &observer.id)
                .await
                .expect("observer permissions lookup")
                .expect("observer should have doc permissions");
        assert!(
            !observer_permissions.can_manage_doc_users(),
            "observer should not have Doc.Users.Manage capability"
        );

        let schema = graphql::build_schema(state.clone());
        let grant_mutation = format!(
            r#"
        mutation {{
            grantDocUserRoles(input: {{
                workspaceId: "{workspace_id}",
                docId: "{doc_id}",
                userIds: ["{target_id}"],
                role: COMMENTER
            }})
        }}
        "#,
            workspace_id = workspace_id,
            doc_id = doc_id,
            target_id = target.id
        );

        let grant_by_manager = schema
            .execute(
                GraphQLRequest::new(grant_mutation.clone())
                    .data(RequestUser::new(manager.id.clone())),
            )
            .await;
        assert!(
            grant_by_manager.errors.is_empty(),
            "manager should be able to manage doc users: {:?}",
            grant_by_manager.errors
        );

        let assigned_role = state
            .doc_role_store
            .find_for_user(&workspace_id, &doc_id, &target.id)
            .await
            .expect("query target doc role")
            .expect("target role exists");
        assert_eq!(assigned_role.role, "commenter");

        let grant_by_observer = schema
            .execute(
                GraphQLRequest::new(grant_mutation.clone())
                    .data(RequestUser::new(observer.id.clone())),
            )
            .await;
        assert!(
            !grant_by_observer.errors.is_empty(),
            "non-managers should not be allowed"
        );
        assert_eq!(
            grant_by_observer.errors[0].message,
            "Doc.Users.Manage permission required"
        );

        let owner_attempt = schema
            .execute(
                GraphQLRequest::new(grant_mutation.clone())
                    .data(RequestUser::new(owner_id.clone())),
            )
            .await;
        assert!(
            owner_attempt.errors.is_empty(),
            "workspace owner should retain ability: {:?}",
            owner_attempt.errors
        );
    }

    #[tokio::test]
    async fn graphql_workspace_doc_lists_respect_acl() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, _owner_id) = seed_workspace(&state).await;
        let readable_doc = Uuid::new_v4().to_string();
        let hidden_doc = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &readable_doc, false, "page").await;
        insert_document(&database, &workspace_id, &hidden_doc, false, "page").await;

        set_default_role(&database, &workspace_id, &readable_doc, "none").await;
        set_default_role(&database, &workspace_id, &hidden_doc, "none").await;

        let password_hash = generate_password_hash("secret").expect("hash password");
        let collaborator = state
            .user_store
            .create(
                "doc-collab@example.com",
                &password_hash,
                Some("Doc Collaborator"),
            )
            .await
            .expect("create collaborator");

        state
            .workspace_store
            .set_member_role(&workspace_id, &collaborator.id, "member")
            .await
            .expect("add collaborator membership");
        state
            .doc_role_store
            .upsert(&workspace_id, &readable_doc, &collaborator.id, "reader")
            .await
            .expect("grant reader role");

        let schema = graphql::build_schema(state.clone());
        let query_docs = format!(
            r#"
        {{
            workspace(id: "{workspace_id}") {{
                docs(pagination: {{ first: 10 }}) {{ edges {{ node {{ id }} }} }}
                recentlyUpdatedDocs(pagination: {{ first: 10 }}) {{ edges {{ node {{ id }} }} }}
            }}
        }}
        "#,
            workspace_id = workspace_id
        );

        let response = schema
            .execute(
                GraphQLRequest::new(query_docs).data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            response.errors.is_empty(),
            "list query errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");

        let docs_edges = data["workspace"]["docs"]["edges"]
            .as_array()
            .expect("docs edges array");
        let doc_ids: Vec<String> = docs_edges
            .iter()
            .filter_map(|edge| edge["node"]["id"].as_str())
            .map(|value| value.to_string())
            .collect();
        assert!(doc_ids.contains(&readable_doc));
        assert!(!doc_ids.contains(&hidden_doc));

        let recent_edges = data["workspace"]["recentlyUpdatedDocs"]["edges"]
            .as_array()
            .expect("recent edges array");
        let recent_ids: Vec<String> = recent_edges
            .iter()
            .filter_map(|edge| edge["node"]["id"].as_str())
            .map(|value| value.to_string())
            .collect();
        assert!(recent_ids.contains(&readable_doc));
        assert!(!recent_ids.contains(&hidden_doc));
    }

    #[tokio::test]
    async fn graphql_workspace_doc_field_respects_permissions() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, _owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;
        set_default_role(&database, &workspace_id, &doc_id, "none").await;

        let password_hash = generate_password_hash("secret").expect("hash password");
        let collaborator = state
            .user_store
            .create("doc-reader@example.com", &password_hash, Some("Doc Reader"))
            .await
            .expect("create collaborator");
        state
            .workspace_store
            .set_member_role(&workspace_id, &collaborator.id, "member")
            .await
            .expect("add collaborator membership");

        let schema = graphql::build_schema(state.clone());
        let query_doc = format!(
            "{{ workspace(id: \"{workspace_id}\") {{ doc(docId: \"{doc_id}\") {{ id }} }} }}",
            workspace_id = workspace_id,
            doc_id = doc_id
        );

        let forbidden = schema
            .execute(
                GraphQLRequest::new(query_doc.clone())
                    .data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            !forbidden.errors.is_empty(),
            "doc query should fail without Doc.Read"
        );
        assert_eq!(
            forbidden.errors[0].message, "Doc.Read permission required",
            "expected Doc.Read error"
        );

        state
            .doc_role_store
            .upsert(&workspace_id, &doc_id, &collaborator.id, "reader")
            .await
            .expect("grant reader role");

        let permitted = schema
            .execute(GraphQLRequest::new(query_doc).data(RequestUser::new(collaborator.id.clone())))
            .await;
        assert!(
            permitted.errors.is_empty(),
            "doc query errors: {:?}",
            permitted.errors
        );
        let data = permitted.data.into_json().expect("valid json");
        assert_eq!(
            data["workspace"]["doc"]["id"].as_str(),
            Some(doc_id.as_str())
        );
    }

    #[tokio::test]
    async fn graphql_workspace_histories_require_visibility() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, _owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;
        set_default_role(&database, &workspace_id, &doc_id, "none").await;

        let password_hash = generate_password_hash("secret").expect("hash password");
        let collaborator = state
            .user_store
            .create(
                "history-reader@example.com",
                &password_hash,
                Some("History Reader"),
            )
            .await
            .expect("create collaborator");
        state
            .workspace_store
            .set_member_role(&workspace_id, &collaborator.id, "member")
            .await
            .expect("add collaborator membership");

        let schema = graphql::build_schema(state.clone());
        let query_histories = format!(
            "{{ workspace(id: \"{workspace_id}\") {{ histories(guid: \"{doc_id}\") {{ timestamp }} }} }}",
            workspace_id = workspace_id,
            doc_id = doc_id
        );

        let forbidden = schema
            .execute(
                GraphQLRequest::new(query_histories.clone())
                    .data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            !forbidden.errors.is_empty(),
            "histories should fail without Doc.Read"
        );
        assert_eq!(forbidden.errors[0].message, "Doc.Read permission required");

        state
            .doc_role_store
            .upsert(&workspace_id, &doc_id, &collaborator.id, "reader")
            .await
            .expect("grant reader role for histories");

        let permitted = schema
            .execute(
                GraphQLRequest::new(query_histories)
                    .data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            permitted.errors.is_empty(),
            "histories errors: {:?}",
            permitted.errors
        );
        let data = permitted.data.into_json().expect("valid json");
        assert!(
            data["workspace"]["histories"].as_array().is_some(),
            "histories should resolve to array"
        );
    }

    #[tokio::test]
    async fn graphql_public_doc_mode_matches_pascal_case() {
        let (_temp_dir, _database, state) = setup_state().await;
        let schema = graphql::build_schema(state.clone());
        let response = schema
            .execute(GraphQLRequest::new(
                "{ __type(name: \"PublicDocMode\") { enumValues { name } } }",
            ))
            .await;

        assert!(
            response.errors.is_empty(),
            "introspection failed: {:?}",
            response.errors
        );

        let data = to_value(response.data).expect("introspection serializes");
        let values = data["__type"]["enumValues"]
            .as_array()
            .expect("enum values");
        let names: Vec<String> = values
            .iter()
            .map(|value| value["name"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(names, ["Page", "Edgeless"]);
    }

    async fn set_default_role(
        database: &barffine_core::db::Database,
        workspace_id: &str,
        doc_id: &str,
        role: &str,
    ) {
        database
            .repositories()
            .doc_repo()
            .update_default_role_entry(workspace_id, doc_id, role)
            .await
            .expect("update default role");
    }
}
