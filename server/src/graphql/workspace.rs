use std::collections::{HashMap, HashSet};

use anyhow::{Error as AnyError, anyhow};
use async_graphql::{Context, Enum, ID, Object, Result as GraphQLResult, SimpleObject};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};

use barffine_core::{
    doc_store::DocumentMetadata,
    feature::FeatureNamespace,
    notification::{CommentChangeRecord, CommentCursor, CommentStore},
    workspace,
    workspace::{WorkspaceRecord, WorkspaceStore},
};

use crate::{
    AppError, AppState,
    comment::fetch as comment_fetch,
    doc::{history, metadata as doc_metadata},
    user::helpers::display_name_from_parts,
    workspace::{invites as workspace_invites, service::WorkspaceService},
};

use super::{
    RequestUser,
    constants::*,
    cursors::decode_cursor_payload_graphql,
    doc::{
        DocCursorKind, DocHistoryType, DocPermissions, DocTypeEdge, PaginatedDocType,
        WorkspaceUserGraph, decode_doc_cursor, encode_doc_cursor, ensure_doc_owner_role,
    },
    helpers::{
        doc_permissions_for_user, ensure_document_exists, format_bytes, format_history_period,
        map_anyhow, map_app_error, require_doc_permission, require_request_user,
        require_workspace_permission, timestamp_to_datetime, workspace_permission_for_user,
    },
    types::{
        PageInfo, PaginationInput, SubscriptionType,
        blob::ListedBlob,
        comment::{
            CommentChangeObjectType, CommentChangeObjectTypeEdge, CommentObjectType,
            CommentObjectTypeEdge, PaginatedCommentChangeObjectType, PaginatedCommentObjectType,
        },
        doc::DocType,
        user::UserType,
    },
};
use serde_json::{Value as JsonValue, json};

#[derive(Clone)]
pub(crate) struct WorkspaceType {
    pub(crate) record: WorkspaceRecord,
}

impl From<WorkspaceRecord> for WorkspaceType {
    fn from(record: WorkspaceRecord) -> Self {
        Self { record }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(name = "Permission")]
pub(crate) enum Permission {
    #[graphql(name = "Owner")]
    Owner,
    #[graphql(name = "Admin")]
    Admin,
    #[graphql(name = "Collaborator")]
    Collaborator,
    #[graphql(name = "External")]
    External,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(name = "WorkspaceInviteLinkExpireTime")]
pub(crate) enum WorkspaceInviteLinkExpireTime {
    #[graphql(name = "OneDay")]
    OneDay,
    #[graphql(name = "ThreeDays")]
    ThreeDays,
    #[graphql(name = "OneWeek")]
    OneWeek,
    #[graphql(name = "OneMonth")]
    OneMonth,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "WorkspacePermissions")]
pub(crate) struct WorkspacePermissions {
    #[graphql(name = "Workspace_Administrators_Manage")]
    pub(crate) workspace_administrators_manage: bool,
    #[graphql(name = "Workspace_Blobs_List")]
    pub(crate) workspace_blobs_list: bool,
    #[graphql(name = "Workspace_Blobs_Read")]
    pub(crate) workspace_blobs_read: bool,
    #[graphql(name = "Workspace_Blobs_Write")]
    pub(crate) workspace_blobs_write: bool,
    #[graphql(name = "Workspace_Copilot")]
    pub(crate) workspace_copilot: bool,
    #[graphql(name = "Workspace_CreateDoc")]
    pub(crate) workspace_create_doc: bool,
    #[graphql(name = "Workspace_Delete")]
    pub(crate) workspace_delete: bool,
    #[graphql(name = "Workspace_Organize_Read")]
    pub(crate) workspace_organize_read: bool,
    #[graphql(name = "Workspace_Payment_Manage")]
    pub(crate) workspace_payment_manage: bool,
    #[graphql(name = "Workspace_Properties_Create")]
    pub(crate) workspace_properties_create: bool,
    #[graphql(name = "Workspace_Properties_Delete")]
    pub(crate) workspace_properties_delete: bool,
    #[graphql(name = "Workspace_Properties_Read")]
    pub(crate) workspace_properties_read: bool,
    #[graphql(name = "Workspace_Properties_Update")]
    pub(crate) workspace_properties_update: bool,
    #[graphql(name = "Workspace_Read")]
    pub(crate) workspace_read: bool,
    #[graphql(name = "Workspace_Settings_Read")]
    pub(crate) workspace_settings_read: bool,
    #[graphql(name = "Workspace_Settings_Update")]
    pub(crate) workspace_settings_update: bool,
    #[graphql(name = "Workspace_Sync")]
    pub(crate) workspace_sync: bool,
    #[graphql(name = "Workspace_TransferOwner")]
    pub(crate) workspace_transfer_owner: bool,
    #[graphql(name = "Workspace_Users_Manage")]
    pub(crate) workspace_users_manage: bool,
    #[graphql(name = "Workspace_Users_Read")]
    pub(crate) workspace_users_read: bool,
}

impl WorkspacePermissions {
    pub(crate) fn from_role(role: Permission) -> Self {
        let mut perms = Self {
            workspace_administrators_manage: false,
            workspace_blobs_list: false,
            workspace_blobs_read: true,
            workspace_blobs_write: false,
            workspace_copilot: false,
            workspace_create_doc: false,
            workspace_delete: false,
            workspace_organize_read: true,
            workspace_payment_manage: false,
            workspace_properties_create: false,
            workspace_properties_delete: false,
            workspace_properties_read: true,
            workspace_properties_update: false,
            workspace_read: true,
            workspace_settings_read: false,
            workspace_settings_update: false,
            workspace_sync: false,
            workspace_transfer_owner: false,
            workspace_users_manage: false,
            workspace_users_read: false,
        };

        if matches!(
            role,
            Permission::Collaborator | Permission::Admin | Permission::Owner
        ) {
            perms.workspace_sync = true;
            perms.workspace_create_doc = true;
            perms.workspace_users_read = true;
            perms.workspace_settings_read = true;
            perms.workspace_blobs_write = true;
            perms.workspace_blobs_list = true;
            perms.workspace_copilot = true;
        }

        if matches!(role, Permission::Admin | Permission::Owner) {
            perms.workspace_users_manage = true;
            perms.workspace_settings_update = true;
            perms.workspace_properties_create = true;
            perms.workspace_properties_update = true;
            perms.workspace_properties_delete = true;
        }

        if matches!(role, Permission::Owner) {
            perms.workspace_delete = true;
            perms.workspace_administrators_manage = true;
            perms.workspace_transfer_owner = true;
            perms.workspace_payment_manage = true;
        }

        perms
    }
}

impl Default for WorkspacePermissions {
    fn default() -> Self {
        WorkspacePermissions::from_role(Permission::External)
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "WorkspaceQuotaHumanReadableType")]
pub(crate) struct WorkspaceQuotaHumanReadableType {
    #[graphql(name = "blobLimit")]
    pub(crate) blob_limit: String,
    #[graphql(name = "historyPeriod")]
    pub(crate) history_period: String,
    #[graphql(name = "memberCount")]
    pub(crate) member_count: String,
    #[graphql(name = "memberLimit")]
    pub(crate) member_limit: String,
    pub(crate) name: String,
    #[graphql(name = "overcapacityMemberCount")]
    pub(crate) overcapacity_member_count: String,
    #[graphql(name = "storageQuota")]
    pub(crate) storage_quota: String,
    #[graphql(name = "storageQuotaUsed")]
    pub(crate) storage_quota_used: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "WorkspaceBlobSizes")]
pub(crate) struct WorkspaceBlobSizes {
    pub(crate) size: i64,
}

impl WorkspaceBlobSizes {
    pub(crate) fn new(size: i64) -> Self {
        Self { size }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "ContextWorkspaceEmbeddingStatus")]
pub(crate) struct ContextWorkspaceEmbeddingStatus {
    pub(crate) embedded: i32,
    pub(crate) total: i32,
}

impl ContextWorkspaceEmbeddingStatus {
    pub(crate) fn new(total: usize, embedded: usize) -> Self {
        Self {
            embedded: embedded.min(i32::MAX as usize) as i32,
            total: total.min(i32::MAX as usize) as i32,
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "WorkspaceRolePermissions")]
pub(crate) struct WorkspaceRolePermissions {
    pub(crate) permissions: WorkspacePermissions,
    pub(crate) role: Permission,
}

impl WorkspaceRolePermissions {
    pub(crate) fn new(role: Permission) -> Self {
        Self {
            permissions: WorkspacePermissions::from_role(role),
            role,
        }
    }
}

impl Default for WorkspaceQuotaHumanReadableType {
    fn default() -> Self {
        Self {
            blob_limit: "0 B".to_string(),
            history_period: "30 days".to_string(),
            member_count: "1".to_string(),
            member_limit: "1".to_string(),
            name: "Free".to_string(),
            overcapacity_member_count: "0".to_string(),
            storage_quota: "0 B".to_string(),
            storage_quota_used: "0 B".to_string(),
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "WorkspaceQuotaType")]
pub(crate) struct WorkspaceQuotaType {
    #[graphql(name = "blobLimit")]
    pub(crate) blob_limit: i64,
    #[graphql(name = "historyPeriod")]
    pub(crate) history_period: i64,
    #[graphql(name = "humanReadable")]
    pub(crate) human_readable: WorkspaceQuotaHumanReadableType,
    #[graphql(name = "memberCount")]
    pub(crate) member_count: i32,
    #[graphql(name = "memberLimit")]
    pub(crate) member_limit: i32,
    pub(crate) name: String,
    #[graphql(name = "overcapacityMemberCount")]
    pub(crate) overcapacity_member_count: i32,
    #[graphql(name = "storageQuota")]
    pub(crate) storage_quota: i64,
    #[graphql(name = "usedSize")]
    pub(crate) used_size: i64,
    #[graphql(name = "usedStorageQuota")]
    pub(crate) used_storage_quota: i64,
}

impl Default for WorkspaceQuotaType {
    fn default() -> Self {
        Self {
            blob_limit: 0,
            history_period: 30,
            human_readable: WorkspaceQuotaHumanReadableType::default(),
            member_count: 1,
            member_limit: 1,
            name: "Free".to_string(),
            overcapacity_member_count: 0,
            storage_quota: 0,
            used_size: 0,
            used_storage_quota: 0,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(name = "WorkspaceMemberStatus")]
pub enum WorkspaceMemberStatus {
    #[graphql(name = "Accepted")]
    Accepted,
    #[graphql(name = "AllocatingSeat")]
    AllocatingSeat,
    #[graphql(name = "NeedMoreSeat")]
    NeedMoreSeat,
    #[graphql(name = "NeedMoreSeatAndReview")]
    NeedMoreSeatAndReview,
    #[graphql(name = "Pending")]
    Pending,
    #[graphql(name = "UnderReview")]
    UnderReview,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "InviteResult")]
pub(crate) struct InviteResultType {
    pub(crate) email: String,
    #[graphql(name = "inviteId")]
    pub(crate) invite_id: Option<String>,
    #[graphql(name = "sentSuccess")]
    pub(crate) sent_success: bool,
    pub(crate) error: Option<JsonValue>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "InviteLink")]
pub(crate) struct InviteLinkType {
    #[graphql(name = "expireTime")]
    pub(crate) expire_time: DateTime<Utc>,
    pub(crate) link: String,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "InvitationWorkspaceType")]
pub(crate) struct InvitationWorkspaceType {
    pub(crate) avatar: String,
    pub(crate) id: ID,
    pub(crate) name: String,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "InvitationType")]
pub(crate) struct InvitationType {
    pub(crate) invitee: WorkspaceUserGraph,
    pub(crate) status: Option<WorkspaceMemberStatus>,
    pub(crate) user: WorkspaceUserGraph,
    pub(crate) workspace: InvitationWorkspaceType,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InviteUserType")]
pub(crate) struct InviteUserType {
    #[graphql(name = "avatarUrl")]
    avatar_url: Option<String>,
    #[graphql(name = "createdAt", deprecation = "useless")]
    created_at: DateTime<Utc>,
    #[graphql(name = "disabled")]
    disabled: Option<bool>,
    email: Option<String>,
    #[graphql(name = "emailVerified")]
    email_verified: Option<bool>,
    #[graphql(name = "hasPassword")]
    has_password: Option<bool>,
    id: ID,
    #[graphql(name = "inviteId")]
    invite_id: String,
    name: Option<String>,
    #[graphql(deprecation = "Use role instead", name = "permission")]
    permission: Permission,
    role: Permission,
    status: WorkspaceMemberStatus,
}

impl InviteUserType {
    fn from_member(member: workspace::WorkspaceMemberWithUser) -> Self {
        let permission = workspace_role_from_str(&member.role);
        let invite_id = membership_invite_id(&member.workspace_id, &member.user_id);
        let status = workspace_member_status_from_str(&member.status);

        Self {
            avatar_url: member.avatar_url.clone(),
            created_at: timestamp_to_datetime(member.user_created_at),
            disabled: Some(member.disabled),
            email: Some(member.email.clone()),
            email_verified: Some(member.email_verified_at.is_some()),
            has_password: Some(member.has_password),
            id: ID(member.user_id.clone()),
            invite_id,
            name: Some(display_name_from_parts(
                member.name.as_deref(),
                &member.email,
            )),
            permission,
            role: permission,
            status,
        }
    }
}

#[Object(name = "WorkspaceType")]
impl WorkspaceType {
    async fn id(&self) -> ID {
        ID(self.record.id.clone())
    }

    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> DateTime<Utc> {
        timestamp_to_datetime(self.record.created_at)
    }

    async fn public(&self) -> bool {
        self.record.public
    }

    async fn name(&self) -> &str {
        &self.record.name
    }

    #[graphql(name = "inviteLink")]
    async fn invite_link(&self, ctx: &Context<'_>) -> GraphQLResult<Option<InviteLinkType>> {
        let Some(_request_user) = ctx.data_opt::<RequestUser>() else {
            return Ok(None);
        };

        let role = self.workspace_role_for_user(ctx).await?;
        let Some(role) = role else {
            return Ok(None);
        };

        let permissions = WorkspacePermissions::from_role(role);
        if !permissions.workspace_users_manage {
            return Ok(None);
        }

        let state = ctx.data::<AppState>()?;
        if let Some(record) = workspace_invites::find_active_invite_link(state, &self.record.id)
            .await
            .map_err(map_app_error)?
        {
            let expire_time = timestamp_to_datetime(record.expires_at);
            let link = InviteLinkType {
                expire_time,
                link: format!("/invite/{}", record.token),
            };

            return Ok(Some(link));
        }

        Ok(None)
    }

    async fn initialized(&self, ctx: &Context<'_>) -> GraphQLResult<bool> {
        let state = ctx.data::<AppState>()?;
        let doc = state
            .document_store
            .find_metadata(&self.record.id, &self.record.id)
            .await
            .map_err(map_anyhow)?;
        Ok(doc.is_some())
    }

    #[graphql(name = "enableAi")]
    async fn enable_ai(&self) -> bool {
        self.record.enable_ai
    }

    #[graphql(name = "enableDocEmbedding")]
    async fn enable_doc_embedding(&self) -> bool {
        self.record.enable_doc_embedding
    }

    #[graphql(name = "enableUrlPreview")]
    async fn enable_url_preview(&self) -> bool {
        self.record.enable_url_preview
    }

    #[graphql(name = "avatarKey")]
    async fn avatar_key(&self) -> Option<&str> {
        self.record.avatar_key.as_deref()
    }

    async fn indexed(&self) -> bool {
        self.record.indexed
    }

    #[graphql(name = "lastCheckEmbeddings")]
    async fn last_check_embeddings(&self) -> DateTime<Utc> {
        timestamp_to_datetime(self.record.last_check_embeddings)
    }

    async fn team(&self, ctx: &Context<'_>) -> GraphQLResult<bool> {
        let state = ctx.data::<AppState>()?;
        let namespace = FeatureNamespace::workspace(self.record.id.clone());
        let snapshot = state
            .feature_service
            .snapshot(&namespace)
            .await
            .map_err(map_anyhow)?;
        Ok(snapshot
            .flags
            .iter()
            .any(|flag| flag.enabled && flag.key == "team_plan_v1"))
    }

    async fn owner(&self, ctx: &Context<'_>) -> GraphQLResult<UserType> {
        let state = ctx.data::<AppState>()?;
        let owner = state
            .user_store
            .find_by_id(&self.record.owner_id)
            .await
            .map_err(map_anyhow)?;

        let owner = owner.ok_or_else(|| {
            map_app_error(AppError::internal(anyhow!(
                "owner {} not found for workspace {}",
                self.record.owner_id,
                self.record.id
            )))
        })?;

        Ok(UserType::from(owner))
    }

    async fn role(&self, ctx: &Context<'_>) -> GraphQLResult<Permission> {
        Ok(self
            .workspace_role_for_user(ctx)
            .await?
            .unwrap_or(Permission::External))
    }

    async fn permissions(&self, ctx: &Context<'_>) -> GraphQLResult<WorkspacePermissions> {
        let role = self.workspace_role_for_user(ctx).await?;
        Ok(WorkspacePermissions::from_role(
            role.unwrap_or(Permission::External),
        ))
    }

    async fn quota(&self, ctx: &Context<'_>) -> GraphQLResult<WorkspaceQuotaType> {
        let state = ctx.data::<AppState>()?;
        let members = self.fetch_members(state).await.map_err(map_anyhow)?;
        Ok(build_workspace_quota(members.len() as i32))
    }

    #[graphql(name = "memberCount")]
    async fn member_count(&self, ctx: &Context<'_>) -> GraphQLResult<i32> {
        let state = ctx.data::<AppState>()?;
        let members = self.fetch_members(state).await.map_err(map_anyhow)?;
        Ok(members.len() as i32)
    }

    async fn members(
        &self,
        ctx: &Context<'_>,
        query: Option<String>,
        skip: Option<i32>,
        take: Option<i32>,
    ) -> GraphQLResult<Vec<InviteUserType>> {
        require_workspace_permission(ctx, &self.record.id, Permission::Collaborator).await?;
        let state = ctx.data::<AppState>()?;

        let offset = skip.unwrap_or(0).max(0) as i64;
        let requested_limit = take.unwrap_or(8).max(0) as i64;

        if requested_limit == 0 {
            return Ok(Vec::new());
        }

        if let Some(query) = query {
            let trimmed = query.trim();
            if !trimmed.is_empty() {
                if trimmed.len() > 255 {
                    return Err(map_app_error(AppError::bad_request(
                        "query must not exceed 255 characters",
                    )));
                }

                let members = state
                    .workspace_store
                    .search_members_with_users(&self.record.id, trimmed, offset, requested_limit)
                    .await
                    .map_err(map_anyhow)?;

                return Ok(members
                    .into_iter()
                    .map(InviteUserType::from_member)
                    .collect());
            }
        }

        let members = self.fetch_members(state).await.map_err(map_anyhow)?;
        let start = offset as usize;
        let limit = if take.is_some() {
            requested_limit as usize
        } else {
            members.len().saturating_sub(start)
        };
        let end = start.saturating_add(limit);

        Ok(members
            .into_iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .map(InviteUserType::from_member)
            .collect())
    }

    async fn blobs(&self, ctx: &Context<'_>) -> GraphQLResult<Vec<ListedBlob>> {
        require_workspace_permission(ctx, &self.record.id, Permission::Collaborator).await?;
        let state = ctx.data::<AppState>()?;
        let records = state
            .blob_store
            .list(&self.record.id)
            .await
            .map_err(map_anyhow)?;
        Ok(records.into_iter().map(ListedBlob::from_record).collect())
    }

    #[graphql(name = "blobsSize")]
    async fn blobs_size(&self, ctx: &Context<'_>) -> GraphQLResult<i32> {
        require_workspace_permission(ctx, &self.record.id, Permission::Collaborator).await?;
        let state = ctx.data::<AppState>()?;
        let size = state
            .blob_store
            .total_size(&self.record.id)
            .await
            .map_err(map_anyhow)?;
        Ok(size.min(i32::MAX as i64) as i32)
    }

    async fn doc(&self, ctx: &Context<'_>, doc_id: ID) -> GraphQLResult<DocType> {
        let state = ctx.data::<AppState>()?;

        let doc_id_str = doc_id.as_str();

        let existing = state
            .document_store
            .find_metadata(&self.record.id, doc_id_str)
            .await
            .map_err(map_anyhow)?;

        if let Some(metadata) = existing {
            let request_user = ctx.data_opt::<RequestUser>().cloned();
            if !Self::doc_is_visible_to_request(
                state,
                &self.record.id,
                &metadata,
                request_user.as_ref(),
            )
            .await?
            {
                return Err(map_app_error(AppError::forbidden(DOC_READ_FORBIDDEN)));
            }

            return Ok(DocType::from_metadata(metadata));
        }

        let request_user =
            require_workspace_permission(ctx, &self.record.id, Permission::Collaborator).await?;

        let workspace_title = WorkspaceService::normalized_workspace_title(&self.record.name);

        let doc_title = if doc_id_str == self.record.id {
            workspace_title
        } else {
            None
        };

        let metadata = state
            .document_store
            .ensure_doc_record(
                &self.record.id,
                doc_id_str,
                &self.record.owner_id,
                doc_title,
            )
            .await
            .map_err(map_anyhow)?;

        ensure_doc_owner_role(state, &self.record.id, doc_id_str).await?;

        if !Self::doc_is_visible_to_request(state, &self.record.id, &metadata, Some(&request_user))
            .await?
        {
            return Err(map_app_error(AppError::forbidden(DOC_READ_FORBIDDEN)));
        }

        Ok(DocType::from_metadata(metadata))
    }

    async fn comments(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "docId")] doc_id: ID,
        #[graphql(name = "pagination")] pagination: Option<PaginationInput>,
    ) -> GraphQLResult<PaginatedCommentObjectType> {
        let doc_id_str = doc_id.as_str();
        let pagination = pagination.unwrap_or_default();
        let (limit, offset, after) = pagination.normalize().map_err(map_app_error)?;
        let cursor = after.as_deref().map(decode_comment_cursor).transpose()?;
        let after_sid = cursor.as_ref().and_then(|value| value.sid);

        require_doc_permission(
            ctx,
            &self.record.id,
            doc_id_str,
            |perms: &DocPermissions| perms.can_read_comments(),
            "Doc.Comments.Read permission required",
        )
        .await?;

        let state = ctx.data::<AppState>()?;
        ensure_document_exists(state, self.record.id.as_str(), doc_id_str).await?;

        let fetch_limit = if cursor.is_some() || offset == 0 {
            limit
        } else {
            limit + offset
        };

        let mut entries = state
            .comment_store
            .list_comments_paginated(&self.record.id, doc_id_str, fetch_limit, after_sid)
            .await
            .map_err(map_anyhow)?;

        if cursor.is_none() && offset > 0 {
            let skip = offset.min(entries.len() as i64) as usize;
            entries.drain(0..skip);
        }

        if entries.len() as i64 > limit {
            entries.truncate(limit as usize);
        }

        let total_count = state
            .comment_store
            .count_for_doc(&self.record.id, doc_id_str)
            .await
            .map_err(map_anyhow)?;

        let mut edges = Vec::with_capacity(entries.len());
        let mut latest_reply_time: Option<DateTime<Utc>> = None;

        let comment_ids: Vec<String> = entries
            .iter()
            .map(|entry| entry.record.id.clone())
            .collect();
        let mut replies_by_comment = comment_fetch::fetch_replies_for_comments(state, &comment_ids)
            .await
            .map_err(map_app_error)?;

        for entry in &entries {
            let record = entry.record.clone();

            let replies = replies_by_comment.remove(&record.id).unwrap_or_default();

            if let Some(reply_max) = replies.iter().map(|reply| reply.updated_at).max() {
                if latest_reply_time.map_or(true, |current| reply_max > current) {
                    latest_reply_time = Some(reply_max);
                }
            }

            edges.push(CommentObjectTypeEdge {
                cursor: String::new(),
                node: CommentObjectType::from_record_with_replies(record, replies),
            });
        }

        let mut start_cursor_payload = CommentCursor::default();
        if let Some(first) = entries.first() {
            start_cursor_payload.sid = Some(first.sid);
            start_cursor_payload.comment_updated_at = Some(first.record.updated_at);
            start_cursor_payload.reply_updated_at =
                latest_reply_time.or(Some(first.record.updated_at));
        }

        let mut end_cursor_payload = CommentCursor::default();
        if let Some(last) = entries.last() {
            end_cursor_payload.sid = Some(last.sid);
            end_cursor_payload.comment_updated_at = Some(last.record.updated_at);
            end_cursor_payload.reply_updated_at =
                latest_reply_time.or(Some(last.record.updated_at));
        }

        let start_cursor = encode_comment_cursor(Some(&start_cursor_payload));
        let end_cursor = encode_comment_cursor(Some(&end_cursor_payload));

        let has_next_page = (entries.len() as i64) == limit;

        Ok(PaginatedCommentObjectType {
            edges,
            page_info: PageInfo {
                start_cursor: Some(start_cursor),
                end_cursor: Some(end_cursor),
                has_next_page,
                has_previous_page: false,
            },
            total_count: total_count as i32,
        })
    }

    #[graphql(name = "commentChanges")]
    async fn comment_changes(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "docId")] doc_id: ID,
        #[graphql(name = "pagination")] pagination: PaginationInput,
    ) -> GraphQLResult<PaginatedCommentChangeObjectType> {
        let doc_id_str = doc_id.as_str();
        let (limit, offset, after) = pagination.normalize().map_err(map_app_error)?;

        if offset > 0 {
            return Err(map_app_error(AppError::bad_request(
                "commentChanges does not support offset pagination",
            )));
        }

        let cursor = after.as_deref().map(decode_comment_cursor).transpose()?;

        require_doc_permission(
            ctx,
            &self.record.id,
            doc_id_str,
            |perms: &DocPermissions| perms.can_read_comments(),
            "Doc.Comments.Read permission required",
        )
        .await?;

        let state = ctx.data::<AppState>()?;
        ensure_document_exists(state, self.record.id.as_str(), doc_id_str).await?;

        let mut end_cursor_payload = cursor.unwrap_or_default();

        let comment_ts = end_cursor_payload
            .comment_updated_at
            .as_ref()
            .map(|value| value.timestamp());
        let reply_ts = end_cursor_payload
            .reply_updated_at
            .as_ref()
            .map(|value| value.timestamp());

        let mut changes: Vec<CommentChangeRecord> = state
            .comment_store
            .list_comment_changes(&self.record.id, doc_id_str, comment_ts, reply_ts, limit)
            .await
            .map_err(map_anyhow)?;

        let mut unique_ids: HashSet<String> = HashSet::new();
        let mut user_ids: Vec<String> = Vec::new();
        for change in &changes {
            if let Some(author_id) = change.item.get("authorId").and_then(|value| value.as_str()) {
                if unique_ids.insert(author_id.to_owned()) {
                    user_ids.push(author_id.to_owned());
                }
            }
        }

        let mut user_map: HashMap<String, JsonValue> = HashMap::new();
        if !user_ids.is_empty() {
            let users = state
                .user_store
                .find_by_ids(&user_ids)
                .await
                .map_err(map_anyhow)?;
            for user in users {
                let display_name = display_name_from_parts(user.name.as_deref(), &user.email);
                user_map.insert(
                    user.id.clone(),
                    json!({
                        "id": user.id,
                        "name": display_name,
                        "avatarUrl": user.avatar_url,
                    }),
                );
            }
        }

        for change in &mut changes {
            if let Some(author_id) = change
                .item
                .get("authorId")
                .and_then(|value| value.as_str())
                .map(|value| value.to_owned())
            {
                if let Some(map) = change.item.as_object_mut() {
                    if let Some(user_json) = user_map.get(&author_id) {
                        map.insert("user".to_owned(), user_json.clone());
                    }
                }
            }
        }

        for change in &changes {
            match change {
                CommentChangeRecord {
                    comment_id: Some(_),
                    ..
                } => {
                    end_cursor_payload.reply_updated_at = Some(change.updated_at);
                }
                _ => {
                    end_cursor_payload.comment_updated_at = Some(change.updated_at);
                }
            }
        }

        let edges: Vec<CommentChangeObjectTypeEdge> = changes
            .iter()
            .cloned()
            .map(|record| CommentChangeObjectTypeEdge {
                cursor: String::new(),
                node: CommentChangeObjectType::from(record),
            })
            .collect();
        let total_count = edges.len() as i32;

        let start_cursor = encode_comment_cursor(None);
        let end_cursor = encode_comment_cursor(Some(&end_cursor_payload));
        let has_next_page = !edges.is_empty();

        Ok(PaginatedCommentChangeObjectType {
            edges,
            page_info: PageInfo {
                start_cursor: Some(start_cursor),
                end_cursor: Some(end_cursor),
                has_next_page,
                has_previous_page: false,
            },
            total_count,
        })
    }

    async fn docs(
        &self,
        ctx: &Context<'_>,
        pagination: PaginationInput,
    ) -> GraphQLResult<PaginatedDocType> {
        let _request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;
        let (limit, offset, after) = pagination.normalize().map_err(map_app_error)?;
        let cursor = after
            .as_deref()
            .map(decode_doc_cursor)
            .transpose()
            .map_err(map_app_error)?;
        let had_cursor = cursor.is_some();

        let docs = state
            .document_store
            .list_by_workspace_created(&self.record.id, limit + 1, offset, cursor.as_ref())
            .await
            .map_err(map_anyhow)?;
        let total_count = state
            .document_store
            .count_by_workspace(&self.record.id)
            .await
            .map_err(map_anyhow)?;

        self.build_doc_connection(
            ctx,
            docs,
            limit,
            DocCursorKind::CreatedAt,
            had_cursor,
            offset,
            total_count,
        )
        .await
    }

    #[graphql(name = "publicDocs")]
    async fn public_docs(&self, ctx: &Context<'_>) -> GraphQLResult<Vec<DocType>> {
        self.fetch_public_docs(ctx).await
    }

    #[graphql(name = "publicPages", deprecation = "use publicDocs instead")]
    async fn public_pages(&self, ctx: &Context<'_>) -> GraphQLResult<Vec<DocType>> {
        self.fetch_public_docs(ctx).await
    }

    async fn subscription(&self) -> GraphQLResult<Option<SubscriptionType>> {
        Ok(None)
    }

    #[graphql(name = "recentlyUpdatedDocs")]
    async fn recently_updated_docs(
        &self,
        ctx: &Context<'_>,
        pagination: PaginationInput,
    ) -> GraphQLResult<PaginatedDocType> {
        let _request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;
        let (limit, offset, after) = pagination.normalize().map_err(map_app_error)?;
        let cursor = after
            .as_deref()
            .map(decode_doc_cursor)
            .transpose()
            .map_err(map_app_error)?;
        let had_cursor = cursor.is_some();

        let docs = state
            .document_store
            .list_by_workspace_recent(&self.record.id, limit + 1, offset, cursor.as_ref())
            .await
            .map_err(map_anyhow)?;
        let total_count = state
            .document_store
            .count_by_workspace(&self.record.id)
            .await
            .map_err(map_anyhow)?;
        self.build_doc_connection(
            ctx,
            docs,
            limit,
            DocCursorKind::UpdatedAt,
            had_cursor,
            offset,
            total_count,
        )
        .await
    }

    async fn histories(
        &self,
        ctx: &Context<'_>,
        guid: String,
        take: Option<i32>,
        before: Option<DateTime<Utc>>,
    ) -> GraphQLResult<Vec<DocHistoryType>> {
        let state = ctx.data::<AppState>()?;
        let metadata = doc_metadata::fetch_required(state, self.record.id.as_str(), &guid)
            .await
            .map_err(map_app_error)?;

        let request_user = ctx.data_opt::<RequestUser>().cloned();
        if !Self::doc_is_visible_to_request(
            state,
            &self.record.id,
            &metadata,
            request_user.as_ref(),
        )
        .await?
        {
            return Err(map_app_error(AppError::forbidden(DOC_READ_FORBIDDEN)));
        }

        let requested_limit = take.map(|value| value as i64);
        let limit = history::normalize_history_limit(requested_limit);

        let mut records =
            history::fetch_history_records(state, self.record.id.as_str(), &guid, requested_limit)
                .await
                .map_err(map_app_error)?;

        if let Some(before_dt) = before {
            let cutoff = before_dt.timestamp();
            records.retain(|record| record.created_at < cutoff);
        }

        let mut filtered = Vec::with_capacity(records.len());
        for record in records {
            if filtered.len() as i64 >= limit {
                break;
            }
            filtered.push(DocHistoryType::from_record(record, self.record.id.clone()));
        }

        Ok(filtered)
    }
}

const DOC_READ_FORBIDDEN: &str = "Doc.Read permission required";

impl WorkspaceType {
    async fn filter_visible_doc_metadata(
        &self,
        ctx: &Context<'_>,
        docs: Vec<DocumentMetadata>,
    ) -> GraphQLResult<Vec<DocumentMetadata>> {
        let request_user = ctx.data_opt::<RequestUser>().cloned();
        let state = ctx.data::<AppState>()?;

        let mut visible = Vec::with_capacity(docs.len());
        for doc in docs {
            if Self::doc_is_visible_to_request(state, &self.record.id, &doc, request_user.as_ref())
                .await?
            {
                visible.push(doc);
            }
        }

        Ok(visible)
    }

    async fn doc_is_visible_to_request(
        state: &AppState,
        workspace_id: &str,
        metadata: &DocumentMetadata,
        request_user: Option<&RequestUser>,
    ) -> GraphQLResult<bool> {
        if let Some(request_user) = request_user {
            if let Some(perms) =
                doc_permissions_for_user(state, workspace_id, &metadata.id, &request_user.user_id)
                    .await?
            {
                return Ok(perms.can_read_doc());
            }

            return Ok(metadata.public);
        }

        Ok(metadata.public)
    }

    async fn workspace_role_for_user(
        &self,
        ctx: &Context<'_>,
    ) -> GraphQLResult<Option<Permission>> {
        let Some(request_user) = ctx.data_opt::<RequestUser>() else {
            return Ok(None);
        };

        let state = ctx.data::<AppState>()?;
        let resolved = workspace_permission_for_user(state, &self.record, &request_user.user_id)
            .await
            .map_err(map_app_error)?;

        Ok(resolved.map(|value| value.permission))
    }

    async fn fetch_members(
        &self,
        state: &AppState,
    ) -> Result<Vec<workspace::WorkspaceMemberWithUser>, AnyError> {
        state
            .workspace_store
            .list_members_with_users(&self.record.id)
            .await
            .map_err(AnyError::from)
    }

    async fn build_doc_connection(
        &self,
        ctx: &Context<'_>,
        docs: Vec<DocumentMetadata>,
        limit: i64,
        cursor_kind: DocCursorKind,
        had_cursor: bool,
        offset: i64,
        total_count: i64,
    ) -> GraphQLResult<PaginatedDocType> {
        let mut filtered = self.filter_visible_doc_metadata(ctx, docs).await?;

        let has_next_page = (filtered.len() as i64) > limit;
        if has_next_page {
            filtered.truncate(limit as usize);
        }

        let edges: Vec<DocTypeEdge> = filtered
            .into_iter()
            .map(|metadata| DocTypeEdge {
                cursor: encode_doc_cursor(&metadata, cursor_kind),
                node: DocType::from_metadata(metadata),
            })
            .collect();

        let start_cursor = edges.first().map(|edge| edge.cursor.clone());
        let end_cursor = edges.last().map(|edge| edge.cursor.clone());

        let page_info = PageInfo {
            end_cursor,
            has_next_page,
            has_previous_page: had_cursor || offset > 0,
            start_cursor,
        };

        Ok(PaginatedDocType {
            edges,
            page_info,
            total_count: total_count as i32,
        })
    }

    async fn ensure_workspace_read_access(&self, ctx: &Context<'_>) -> GraphQLResult<()> {
        let state = ctx.data::<AppState>()?;
        let request_user = ctx.data_opt::<RequestUser>();
        let allowed = state
            .workspace_service
            .has_workspace_read_access(&self.record, request_user.map(|user| user.user_id.as_str()))
            .await
            .map_err(map_app_error)?;

        if allowed {
            Ok(())
        } else {
            Err(map_app_error(AppError::forbidden(
                "Workspace.Read permission required",
            )))
        }
    }

    async fn fetch_public_docs(&self, ctx: &Context<'_>) -> GraphQLResult<Vec<DocType>> {
        self.ensure_workspace_read_access(ctx).await?;
        let state = ctx.data::<AppState>()?;
        let docs = state
            .document_store
            .list_public(&self.record.id)
            .await
            .map_err(map_anyhow)?;
        Ok(docs.into_iter().map(DocType::from_metadata).collect())
    }
}

pub(crate) fn workspace_role_from_str(role: &str) -> Permission {
    let normalized = role.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "owner" => Permission::Owner,
        "admin" => Permission::Admin,
        "member" | "collaborator" | "editor" => Permission::Collaborator,
        "viewer" | "external" | "guest" => Permission::External,
        _ => Permission::External,
    }
}

pub(crate) fn permission_to_workspace_role(role: Permission) -> &'static str {
    match role {
        Permission::Owner => "owner",
        Permission::Admin => "admin",
        Permission::Collaborator => "member",
        Permission::External => "external",
    }
}

pub(crate) fn permission_rank(role: Permission) -> i32 {
    match role {
        Permission::External => 0,
        Permission::Collaborator => 1,
        Permission::Admin => 2,
        Permission::Owner => 3,
    }
}

pub(crate) fn workspace_member_status_from_str(value: &str) -> WorkspaceMemberStatus {
    let canonical = WorkspaceStore::canonical_member_status(value).unwrap_or("Accepted");
    match canonical {
        "Accepted" => WorkspaceMemberStatus::Accepted,
        "AllocatingSeat" => WorkspaceMemberStatus::AllocatingSeat,
        "NeedMoreSeat" => WorkspaceMemberStatus::NeedMoreSeat,
        "NeedMoreSeatAndReview" => WorkspaceMemberStatus::NeedMoreSeatAndReview,
        "Pending" => WorkspaceMemberStatus::Pending,
        "UnderReview" => WorkspaceMemberStatus::UnderReview,
        _ => WorkspaceMemberStatus::Accepted,
    }
}

pub fn workspace_member_status_to_str(status: WorkspaceMemberStatus) -> &'static str {
    match status {
        WorkspaceMemberStatus::Accepted => "Accepted",
        WorkspaceMemberStatus::AllocatingSeat => "AllocatingSeat",
        WorkspaceMemberStatus::NeedMoreSeat => "NeedMoreSeat",
        WorkspaceMemberStatus::NeedMoreSeatAndReview => "NeedMoreSeatAndReview",
        WorkspaceMemberStatus::Pending => "Pending",
        WorkspaceMemberStatus::UnderReview => "UnderReview",
    }
}

pub(crate) fn workspace_invite_link_expiry_seconds(
    expire_time: WorkspaceInviteLinkExpireTime,
) -> i64 {
    const DAY: i64 = 86_400;
    match expire_time {
        WorkspaceInviteLinkExpireTime::OneDay => DAY,
        WorkspaceInviteLinkExpireTime::ThreeDays => 3 * DAY,
        WorkspaceInviteLinkExpireTime::OneWeek => 7 * DAY,
        WorkspaceInviteLinkExpireTime::OneMonth => 30 * DAY,
    }
}

pub(crate) fn membership_invite_id(workspace_id: &str, user_id: &str) -> String {
    format!("membership:{workspace_id}:{user_id}")
}

pub(crate) fn parse_membership_invite_id(invite_id: &str) -> Option<(String, String)> {
    let mut parts = invite_id.splitn(3, ':');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("membership"), Some(workspace_id), Some(user_id))
            if !workspace_id.is_empty() && !user_id.is_empty() =>
        {
            Some((workspace_id.to_string(), user_id.to_string()))
        }
        _ => None,
    }
}

fn encode_comment_cursor(cursor: Option<&CommentCursor>) -> String {
    let encoded = match cursor {
        Some(data) => serde_json::to_string(data).unwrap_or_else(|_| "null".to_owned()),
        None => "null".to_owned(),
    };
    BASE64.encode(encoded)
}

fn decode_comment_cursor(cursor: &str) -> GraphQLResult<CommentCursor> {
    let decoded = decode_cursor_payload_graphql(cursor)?;

    if decoded.trim().is_empty() || decoded.trim() == "null" {
        return Ok(CommentCursor::default());
    }

    serde_json::from_str::<CommentCursor>(&decoded)
        .map_err(|_| map_app_error(AppError::bad_request("invalid cursor payload")))
}

pub(crate) fn build_workspace_quota(member_count: i32) -> WorkspaceQuotaType {
    let count = member_count.max(0);
    let overcapacity = (count - FREE_MEMBER_LIMIT).max(0);

    let human = WorkspaceQuotaHumanReadableType {
        blob_limit: format_bytes(FREE_BLOB_LIMIT),
        history_period: format_history_period(FREE_HISTORY_PERIOD_MS),
        member_count: count.to_string(),
        member_limit: FREE_MEMBER_LIMIT.to_string(),
        name: FREE_PLAN_NAME.to_string(),
        overcapacity_member_count: overcapacity.to_string(),
        storage_quota: format_bytes(FREE_STORAGE_QUOTA),
        storage_quota_used: format_bytes(0),
    };

    WorkspaceQuotaType {
        blob_limit: FREE_BLOB_LIMIT,
        history_period: FREE_HISTORY_PERIOD_MS,
        human_readable: human,
        member_count: count,
        member_limit: FREE_MEMBER_LIMIT,
        name: FREE_PLAN_NAME.to_string(),
        overcapacity_member_count: overcapacity,
        storage_quota: FREE_STORAGE_QUOTA,
        used_size: 0,
        used_storage_quota: 0,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        auth::generate_password_hash,
        graphql::{self, RequestUser},
        test_support::{insert_document, seed_workspace, setup_state},
    };
    use async_graphql::Request as GraphQLRequest;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[tokio::test]
    async fn graphql_update_workspace_updates_flags() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            r#"
			mutation {{
				updateWorkspace(input: {{
					id: "{workspace_id}",
					name: "Renamed Workspace",
					public: true,
					enableAi: true,
					enableDocEmbedding: true,
					enableUrlPreview: true,
                    avatarKey: "logo-key",
                    indexed: true,
                    lastCheckEmbeddings: "2024-01-02T03:04:05Z"
				}}) {{
					id
					public
					enableAi
					enableDocEmbedding
					enableUrlPreview
                    avatarKey
                    indexed
                    lastCheckEmbeddings
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

        let updated = state
            .workspace_store
            .find_by_id(&workspace_id)
            .await
            .expect("query workspace")
            .expect("workspace present");
        assert_eq!(updated.name, "Renamed Workspace");
        assert!(updated.public);
        assert!(updated.enable_ai);
        assert!(updated.enable_doc_embedding);
        assert!(updated.enable_url_preview);
        assert_eq!(updated.avatar_key.as_deref(), Some("logo-key"));
        assert!(updated.indexed);
        assert_eq!(
            updated.last_check_embeddings,
            chrono::DateTime::parse_from_rfc3339("2024-01-02T03:04:05Z")
                .expect("parse timestamp")
                .timestamp()
        );
    }

    #[tokio::test]
    async fn graphql_update_workspace_requires_admin_role() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let collaborator = state
            .user_store
            .create("collab@example.com", &password_hash, Some("Collaborator"))
            .await
            .expect("create collaborator");
        state
            .workspace_store
            .set_member_role(&workspace_id, &collaborator.id, "member")
            .await
            .expect("add collaborator");

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            r#"
			mutation {{
				updateWorkspace(input: {{
					id: "{workspace_id}",
					public: true
				}}) {{
					id
				}}
			}}
			"#
        );

        let response = schema
            .execute(GraphQLRequest::new(mutation).data(RequestUser::new(collaborator.id.clone())))
            .await;

        assert!(!response.errors.is_empty(), "expected authorization error");
        let error = &response.errors[0];
        assert_eq!(error.message, "insufficient workspace permissions");

        let owner_attempt = schema
            .execute(
                GraphQLRequest::new(format!(
                    r#"
					mutation {{
						updateWorkspace(input: {{
							id: "{workspace_id}",
							public: false
						}}) {{
							id
						}}
					}}
					"#
                ))
                .data(RequestUser::new(owner_id.clone())),
            )
            .await;
        assert!(
            owner_attempt.errors.is_empty(),
            "owner update failed: {:?}",
            owner_attempt.errors
        );
    }

    #[tokio::test]
    async fn graphql_delete_workspace_removes_record() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            r#"
			mutation {{
				deleteWorkspace(id: "{workspace_id}")
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

        let record = state
            .workspace_store
            .find_by_id(&workspace_id)
            .await
            .expect("query workspace");
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn graphql_workspace_member_management() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let member = state
            .user_store
            .create("member@example.com", &password_hash, Some("Member"))
            .await
            .expect("create member");

        let schema = graphql::build_schema(state.clone());
        let grant_mutation = format!(
            r#"
			mutation {{
				grantMember(
					permission: Collaborator,
					userId: "{user_id}",
					workspaceId: "{workspace_id}"
				)
			}}
			"#,
            user_id = member.id,
            workspace_id = workspace_id
        );

        let response = schema
            .execute(GraphQLRequest::new(grant_mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let role = state
            .workspace_store
            .find_member_role(&workspace_id, &member.id)
            .await
            .expect("query member role")
            .expect("role set");
        assert_eq!(role, "member");

        let revoke_mutation = format!(
            r#"
			mutation {{
				revokeMember(
					userId: "{user_id}",
					workspaceId: "{workspace_id}"
				)
			}}
			"#,
            user_id = member.id,
            workspace_id = workspace_id
        );

        let revoke_response = schema
            .execute(GraphQLRequest::new(revoke_mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            revoke_response.errors.is_empty(),
            "unexpected errors: {:?}",
            revoke_response.errors
        );

        let role = state
            .workspace_store
            .find_member_role(&workspace_id, &member.id)
            .await
            .expect("query member role");
        assert!(role.is_none());
    }

    #[tokio::test]
    async fn graphql_workspace_members_requires_permission() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let outsider = state
            .user_store
            .create("outsider@example.com", &password_hash, Some("Outsider"))
            .await
            .expect("create outsider");

        state
            .workspace_store
            .set_member_role_with_status(
                &workspace_id,
                &owner_id,
                "owner",
                "Accepted",
                Some(owner_id.as_str()),
                Some("system"),
                None,
            )
            .await
            .expect("ensure owner membership");

        let schema = graphql::build_schema(state.clone());
        let query = format!(
            r#"
        {{
            workspace(id: "{workspace_id}") {{
                members {{ id }}
            }}
        }}
        "#
        );

        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(outsider.id)))
            .await;

        assert!(
            !response.errors.is_empty(),
            "expected members query to be forbidden"
        );
        let message = response.errors[0].message.clone();
        assert!(
            message.contains("Workspace.Read permission required")
                || message.contains("Doc.Comments.Read permission required")
        );
    }

    #[tokio::test]
    async fn graphql_workspace_members_search_filters_pending_and_is_case_insensitive() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let password_hash = generate_password_hash("secret").expect("hash password");

        let accepted = state
            .user_store
            .create("alice@example.com", &password_hash, Some("Alice Example"))
            .await
            .expect("create accepted member");
        let pending = state
            .user_store
            .create(
                "pendingalice@example.com",
                &password_hash,
                Some("Pending Alice"),
            )
            .await
            .expect("create pending member");

        state
            .workspace_store
            .set_member_role_with_status(
                &workspace_id,
                &accepted.id,
                "member",
                "Accepted",
                Some(owner_id.as_str()),
                Some("owner"),
                None,
            )
            .await
            .expect("persist accepted member");

        state
            .workspace_store
            .set_member_role_with_status(
                &workspace_id,
                &pending.id,
                "member",
                "Pending",
                Some(owner_id.as_str()),
                Some("owner"),
                None,
            )
            .await
            .expect("persist pending member");

        let schema = graphql::build_schema(state.clone());
        let query = format!(
            r#"
        {{
            workspace(id: "{workspace_id}") {{
                members(query: "ALICE", take: 10) {{
                    id
                    email
                    status
                }}
            }}
        }}
        "#
        );

        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("json data");
        let members = data["workspace"]["members"]
            .as_array()
            .expect("members array");
        assert_eq!(
            members.len(),
            2,
            "accepted and pending members should be returned"
        );
        let status_map = members
            .iter()
            .map(|member| {
                (
                    member["id"].as_str().unwrap().to_string(),
                    member["status"].as_str().unwrap().to_string(),
                )
            })
            .collect::<HashMap<_, _>>();

        assert_eq!(
            status_map.get(&accepted.id).map(String::as_str),
            Some("Accepted")
        );
        assert_eq!(
            status_map.get(&pending.id).map(String::as_str),
            Some("Pending")
        );
    }

    #[tokio::test]
    async fn graphql_workspace_info_returns_role_and_team() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;

        let schema = graphql::build_schema(state.clone());
        let query = format!(
            r#"
        {{
            workspace(id: "{workspace_id}") {{
                role
                team
            }}
        }}
        "#
        );

        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(owner_id)))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("json data");
        assert_eq!(data["workspace"]["role"], "Owner");
        assert_eq!(data["workspace"]["team"], false);
    }

    #[tokio::test]
    async fn graphql_create_workspace_invites_and_features() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let owner = state
            .user_store
            .create("owner@example.com", &password_hash, Some("Owner"))
            .await
            .expect("create owner");
        let invitee = state
            .user_store
            .create("invitee@example.com", &password_hash, Some("Invitee"))
            .await
            .expect("create invitee");

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            r#"
		mutation {{
			createWorkspace(input: {{
				name: "Team Workspace",
				inviteUserIds: ["{invitee}"],
				public: true,
				enableAi: true,
				enableDocEmbedding: true,
				enableUrlPreview: true
			}}) {{
				id
				name
				public
				enableAi
				enableDocEmbedding
				enableUrlPreview
			}}
		}}
		"#,
            invitee = invitee.id,
        );

        let response = schema
            .execute(GraphQLRequest::new(mutation).data(RequestUser::new(owner.id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("valid json");
        let workspace = &data["createWorkspace"];
        assert_eq!(workspace["name"], "Team Workspace");
        assert_eq!(workspace["public"], true);
        assert_eq!(workspace["enableAi"], true);
        assert_eq!(workspace["enableDocEmbedding"], true);
        assert_eq!(workspace["enableUrlPreview"], true);

        let workspace_id = workspace["id"].as_str().expect("workspace id").to_string();

        let record = state
            .workspace_store
            .find_by_id(&workspace_id)
            .await
            .expect("query workspace")
            .expect("workspace exists");
        assert!(record.public);
        assert!(record.enable_ai);
        assert!(record.enable_doc_embedding);
        assert!(record.enable_url_preview);

        let membership = state
            .workspace_store
            .find_member_role(&workspace_id, &invitee.id)
            .await
            .expect("query membership");
        assert_eq!(membership.as_deref(), Some("member"));
    }

    #[tokio::test]
    async fn graphql_accept_invite_by_id_accepts_membership() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let invitee = state
            .user_store
            .create("new-member@example.com", &password_hash, Some("Member"))
            .await
            .expect("create invitee");

        let schema = graphql::build_schema(state.clone());
        let invite_mutation = format!(
            r#"
		mutation {{
			inviteMembers(workspaceId: "{workspace_id}", emails: ["{email}"]) {{
				inviteId
				sentSuccess
			}}
		}}
		"#,
            workspace_id = workspace_id,
            email = invitee.email
        );

        let invite_response = schema
            .execute(GraphQLRequest::new(invite_mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            invite_response.errors.is_empty(),
            "unexpected errors: {:?}",
            invite_response.errors
        );

        let invite_data = invite_response.data.into_json().expect("valid json");
        let invite_entries = invite_data["inviteMembers"]
            .as_array()
            .expect("invite array");
        let invite_id = invite_entries[0]["inviteId"].as_str().expect("invite id");
        assert!(
            invite_entries[0]["sentSuccess"]
                .as_bool()
                .unwrap_or_default()
        );

        let accept_mutation = format!(
            r#"
		mutation {{
			acceptInviteById(inviteId: "{invite_id}")
		}}
		"#,
            invite_id = invite_id
        );

        let accept_response = schema.execute(GraphQLRequest::new(accept_mutation)).await;
        assert!(
            accept_response.errors.is_empty(),
            "unexpected errors: {:?}",
            accept_response.errors
        );
        let accept_data = accept_response.data.into_json().expect("valid json");
        assert_eq!(accept_data["acceptInviteById"], true);

        let membership = state
            .workspace_store
            .get_member(&workspace_id, &invitee.id)
            .await
            .expect("query membership")
            .expect("membership exists");
        assert_eq!(membership.status, "Accepted");
    }

    #[tokio::test]
    async fn graphql_accept_invite_by_id_accepts_link_invitation() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let joiner = state
            .user_store
            .create("link-member@example.com", &password_hash, Some("Joiner"))
            .await
            .expect("create joiner");

        let schema = graphql::build_schema(state.clone());
        let link_mutation = format!(
            r#"
		mutation {{
			createInviteLink(workspaceId: "{workspace_id}", expireTime: OneDay) {{
				link
			}}
		}}
		"#,
            workspace_id = workspace_id
        );

        let link_response = schema
            .execute(GraphQLRequest::new(link_mutation).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            link_response.errors.is_empty(),
            "unexpected errors: {:?}",
            link_response.errors
        );
        let link_data = link_response.data.into_json().expect("valid json");
        let link_value = link_data["createInviteLink"]["link"]
            .as_str()
            .expect("invite link");
        let token = link_value.rsplit('/').next().expect("token");

        let accept_mutation = format!(
            r#"
		mutation {{
			acceptInviteById(inviteId: "{token}")
		}}
		"#,
            token = token
        );

        let accept_response = schema
            .execute(GraphQLRequest::new(accept_mutation).data(RequestUser::new(joiner.id.clone())))
            .await;

        assert!(
            accept_response.errors.is_empty(),
            "unexpected errors: {:?}",
            accept_response.errors
        );
        let accept_data = accept_response.data.into_json().expect("valid json");
        assert_eq!(accept_data["acceptInviteById"], true);

        let membership = state
            .workspace_store
            .get_member(&workspace_id, &joiner.id)
            .await
            .expect("query membership")
            .expect("membership exists");
        assert_eq!(membership.status, "Accepted");
    }

    #[tokio::test]
    async fn graphql_comment_queries_support_pagination() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let schema = graphql::build_schema(state.clone());
        let mut comment_ids = Vec::new();
        for idx in 0..3 {
            let create_comment = format!(
                r#"
		mutation {{
			createComment(input: {{
				workspaceId: "{workspace_id}",
				docId: "{doc_id}",
				content: {{ message: "Comment {idx}" }}
			}}) {{
				id
			}}
		}}
		"#,
            );

            let response = schema
                .execute(
                    GraphQLRequest::new(create_comment.clone())
                        .data(RequestUser::new(owner_id.clone())),
                )
                .await;
            assert!(
                response.errors.is_empty(),
                "comment create failed: {:?}",
                response.errors
            );

            let json = response.data.into_json().expect("valid json");
            let comment_id = json["createComment"]["id"]
                .as_str()
                .expect("comment id")
                .to_string();
            comment_ids.push(comment_id);
        }

        let reply_mutation = format!(
            r#"
	mutation {{
		createReply(input: {{
			commentId: "{comment_id}",
			content: {{ message: "Thread reply" }}
		}}) {{
			id
		}}
	}}
	"#,
            comment_id = comment_ids[0]
        );

        let reply_response = schema
            .execute(GraphQLRequest::new(reply_mutation).data(RequestUser::new(owner_id.clone())))
            .await;
        assert!(
            reply_response.errors.is_empty(),
            "reply create failed: {:?}",
            reply_response.errors
        );

        let list_first_page = format!(
            r#"
	query {{
		workspace(id: "{workspace_id}") {{
			comments(docId: "{doc_id}", pagination: {{ first: 2 }}) {{
				totalCount
				edges {{
					node {{
						id
						content
						replies {{
							id
							content
						}}
					}}
				}}
				pageInfo {{
					hasNextPage
					hasPreviousPage
					startCursor
					endCursor
				}}
			}}
		}}
	}}
	"#,
        );

        let first_page = schema
            .execute(
                GraphQLRequest::new(list_first_page.clone())
                    .data(RequestUser::new(owner_id.clone())),
            )
            .await;
        assert!(
            first_page.errors.is_empty(),
            "comment page query failed: {:?}",
            first_page.errors
        );

        let first_json = first_page.data.into_json().expect("valid json");
        let comments = &first_json["workspace"]["comments"];
        assert_eq!(comments["totalCount"].as_i64().unwrap(), 3);

        let first_edges = comments["edges"].as_array().expect("edges array");
        assert_eq!(first_edges.len(), 2);

        let page_info = &comments["pageInfo"];
        assert!(page_info["hasNextPage"].as_bool().unwrap());
        assert!(!page_info["hasPreviousPage"].as_bool().unwrap());

        let start_cursor = page_info["startCursor"]
            .as_str()
            .expect("start cursor")
            .to_string();
        assert!(!start_cursor.is_empty());
        let end_cursor = page_info["endCursor"]
            .as_str()
            .expect("end cursor")
            .to_string();
        assert!(!end_cursor.is_empty());

        let list_second_page = format!(
            r#"
	query {{
		workspace(id: "{workspace_id}") {{
			comments(docId: "{doc_id}", pagination: {{ first: 2, after: "{after_cursor}" }}) {{
				totalCount
				edges {{
					node {{
						id
						replies {{
							id
							content
						}}
					}}
				}}
				pageInfo {{
					hasNextPage
					hasPreviousPage
				}}
			}}
		}}
	}}
	"#,
            after_cursor = end_cursor
        );

        let second_page = schema
            .execute(
                GraphQLRequest::new(list_second_page.clone())
                    .data(RequestUser::new(owner_id.clone())),
            )
            .await;
        assert!(
            second_page.errors.is_empty(),
            "second comment page query failed: {:?}",
            second_page.errors
        );
        let second_json = second_page.data.into_json().expect("valid json");
        let second_comments = &second_json["workspace"]["comments"];
        let second_edges = second_comments["edges"].as_array().expect("edges array");
        assert_eq!(second_edges.len(), 1);
        let last_node = &second_edges[0]["node"];
        assert_eq!(last_node["id"].as_str().expect("node id"), comment_ids[0]);
        let replies = last_node["replies"].as_array().expect("replies array");
        assert_eq!(replies.len(), 1);
        assert_eq!(
            replies[0]["content"]["message"]
                .as_str()
                .expect("reply message"),
            "Thread reply"
        );
        let second_page_info = &second_comments["pageInfo"];
        assert!(!second_page_info["hasNextPage"].as_bool().unwrap());
        assert!(!second_page_info["hasPreviousPage"].as_bool().unwrap());

        let full_comments_query = format!(
            r#"
	{{ workspace(id: "{workspace_id}") {{
		comments(docId: "{doc_id}", pagination: {{ first: 10 }}) {{
			pageInfo {{ startCursor }}
		}}
	}} }}"#,
        );

        let full_page = schema
            .execute(
                GraphQLRequest::new(full_comments_query.clone())
                    .data(RequestUser::new(owner_id.clone())),
            )
            .await;
        assert!(
            full_page.errors.is_empty(),
            "full comments query failed: {:?}",
            full_page.errors
        );
        let full_json = full_page.data.into_json().expect("valid json");
        let full_cursor = full_json["workspace"]["comments"]["pageInfo"]["startCursor"]
            .as_str()
            .expect("start cursor");

        let comment_changes_query = format!(
            r#"
	query {{
		workspace(id: "{workspace_id}") {{
			commentChanges(docId: "{doc_id}", pagination: {{ first: 10, after: "{after_cursor}" }}) {{
				totalCount
				edges {{
					node {{
						action
						commentId
						item
					}}
				}}
				pageInfo {{
					hasNextPage
					endCursor
				}}
			}}
		}}
	}}
	"#,
            after_cursor = full_cursor
        );

        let changes = schema
            .execute(
                GraphQLRequest::new(comment_changes_query.clone())
                    .data(RequestUser::new(owner_id.clone())),
            )
            .await;
        assert!(
            changes.errors.is_empty(),
            "commentChanges query failed: {:?}",
            changes.errors
        );
        let changes_json = changes.data.into_json().expect("valid json");
        let comment_changes = &changes_json["workspace"]["commentChanges"];
        let change_edges = comment_changes["edges"].as_array().expect("edges array");
        assert!(
            change_edges.is_empty(),
            "initial changefeed should be empty when using start cursor"
        );
        let followup_cursor = comment_changes["pageInfo"]["endCursor"]
            .as_str()
            .expect("end cursor")
            .to_string();

        let followup_reply = format!(
            r#"
	mutation {{
		createReply(input: {{
			commentId: "{comment_id}",
			content: {{ message: "Follow up" }}
		}}) {{
			id
		}}
	}}
	"#,
            comment_id = comment_ids[0]
        );
        let followup_response = schema
            .execute(GraphQLRequest::new(followup_reply).data(RequestUser::new(owner_id.clone())))
            .await;
        assert!(
            followup_response.errors.is_empty(),
            "followup reply failed: {:?}",
            followup_response.errors
        );

        let followup_changes_query = format!(
            r#"
	query {{
		workspace(id: "{workspace_id}") {{
			commentChanges(docId: "{doc_id}", pagination: {{ first: 10, after: "{after_cursor}" }}) {{
				totalCount
				edges {{
					node {{
						action
						commentId
						item
					}}
				}}
				pageInfo {{
					hasNextPage
					endCursor
				}}
			}}
		}}
	}}
	"#,
            after_cursor = followup_cursor
        );

        let followup_changes = schema
            .execute(
                GraphQLRequest::new(followup_changes_query)
                    .data(RequestUser::new(owner_id.clone())),
            )
            .await;
        assert!(
            followup_changes.errors.is_empty(),
            "followup commentChanges query failed: {:?}",
            followup_changes.errors
        );
        let followup_json = followup_changes.data.into_json().expect("valid json");
        let followup_changes = &followup_json["workspace"]["commentChanges"];
        let followup_edges = followup_changes["edges"].as_array().expect("edges array");
        assert!(
            !followup_edges.is_empty(),
            "changefeed should include new reply"
        );
        assert_eq!(
            followup_changes["totalCount"].as_i64().unwrap(),
            followup_edges.len() as i64
        );
        let followup_item = &followup_edges[0]["node"]["item"];
        assert_eq!(
            followup_item["user"]["id"]
                .as_str()
                .expect("user id in change"),
            owner_id
        );
        assert!(
            followup_changes["pageInfo"]["endCursor"]
                .as_str()
                .expect("end cursor")
                .len()
                > 0
        );
    }
}
