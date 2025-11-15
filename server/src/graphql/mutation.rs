use std::collections::{HashMap, HashSet};

use async_graphql::{Context, ID, Object, Result as GraphQLResult, Upload};

use crate::{
    AppError, AppState,
    auth::authenticate_with_password,
    comment::notifications::{notify_comment_creation, notify_comment_reply},
    doc::{
        channels::comment_attachment_blob_key,
        metadata as doc_metadata,
        mode::DocPublishMode,
        roles as doc_roles,
        sync::{SnapshotCachePolicy, broadcast_workspace_snapshot},
    },
    graphql::copilot::{CopilotMessageRecord, CopilotSessionRecord},
    state::WorkspaceEmbeddingFileRecord,
    user::avatar::{
        MAX_AVATAR_UPLOAD_BYTES, avatar_descriptor, avatar_url, parse_avatar_key_from_url,
        sniff_mime as sniff_avatar_mime,
    },
    user::helpers::is_valid_email,
    utils::{attachments::sanitize_attachment_filename, db::is_unique_violation},
    workspace::{
        invites::{self as workspace_invites, InviteLinkLookup},
        members::{self as workspace_members, MemberAcceptance},
        service::WorkspaceCreationParams,
    },
};
use barffine_core::{
    blob::{BlobDescriptor, BlobMetadata},
    comment_attachment::CommentAttachmentUpsert,
    user_settings::UserNotificationSettingsUpdate,
};
use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue, json};

#[cfg(feature = "barffine_extra")]
use super::doc_permissions_for_user;
use super::{
    CommentCreateInput, CommentObjectType, CommentResolveInput, CommentUpdateInput,
    CreateSessionInput, CreateUserInput, CreateWorkspaceInput, DocRole, DocType, DuplicateDocInput,
    FeatureTypeEnum, GenerateAccessTokenInput, GrantDocUserRolesInput, ManageUserInput,
    PublicDocMode, RemoveAvatarType, ReplyCreateInput, ReplyObjectType, ReplyUpdateInput,
    RequestUser, RestoreDocHistoryInput, RevealedAccessTokenObject, RevokeDocUserRoleInput,
    SESSION_OWNERSHIP_ERROR, SessionObject, UpdateDocDefaultRoleInput, UpdateDocUserRoleInput,
    UpdateProfileInput, UpdateUserSettingsInput, UpdateWorkspaceInput, UserSettingsType, UserType,
    WorkspaceType, build_comment_record, build_reply_record, build_session_cookie,
    build_user_cookie, clear_session_cookie, clear_user_cookie,
    constants::FREE_STORAGE_QUOTA,
    copilot::{
        AddContextBlobInput, AddContextCategoryInput, AddContextDocInput, AddContextFileInput,
        CopilotContextBlob, CopilotContextCategory, CopilotContextDoc, CopilotContextFile,
        CopilotWorkspaceFileType, CreateChatMessageInput, CreateChatSessionInput,
        DeleteSessionInput, ForkChatSessionInput, RemoveContextBlobInput,
        RemoveContextCategoryInput, RemoveContextDocInput, RemoveContextFileInput,
        UpdateChatSessionInput,
    },
    doc_role_to_str, ensure_doc_not_workspace, ensure_doc_owner_role, ensure_document_exists,
    ensure_session_owner, ensure_workspace_and_doc, ensure_workspace_exists,
    fetch_workspace_record, generate_password_hash, map_anyhow, map_app_error, map_password,
    normalize_non_empty_input, normalize_optional_input, read_upload, read_upload_with_limit,
    require_admin_user, require_doc_permission, require_request_user, require_workspace_permission,
    resolve_session_id, timestamp_to_datetime,
    workspace::{
        InviteLinkType, InviteResultType, Permission, WorkspaceInviteLinkExpireTime,
        WorkspaceMemberStatus, membership_invite_id, parse_membership_invite_id,
        permission_to_workspace_role, workspace_invite_link_expiry_seconds,
        workspace_member_status_from_str, workspace_member_status_to_str,
    },
    workspace_permission_for_user,
};
#[cfg(feature = "barffine_extra")]
use super::{GenerateRpcAccessTokenInput, RpcAccessTokenObject};
use uuid::Uuid;

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn create_user(
        &self,
        ctx: &Context<'_>,
        input: CreateUserInput,
    ) -> GraphQLResult<UserType> {
        let state = ctx.data::<AppState>()?;
        let password_hash = generate_password_hash(&input.password).map_err(map_password)?;

        let user = state
            .user_store
            .create(&input.email, &password_hash, input.name.as_deref())
            .await
            .map_err(map_anyhow)?;

        Ok(UserType::from(user))
    }

    async fn create_session(
        &self,
        ctx: &Context<'_>,
        input: CreateSessionInput,
    ) -> GraphQLResult<SessionObject> {
        let state = ctx.data::<AppState>()?;
        let (user, session) =
            authenticate_with_password(state, input.email.trim(), &input.password)
                .await
                .map_err(map_app_error)?;

        let session_cookie = build_session_cookie(&session.id, session.expires_at);
        let user_cookie = build_user_cookie(&user.id, session.expires_at);

        ctx.insert_http_header("Set-Cookie", session_cookie.to_string());
        ctx.append_http_header("Set-Cookie", user_cookie.to_string());

        Ok(SessionObject::from(session))
    }

    #[graphql(name = "updateProfile")]
    async fn update_profile(
        &self,
        ctx: &Context<'_>,
        input: UpdateProfileInput,
    ) -> GraphQLResult<UserType> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let UpdateProfileInput {
            name,
            avatar_url,
            remove_avatar,
        } = input;

        let name_update: Option<Option<String>> = name.map(|value| {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        let avatar_update: Option<Option<String>> = if remove_avatar.unwrap_or(false) {
            Some(None)
        } else {
            avatar_url.map(Some)
        };

        let user = state
            .user_store
            .update_profile(&request_user.user_id, name_update, avatar_update)
            .await
            .map_err(map_anyhow)?;

        state
            .user_service
            .invalidate_user_cache(&request_user.user_id);

        Ok(UserType::from(user))
    }

    #[graphql(name = "uploadAvatar")]
    async fn upload_avatar(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "avatar")] upload: Upload,
    ) -> GraphQLResult<UserType> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let current_user = state
            .user_store
            .find_by_id(&request_user.user_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::unauthorized("authentication required")))?;

        let upload_data = read_upload_with_limit(ctx, upload, MAX_AVATAR_UPLOAD_BYTES)?;
        if upload_data.bytes.is_empty() {
            return Err(map_app_error(
                AppError::bad_request("avatar file cannot be empty")
                    .with_name("EMPTY_AVATAR_UPLOAD"),
            ));
        }

        let detected = sniff_avatar_mime(&upload_data.bytes, upload_data.content_type.as_deref());
        let Some(content_type) = detected else {
            return Err(map_app_error(
                AppError::bad_request("invalid avatar file type").with_name("INVALID_AVATAR_TYPE"),
            ));
        };

        if !content_type.starts_with("image/") {
            return Err(map_app_error(
                AppError::bad_request("avatar must be an image").with_name("INVALID_AVATAR_TYPE"),
            ));
        }

        let key = format!("{}-avatar-{}", request_user.user_id, Uuid::new_v4());
        let descriptor = avatar_descriptor(&key);

        let mut metadata = BlobMetadata::default();
        metadata.content_type = Some(content_type);
        metadata.content_length = Some(upload_data.bytes.len() as u64);
        metadata.last_modified = Some(Utc::now());

        state
            .blob_store
            .put(&descriptor, &upload_data.bytes, metadata)
            .await
            .map_err(map_anyhow)?;

        if let Some(old_url) = current_user.avatar_url.as_deref() {
            if let Some(old_key) = parse_avatar_key_from_url(&state.base_url, old_url) {
                let old_descriptor = avatar_descriptor(&old_key);
                state
                    .blob_store
                    .delete(&old_descriptor, true)
                    .await
                    .map_err(map_anyhow)?;
            }
        }

        let avatar_url_value = avatar_url(&state.base_url, &key);
        let updated = match state
            .user_store
            .update_profile(&request_user.user_id, None, Some(Some(avatar_url_value)))
            .await
        {
            Ok(user) => user,
            Err(err) => {
                let _ = state.blob_store.delete(&descriptor, true).await;
                return Err(map_anyhow(err));
            }
        };

        Ok(UserType::from(updated))
    }

    #[graphql(name = "removeAvatar")]
    async fn remove_avatar(&self, ctx: &Context<'_>) -> GraphQLResult<RemoveAvatarType> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let current_user = state
            .user_store
            .find_by_id(&request_user.user_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::unauthorized("authentication required")))?;

        if let Some(old_url) = current_user.avatar_url.as_deref() {
            if let Some(old_key) = parse_avatar_key_from_url(&state.base_url, old_url) {
                let descriptor = avatar_descriptor(&old_key);
                state
                    .blob_store
                    .delete(&descriptor, true)
                    .await
                    .map_err(map_anyhow)?;
            }
        }

        state
            .user_store
            .update_profile(&request_user.user_id, None, Some(None))
            .await
            .map_err(map_anyhow)?;

        state
            .user_service
            .invalidate_user_cache(&request_user.user_id);

        Ok(RemoveAvatarType { success: true })
    }

    #[graphql(name = "updateUser")]
    async fn update_user(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "id")] id: ID,
        #[graphql(name = "input")] input: ManageUserInput,
    ) -> GraphQLResult<UserType> {
        require_admin_user(ctx).await?;
        let state = ctx.data::<AppState>()?;
        let user_id = id.to_string();

        let ManageUserInput { email, name } = input;

        let normalized_email = email
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty());

        if let Some(ref email_value) = normalized_email {
            if !is_valid_email(email_value) {
                return Err(map_app_error(
                    AppError::bad_request("email is invalid").with_name("INVALID_EMAIL"),
                ));
            }
        }

        let normalized_name: Option<Option<String>> = match name {
            None => None,
            Some(maybe_value) => Some(maybe_value.and_then(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })),
        };

        if normalized_email.is_none() && normalized_name.is_none() {
            let user = state
                .user_service
                .fetch_user_with(&user_id, || {
                    AppError::not_found("user not found").with_name("USER_NOT_FOUND")
                })
                .await
                .map_err(map_app_error)?;
            return Ok(UserType::from(user));
        }

        let result = state
            .user_store
            .update_account(
                &user_id,
                normalized_email.as_deref(),
                normalized_name
                    .as_ref()
                    .map(|value| value.as_ref().map(|inner| inner.as_str())),
            )
            .await;

        match result {
            Ok(user) => {
                state.user_service.invalidate_user_cache(&user_id);
                Ok(UserType::from(user))
            }
            Err(err) => {
                if err.to_string().contains("user not found") {
                    return Err(map_app_error(
                        AppError::not_found("user not found").with_name("USER_NOT_FOUND"),
                    ));
                }

                if is_unique_violation(&err) {
                    return Err(map_app_error(
                        AppError::conflict("email already used").with_name("EMAIL_ALREADY_USED"),
                    ));
                }

                Err(map_app_error(AppError::from_anyhow(err)))
            }
        }
    }

    #[graphql(name = "updateUserSettings")]
    async fn update_user_settings(
        &self,
        ctx: &Context<'_>,
        input: UpdateUserSettingsInput,
    ) -> GraphQLResult<UserSettingsType> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let update = UserNotificationSettingsUpdate {
            receive_comment_email: input.receive_comment_email,
            receive_invitation_email: input.receive_invitation_email,
            receive_mention_email: input.receive_mention_email,
            notify_doc_activity: input.receive_doc_activity_notification,
            notify_permission_change: input.receive_permission_change_notification,
        };

        let settings = state
            .user_settings
            .update(&request_user.user_id, update)
            .await
            .map_err(map_anyhow)?;

        Ok(UserSettingsType::from(settings))
    }

    #[graphql(name = "updateSettings")]
    async fn update_settings(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "input")] input: UpdateUserSettingsInput,
    ) -> GraphQLResult<bool> {
        let _ = Self::update_user_settings(self, ctx, input.clone()).await?;
        Ok(true)
    }

    #[graphql(name = "readNotification")]
    async fn read_notification(&self, ctx: &Context<'_>, id: ID) -> GraphQLResult<bool> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let updated = state
            .notification_center
            .mark_read(id.as_str(), &request_user.user_id)
            .await
            .map_err(map_anyhow)?;

        if updated {
            Ok(true)
        } else {
            Err(map_app_error(AppError::not_found("notification not found")))
        }
    }

    #[graphql(name = "readAllNotifications")]
    async fn read_all_notifications(&self, ctx: &Context<'_>) -> GraphQLResult<bool> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        state
            .notification_center
            .mark_all_read(&request_user.user_id)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "deleteSession")]
    async fn delete_session(
        &self,
        ctx: &Context<'_>,
        session_id: Option<ID>,
    ) -> GraphQLResult<bool> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let resolved_id = resolve_session_id(session_id, &request_user)?;

        if let Some(session) = state
            .user_store
            .find_session(&resolved_id)
            .await
            .map_err(map_anyhow)?
        {
            ensure_session_owner(&session.user_id, &request_user.user_id, || {
                AppError::unauthorized(SESSION_OWNERSHIP_ERROR)
            })?;
        }

        state
            .user_store
            .delete_session(&resolved_id)
            .await
            .map_err(map_anyhow)?;

        ctx.insert_http_header("Set-Cookie", clear_session_cookie());
        ctx.append_http_header("Set-Cookie", clear_user_cookie());

        Ok(true)
    }

    #[graphql(name = "refreshSession")]
    async fn refresh_session(
        &self,
        ctx: &Context<'_>,
        session_id: Option<ID>,
    ) -> GraphQLResult<SessionObject> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let resolved_id = resolve_session_id(session_id, &request_user)?;

        let Some(session) = state
            .user_store
            .refresh_session(&resolved_id)
            .await
            .map_err(map_anyhow)?
        else {
            ctx.insert_http_header("Set-Cookie", clear_session_cookie());
            ctx.append_http_header("Set-Cookie", clear_user_cookie());
            return Err(map_app_error(AppError::unauthorized("session expired")));
        };

        ensure_session_owner(&session.user_id, &request_user.user_id, || {
            AppError::unauthorized(SESSION_OWNERSHIP_ERROR)
        })
        .map_err(|err| {
            ctx.insert_http_header("Set-Cookie", clear_session_cookie());
            ctx.append_http_header("Set-Cookie", clear_user_cookie());
            err
        })?;

        ctx.insert_http_header(
            "Set-Cookie",
            build_session_cookie(&session.id, session.expires_at),
        );
        ctx.append_http_header(
            "Set-Cookie",
            build_user_cookie(&session.user_id, session.expires_at),
        );

        Ok(SessionObject::from(session))
    }

    #[graphql(name = "changePassword")]
    async fn change_password(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "newPassword")] new_password: String,
        token: String,
        #[graphql(name = "userId")] user_id: Option<String>,
    ) -> GraphQLResult<bool> {
        let state = ctx.data::<AppState>()?;

        let user_id = user_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                map_app_error(
                    AppError::bad_request("password reset link has expired")
                        .with_name("LINK_EXPIRED"),
                )
            })?;

        let token = token.trim();
        if token.is_empty() {
            return Err(map_app_error(
                AppError::bad_request("invalid password reset token")
                    .with_name("INVALID_EMAIL_TOKEN"),
            ));
        }

        if new_password.trim().len() < 8 {
            return Err(map_app_error(
                AppError::bad_request("password must be at least 8 characters")
                    .with_name("INVALID_PASSWORD_LENGTH"),
            ));
        }

        let session = state
            .user_store
            .find_session(token)
            .await
            .map_err(map_anyhow)?;

        let Some(session) = session else {
            return Err(map_app_error(
                AppError::bad_request("invalid password reset token")
                    .with_name("INVALID_EMAIL_TOKEN"),
            ));
        };

        if session.user_id != user_id {
            return Err(map_app_error(
                AppError::bad_request("invalid password reset token")
                    .with_name("INVALID_EMAIL_TOKEN"),
            ));
        }

        if let Some(request_user) = ctx.data_opt::<RequestUser>() {
            if request_user.user_id != user_id {
                return Err(map_app_error(AppError::forbidden(
                    "authentication required for password change",
                )));
            }
        }

        let password_hash = generate_password_hash(&new_password).map_err(map_password)?;

        state
            .user_store
            .update_password(&user_id, &password_hash)
            .await
            .map_err(map_anyhow)?;

        state
            .user_store
            .delete_sessions_by_user(&user_id)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "createCopilotSession")]
    async fn create_copilot_session(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: CreateChatSessionInput,
    ) -> GraphQLResult<String> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let CreateChatSessionInput {
            workspace_id,
            doc_id,
            prompt_name,
            pinned: _,
            reuse_latest_chat: _,
        } = input;

        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;

        let normalized_doc_id = normalize_optional_input("docId", doc_id)?;
        ensure_workspace_and_doc(state, &workspace_id, normalized_doc_id.as_deref()).await?;

        let prompt_name = normalize_non_empty_input("promptName", prompt_name)?;

        let session_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let record = CopilotSessionRecord {
            id: session_id.clone(),
            workspace_id,
            doc_id: normalized_doc_id,
            user_id: request_user.user_id.clone(),
            prompt_name,
            pinned: input.pinned.unwrap_or(false),
            parent_session_id: None,
            created_at: now,
            updated_at: now,
            model: "disabled".to_string(),
            messages: Vec::new(),
        };

        state.copilot_sessions.insert(session_id.clone(), record);

        Ok(session_id)
    }

    #[graphql(name = "updateCopilotSession")]
    async fn update_copilot_session(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: UpdateChatSessionInput,
    ) -> GraphQLResult<String> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let UpdateChatSessionInput {
            session_id,
            doc_id,
            pinned: _,
            prompt_name,
        } = input;

        let session_id = normalize_non_empty_input("sessionId", session_id)?;
        let normalized_doc = normalize_optional_input("docId", doc_id)?;
        let normalized_prompt = normalize_optional_input("promptName", prompt_name)?;

        let existing = if let Some(entry) = state.copilot_sessions.get(&session_id) {
            entry.value().clone()
        } else {
            return Err(map_app_error(AppError::not_found("session not found")));
        };

        ensure_session_owner(&existing.user_id, &request_user.user_id, || {
            AppError::forbidden(SESSION_OWNERSHIP_ERROR)
        })?;

        if let Some(doc_value) = normalized_doc.as_deref() {
            ensure_doc_not_workspace(&existing.workspace_id, doc_value)?;
            ensure_document_exists(state, &existing.workspace_id, doc_value).await?;
        }

        let mut entry = state
            .copilot_sessions
            .get_mut(&session_id)
            .ok_or_else(|| map_app_error(AppError::not_found("session not found")))?;
        let record = entry.value_mut();

        if let Some(doc_value) = normalized_doc {
            record.doc_id = Some(doc_value);
        }

        if let Some(pin) = input.pinned {
            record.pinned = pin;
        }

        if let Some(name) = normalized_prompt {
            record.prompt_name = name;
        }

        record.updated_at = Utc::now();

        Ok(session_id)
    }

    #[graphql(name = "forkCopilotSession")]
    async fn fork_copilot_session(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: ForkChatSessionInput,
    ) -> GraphQLResult<String> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let ForkChatSessionInput {
            workspace_id,
            doc_id,
            session_id,
            latest_message_id: _,
        } = input;

        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;
        let doc_id = normalize_non_empty_input("docId", doc_id)?;

        let session_id = normalize_non_empty_input("sessionId", session_id)?;

        ensure_workspace_and_doc(state, &workspace_id, Some(doc_id.as_str())).await?;

        let parent_entry = state
            .copilot_sessions
            .get(&session_id)
            .ok_or_else(|| map_app_error(AppError::not_found("session not found")))?;
        let parent = parent_entry.value().clone();

        ensure_session_owner(&parent.user_id, &request_user.user_id, || {
            AppError::forbidden(SESSION_OWNERSHIP_ERROR)
        })?;

        if parent.workspace_id != workspace_id {
            return Err(map_app_error(AppError::not_found("session not found")));
        };

        let new_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let record = CopilotSessionRecord {
            id: new_id.clone(),
            workspace_id: workspace_id.clone(),
            doc_id: Some(doc_id),
            user_id: request_user.user_id.clone(),
            prompt_name: parent.prompt_name.clone(),
            pinned: false,
            parent_session_id: Some(parent.id.clone()),
            created_at: now,
            updated_at: now,
            model: parent.model.clone(),
            messages: Vec::new(),
        };
        state.copilot_sessions.insert(new_id.clone(), record);

        Ok(new_id)
    }

    #[graphql(name = "cleanupCopilotSession")]
    async fn cleanup_copilot_session(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: DeleteSessionInput,
    ) -> GraphQLResult<Vec<String>> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let DeleteSessionInput {
            workspace_id,
            doc_id,
            session_ids,
        } = input;

        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;
        let doc_id = normalize_optional_input("docId", doc_id)?;

        ensure_workspace_and_doc(state, &workspace_id, doc_id.as_deref()).await?;

        let normalized: Vec<String> = session_ids
            .into_iter()
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty())
            .collect();

        if normalized.is_empty() {
            return Err(map_app_error(AppError::bad_request(
                "sessionIds must not be empty",
            )));
        }

        let mut removed = Vec::new();
        let doc_filter = doc_id.as_deref();

        for session_id in normalized {
            let remove = if let Some(entry) = state.copilot_sessions.get(&session_id) {
                let record = entry.value();
                let doc_matches = match doc_filter {
                    Some(doc_filter) => record.doc_id.as_deref() == Some(doc_filter),
                    None => true,
                };
                record.user_id == request_user.user_id
                    && record.workspace_id == workspace_id
                    && doc_matches
            } else {
                false
            };

            if remove {
                state.copilot_sessions.remove(&session_id);
                removed.push(session_id);
            }
        }

        Ok(removed)
    }

    #[graphql(name = "createCopilotMessage")]
    async fn create_copilot_message(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: CreateChatMessageInput,
    ) -> GraphQLResult<String> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let CreateChatMessageInput {
            session_id,
            content,
            attachments,
            blob,
            blobs,
            params: _,
        } = input;

        if blob.is_some() || blobs.is_some() {
            return Err(map_app_error(AppError::bad_request(
                "blob uploads are not supported",
            )));
        }

        let session_id = normalize_non_empty_input("sessionId", session_id)?;

        let message_content = content
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());

        let attachment_ids: Vec<String> = attachments
            .unwrap_or_default()
            .into_iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();

        if message_content.is_none() && attachment_ids.is_empty() {
            return Err(map_app_error(AppError::bad_request(
                "message payload must include content or attachments",
            )));
        }

        let mut entry = state
            .copilot_sessions
            .get_mut(&session_id)
            .ok_or_else(|| map_app_error(AppError::not_found("session not found")))?;
        let record = entry.value_mut();

        ensure_session_owner(&record.user_id, &request_user.user_id, || {
            AppError::forbidden(SESSION_OWNERSHIP_ERROR)
        })?;

        let now = Utc::now();
        record.updated_at = now;
        let message_id = Uuid::new_v4().to_string();
        let message = CopilotMessageRecord {
            id: message_id.clone(),
            session_id: record.id.clone(),
            role: "user".to_string(),
            content: message_content,
            attachments: attachment_ids,
            created_at: now,
        };
        record.messages.push(message);

        Ok(message_id)
    }

    #[graphql(name = "createCopilotContext")]
    async fn create_copilot_context(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "sessionId")] session_id: String,
    ) -> GraphQLResult<String> {
        let _request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;
        let _session_id = normalize_non_empty_input("sessionId", session_id)?;

        ensure_workspace_exists(state, &workspace_id).await?;

        Ok(Uuid::new_v4().to_string())
    }

    #[graphql(name = "queueWorkspaceEmbedding")]
    async fn queue_workspace_embedding(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "docId")] doc_ids: Vec<String>,
    ) -> GraphQLResult<bool> {
        let state = ctx.data::<AppState>()?;

        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;

        require_workspace_permission(ctx, &workspace_id, Permission::Admin).await?;
        ensure_workspace_exists(state, &workspace_id).await?;

        let mut has_valid_doc = false;
        for doc_id in &doc_ids {
            let trimmed = doc_id.trim();
            if trimmed.is_empty() {
                continue;
            }

            if trimmed == workspace_id {
                continue;
            }

            ensure_document_exists(state, &workspace_id, trimmed).await?;
            has_valid_doc = true;
        }

        Ok(has_valid_doc)
    }

    #[graphql(name = "setBlob")]
    async fn set_blob(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "blob")] upload: Upload,
    ) -> GraphQLResult<String> {
        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;

        let _request_user =
            require_workspace_permission(ctx, &workspace_id, Permission::Collaborator).await?;
        let state = ctx.data::<AppState>()?;

        let upload_data = read_upload(ctx, upload)?;
        let filename = upload_data.filename;
        let content_type = upload_data.content_type;
        let bytes = upload_data.bytes;

        if filename.trim().is_empty() {
            return Err(map_app_error(AppError::bad_request(
                "blob filename must not be empty",
            )));
        }
        let key = filename;

        let mut metadata = BlobMetadata::default();
        metadata.content_type = content_type.filter(|value| !value.is_empty());
        metadata.content_length = Some(bytes.len() as u64);
        metadata.last_modified = Some(Utc::now());

        let descriptor = BlobDescriptor::new(&workspace_id, &key);

        let blob_bytes = bytes.len() as i64;
        let current_size = state
            .blob_store
            .total_size(&workspace_id)
            .await
            .map_err(map_anyhow)?;
        let projected_size = current_size + blob_bytes;
        let quota_limit = FREE_STORAGE_QUOTA;
        if projected_size > quota_limit {
            return Err(map_app_error(AppError::forbidden(
                "workspace storage quota exceeded",
            )));
        }

        state
            .blob_store
            .put(&descriptor, &bytes, metadata)
            .await
            .map_err(map_anyhow)?;

        Ok(key)
    }

    #[graphql(name = "deleteBlob")]
    async fn delete_blob(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        hash: Option<String>,
        key: Option<String>,
        #[graphql(name = "permanently")] _permanently: bool,
    ) -> GraphQLResult<bool> {
        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;

        let _request_user =
            require_workspace_permission(ctx, &workspace_id, Permission::Collaborator).await?;
        let state = ctx.data::<AppState>()?;

        let resolved_key = key
            .or(hash)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        let Some(resolved_key) = resolved_key else {
            return Ok(false);
        };

        let descriptor = BlobDescriptor::new(&workspace_id, &resolved_key);
        state
            .blob_store
            .delete(&descriptor, _permanently)
            .await
            .map_err(map_anyhow)?;

        if let Some((doc_id, attachment_key)) = parse_comment_attachment_blob_key(&resolved_key) {
            state
                .comment_attachment_store
                .delete(&workspace_id, &doc_id, &attachment_key)
                .await
                .map_err(map_anyhow)?;
        }

        Ok(true)
    }

    #[graphql(name = "releaseDeletedBlobs")]
    async fn release_deleted_blobs(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
    ) -> GraphQLResult<bool> {
        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;

        require_workspace_permission(ctx, &workspace_id, Permission::Collaborator).await?;
        let state = ctx.data::<AppState>()?;

        state
            .blob_store
            .release_deleted(&workspace_id)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "addWorkspaceEmbeddingFiles")]
    async fn add_workspace_embedding_files(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "blob")] upload: Upload,
    ) -> GraphQLResult<CopilotWorkspaceFileType> {
        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;

        require_workspace_permission(ctx, &workspace_id, Permission::Admin).await?;
        let state = ctx.data::<AppState>()?;
        ensure_workspace_exists(state, &workspace_id).await?;

        let upload_value = upload.value(ctx)?;
        let file_id = Uuid::new_v4().to_string();
        let blob_id = Uuid::new_v4().to_string();
        let raw_name = upload_value.filename.clone();
        let file_name = if raw_name.trim().is_empty() {
            format!("workspace-file-{file_id}")
        } else {
            raw_name
        };
        let _ = upload_value.content;

        let record = WorkspaceEmbeddingFileRecord {
            file_name: file_name.clone(),
            blob_id: blob_id.clone(),
        };

        let mut files = state
            .workspace_embedding_files
            .entry(workspace_id.clone())
            .or_insert_with(HashMap::new);
        files.insert(file_id.clone(), record);
        drop(files);

        Ok(CopilotWorkspaceFileType::new(
            &workspace_id,
            &file_id,
            &file_name,
            &blob_id,
        ))
    }

    #[graphql(name = "removeWorkspaceEmbeddingFiles")]
    async fn remove_workspace_embedding_files(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "fileId")] file_id: String,
    ) -> GraphQLResult<bool> {
        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;
        let file_id = normalize_non_empty_input("fileId", file_id)?;

        require_workspace_permission(ctx, &workspace_id, Permission::Admin).await?;
        let state = ctx.data::<AppState>()?;
        ensure_workspace_exists(state, &workspace_id).await?;

        let mut removed = false;
        if let Some(mut files) = state.workspace_embedding_files.get_mut(&workspace_id) {
            removed = files.remove(&file_id).is_some();
            let empty = files.is_empty();
            drop(files);
            if empty {
                state.workspace_embedding_files.remove(&workspace_id);
            }
        }

        Ok(removed)
    }

    #[graphql(name = "updateWorkspaceEmbeddingIgnoredDocs")]
    async fn update_workspace_embedding_ignored_docs(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        add: Option<Vec<String>>,
        remove: Option<Vec<String>>,
    ) -> GraphQLResult<i32> {
        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;

        require_workspace_permission(ctx, &workspace_id, Permission::Admin).await?;
        let state = ctx.data::<AppState>()?;
        ensure_workspace_exists(state, &workspace_id).await?;

        let mut docs = state
            .workspace_embedding_ignored_docs
            .entry(workspace_id.clone())
            .or_insert_with(HashSet::new);

        if let Some(list) = add {
            for value in list {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    docs.insert(trimmed.to_string());
                }
            }
        }

        if let Some(list) = remove {
            for value in list {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    docs.remove(trimmed);
                }
            }
        }

        let count = docs.len() as i32;
        let empty = docs.is_empty();
        drop(docs);
        if empty {
            state.workspace_embedding_ignored_docs.remove(&workspace_id);
        }

        Ok(count)
    }

    #[graphql(name = "addContextDoc")]
    async fn add_context_doc(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: AddContextDocInput,
    ) -> GraphQLResult<CopilotContextDoc> {
        let _request_user = require_request_user(ctx)?;
        let AddContextDocInput { context_id, doc_id } = input;

        normalize_non_empty_input("contextId", context_id)?;
        normalize_non_empty_input("docId", doc_id)?;

        Ok(CopilotContextDoc::default())
    }

    #[graphql(name = "removeContextDoc")]
    async fn remove_context_doc(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: RemoveContextDocInput,
    ) -> GraphQLResult<bool> {
        let _request_user = require_request_user(ctx)?;
        let RemoveContextDocInput { context_id, doc_id } = input;

        normalize_non_empty_input("contextId", context_id)?;
        normalize_non_empty_input("docId", doc_id)?;

        Ok(true)
    }

    #[graphql(name = "addContextFile")]
    async fn add_context_file(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "content")] _content: Upload,
        #[graphql(name = "options")] input: AddContextFileInput,
    ) -> GraphQLResult<CopilotContextFile> {
        let _request_user = require_request_user(ctx)?;
        let AddContextFileInput {
            context_id,
            blob_id: _,
        } = input;

        normalize_non_empty_input("contextId", context_id)?;

        Ok(CopilotContextFile::default())
    }

    #[graphql(name = "removeContextFile")]
    async fn remove_context_file(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: RemoveContextFileInput,
    ) -> GraphQLResult<bool> {
        let _request_user = require_request_user(ctx)?;
        let RemoveContextFileInput {
            context_id,
            file_id,
        } = input;

        normalize_non_empty_input("contextId", context_id)?;
        normalize_non_empty_input("fileId", file_id)?;

        Ok(true)
    }

    #[graphql(name = "addContextBlob")]
    async fn add_context_blob(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: AddContextBlobInput,
    ) -> GraphQLResult<CopilotContextBlob> {
        let _request_user = require_request_user(ctx)?;
        let AddContextBlobInput {
            context_id,
            blob_id,
        } = input;

        normalize_non_empty_input("contextId", context_id)?;
        normalize_non_empty_input("blobId", blob_id)?;

        Ok(CopilotContextBlob::default())
    }

    #[graphql(name = "removeContextBlob")]
    async fn remove_context_blob(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: RemoveContextBlobInput,
    ) -> GraphQLResult<bool> {
        let _request_user = require_request_user(ctx)?;
        let RemoveContextBlobInput {
            context_id,
            blob_id,
        } = input;

        normalize_non_empty_input("contextId", context_id)?;
        normalize_non_empty_input("blobId", blob_id)?;

        Ok(true)
    }

    #[graphql(name = "addContextCategory")]
    async fn add_context_category(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: AddContextCategoryInput,
    ) -> GraphQLResult<CopilotContextCategory> {
        let _request_user = require_request_user(ctx)?;
        let AddContextCategoryInput {
            context_id,
            context_type: _,
            category_id,
            docs: _,
        } = input;

        normalize_non_empty_input("contextId", context_id)?;
        normalize_non_empty_input("categoryId", category_id)?;

        Ok(CopilotContextCategory::default())
    }

    #[graphql(name = "removeContextCategory")]
    async fn remove_context_category(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "options")] input: RemoveContextCategoryInput,
    ) -> GraphQLResult<bool> {
        let _request_user = require_request_user(ctx)?;
        let RemoveContextCategoryInput {
            context_id,
            context_type: _,
            category_id,
        } = input;

        normalize_non_empty_input("contextId", context_id)?;
        normalize_non_empty_input("categoryId", category_id)?;

        Ok(true)
    }

    #[graphql(name = "generateUserAccessToken")]
    async fn generate_user_access_token(
        &self,
        ctx: &Context<'_>,
        input: GenerateAccessTokenInput,
    ) -> GraphQLResult<RevealedAccessTokenObject> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let expires_at = input.expires_at.map(|dt| dt.timestamp());
        let token = state
            .access_token_store
            .create(&request_user.user_id, &input.name, expires_at)
            .await
            .map_err(map_anyhow)?;

        Ok(RevealedAccessTokenObject::from(token))
    }

    #[graphql(name = "revokeUserAccessToken")]
    async fn revoke_user_access_token(&self, ctx: &Context<'_>, id: ID) -> GraphQLResult<bool> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        state
            .access_token_store
            .revoke(id.as_str(), &request_user.user_id)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[cfg(feature = "barffine_extra")]
    #[graphql(name = "generateRpcAccessToken")]
    async fn generate_rpc_access_token(
        &self,
        ctx: &Context<'_>,
        input: GenerateRpcAccessTokenInput,
    ) -> GraphQLResult<RpcAccessTokenObject> {
        const DOC_READ_REQUIRED: &str = "Doc.Read permission required";

        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let workspace_id = input.workspace_id.to_string();
        let doc_id = input.doc_id.as_ref().map(|value| value.to_string());

        let payload = if let Some(doc_id) = &doc_id {
            let Some(perms) =
                doc_permissions_for_user(state, &workspace_id, doc_id, &request_user.user_id)
                    .await?
            else {
                return Err(map_app_error(AppError::forbidden(DOC_READ_REQUIRED)));
            };

            if !perms.can_read_doc() {
                return Err(map_app_error(AppError::forbidden(DOC_READ_REQUIRED)));
            }

            doc_id.clone()
        } else {
            ensure_workspace_exists(state, &workspace_id).await?;

            let workspace = state
                .workspace_service
                .fetch_workspace(&workspace_id)
                .await
                .map_err(map_app_error)?;

            let has_access = state
                .workspace_service
                .has_workspace_read_access(&workspace, Some(&request_user.user_id))
                .await
                .map_err(map_app_error)?;

            if !has_access {
                return Err(map_app_error(AppError::forbidden(
                    "insufficient workspace permissions",
                )));
            }

            workspace_id.clone()
        };

        let token = state.doc_token_signer.sign(&payload);

        Ok(RpcAccessTokenObject::new(
            workspace_id,
            doc_id.clone().map(ID::from),
            token,
        ))
    }

    #[graphql(name = "setEmailVerified")]
    async fn set_email_verified(
        &self,
        ctx: &Context<'_>,
        verified: bool,
        #[graphql(name = "userId")] user_id: Option<ID>,
    ) -> GraphQLResult<UserType> {
        let request_user = require_request_user(ctx)?;
        let target_id = user_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| request_user.user_id.clone());

        if target_id != request_user.user_id {
            return Err(map_app_error(AppError::unauthorized(
                "not permitted to modify other users",
            )));
        }

        let state = ctx.data::<AppState>()?;
        let user = state
            .user_store
            .set_email_verified(&target_id, verified)
            .await
            .map_err(map_anyhow)?;

        Ok(UserType::from(user))
    }

    async fn create_workspace(
        &self,
        ctx: &Context<'_>,
        input: Option<CreateWorkspaceInput>,
    ) -> GraphQLResult<WorkspaceType> {
        let request_user = require_request_user(ctx)?;
        let CreateWorkspaceInput {
            owner_id,
            name,
            public,
            enable_ai,
            enable_doc_embedding,
            enable_url_preview,
            invite_user_ids,
        } = input.unwrap_or_default();

        let state = ctx.data::<AppState>()?;

        let resolved_owner = match owner_id {
            Some(owner) if owner != request_user.user_id => {
                state
                    .user_service
                    .ensure_admin_user(&request_user.user_id, "administrator privileges required")
                    .await
                    .map_err(map_app_error)?;
                owner
            }
            Some(owner) => owner,
            None => request_user.user_id.clone(),
        };

        let workspace = state
            .workspace_service
            .create_workspace_with_defaults(WorkspaceCreationParams {
                owner_id: &resolved_owner,
                name: name.as_deref(),
                public,
                enable_ai,
                enable_doc_embedding,
                enable_url_preview,
            })
            .await
            .map_err(map_app_error)?;

        if let Some(invitees) = invite_user_ids {
            for user_id in invitees {
                if user_id == resolved_owner {
                    continue;
                }

                if state
                    .user_store
                    .find_by_id(&user_id)
                    .await
                    .map_err(map_anyhow)?
                    .is_some()
                {
                    state
                        .workspace_store
                        .set_member_role(&workspace.id, &user_id, "member")
                        .await
                        .map_err(map_anyhow)?;
                }
            }
        }

        Ok(WorkspaceType::from(workspace))
    }

    #[graphql(name = "updateWorkspace")]
    async fn update_workspace(
        &self,
        ctx: &Context<'_>,
        input: UpdateWorkspaceInput,
    ) -> GraphQLResult<WorkspaceType> {
        let UpdateWorkspaceInput {
            id,
            name,
            public,
            enable_ai,
            enable_doc_embedding,
            enable_url_preview,
            avatar_key,
            indexed,
            last_check_embeddings,
        } = input;

        let workspace_id = id.as_str();
        require_workspace_permission(ctx, workspace_id, Permission::Admin).await?;

        let state = ctx.data::<AppState>()?;
        let avatar_key_update = avatar_key.as_ref().map(|value| value.as_deref());
        let last_check_embeddings = last_check_embeddings.map(|timestamp| timestamp.timestamp());
        let updated = state
            .workspace_store
            .update(
                workspace_id,
                name.as_deref(),
                public,
                enable_ai,
                enable_doc_embedding,
                enable_url_preview,
                avatar_key_update,
                indexed,
                last_check_embeddings,
            )
            .await
            .map_err(map_anyhow)?;

        match updated {
            Some(record) => Ok(WorkspaceType::from(record)),
            None => Err(map_app_error(AppError::workspace_not_found(workspace_id))),
        }
    }

    #[graphql(name = "addWorkspaceFeature")]
    async fn add_workspace_feature(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        feature: FeatureTypeEnum,
    ) -> GraphQLResult<bool> {
        require_admin_user(ctx).await?;
        let state = ctx.data::<AppState>()?;

        ensure_workspace_exists(state, &workspace_id).await?;

        let key = feature
            .workspace_flag_key()
            .ok_or_else(|| map_app_error(AppError::bad_request("feature not supported")))?;

        state
            .feature_service
            .enable_workspace_feature(&workspace_id, key)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "removeWorkspaceFeature")]
    async fn remove_workspace_feature(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        feature: FeatureTypeEnum,
    ) -> GraphQLResult<bool> {
        require_admin_user(ctx).await?;
        let state = ctx.data::<AppState>()?;

        ensure_workspace_exists(state, &workspace_id).await?;

        let key = feature
            .workspace_flag_key()
            .ok_or_else(|| map_app_error(AppError::bad_request("feature not supported")))?;

        state
            .feature_service
            .disable_workspace_feature(&workspace_id, key)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "deleteWorkspace")]
    async fn delete_workspace(&self, ctx: &Context<'_>, id: ID) -> GraphQLResult<bool> {
        let workspace_id = id.as_str();
        require_workspace_permission(ctx, workspace_id, Permission::Owner).await?;

        let state = ctx.data::<AppState>()?;
        // Best-effort cleanup of workspace-scoped documents before removing the
        // workspace record itself. This keeps Rocks-backed doc snapshots,
        // history, logs and share links in sync with SQL metadata and also
        // clears doc-level roles.
        let doc_ids = state
            .document_store
            .delete_all_docs_in_workspace(workspace_id)
            .await
            .map_err(map_anyhow)?;
        for doc_id in doc_ids {
            state
                .doc_role_store
                .remove_all_for_doc(workspace_id, &doc_id)
                .await
                .map_err(map_anyhow)?;
        }

        let deleted = state
            .workspace_store
            .delete(workspace_id)
            .await
            .map_err(map_anyhow)?;

        if !deleted {
            return Err(map_app_error(AppError::workspace_not_found(workspace_id)));
        }

        Ok(true)
    }

    #[graphql(name = "grantMember")]
    async fn grant_member(
        &self,
        ctx: &Context<'_>,
        permission: Permission,
        #[graphql(name = "userId")] user_id: ID,
        #[graphql(name = "workspaceId")] workspace_id: ID,
    ) -> GraphQLResult<bool> {
        if matches!(permission, Permission::Owner) {
            return Err(map_app_error(AppError::bad_request(
                "transferring ownership is not supported via grantMember",
            )));
        }

        let workspace_id_str = workspace_id.as_str();
        require_workspace_permission(ctx, workspace_id_str, Permission::Admin).await?;

        let state = ctx.data::<AppState>()?;

        state
            .user_service
            .fetch_user(user_id.as_str())
            .await
            .map_err(map_app_error)?;

        let role_str = permission_to_workspace_role(permission);
        state
            .workspace_store
            .set_member_role(workspace_id_str, user_id.as_str(), role_str)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "inviteMembers")]
    async fn invite_members(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        emails: Vec<String>,
    ) -> GraphQLResult<Vec<InviteResultType>> {
        let request_user =
            require_workspace_permission(ctx, &workspace_id, Permission::Admin).await?;
        let state = ctx.data::<AppState>()?;

        let workspace = fetch_workspace_record(state, &workspace_id).await?;

        if emails.is_empty() {
            return Ok(Vec::new());
        }

        let mut seen = HashSet::new();
        let mut results = Vec::with_capacity(emails.len());

        for raw_email in emails {
            let trimmed = raw_email.trim();
            if trimmed.is_empty() {
                results.push(InviteResultType {
                    email: raw_email,
                    invite_id: None,
                    sent_success: false,
                    error: Some(json!({ "message": "email must not be empty" })),
                });
                continue;
            }

            let normalized = trimmed.to_ascii_lowercase();
            if !seen.insert(normalized.clone()) {
                results.push(InviteResultType {
                    email: trimmed.to_string(),
                    invite_id: None,
                    sent_success: false,
                    error: Some(json!({ "message": "duplicate email" })),
                });
                continue;
            }

            let mut sent_success = true;
            let mut invite_id: Option<String> = None;
            let mut error_payload: Option<JsonValue> = None;

            let user_record = match state
                .user_store
                .find_by_email(&normalized)
                .await
                .map_err(map_anyhow)?
            {
                Some(user) => user,
                None => {
                    let password_seed = Uuid::new_v4().to_string();
                    let password_hash =
                        generate_password_hash(&password_seed).map_err(map_password)?;
                    state
                        .user_store
                        .create(&normalized, &password_hash, None)
                        .await
                        .map_err(map_anyhow)?
                }
            };

            if user_record.id == workspace.owner_id {
                sent_success = false;
                error_payload = Some(json!({ "message": "owner already has access" }));
            } else if user_record.id == request_user.user_id {
                sent_success = false;
                error_payload = Some(json!({ "message": "cannot invite yourself" }));
            } else {
                let existing_member = state
                    .workspace_store
                    .get_member(&workspace_id, &user_record.id)
                    .await
                    .map_err(map_anyhow)?;

                #[derive(PartialEq, Eq)]
                enum MembershipState {
                    Accepted,
                    PendingLike,
                    None,
                }

                let membership_state = existing_member.map_or(MembershipState::None, |member| {
                    let status = workspace_member_status_from_str(&member.status);
                    match status {
                        WorkspaceMemberStatus::Accepted => MembershipState::Accepted,
                        WorkspaceMemberStatus::Pending
                        | WorkspaceMemberStatus::UnderReview
                        | WorkspaceMemberStatus::AllocatingSeat
                        | WorkspaceMemberStatus::NeedMoreSeat
                        | WorkspaceMemberStatus::NeedMoreSeatAndReview => {
                            MembershipState::PendingLike
                        }
                    }
                });

                let mut notify_invite_id: Option<String> = None;
                match membership_state {
                    MembershipState::Accepted => {
                        sent_success = false;
                        error_payload =
                            Some(json!({ "message": "user already has workspace access" }));
                    }
                    MembershipState::PendingLike => {
                        let membership_id = membership_invite_id(&workspace_id, &user_record.id);
                        invite_id = Some(membership_id.clone());
                        notify_invite_id = Some(membership_id);
                    }
                    MembershipState::None => {
                        state
                            .workspace_store
                            .set_member_role_with_status(
                                &workspace_id,
                                &user_record.id,
                                permission_to_workspace_role(Permission::Collaborator),
                                workspace_member_status_to_str(WorkspaceMemberStatus::Pending),
                                Some(request_user.user_id.as_str()),
                                Some("email"),
                                None,
                            )
                            .await
                            .map_err(map_anyhow)?;

                        let membership_id = membership_invite_id(&workspace_id, &user_record.id);
                        invite_id = Some(membership_id.clone());
                        notify_invite_id = Some(membership_id);
                    }
                }

                if let Some(ref invite_token) = notify_invite_id {
                    let payload = json!({
                        "workspaceId": workspace.id,
                        "createdByUserId": request_user.user_id,
                        "inviteId": invite_token,
                    });
                    doc_roles::enqueue_notification_record(
                        state,
                        &user_record.id,
                        &request_user.user_id,
                        "invitation",
                        payload,
                    )
                    .await?;
                }
            }

            results.push(InviteResultType {
                email: trimmed.to_string(),
                invite_id,
                sent_success,
                error: error_payload,
            });
        }

        Ok(results)
    }

    #[graphql(name = "acceptInviteById")]
    async fn accept_invite_by_id(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "inviteId")] invite_id: String,
        #[graphql(name = "sendAcceptMail")] _send_accept_mail: Option<bool>,
        #[graphql(name = "workspaceId")] workspace_id_hint: Option<String>,
    ) -> GraphQLResult<bool> {
        let invite_id = invite_id.trim().to_string();
        if invite_id.is_empty() {
            return Err(map_app_error(
                AppError::bad_request("invalid invitation").with_name("INVALID_INVITATION"),
            ));
        }

        let state = ctx.data::<AppState>()?;
        let now = Utc::now().timestamp();

        if let Some((workspace_id, target_user_id)) = parse_membership_invite_id(&invite_id) {
            if let Some(ref hinted) = workspace_id_hint {
                if !hinted.trim().is_empty() && hinted.trim() != workspace_id {
                    return Err(map_app_error(
                        AppError::bad_request("invalid invitation").with_name("INVALID_INVITATION"),
                    ));
                }
            }

            if let Some(request_user) = ctx.data_opt::<RequestUser>() {
                if request_user.user_id != target_user_id {
                    return Err(map_app_error(
                        AppError::bad_request("invalid invitation").with_name("INVALID_INVITATION"),
                    ));
                }
            }

            ensure_workspace_exists(state, &workspace_id).await?;

            return match workspace_members::accept_member_if_pending(
                state,
                &workspace_id,
                &target_user_id,
            )
            .await
            .map_err(map_app_error)?
            {
                MemberAcceptance::Missing => Err(map_app_error(
                    AppError::bad_request("invalid invitation").with_name("INVALID_INVITATION"),
                )),
                MemberAcceptance::AlreadyAccepted => Err(map_app_error(
                    AppError::bad_request("member already accepted").with_name("ALREADY_IN_SPACE"),
                )),
                MemberAcceptance::Updated => Ok(true),
            };
        }

        let Some(request_user) = ctx.data_opt::<RequestUser>() else {
            return Err(map_app_error(AppError::unauthorized(
                "authentication required",
            )));
        };

        let invitation = match workspace_invites::lookup_invite_link_by_token(state, &invite_id)
            .await
            .map_err(map_app_error)?
        {
            InviteLinkLookup::Active(link) => link,
            InviteLinkLookup::Missing => {
                return Err(map_app_error(
                    AppError::bad_request("invalid invitation").with_name("INVALID_INVITATION"),
                ));
            }
            InviteLinkLookup::Expired => {
                return Err(map_app_error(
                    AppError::bad_request("invitation link expired").with_name("LINK_EXPIRED"),
                ));
            }
        };

        if let Some(ref hinted) = workspace_id_hint {
            if !hinted.trim().is_empty() && hinted.trim() != invitation.workspace_id {
                return Err(map_app_error(
                    AppError::bad_request("invalid invitation").with_name("INVALID_INVITATION"),
                ));
            }
        }

        ensure_workspace_exists(state, &invitation.workspace_id).await?;

        let existing_member = state
            .workspace_store
            .get_member(&invitation.workspace_id, &request_user.user_id)
            .await
            .map_err(map_anyhow)?;

        if let Some(member) = existing_member {
            let status = workspace_member_status_from_str(&member.status);
            if matches!(status, WorkspaceMemberStatus::Accepted) {
                return Err(map_app_error(
                    AppError::bad_request("member already accepted").with_name("ALREADY_IN_SPACE"),
                ));
            }

            state
                .workspace_store
                .set_member_status(
                    &invitation.workspace_id,
                    &request_user.user_id,
                    workspace_member_status_to_str(WorkspaceMemberStatus::Accepted),
                )
                .await
                .map_err(map_anyhow)?;
        } else {
            state
                .workspace_store
                .set_member_role_with_status(
                    &invitation.workspace_id,
                    &request_user.user_id,
                    permission_to_workspace_role(Permission::Collaborator),
                    workspace_member_status_to_str(WorkspaceMemberStatus::Accepted),
                    Some(invitation.inviter_id.as_str()),
                    Some("link"),
                    Some(now),
                )
                .await
                .map_err(map_anyhow)?;
        }

        Ok(true)
    }

    #[graphql(name = "createInviteLink")]
    async fn create_invite_link(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        expire_time: WorkspaceInviteLinkExpireTime,
    ) -> GraphQLResult<InviteLinkType> {
        let request_user =
            require_workspace_permission(ctx, &workspace_id, Permission::Admin).await?;
        let state = ctx.data::<AppState>()?;

        let expires_at = Utc::now().timestamp() + workspace_invite_link_expiry_seconds(expire_time);
        let token = Uuid::new_v4().to_string();

        state
            .workspace_store
            .upsert_invite_link(&workspace_id, &token, expires_at, &request_user.user_id)
            .await
            .map_err(map_anyhow)?;

        Ok(InviteLinkType {
            expire_time: timestamp_to_datetime(expires_at),
            link: format!("/invite/{token}"),
        })
    }

    #[graphql(name = "revokeInviteLink")]
    async fn revoke_invite_link(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
    ) -> GraphQLResult<bool> {
        let _request_user =
            require_workspace_permission(ctx, &workspace_id, Permission::Admin).await?;
        let state = ctx.data::<AppState>()?;

        let removed = state
            .workspace_store
            .delete_invite_link(&workspace_id)
            .await
            .map_err(map_anyhow)?;

        Ok(removed)
    }

    #[graphql(name = "approveMember")]
    async fn approve_member(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "userId")] user_id: ID,
    ) -> GraphQLResult<bool> {
        require_workspace_permission(ctx, &workspace_id, Permission::Admin).await?;
        let state = ctx.data::<AppState>()?;

        match workspace_members::accept_member_if_pending(state, &workspace_id, user_id.as_str())
            .await
            .map_err(map_app_error)?
        {
            MemberAcceptance::Missing => {
                Err(map_app_error(AppError::not_found("member not found")))
            }
            MemberAcceptance::AlreadyAccepted | MemberAcceptance::Updated => Ok(true),
        }
    }

    #[graphql(name = "leaveWorkspace")]
    async fn leave_workspace(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "sendLeaveMail")] _send_leave_mail: Option<bool>,
    ) -> GraphQLResult<bool> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        let workspace = fetch_workspace_record(state, &workspace_id).await?;

        workspace_members::leave_workspace(state, &workspace, &request_user.user_id)
            .await
            .map_err(map_app_error)?;

        Ok(true)
    }

    #[graphql(name = "revokeMember")]
    async fn revoke_member(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "userId")] user_id: ID,
        #[graphql(name = "workspaceId")] workspace_id: ID,
    ) -> GraphQLResult<bool> {
        let workspace_id_str = workspace_id.as_str();
        require_workspace_permission(ctx, workspace_id_str, Permission::Admin).await?;

        let state = ctx.data::<AppState>()?;
        let workspace = fetch_workspace_record(state, workspace_id_str).await?;

        workspace_members::revoke_member(state, &workspace, user_id.as_str())
            .await
            .map_err(map_app_error)?;

        Ok(true)
    }

    #[graphql(name = "grantDocUserRoles")]
    async fn grant_doc_user_roles(
        &self,
        ctx: &Context<'_>,
        input: GrantDocUserRolesInput,
    ) -> GraphQLResult<bool> {
        let GrantDocUserRolesInput {
            workspace_id,
            doc_id,
            user_ids,
            role,
        } = input;

        ensure_doc_not_workspace(&workspace_id, &doc_id)?;

        if user_ids.is_empty() {
            return Err(map_app_error(AppError::bad_request(
                "userIds must not be empty",
            )));
        }

        if matches!(role, DocRole::Owner | DocRole::None) {
            return Err(map_app_error(AppError::bad_request(
                "unsupported doc role assignment",
            )));
        }

        let request_user = require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_manage_doc_users(),
            "Doc.Users.Manage permission required",
        )
        .await?;
        let state = ctx.data::<AppState>()?;
        let workspace = fetch_workspace_record(state, &workspace_id).await?;

        let role_str = doc_role_to_str(role);
        doc_roles::assign_doc_roles(
            state,
            &workspace,
            &doc_id,
            &request_user.user_id,
            &user_ids,
            role_str,
        )
        .await
        .map_err(map_app_error)?;

        ensure_doc_owner_role(state, &workspace_id, &doc_id).await?;
        Ok(true)
    }

    #[graphql(name = "revokeDocUserRoles")]
    async fn revoke_doc_user_roles(
        &self,
        ctx: &Context<'_>,
        input: RevokeDocUserRoleInput,
    ) -> GraphQLResult<bool> {
        let RevokeDocUserRoleInput {
            workspace_id,
            doc_id,
            user_id,
        } = input;

        ensure_doc_not_workspace(&workspace_id, &doc_id)?;

        let request_user = require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_manage_doc_users(),
            "Doc.Users.Manage permission required",
        )
        .await?;
        let state = ctx.data::<AppState>()?;

        let workspace = fetch_workspace_record(state, &workspace_id).await?;
        doc_roles::revoke_doc_role(state, &workspace, &doc_id, &request_user.user_id, &user_id)
            .await
            .map_err(map_app_error)?;

        ensure_doc_owner_role(state, &workspace_id, &doc_id).await?;
        Ok(true)
    }

    #[graphql(name = "updateDocUserRole")]
    async fn update_doc_user_role(
        &self,
        ctx: &Context<'_>,
        input: UpdateDocUserRoleInput,
    ) -> GraphQLResult<bool> {
        let UpdateDocUserRoleInput {
            workspace_id,
            doc_id,
            user_id,
            role,
        } = input;

        ensure_doc_not_workspace(&workspace_id, &doc_id)?;

        if matches!(role, DocRole::None) {
            return Err(map_app_error(AppError::bad_request(
                "unsupported doc role assignment",
            )));
        }

        let state = ctx.data::<AppState>()?;
        let workspace = fetch_workspace_record(state, &workspace_id).await?;

        if matches!(role, DocRole::Owner) {
            let request_user = require_doc_permission(
                ctx,
                &workspace_id,
                &doc_id,
                |perms| perms.can_transfer_doc_owner(),
                "Doc.TransferOwner permission required",
            )
            .await?;

            doc_roles::transfer_doc_owner(
                state,
                &workspace,
                &doc_id,
                &request_user.user_id,
                &user_id,
            )
            .await
            .map_err(map_app_error)?;
        } else {
            let request_user = require_doc_permission(
                ctx,
                &workspace_id,
                &doc_id,
                |perms| perms.can_manage_doc_users(),
                "Doc.Users.Manage permission required",
            )
            .await?;

            doc_roles::update_doc_role(
                state,
                &workspace,
                &doc_id,
                &request_user.user_id,
                &user_id,
                doc_role_to_str(role),
            )
            .await
            .map_err(map_app_error)?;
        }

        ensure_doc_owner_role(state, &workspace_id, &doc_id).await?;
        Ok(true)
    }

    #[graphql(name = "updateDocDefaultRole")]
    async fn update_doc_default_role(
        &self,
        ctx: &Context<'_>,
        input: UpdateDocDefaultRoleInput,
    ) -> GraphQLResult<bool> {
        let UpdateDocDefaultRoleInput {
            workspace_id,
            doc_id,
            role,
        } = input;

        ensure_doc_not_workspace(&workspace_id, &doc_id)?;

        if matches!(role, DocRole::Owner | DocRole::None) {
            return Err(map_app_error(AppError::bad_request(
                "unsupported doc default role",
            )));
        }

        let _request_user = require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_manage_doc_users(),
            "Doc.Users.Manage permission required",
        )
        .await?;
        let state = ctx.data::<AppState>()?;

        let updated = state
            .document_store
            .update_default_role(&workspace_id, &doc_id, doc_role_to_str(role))
            .await
            .map_err(map_anyhow)?;

        if !updated {
            return Err(map_app_error(AppError::doc_not_found(
                &workspace_id,
                &doc_id,
            )));
        }

        Ok(true)
    }

    #[graphql(name = "uploadCommentAttachment")]
    async fn upload_comment_attachment(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "docId")] doc_id: String,
        #[graphql(name = "attachment")] upload: Upload,
    ) -> GraphQLResult<String> {
        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)?;
        let doc_id = normalize_non_empty_input("docId", doc_id)?;

        ensure_doc_not_workspace(&workspace_id, &doc_id)?;

        let request_user = require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_create_comments(),
            "Doc.Comments.Create permission required",
        )
        .await?;

        let state = ctx.data::<AppState>()?;
        ensure_document_exists(state, &workspace_id, &doc_id).await?;

        let upload_data = read_upload(ctx, upload)?;
        let filename = upload_data.filename;
        let content_type = upload_data.content_type;
        let bytes = upload_data.bytes;

        let attachment_key = Uuid::new_v4().to_string();
        let blob_key = comment_attachment_blob_key(&doc_id, &attachment_key);

        let mut metadata = BlobMetadata::default();
        metadata.content_type = content_type.clone().filter(|value| !value.is_empty());
        metadata.content_length = Some(bytes.len() as u64);
        metadata.last_modified = Some(Utc::now());

        let descriptor = BlobDescriptor::new(&workspace_id, &blob_key);
        state
            .blob_store
            .put(&descriptor, &bytes, metadata)
            .await
            .map_err(map_anyhow)?;

        let stored_name = if filename.trim().is_empty() {
            sanitize_attachment_filename("attachment")
        } else {
            sanitize_attachment_filename(&filename)
        };

        let mime_type = content_type
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "application/octet-stream".to_string());

        state
            .comment_attachment_store
            .upsert(CommentAttachmentUpsert {
                workspace_id: &workspace_id,
                doc_id: &doc_id,
                key: &attachment_key,
                name: &stored_name,
                mime: &mime_type,
                size: bytes.len() as i64,
                created_by: Some(&request_user.user_id),
            })
            .await
            .map_err(map_anyhow)?;

        Ok(attachment_key)
    }

    #[graphql(name = "createComment")]
    async fn create_comment(
        &self,
        ctx: &Context<'_>,
        input: CommentCreateInput,
    ) -> GraphQLResult<CommentObjectType> {
        let CommentCreateInput {
            workspace_id,
            doc_id,
            doc_mode,
            doc_title,
            mentions,
            content,
        } = input;

        ensure_doc_not_workspace(&workspace_id, &doc_id)?;

        let request_user = require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_create_comments(),
            "Doc.Comments.Create permission required",
        )
        .await?;

        let state = ctx.data::<AppState>()?;
        ensure_document_exists(state, &workspace_id, &doc_id).await?;

        let mention_ids = mentions.unwrap_or_default();
        let metadata = json!({
            "docTitle": doc_title,
            "docMode": doc_mode,
            "mentions": mention_ids.clone(),
        });

        let comment_record = build_comment_record(
            Uuid::new_v4().to_string(),
            workspace_id.clone(),
            doc_id.clone(),
            request_user.user_id.clone(),
            content,
            metadata,
        );
        let comment = state
            .comment_store
            .create(comment_record)
            .await
            .map_err(map_anyhow)?;

        notify_comment_creation(
            state,
            &request_user.user_id,
            &comment,
            doc_title.as_deref(),
            doc_mode.as_deref(),
            &mention_ids,
        )
        .await?;

        Ok(CommentObjectType::from_record(comment))
    }

    #[graphql(name = "updateComment")]
    async fn update_comment(
        &self,
        ctx: &Context<'_>,
        input: CommentUpdateInput,
    ) -> GraphQLResult<bool> {
        let CommentUpdateInput { id, content } = input;
        let comment_id = id.to_string();

        let state = ctx.data::<AppState>()?;
        let mut comment = state
            .comment_store
            .find(&comment_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::not_found("comment not found")))?;

        require_doc_permission(
            ctx,
            &comment.workspace_id,
            &comment.doc_id,
            |perms| perms.can_update_comments(),
            "Doc.Comments.Update permission required",
        )
        .await?;

        comment.body = serde_json::to_string(&content).unwrap_or_else(|_| "null".to_string());
        comment.updated_at = Utc::now();

        state
            .comment_store
            .update(comment)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "resolveComment")]
    async fn resolve_comment(
        &self,
        ctx: &Context<'_>,
        input: CommentResolveInput,
    ) -> GraphQLResult<bool> {
        let CommentResolveInput { id, resolved } = input;
        let comment_id = id.to_string();

        let state = ctx.data::<AppState>()?;
        let comment = state
            .comment_store
            .find(&comment_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::not_found("comment not found")))?;

        require_doc_permission(
            ctx,
            &comment.workspace_id,
            &comment.doc_id,
            |perms| perms.can_resolve_comments(),
            "Doc.Comments.Resolve permission required",
        )
        .await?;

        state
            .comment_store
            .resolve(&comment_id, resolved)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "deleteComment")]
    async fn delete_comment(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "id")] id: ID,
    ) -> GraphQLResult<bool> {
        let comment_id = id.to_string();
        let state = ctx.data::<AppState>()?;

        let comment = state
            .comment_store
            .find(&comment_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::not_found("comment not found")))?;

        require_doc_permission(
            ctx,
            &comment.workspace_id,
            &comment.doc_id,
            |perms| perms.can_delete_comments(),
            "Doc.Comments.Delete permission required",
        )
        .await?;

        state
            .comment_store
            .delete(&comment_id)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "createReply")]
    async fn create_reply(
        &self,
        ctx: &Context<'_>,
        input: ReplyCreateInput,
    ) -> GraphQLResult<ReplyObjectType> {
        let ReplyCreateInput {
            comment_id,
            content,
            doc_mode,
            doc_title,
            mentions,
        } = input;

        let comment_id = comment_id.to_string();
        let state = ctx.data::<AppState>()?;
        let comment = state
            .comment_store
            .find(&comment_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::not_found("comment not found")))?;

        let request_user = require_doc_permission(
            ctx,
            &comment.workspace_id,
            &comment.doc_id,
            |perms| perms.can_create_comments(),
            "Doc.Comments.Create permission required",
        )
        .await?;

        let mention_ids = mentions.unwrap_or_default();
        let metadata = json!({
            "docTitle": doc_title,
            "docMode": doc_mode,
            "mentions": mention_ids.clone(),
        });

        let reply_record = build_reply_record(
            Uuid::new_v4().to_string(),
            comment.id.clone(),
            request_user.user_id.clone(),
            content,
            metadata,
        );

        let reply = state
            .comment_store
            .create_reply(reply_record)
            .await
            .map_err(map_anyhow)?;

        notify_comment_reply(
            state,
            &request_user.user_id,
            &comment,
            &reply,
            doc_title.as_deref(),
            doc_mode.as_deref(),
            &mention_ids,
        )
        .await?;

        Ok(ReplyObjectType::from_record(reply))
    }

    #[graphql(name = "updateReply")]
    async fn update_reply(
        &self,
        ctx: &Context<'_>,
        input: ReplyUpdateInput,
    ) -> GraphQLResult<bool> {
        let ReplyUpdateInput { id, content } = input;
        let reply_id = id.to_string();

        let state = ctx.data::<AppState>()?;
        let mut reply = state
            .comment_store
            .find_reply(&reply_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::not_found("reply not found")))?;

        let comment = state
            .comment_store
            .find(&reply.comment_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::not_found("comment not found")))?;

        require_doc_permission(
            ctx,
            &comment.workspace_id,
            &comment.doc_id,
            |perms| perms.can_update_comments(),
            "Doc.Comments.Update permission required",
        )
        .await?;

        reply.body = serde_json::to_string(&content).unwrap_or_else(|_| "null".to_string());
        reply.updated_at = Utc::now();

        state
            .comment_store
            .update_reply(reply)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "deleteReply")]
    async fn delete_reply(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "id")] id: ID,
    ) -> GraphQLResult<bool> {
        let reply_id = id.to_string();
        let state = ctx.data::<AppState>()?;
        let reply = state
            .comment_store
            .find_reply(&reply_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::not_found("reply not found")))?;

        let comment = state
            .comment_store
            .find(&reply.comment_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::not_found("comment not found")))?;

        require_doc_permission(
            ctx,
            &comment.workspace_id,
            &comment.doc_id,
            |perms| perms.can_delete_comments(),
            "Doc.Comments.Delete permission required",
        )
        .await?;

        state
            .comment_store
            .delete_reply(&reply_id)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "duplicateDoc")]
    async fn duplicate_doc(
        &self,
        ctx: &Context<'_>,
        input: DuplicateDocInput,
    ) -> GraphQLResult<DocType> {
        let DuplicateDocInput {
            workspace_id,
            doc_id,
            target_workspace_id,
            target_doc_id,
            title,
        } = input;

        ensure_doc_not_workspace(&workspace_id, &doc_id)?;

        let request_user = require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_duplicate_doc(),
            "Doc.Duplicate permission required",
        )
        .await?;

        let state = ctx.data::<AppState>()?;
        let target_workspace_id = target_workspace_id.unwrap_or_else(|| workspace_id.clone());

        let target_workspace = fetch_workspace_record(state, &target_workspace_id).await?;

        let has_create_permission =
            workspace_permission_for_user(state, &target_workspace, &request_user.user_id)
                .await
                .map_err(map_app_error)?
                .map(|resolved| {
                    matches!(
                        resolved.permission,
                        Permission::Collaborator | Permission::Admin | Permission::Owner
                    )
                })
                .unwrap_or(false);

        if !has_create_permission {
            return Err(map_app_error(AppError::forbidden(
                "Workspace.CreateDoc permission required",
            )));
        }

        let target_doc_id = target_doc_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        ensure_doc_not_workspace(&target_workspace_id, &target_doc_id)?;

        let metadata = state
            .document_store
            .duplicate_doc(
                &workspace_id,
                &doc_id,
                &target_workspace_id,
                &target_doc_id,
                &request_user.user_id,
                title.as_deref(),
            )
            .await
            .map_err(map_anyhow)?;

        let metadata = metadata
            .ok_or_else(|| map_app_error(AppError::doc_not_found(&workspace_id, &doc_id)))?;

        ensure_doc_owner_role(state, &target_workspace_id, &target_doc_id).await?;

        Ok(DocType::from_metadata(metadata))
    }

    #[graphql(name = "deleteDoc")]
    async fn delete_doc(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "docId")] doc_id: String,
    ) -> GraphQLResult<bool> {
        ensure_doc_not_workspace(&workspace_id, &doc_id)?;

        require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_delete_doc(),
            "Doc.Delete permission required",
        )
        .await?;

        let state = ctx.data::<AppState>()?;
        ensure_workspace_exists(state, &workspace_id).await?;
        ensure_document_exists(state, &workspace_id, &doc_id).await?;

        let deleted = state
            .document_store
            .delete_doc(&workspace_id, &doc_id)
            .await
            .map_err(map_anyhow)?;

        if !deleted {
            return Err(map_app_error(AppError::doc_not_found(
                &workspace_id,
                &doc_id,
            )));
        }

        state
            .doc_role_store
            .remove_all_for_doc(&workspace_id, &doc_id)
            .await
            .map_err(map_anyhow)?;

        Ok(true)
    }

    #[graphql(name = "trashDoc")]
    async fn trash_doc(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "docId")] doc_id: String,
    ) -> GraphQLResult<DocType> {
        ensure_doc_not_workspace(&workspace_id, &doc_id)?;

        let request_user = require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_trash_doc(),
            "Doc.Trash permission required",
        )
        .await?;

        let state = ctx.data::<AppState>()?;
        let workspace = fetch_workspace_record(state, &workspace_id).await?;

        let existing = doc_metadata::fetch_required(state, &workspace_id, &doc_id)
            .await
            .map_err(map_app_error)?;

        if existing.trashed_at.is_some() {
            return Err(map_app_error(AppError::bad_request(
                "doc is already trashed",
            )));
        }

        let metadata = state
            .document_store
            .trash_doc(&workspace_id, &doc_id, &request_user.user_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::doc_not_found(&workspace_id, &doc_id)))?;

        doc_roles::notify_owner_and_creator(
            state,
            &request_user.user_id,
            &workspace,
            &doc_id,
            existing.creator_id.as_deref(),
            "doc.activity.trashed",
        )
        .await
        .map_err(map_app_error)?;

        Ok(DocType::from_metadata(metadata))
    }

    #[graphql(name = "restoreDoc")]
    async fn restore_doc(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "docId")] doc_id: String,
    ) -> GraphQLResult<DocType> {
        ensure_doc_not_workspace(&workspace_id, &doc_id)?;

        let request_user = require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_restore_doc(),
            "Doc.Restore permission required",
        )
        .await?;

        let state = ctx.data::<AppState>()?;
        ensure_document_exists(state, &workspace_id, &doc_id).await?;

        let workspace = fetch_workspace_record(state, &workspace_id).await?;

        let metadata = state
            .document_store
            .restore_doc(&workspace_id, &doc_id, &request_user.user_id)
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::doc_not_found(&workspace_id, &doc_id)))?;

        doc_roles::notify_owner_and_creator(
            state,
            &request_user.user_id,
            &workspace,
            &doc_id,
            metadata.creator_id.as_deref(),
            "doc.activity.restored",
        )
        .await
        .map_err(map_app_error)?;

        broadcast_workspace_snapshot(
            state,
            &workspace_id,
            &doc_id,
            SnapshotCachePolicy::RefreshCache,
        )
        .await
        .map_err(map_app_error)?;

        Ok(DocType::from_metadata(metadata))
    }

    #[graphql(name = "publishDoc")]
    async fn publish_doc(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "docId")] doc_id: String,
        mode: Option<PublicDocMode>,
    ) -> GraphQLResult<DocType> {
        publish_doc_impl(ctx, workspace_id, doc_id, mode).await
    }

    #[graphql(name = "publishPage", deprecation = "use publishDoc instead")]
    async fn publish_page(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "pageId")] doc_id: String,
        #[graphql(name = "mode")] mode: Option<PublicDocMode>,
    ) -> GraphQLResult<DocType> {
        publish_doc_impl(ctx, workspace_id, doc_id, mode).await
    }

    #[graphql(name = "revokePublicDoc")]
    async fn revoke_public_doc(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "docId")] doc_id: String,
    ) -> GraphQLResult<DocType> {
        revoke_public_doc_impl(ctx, workspace_id, doc_id).await
    }

    #[graphql(name = "revokePublicPage", deprecation = "use revokePublicDoc instead")]
    async fn revoke_public_page(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "pageId")] doc_id: String,
    ) -> GraphQLResult<DocType> {
        revoke_public_doc_impl(ctx, workspace_id, doc_id).await
    }

    #[graphql(name = "restoreDocHistory")]
    async fn restore_doc_history(
        &self,
        ctx: &Context<'_>,
        input: RestoreDocHistoryInput,
    ) -> GraphQLResult<DocType> {
        let RestoreDocHistoryInput {
            workspace_id,
            doc_id,
            history_id,
        } = input;

        let request_user = require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_restore_doc(),
            "Doc.Restore permission required",
        )
        .await?;

        let parsed_history_id = history_id
            .as_str()
            .parse::<i64>()
            .map_err(|_| map_app_error(AppError::bad_request("invalid historyId")))?;

        let state = ctx.data::<AppState>()?;
        ensure_document_exists(state, &workspace_id, &doc_id).await?;

        let metadata = state
            .document_store
            .restore_doc_history(
                &workspace_id,
                &doc_id,
                parsed_history_id,
                &request_user.user_id,
            )
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::doc_not_found(&workspace_id, &doc_id)))?;

        broadcast_workspace_snapshot(
            state,
            &workspace_id,
            &doc_id,
            SnapshotCachePolicy::RefreshCache,
        )
        .await
        .map_err(map_app_error)?;

        Ok(DocType::from_metadata(metadata))
    }

    #[graphql(name = "recoverDoc")]
    async fn recover_doc(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "guid")] doc_id: String,
        #[graphql(name = "timestamp")] timestamp: DateTime<Utc>,
    ) -> GraphQLResult<DateTime<Utc>> {
        ensure_doc_not_workspace(&workspace_id, &doc_id)?;

        let request_user = require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_restore_doc(),
            "Doc.Restore permission required",
        )
        .await?;

        let state = ctx.data::<AppState>()?;
        ensure_document_exists(state, &workspace_id, &doc_id).await?;

        let _restored = state
            .document_store
            .restore_doc_at_timestamp(
                &workspace_id,
                &doc_id,
                timestamp.timestamp(),
                &request_user.user_id,
            )
            .await
            .map_err(map_anyhow)?
            .ok_or_else(|| map_app_error(AppError::doc_not_found(&workspace_id, &doc_id)))?;

        broadcast_workspace_snapshot(
            state,
            &workspace_id,
            &doc_id,
            SnapshotCachePolicy::RefreshCache,
        )
        .await
        .map_err(map_app_error)?;

        Ok(timestamp)
    }
}

fn parse_comment_attachment_blob_key(key: &str) -> Option<(String, String)> {
    let Some(rest) = key.strip_prefix("comment-attachments/") else {
        return None;
    };

    let (doc_id, attachment_key) = rest.split_once('/')?;
    if doc_id.is_empty() || attachment_key.is_empty() {
        return None;
    }

    Some((doc_id.to_string(), attachment_key.to_string()))
}

async fn publish_doc_impl(
    ctx: &Context<'_>,
    workspace_id: String,
    doc_id: String,
    mode: Option<PublicDocMode>,
) -> GraphQLResult<DocType> {
    ensure_doc_not_workspace(&workspace_id, &doc_id)?;

    require_doc_permission(
        ctx,
        workspace_id.as_str(),
        doc_id.as_str(),
        |perms| perms.can_publish_doc(),
        "Doc.Publish permission required",
    )
    .await?;

    let resolved_mode = mode.unwrap_or(PublicDocMode::Page);
    let publish_mode: DocPublishMode = resolved_mode.into();

    let state = ctx.data::<AppState>()?;
    let metadata =
        doc_metadata::publish_doc(state, workspace_id.as_str(), doc_id.as_str(), publish_mode)
            .await
            .map_err(map_app_error)?;

    broadcast_workspace_snapshot(
        state,
        workspace_id.as_str(),
        doc_id.as_str(),
        SnapshotCachePolicy::UseCache,
    )
    .await
    .map_err(map_app_error)?;

    Ok(DocType::from_metadata(metadata))
}

async fn revoke_public_doc_impl(
    ctx: &Context<'_>,
    workspace_id: String,
    doc_id: String,
) -> GraphQLResult<DocType> {
    ensure_doc_not_workspace(&workspace_id, &doc_id)?;

    require_doc_permission(
        ctx,
        workspace_id.as_str(),
        doc_id.as_str(),
        |perms| perms.can_publish_doc(),
        "Doc.Publish permission required",
    )
    .await?;

    let state = ctx.data::<AppState>()?;
    let metadata = doc_metadata::unpublish_doc(state, workspace_id.as_str(), doc_id.as_str())
        .await
        .map_err(map_app_error)?;

    Ok(DocType::from_metadata(metadata))
}

#[cfg(test)]
mod tests {
    use crate::{
        auth::generate_password_hash,
        graphql::{self, RequestUser},
        testing::{insert_document, seed_workspace, setup_state},
    };
    use argon2::{
        Argon2,
        password_hash::{PasswordHash, PasswordVerifier},
    };
    use async_graphql::Request as GraphQLRequest;
    use serde_json::Value as JsonValue;
    use uuid::Uuid;

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

    #[tokio::test]
    async fn graphql_change_password_revokes_sessions() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("old-secret").expect("hash password");
        let user = state
            .user_store
            .create(
                "password@example.com",
                &password_hash,
                Some("Password User"),
            )
            .await
            .expect("create user");

        let session = state
            .user_store
            .create_session(&user.id)
            .await
            .expect("create session");

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            r#"
		mutation {{
			changePassword(token: "{token}", userId: "{user_id}", newPassword: "supersecret")
		}}
		"#,
            token = session.id,
            user_id = user.id
        );

        let response = schema.execute(GraphQLRequest::new(mutation)).await;
        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        assert_eq!(data["changePassword"], true);

        let refreshed = state
            .user_store
            .find_session(&session.id)
            .await
            .expect("query session");
        assert!(refreshed.is_none(), "session should be revoked");

        let updated_user = state
            .user_store
            .find_by_id(&user.id)
            .await
            .expect("query user")
            .expect("user exists");

        let parsed_hash = PasswordHash::new(&updated_user.password_hash).expect("parse hash");
        Argon2::default()
            .verify_password("supersecret".as_bytes(), &parsed_hash)
            .expect("password updated");
    }

    #[tokio::test]
    async fn graphql_update_user_allows_admin_changes() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let admin = state
            .user_store
            .create("admin@example.com", &password_hash, Some("Admin"))
            .await
            .expect("create admin");
        state
            .user_store
            .add_admin(&admin.id)
            .await
            .expect("grant admin");

        let target = state
            .user_store
            .create("target@example.com", &password_hash, Some("Target"))
            .await
            .expect("create user");

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            r#"
		mutation {{
			updateUser(id: "{user_id}", input: {{
				email: "updated@example.com",
				name: "Updated User"
			}}) {{
				id
				email
				name
			}}
		}}
		"#,
            user_id = target.id
        );

        let response = schema
            .execute(GraphQLRequest::new(mutation).data(RequestUser::new(admin.id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        let updated = &data["updateUser"];
        assert_eq!(updated["id"].as_str(), Some(target.id.as_str()));
        assert_eq!(updated["email"].as_str(), Some("updated@example.com"));
        assert_eq!(updated["name"].as_str(), Some("Updated User"));

        let stored = state
            .user_store
            .find_by_id(&target.id)
            .await
            .expect("query user")
            .expect("user exists");
        assert_eq!(stored.email, "updated@example.com");
        assert_eq!(stored.name.as_deref(), Some("Updated User"));
    }

    #[tokio::test]
    async fn graphql_update_user_settings_persists_preferences() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("settings@example.com", &password_hash, None)
            .await
            .expect("create user");

        let schema = graphql::build_schema(state.clone());
        let mutation = r#"
            mutation UpdateSettings {
                updateUserSettings(input: {
                    receiveCommentEmail: false,
                    receiveInvitationEmail: true,
                    receiveMentionEmail: false,
                    receiveDocActivityNotification: false,
                    receivePermissionChangeNotification: true
                }) {
                    receiveCommentEmail
                    receiveInvitationEmail
                    receiveMentionEmail
                    receiveDocActivityNotification
                    receivePermissionChangeNotification
                }
            }
        "#;

        let response = schema
            .execute(GraphQLRequest::new(mutation).data(RequestUser::new(user.id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "mutation errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        let result = &data["updateUserSettings"];
        assert_eq!(result["receiveCommentEmail"], false);
        assert_eq!(result["receiveInvitationEmail"], true);
        assert_eq!(result["receiveMentionEmail"], false);
        assert_eq!(result["receiveDocActivityNotification"], false);
        assert_eq!(result["receivePermissionChangeNotification"], true);

        let query = r#"
            {
                currentUser {
                    settings {
                        receiveCommentEmail
                        receiveInvitationEmail
                        receiveMentionEmail
                        receiveDocActivityNotification
                        receivePermissionChangeNotification
                    }
                }
            }
        "#;

        let query_response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(user.id.clone())))
            .await;

        assert!(
            query_response.errors.is_empty(),
            "query errors: {:?}",
            query_response.errors
        );
        let query_json = query_response.data.into_json().expect("valid json");
        let settings = &query_json["currentUser"]["settings"];
        assert_eq!(settings["receiveCommentEmail"], false);
        assert_eq!(settings["receiveInvitationEmail"], true);
        assert_eq!(settings["receiveMentionEmail"], false);
        assert_eq!(settings["receiveDocActivityNotification"], false);
        assert_eq!(settings["receivePermissionChangeNotification"], true);
    }

    #[tokio::test]
    async fn graphql_comment_mutations_return_placeholders() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let schema = graphql::build_schema(state.clone());
        let create_comment = format!(
            r#"
			mutation {{
				createComment(input: {{
					workspaceId: "{workspace_id}",
					docId: "{doc_id}",
					content: {{ message: "Hello" }}
				}}) {{
					id
					resolved
					user {{ id }}
				}}
			}}
			"#
        );

        let response = schema
            .execute(GraphQLRequest::new(create_comment).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("valid json");
        let comment = &data["createComment"];
        assert_eq!(comment["user"]["id"], owner_id);
        assert_eq!(comment["resolved"], false);

        let create_reply = format!(
            r#"
			mutation {{
				createReply(input: {{
					commentId: "{comment_id}",
					content: {{ message: "Reply" }}
				}}) {{
					commentId
					user {{ id }}
				}}
			}}
			"#,
            comment_id = comment["id"].as_str().expect("comment id")
        );

        let reply_response = schema
            .execute(GraphQLRequest::new(create_reply).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            reply_response.errors.is_empty(),
            "unexpected errors: {:?}",
            reply_response.errors
        );

        let data = reply_response.data.into_json().expect("valid json");
        assert_eq!(data["createReply"]["user"]["id"], owner_id);
        assert_eq!(data["createReply"]["commentId"], comment["id"]);
    }

    #[tokio::test]
    async fn graphql_comment_lifecycle_persists_changes() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let schema = graphql::build_schema(state.clone());
        let create_comment = format!(
            r#"
		mutation {{
			createComment(input: {{
				workspaceId: "{workspace_id}",
				docId: "{doc_id}",
				content: {{ message: "Initial" }}
			}}) {{
				id
			}}
		}}
		"#,
        );

        let comment_response = schema
            .execute(GraphQLRequest::new(create_comment).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(comment_response.errors.is_empty());
        let comment_data = comment_response.data.into_json().expect("valid json");
        let comment_id = comment_data["createComment"]["id"]
            .as_str()
            .expect("comment id")
            .to_string();

        let update_comment = format!(
            r#"
		mutation {{
			updateComment(input: {{
				id: "{comment_id}",
				content: {{ message: "Updated" }}
			}})
		}}
		"#,
            comment_id = comment_id
        );

        schema
            .execute(GraphQLRequest::new(update_comment).data(RequestUser::new(owner_id.clone())))
            .await;

        let resolved = format!(
            r#"
		mutation {{
			resolveComment(input: {{ id: "{comment_id}", resolved: true }})
		}}
		"#,
            comment_id = comment_id
        );

        schema
            .execute(GraphQLRequest::new(resolved).data(RequestUser::new(owner_id.clone())))
            .await;

        let comment_record = state
            .comment_store
            .find(&comment_id)
            .await
            .expect("query comment")
            .expect("comment present");
        assert_eq!(
            serde_json::from_str::<JsonValue>(&comment_record.body).unwrap()["message"],
            "Updated"
        );
        assert!(comment_record.resolved);

        let create_reply = format!(
            r#"
		mutation {{
			createReply(input: {{
				commentId: "{comment_id}",
				content: {{ message: "Reply" }}
			}}) {{ id }}
		}}
		"#,
            comment_id = comment_id
        );

        let reply_response = schema
            .execute(GraphQLRequest::new(create_reply).data(RequestUser::new(owner_id.clone())))
            .await;
        assert!(reply_response.errors.is_empty());
        let reply_data = reply_response.data.into_json().expect("valid json");
        let reply_id = reply_data["createReply"]["id"]
            .as_str()
            .expect("reply id")
            .to_string();

        let update_reply = format!(
            r#"
		mutation {{
			updateReply(input: {{ id: "{reply_id}", content: {{ message: "Reply Updated" }} }})
		}}
		"#,
            reply_id = reply_id
        );

        schema
            .execute(GraphQLRequest::new(update_reply).data(RequestUser::new(owner_id.clone())))
            .await;

        let reply_record = state
            .comment_store
            .find_reply(&reply_id)
            .await
            .expect("query reply")
            .expect("reply exists");
        assert_eq!(
            serde_json::from_str::<JsonValue>(&reply_record.body).unwrap()["message"],
            "Reply Updated"
        );

        let delete_reply = format!(
            r#"
		mutation {{
			deleteReply(id: "{reply_id}")
		}}
		"#,
            reply_id = reply_id
        );

        schema
            .execute(GraphQLRequest::new(delete_reply).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            state
                .comment_store
                .find_reply(&reply_id)
                .await
                .expect("query reply")
                .is_none()
        );

        let delete_comment = format!(
            r#"
		mutation {{
			deleteComment(id: "{comment_id}")
		}}
		"#,
            comment_id = comment_id
        );

        schema
            .execute(GraphQLRequest::new(delete_comment).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            state
                .comment_store
                .find(&comment_id)
                .await
                .expect("query comment")
                .is_none()
        );
    }

    #[tokio::test]
    async fn graphql_comment_permissions_enforced() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        set_default_role(&database, &workspace_id, &doc_id, "reader").await;

        let password_hash = generate_password_hash("secret").expect("hash password");
        let collaborator = state
            .user_store
            .create("collab@example.com", &password_hash, Some("Collaborator"))
            .await
            .expect("create collaborator");
        let outsider = state
            .user_store
            .create("outsider@example.com", &password_hash, Some("Outsider"))
            .await
            .expect("create outsider");

        state
            .workspace_store
            .set_member_role(&workspace_id, &collaborator.id, "member")
            .await
            .expect("grant collaborator role");

        let schema = graphql::build_schema(state.clone());
        let create_comment = format!(
            r#"
	mutation {{
		createComment(input: {{
			workspaceId: "{workspace_id}",
			docId: "{doc_id}",
			content: {{ message: "Enforce" }}
		}}) {{ id }}
	}}
	"#,
        );

        let create_response = schema
            .execute(
                GraphQLRequest::new(create_comment.clone())
                    .data(RequestUser::new(owner_id.clone())),
            )
            .await;
        assert!(
            create_response.errors.is_empty(),
            "owner create should succeed"
        );
        let comment_id = create_response.data.into_json().unwrap()["createComment"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        let denied_response = schema
            .execute(
                GraphQLRequest::new(create_comment).data(RequestUser::new(outsider.id.clone())),
            )
            .await;
        assert!(
            !denied_response.errors.is_empty(),
            "outsider create should fail"
        );

        let denied_query = format!(
            r#"
	{{ workspace(id: "{workspace_id}") {{
		commentChanges(docId: "{doc_id}", pagination: {{ first: 1 }}) {{
			totalCount
		}}
	}} }}
	"#
        );
        let denied_changes = schema
            .execute(GraphQLRequest::new(denied_query).data(RequestUser::new(outsider.id.clone())))
            .await;
        assert!(
            !denied_changes.errors.is_empty(),
            "commentChanges query should require Doc.Comments.Read"
        );

        state
            .doc_role_store
            .upsert(&workspace_id, &doc_id, &collaborator.id, "commenter")
            .await
            .expect("grant commenter role");

        let delete_attempt = format!(
            r#"
	mutation {{
		deleteComment(id: "{comment_id}")
	}}
	"#
        );
        let delete_response = schema
            .execute(
                GraphQLRequest::new(delete_attempt.clone())
                    .data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            !delete_response.errors.is_empty(),
            "deleteComment should be denied"
        );
        assert_eq!(
            delete_response.errors[0].message,
            "Doc.Comments.Delete permission required"
        );

        let update_attempt = format!(
            r#"
	mutation {{
		updateComment(input: {{ id: "{comment_id}", content: {{ message: "Update" }} }})
	}}
	"#
        );
        let update_response = schema
            .execute(
                GraphQLRequest::new(update_attempt.clone())
                    .data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            !update_response.errors.is_empty(),
            "updateComment should be denied"
        );
        assert_eq!(
            update_response.errors[0].message,
            "Doc.Comments.Update permission required"
        );

        let resolve_attempt = format!(
            r#"
	mutation {{
		resolveComment(input: {{ id: "{comment_id}", resolved: true }})
	}}
	"#
        );
        let resolve_response = schema
            .execute(
                GraphQLRequest::new(resolve_attempt.clone())
                    .data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            !resolve_response.errors.is_empty(),
            "resolveComment should be denied"
        );
        assert_eq!(
            resolve_response.errors[0].message,
            "Doc.Comments.Resolve permission required"
        );

        let create_reply = format!(
            r#"
	mutation {{
		createReply(input: {{
			commentId: "{comment_id}",
			content: {{ message: "Reply" }}
		}}) {{ id }}
	}}
	"#
        );
        let reply_response = schema
            .execute(GraphQLRequest::new(create_reply).data(RequestUser::new(owner_id.clone())))
            .await;
        assert!(
            reply_response.errors.is_empty(),
            "owner reply should succeed"
        );
        let reply_json = reply_response.data.into_json().expect("valid json");
        let reply_id = reply_json["createReply"]["id"]
            .as_str()
            .expect("reply id")
            .to_string();

        let update_reply_attempt = format!(
            r#"
	mutation {{
		updateReply(input: {{ id: "{reply_id}", content: {{ message: "Nope" }} }})
	}}
	"#
        );
        let update_reply_response = schema
            .execute(
                GraphQLRequest::new(update_reply_attempt.clone())
                    .data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            !update_reply_response.errors.is_empty(),
            "updateReply should be denied"
        );
        assert_eq!(
            update_reply_response.errors[0].message,
            "Doc.Comments.Update permission required"
        );

        let delete_reply_attempt = format!(
            r#"
	mutation {{
		deleteReply(id: "{reply_id}")
	}}
	"#
        );
        let delete_reply_response = schema
            .execute(
                GraphQLRequest::new(delete_reply_attempt)
                    .data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            !delete_reply_response.errors.is_empty(),
            "deleteReply should be denied"
        );
        assert_eq!(
            delete_reply_response.errors[0].message,
            "Doc.Comments.Delete permission required"
        );
    }

    #[tokio::test]
    async fn graphql_comment_notifications_delivered() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        set_default_role(&database, &workspace_id, &doc_id, "commenter").await;

        let password_hash = generate_password_hash("secret").expect("hash password");
        let collaborator = state
            .user_store
            .create("actor@example.com", &password_hash, Some("Actor"))
            .await
            .expect("create collaborator");
        let mention = state
            .user_store
            .create("mention@example.com", &password_hash, Some("Mention"))
            .await
            .expect("create mention user");

        state
            .workspace_store
            .set_member_role(&workspace_id, &collaborator.id, "collaborator")
            .await
            .expect("grant collaborator role");
        state
            .workspace_store
            .set_member_role(&workspace_id, &mention.id, "collaborator")
            .await
            .expect("grant mention collaborator role");

        let schema = graphql::build_schema(state.clone());

        let create_comment = format!(
            r#"
	mutation {{
		createComment(input: {{
			workspaceId: "{workspace_id}",
			docId: "{doc_id}",
			docTitle: "Doc",
			docMode: "page",
			mentions: ["{mention_id}"],
			content: {{ message: "Hello" }}
		}}) {{ id }}
	}}
	"#,
            mention_id = mention.id,
        );

        let comment_response = schema
            .execute(
                GraphQLRequest::new(create_comment).data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(comment_response.errors.is_empty(), "comment create failed");
        let comment_json = comment_response.data.into_json().expect("valid json");
        let comment_id = comment_json["createComment"]["id"]
            .as_str()
            .expect("comment id")
            .to_string();

        let mention_query = r#"
        {
            currentUser {
                notificationCount
                notifications(pagination: { first: 10 }) {
                    totalCount
                    edges {
                        node {
                            type
                            read
                            body
                        }
                    }
                }
            }
        }
        "#;

        let mention_result = schema
            .execute(GraphQLRequest::new(mention_query).data(RequestUser::new(mention.id.clone())))
            .await;
        assert!(
            mention_result.errors.is_empty(),
            "fetch mention notifications"
        );
        let mention_data = mention_result.data.into_json().expect("valid json");
        let mention_notifs = &mention_data["currentUser"]["notifications"];
        assert_eq!(mention_notifs["totalCount"], 1);
        assert_eq!(mention_data["currentUser"]["notificationCount"], 1);
        let mention_edge = &mention_notifs["edges"][0]["node"];
        assert_eq!(mention_edge["type"], "CommentMention");
        assert_eq!(mention_edge["read"], false);
        assert_eq!(mention_edge["body"]["commentId"], comment_id);
        assert_eq!(mention_edge["body"]["isMention"], true);

        let owner_result = schema
            .execute(
                GraphQLRequest::new(
                    r#"
        {
            currentUser {
                notificationCount
                notifications(pagination: { first: 10 }) {
                    totalCount
                    edges { node { type body } }
                }
            }
        }
        "#,
                )
                .data(RequestUser::new(owner_id.clone())),
            )
            .await;
        assert!(owner_result.errors.is_empty(), "owner notifications");
        let owner_data = owner_result.data.into_json().expect("valid json");
        let owner_notifs = &owner_data["currentUser"]["notifications"];
        assert_eq!(owner_notifs["totalCount"], 1);
        let owner_edge = &owner_notifs["edges"][0]["node"];
        assert_eq!(owner_edge["type"], "Comment");
        assert_eq!(owner_edge["body"]["commentId"], comment_id);
        assert_eq!(owner_edge["body"]["isMention"], false);

        let create_reply = format!(
            r#"
	mutation {{
		createReply(input: {{
			commentId: "{comment_id}",
			docTitle: "Doc",
			docMode: "page",
			content: {{ message: "Reply" }}
		}}) {{ id }}
	}}
	"#
        );

        let reply_response = schema
            .execute(GraphQLRequest::new(create_reply).data(RequestUser::new(owner_id.clone())))
            .await;
        assert!(reply_response.errors.is_empty(), "reply create failed");
        let reply_json = reply_response.data.into_json().expect("valid json");
        let reply_id = reply_json["createReply"]["id"]
            .as_str()
            .expect("reply id")
            .to_string();

        let collaborator_result = schema
            .execute(
                GraphQLRequest::new(mention_query).data(RequestUser::new(collaborator.id.clone())),
            )
            .await;
        assert!(
            collaborator_result.errors.is_empty(),
            "collaborator notifications"
        );
        let collaborator_data = collaborator_result.data.into_json().expect("valid json");
        let collaborator_notifs = &collaborator_data["currentUser"]["notifications"];
        assert_eq!(collaborator_notifs["totalCount"], 1);
        let collaborator_edge = &collaborator_notifs["edges"][0]["node"];
        assert_eq!(collaborator_edge["type"], "Comment");
        assert_eq!(collaborator_edge["body"]["replyId"], reply_id);
        assert_eq!(collaborator_edge["body"]["isMention"], false);
    }

    #[tokio::test]
    async fn graphql_invite_members_enqueues_notification() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        insert_document(&database, &workspace_id, &workspace_id, false, "workspace").await;

        let password_hash = generate_password_hash("secret").expect("hash password");
        let invitee = state
            .user_store
            .create("invitee@example.com", &password_hash, Some("Invitee"))
            .await
            .expect("create invitee user");

        let schema = graphql::build_schema(state.clone());

        let invite_mutation = format!(
            r#"
	mutation {{
		inviteMembers(workspaceId: "{workspace_id}", emails: ["invitee@example.com"]) {{
			email
			inviteId
			sentSuccess
			error
		}}
	}}
	"#,
        );

        let invite_response = schema
            .execute(GraphQLRequest::new(invite_mutation).data(RequestUser::new(owner_id.clone())))
            .await;
        assert!(
            invite_response.errors.is_empty(),
            "invite errors: {:?}",
            invite_response.errors
        );
        let invite_json = invite_response.data.into_json().expect("json");
        let entries = invite_json["inviteMembers"].as_array().expect("array");
        assert_eq!(entries.len(), 1);
        let invite_entry = &entries[0];
        assert!(invite_entry["sentSuccess"].as_bool().unwrap());
        assert!(invite_entry["error"].is_null());
        let invite_token = invite_entry["inviteId"]
            .as_str()
            .expect("inviteId")
            .to_string();

        let notification_query = r#"
    {
        currentUser {
            notificationCount
            notifications(pagination: { first: 5 }) {
                edges { node { type body } }
            }
        }
    }"#;

        let notification_response = schema
            .execute(
                GraphQLRequest::new(notification_query).data(RequestUser::new(invitee.id.clone())),
            )
            .await;
        assert!(
            notification_response.errors.is_empty(),
            "notification query errors: {:?}",
            notification_response.errors
        );
        let notification_json = notification_response.data.into_json().expect("json");
        assert_eq!(
            notification_json["currentUser"]["notificationCount"].as_i64(),
            Some(1)
        );

        let edges = notification_json["currentUser"]["notifications"]["edges"]
            .as_array()
            .expect("edges");
        assert_eq!(edges.len(), 1);
        let node = &edges[0]["node"];
        assert_eq!(node["type"].as_str(), Some("Invitation"));
        assert_eq!(
            node["body"]["workspaceId"].as_str(),
            Some(workspace_id.as_str())
        );
        assert_eq!(
            node["body"]["createdByUserId"].as_str(),
            Some(owner_id.as_str())
        );
        assert_eq!(
            node["body"]["inviteId"].as_str(),
            Some(invite_token.as_str())
        );
    }

    #[tokio::test]
    async fn graphql_access_token_lifecycle() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("token@example.com", &password_hash, None)
            .await
            .expect("create user");

        let schema = graphql::build_schema(state.clone());

        let create_mutation = r#"
            mutation {
                generateUserAccessToken(input: { name: "CLI" }) {
                    id
                    name
                    token
                }
            }
        "#;

        let create_response = schema
            .execute(GraphQLRequest::new(create_mutation).data(RequestUser::new(user.id.clone())))
            .await;

        assert!(
            create_response.errors.is_empty(),
            "unexpected errors: {:?}",
            create_response.errors
        );

        let create_json = create_response.data.into_json().expect("valid json");
        let created = &create_json["generateUserAccessToken"];
        let token_id = created["id"].as_str().expect("token id").to_string();
        let issued_token = created["token"].as_str().expect("token");
        assert!(issued_token.starts_with("ut_"));

        let access_tokens_query = "{ accessTokens { id name createdAt expiresAt } }";
        let list_response = schema
            .execute(
                GraphQLRequest::new(access_tokens_query).data(RequestUser::new(user.id.clone())),
            )
            .await;

        assert!(
            list_response.errors.is_empty(),
            "list errors: {:?}",
            list_response.errors
        );
        let list_json = list_response.data.into_json().expect("valid json");
        let tokens = list_json["accessTokens"]
            .as_array()
            .expect("array of tokens");
        assert_eq!(tokens.len(), 1);
        assert!(tokens[0].get("id").is_some());
        assert!(
            tokens[0].get("token").is_none(),
            "token should not be exposed"
        );

        let revealed_query = "{ revealedAccessTokens { id token } }";
        let revealed_response = schema
            .execute(GraphQLRequest::new(revealed_query).data(RequestUser::new(user.id.clone())))
            .await;
        assert!(
            revealed_response.errors.is_empty(),
            "revealed errors: {:?}",
            revealed_response.errors
        );
        let revealed_json = revealed_response.data.into_json().expect("valid json");
        let revealed_tokens = revealed_json["revealedAccessTokens"]
            .as_array()
            .expect("revealed list");
        assert_eq!(revealed_tokens.len(), 1);
        assert_eq!(revealed_tokens[0]["id"].as_str(), Some(token_id.as_str()));
        assert_eq!(revealed_tokens[0]["token"].as_str(), Some(issued_token));

        let revoke_mutation = format!("mutation {{ revokeUserAccessToken(id: \"{}\") }}", token_id);
        let revoke_response = schema
            .execute(GraphQLRequest::new(revoke_mutation).data(RequestUser::new(user.id.clone())))
            .await;
        assert!(
            revoke_response.errors.is_empty(),
            "revoke errors: {:?}",
            revoke_response.errors
        );
        let revoke_json = revoke_response.data.into_json().expect("valid json");
        assert_eq!(revoke_json["revokeUserAccessToken"].as_bool(), Some(true));

        let post_revoke_response = schema
            .execute(
                GraphQLRequest::new(access_tokens_query).data(RequestUser::new(user.id.clone())),
            )
            .await;
        assert!(
            post_revoke_response.errors.is_empty(),
            "post revoke errors: {:?}",
            post_revoke_response.errors
        );
        let post_revoke_json = post_revoke_response.data.into_json().expect("valid json");
        let remaining = post_revoke_json["accessTokens"].as_array().expect("array");
        assert!(remaining.is_empty(), "expected tokens to be revoked");
    }

    #[cfg(feature = "barffine_extra")]
    #[tokio::test]
    async fn graphql_generate_rpc_access_token_enforces_permissions() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = "doc-rpc-token";
        insert_document(&database, &workspace_id, doc_id, false, "page").await;

        let schema = graphql::build_schema(state.clone());

        let doc_mutation = format!(
            r#"
        mutation {{
            generateRpcAccessToken(input: {{ workspaceId: "{workspace_id}", docId: "{doc_id}" }}) {{
                workspaceId
                docId
                token
            }}
        }}
        "#
        );

        let doc_response = schema
            .execute(
                GraphQLRequest::new(doc_mutation.clone()).data(RequestUser::new(owner_id.clone())),
            )
            .await;
        assert!(
            doc_response.errors.is_empty(),
            "doc token errors: {:?}",
            doc_response.errors
        );
        let doc_json = doc_response.data.into_json().expect("valid json");
        let issued_doc_token = doc_json["generateRpcAccessToken"]["token"]
            .as_str()
            .expect("token value");
        assert!(
            issued_doc_token.starts_with(&format!("{doc_id},")),
            "token payload should embed doc id"
        );
        assert_eq!(
            doc_json["generateRpcAccessToken"]["docId"].as_str(),
            Some(doc_id)
        );

        let workspace_mutation = format!(
            r#"
        mutation {{
            generateRpcAccessToken(input: {{ workspaceId: "{workspace_id}" }}) {{
                workspaceId
                docId
                token
            }}
        }}
        "#
        );

        let workspace_response = schema
            .execute(
                GraphQLRequest::new(workspace_mutation.clone())
                    .data(RequestUser::new(owner_id.clone())),
            )
            .await;
        assert!(
            workspace_response.errors.is_empty(),
            "workspace token errors: {:?}",
            workspace_response.errors
        );
        let workspace_json = workspace_response.data.into_json().expect("valid json");
        let workspace_token = workspace_json["generateRpcAccessToken"]["token"]
            .as_str()
            .expect("token value");
        assert!(
            workspace_token.starts_with(&format!("{workspace_id},")),
            "token payload should embed workspace id"
        );
        assert!(
            workspace_json["generateRpcAccessToken"]["docId"].is_null(),
            "workspace tokens should not include docId"
        );

        let outsider_password = generate_password_hash("secret").expect("hash password");
        let outsider = state
            .user_store
            .create("rpc-outsider@example.com", &outsider_password, None)
            .await
            .expect("create outsider");

        let denied_doc = schema
            .execute(
                GraphQLRequest::new(doc_mutation.clone())
                    .data(RequestUser::new(outsider.id.clone())),
            )
            .await;
        assert!(
            !denied_doc.errors.is_empty(),
            "expected unauthorized doc token attempt"
        );

        let denied_workspace = schema
            .execute(GraphQLRequest::new(workspace_mutation).data(RequestUser::new(outsider.id)))
            .await;
        assert!(
            !denied_workspace.errors.is_empty(),
            "expected unauthorized workspace token attempt"
        );
    }

    #[tokio::test]
    async fn graphql_delete_session_removes_record() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("deleter-graphql@example.com", &password_hash, None)
            .await
            .expect("create user");
        let session = state
            .user_store
            .create_session(&user.id)
            .await
            .expect("create session");

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            "mutation {{ deleteSession(sessionId: \"{}\") }}",
            session.id
        );
        let response = schema
            .execute(
                GraphQLRequest::new(mutation).data(RequestUser::with_session(
                    user.id.clone(),
                    session.id.clone(),
                )),
            )
            .await;
        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        assert_eq!(data["deleteSession"], true);
        let stored = state
            .user_store
            .find_session(&session.id)
            .await
            .expect("query session");
        assert!(stored.is_none());
    }

    #[tokio::test]
    async fn graphql_refresh_session_returns_session_object() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("refresher-graphql@example.com", &password_hash, None)
            .await
            .expect("create user");
        let session = state
            .user_store
            .create_session(&user.id)
            .await
            .expect("create session");
        let original_expiry = session.expires_at;

        let schema = graphql::build_schema(state.clone());
        let response = schema
            .execute(
                GraphQLRequest::new("mutation { refreshSession { id userId } }").data(
                    RequestUser::with_session(user.id.clone(), session.id.clone()),
                ),
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        assert_eq!(data["refreshSession"]["id"], session.id);
        assert_eq!(data["refreshSession"]["userId"], user.id);
        let refreshed = state
            .user_store
            .find_session(&session.id)
            .await
            .expect("query session")
            .expect("session present");
        assert!(refreshed.expires_at >= original_expiry);
    }

    #[tokio::test]
    async fn graphql_create_session_rejects_disabled_user() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("disabled-gql@example.com", &password_hash, None)
            .await
            .expect("create user");

        state
            .user_store
            .set_disabled(&user.id, true)
            .await
            .expect("disable user");

        let schema = graphql::build_schema(state.clone());
        let mutation = format!(
            "mutation {{ createSession(input: {{ email: \"{}\", password: \"secret\" }}) {{ id }} }}",
            user.email
        );

        let response = schema.execute(GraphQLRequest::new(mutation)).await;
        assert!(
            !response.errors.is_empty(),
            "expected errors for disabled user"
        );
        assert_eq!(response.errors[0].message, "invalid credentials");
    }
}
