use std::env;

use async_graphql::{Context, ID, Json, Object, Result as GraphQLResult};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

use barffine_core::feature::FeatureNamespace;

use super::context::ClientVersion;
use crate::oauth::OAuthProviderKind;
use crate::{
    AppError, AppState,
    doc::{history, sync::workspace_snapshot_or_not_found},
    workspace::{
        invites::{self as workspace_invites, InviteLinkLookup},
        service::WorkspaceService,
    },
};
use semver::Version;

use super::workspace::{
    ContextWorkspaceEmbeddingStatus, InvitationType, InvitationWorkspaceType, Permission,
    WorkspaceBlobSizes, WorkspaceMemberStatus, WorkspaceRolePermissions,
    parse_membership_invite_id, permission_rank, workspace_member_status_from_str,
};
use super::{
    AccessTokenObject, CopilotPromptType, CredentialsRequirementObject, DocumentHistoryObject,
    DocumentSnapshotObject, ErrorDataUnion, ErrorNames, FeatureSnapshotObject, FeatureTypeEnum,
    GraphqlBadRequestDataType, LimitedUserType, ListUserInput, OAuthProviderTypeEnum,
    PasswordLimitsObject, PublicUserType, RequestUser, RevealedAccessTokenObject,
    ServerConfigObject, ServerDeploymentTypeEnum, ServerFeatureEnum, SubscriptionPrice,
    UserOrLimitedUser, UserType, WorkspaceType, default_copilot_prompts,
    default_subscription_prices, doc::WorkspaceUserGraph, ensure_document_exists,
    ensure_workspace_exists, fetch_workspace_record, map_anyhow, map_app_error,
    normalize_non_empty_input, parse_feature_namespace, require_admin_user, require_doc_permission,
    require_request_user, require_workspace_permission, workspace_permission_for_user,
};
use serde_json::{Value as JsonValue, json};

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    #[graphql(name = "serverConfig")]
    async fn server_config(&self, ctx: &Context<'_>) -> GraphQLResult<ServerConfigObject> {
        let state = ctx.data::<AppState>()?;
        let initialized = state.user_store.count(None).await.map_err(map_anyhow)? > 0;

        let mut features = gather_server_features();
        if features.is_empty() {
            features = default_server_features();
        }

        if state.oauth.has_providers()
            && !features
                .iter()
                .any(|feature| matches!(feature, ServerFeatureEnum::OAuth))
        {
            features.push(ServerFeatureEnum::OAuth);
        }

        let allow_guest_demo_workspace =
            allow_guest_demo_workspace_override().unwrap_or_else(|| {
                features
                    .iter()
                    .any(|feature| matches!(feature, ServerFeatureEnum::LocalWorkspace))
            });

        let credentials_requirement = CredentialsRequirementObject {
            password: password_limits_from_env(),
        };

        let oauth_providers = oauth_providers_for_request(ctx, state);
        let available_user_features: Vec<FeatureTypeEnum> = Vec::new();

        Ok(ServerConfigObject::new(
            server_name(state),
            state.metadata.compatibility.clone(),
            state.base_url.clone(),
            ServerDeploymentTypeEnum::from_metadata(&state.metadata.deployment_type),
            features,
            allow_guest_demo_workspace,
            None,
            available_user_features,
            credentials_requirement,
            initialized,
            oauth_providers,
        ))
    }

    #[graphql(name = "appConfig")]
    async fn app_config(&self, ctx: &Context<'_>) -> GraphQLResult<Json<JsonValue>> {
        require_admin_user(ctx).await?;
        let state = ctx.data::<AppState>()?;

        let config = json!({
            "server": {
                "name": server_name(state),
                "baseUrl": state.base_url.clone(),
                "compatibility": state.metadata.compatibility,
                "deployment": {
                    "type": state.metadata.deployment_type,
                    "flavor": state.metadata.flavor,
                },
            },
        });

        Ok(Json(config))
    }

    async fn error(&self, name: ErrorNames) -> GraphQLResult<ErrorDataUnion> {
        Ok(ErrorDataUnion::GraphqlBadRequestDataType(
            GraphqlBadRequestDataType {
                code: name.as_str().to_string(),
                message: format!("Error {name:?} not yet implemented in Barffine"),
            },
        ))
    }

    async fn feature_flags(
        &self,
        ctx: &Context<'_>,
        namespace: Option<String>,
    ) -> GraphQLResult<FeatureSnapshotObject> {
        let state = ctx.data::<AppState>()?;
        let namespace = if let Some(namespace) = namespace {
            parse_feature_namespace(&namespace).map_err(map_app_error)?
        } else {
            FeatureNamespace::Global
        };

        let snapshot = state
            .feature_service
            .snapshot(&namespace)
            .await
            .map_err(map_anyhow)?;
        Ok(FeatureSnapshotObject::from(snapshot))
    }

    async fn access_tokens(&self, ctx: &Context<'_>) -> GraphQLResult<Vec<AccessTokenObject>> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;
        let tokens = state
            .access_token_store
            .list(&request_user.user_id)
            .await
            .map_err(map_anyhow)?;

        Ok(tokens.into_iter().map(AccessTokenObject::from).collect())
    }

    #[graphql(name = "collectAllBlobSizes")]
    async fn collect_all_blob_sizes(&self, ctx: &Context<'_>) -> GraphQLResult<WorkspaceBlobSizes> {
        let _ = require_request_user(ctx)?;
        Ok(WorkspaceBlobSizes::new(0))
    }

    #[graphql(name = "revealedAccessTokens")]
    async fn revealed_access_tokens(
        &self,
        ctx: &Context<'_>,
    ) -> GraphQLResult<Vec<RevealedAccessTokenObject>> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;
        let tokens = state
            .access_token_store
            .list_revealed(&request_user.user_id)
            .await
            .map_err(map_anyhow)?;

        Ok(tokens
            .into_iter()
            .map(RevealedAccessTokenObject::from)
            .collect())
    }

    async fn current_user(&self, ctx: &Context<'_>) -> GraphQLResult<Option<UserType>> {
        let Some(request_user) = ctx.data_opt::<RequestUser>() else {
            return Ok(None);
        };

        let state = ctx.data::<AppState>()?;
        let user = state
            .user_store
            .find_by_id(&request_user.user_id)
            .await
            .map_err(map_anyhow)?;

        Ok(user.map(UserType::from))
    }

    async fn users(
        &self,
        ctx: &Context<'_>,
        #[graphql(default)] filter: ListUserInput,
    ) -> GraphQLResult<Vec<UserType>> {
        require_admin_user(ctx).await?;
        let state = ctx.data::<AppState>()?;
        let (limit, skip, keyword, cursor) = filter.normalize().map_err(map_app_error)?;

        let users = state
            .user_store
            .list_paginated(skip, limit, keyword.as_deref(), cursor.as_ref())
            .await
            .map_err(map_anyhow)?;

        Ok(users.into_iter().map(UserType::from).collect())
    }

    #[graphql(name = "usersCount")]
    async fn users_count(&self, ctx: &Context<'_>) -> GraphQLResult<i32> {
        require_admin_user(ctx).await?;
        let state = ctx.data::<AppState>()?;

        let total = state.user_store.count(None).await.map_err(map_anyhow)?;
        Ok(total.min(i32::MAX as i64) as i32)
    }

    #[graphql(name = "userById")]
    async fn user_by_id(&self, ctx: &Context<'_>, id: ID) -> GraphQLResult<UserType> {
        let request_user = require_request_user(ctx)?;

        let state = ctx.data::<AppState>()?;
        state
            .user_service
            .ensure_self_or_admin(
                &request_user.user_id,
                id.as_str(),
                "administrator privileges required",
            )
            .await
            .map_err(map_app_error)?;

        let user = state
            .user_service
            .fetch_user(id.as_str())
            .await
            .map_err(map_app_error)?;

        Ok(UserType::from(user))
    }

    #[graphql(name = "userByEmail")]
    async fn user_by_email(
        &self,
        ctx: &Context<'_>,
        email: String,
    ) -> GraphQLResult<Option<UserType>> {
        require_admin_user(ctx).await?;
        let normalized = email.trim();
        if normalized.is_empty() {
            return Ok(None);
        }

        let state = ctx.data::<AppState>()?;
        let user = state
            .user_store
            .find_by_email(normalized)
            .await
            .map_err(map_anyhow)?;

        Ok(user.map(UserType::from))
    }

    async fn user(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "email")] email: String,
    ) -> GraphQLResult<Option<UserOrLimitedUser>> {
        let state = ctx.data::<AppState>()?;
        let user = state
            .user_store
            .find_by_email(&email)
            .await
            .map_err(map_anyhow)?;

        let Some(user) = user else {
            return Ok(None);
        };

        let limited = LimitedUserType::from(&user);

        if ctx.data_opt::<RequestUser>().is_some() {
            Ok(Some(UserOrLimitedUser::User(UserType::from(user))))
        } else {
            Ok(Some(UserOrLimitedUser::Limited(limited)))
        }
    }

    async fn workspaces(&self, ctx: &Context<'_>) -> GraphQLResult<Vec<WorkspaceType>> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        if state
            .user_store
            .is_admin(&request_user.user_id)
            .await
            .map_err(map_anyhow)?
        {
            let workspaces = state.workspace_store.list_all().await.map_err(map_anyhow)?;
            return Ok(workspaces.into_iter().map(WorkspaceType::from).collect());
        }

        let memberships = state
            .workspace_store
            .list_memberships_for_user(&request_user.user_id)
            .await
            .map_err(map_anyhow)?;

        let mut unique_ids = std::collections::HashSet::new();
        let mut results = Vec::with_capacity(memberships.len());
        for membership in memberships {
            if !unique_ids.insert(membership.workspace_id.clone()) {
                continue;
            }
            if let Some(workspace) = state
                .workspace_store
                .find_by_id(&membership.workspace_id)
                .await
                .map_err(map_anyhow)?
            {
                results.push(WorkspaceType::from(workspace));
            }
        }

        Ok(results)
    }

    async fn workspace(&self, ctx: &Context<'_>, id: ID) -> GraphQLResult<WorkspaceType> {
        let state = ctx.data::<AppState>()?;
        let workspace = fetch_workspace_record(state, id.as_str()).await?;

        let request_user = ctx.data_opt::<RequestUser>();
        let authorized = state
            .workspace_service
            .has_workspace_read_access(&workspace, request_user.map(|user| user.user_id.as_str()))
            .await
            .map_err(map_app_error)?;

        if !authorized {
            return Err(map_app_error(AppError::forbidden(
                "Workspace.Read permission required",
            )));
        }

        let workspace_title = WorkspaceService::normalized_workspace_title(&workspace.name);

        state
            .document_store
            .ensure_doc_record(
                &workspace.id,
                &workspace.id,
                &workspace.owner_id,
                workspace_title,
            )
            .await
            .map_err(map_anyhow)?;
        state
            .doc_role_store
            .upsert(&workspace.id, &workspace.id, &workspace.owner_id, "owner")
            .await
            .map_err(map_anyhow)?;
        Ok(WorkspaceType::from(workspace))
    }

    #[graphql(name = "isAdmin", deprecation = "use WorkspaceType[role] instead")]
    async fn is_admin(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: ID,
    ) -> GraphQLResult<bool> {
        let request_user = require_request_user(ctx)?;
        let workspace_id_str = workspace_id.as_str();
        let state = ctx.data::<AppState>()?;

        let workspace = fetch_workspace_record(state, workspace_id_str).await?;

        let resolved = workspace_permission_for_user(state, &workspace, &request_user.user_id)
            .await
            .map_err(map_app_error)?;

        Ok(resolved
            .map(|value| permission_rank(value.permission) >= permission_rank(Permission::Admin))
            .unwrap_or(false))
    }

    #[graphql(name = "isOwner", deprecation = "use WorkspaceType[role] instead")]
    async fn is_owner(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: ID,
    ) -> GraphQLResult<bool> {
        let request_user = require_request_user(ctx)?;
        let workspace_id_str = workspace_id.as_str();
        let state = ctx.data::<AppState>()?;

        let workspace = fetch_workspace_record(state, workspace_id_str).await?;

        let resolved = workspace_permission_for_user(state, &workspace, &request_user.user_id)
            .await
            .map_err(map_app_error)?;

        let is_owner = resolved.map_or(false, |value| {
            matches!(value.permission, Permission::Owner)
                || (matches!(value.permission, Permission::Admin) && !value.from_membership)
        });

        Ok(is_owner)
    }

    #[graphql(name = "getInviteInfo")]
    async fn get_invite_info(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "inviteId")] invite_id: String,
    ) -> GraphQLResult<InvitationType> {
        let invite_id = normalize_non_empty_input("inviteId", invite_id)?;
        let trimmed = invite_id.as_str();

        let state = ctx.data::<AppState>()?;
        let mut workspace_id: Option<String> = None;
        let mut invitee_user_id: Option<String> = None;
        let mut status: Option<WorkspaceMemberStatus> = None;

        if let Some((membership_workspace, membership_user)) = parse_membership_invite_id(trimmed) {
            let member = state
                .workspace_store
                .get_member(&membership_workspace, &membership_user)
                .await
                .map_err(map_anyhow)?
                .ok_or_else(|| map_app_error(AppError::not_found("invitation not found")))?;

            workspace_id = Some(member.workspace_id.clone());
            invitee_user_id = Some(member.user_id.clone());
            status = Some(workspace_member_status_from_str(&member.status));
        } else {
            match workspace_invites::lookup_invite_link_by_token(state, trimmed)
                .await
                .map_err(map_app_error)?
            {
                InviteLinkLookup::Active(link) => {
                    workspace_id = Some(link.workspace_id.clone());
                }
                InviteLinkLookup::Missing => {
                    return Err(map_app_error(AppError::not_found("invitation not found")));
                }
                InviteLinkLookup::Expired => {
                    return Err(map_app_error(AppError::not_found("invitation expired")));
                }
            }
        }

        let workspace_id = workspace_id.expect("workspace id resolved");
        let workspace = state
            .workspace_service
            .fetch_workspace(&workspace_id)
            .await
            .map_err(map_app_error)?;

        let invitee_user_id = if let Some(user_id) = invitee_user_id {
            user_id
        } else if let Some(request_user) = ctx.data_opt::<RequestUser>() {
            request_user.user_id.clone()
        } else {
            return Err(map_app_error(AppError::unauthorized(
                "authentication required to resolve invite link",
            )));
        };

        let invitee = state
            .user_service
            .fetch_user_with(&invitee_user_id, || {
                AppError::not_found("invitee not found")
            })
            .await
            .map_err(map_app_error)?;

        if status.is_none() {
            status = state
                .workspace_store
                .get_member(&workspace.id, &invitee.id)
                .await
                .map_err(map_anyhow)?
                .map(|member| workspace_member_status_from_str(&member.status));
        }

        let owner = state
            .user_service
            .fetch_user_with(&workspace.owner_id, || {
                AppError::not_found("workspace owner not found")
            })
            .await
            .map_err(map_app_error)?;

        let invitee_graph = WorkspaceUserGraph {
            avatar_url: invitee.avatar_url.clone(),
            email: Some(invitee.email.clone()),
            id: invitee.id.clone(),
            name: invitee
                .name
                .clone()
                .unwrap_or_else(|| invitee.email.clone()),
        };

        let owner_graph = WorkspaceUserGraph {
            avatar_url: owner.avatar_url.clone(),
            email: Some(owner.email.clone()),
            id: owner.id.clone(),
            name: owner.name.clone().unwrap_or_else(|| owner.email.clone()),
        };

        let workspace_info = InvitationWorkspaceType {
            avatar: String::new(),
            id: ID(workspace.id.clone()),
            name: workspace.name.clone(),
        };

        Ok(InvitationType {
            invitee: invitee_graph,
            status,
            user: owner_graph,
            workspace: workspace_info,
        })
    }

    #[graphql(name = "listCopilotPrompts")]
    async fn list_copilot_prompts(
        &self,
        ctx: &Context<'_>,
    ) -> GraphQLResult<Vec<CopilotPromptType>> {
        require_admin_user(ctx).await?;
        Ok(default_copilot_prompts())
    }

    async fn prices(&self) -> GraphQLResult<Vec<SubscriptionPrice>> {
        Ok(default_subscription_prices())
    }

    #[graphql(name = "publicUserById")]
    async fn public_user_by_id(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> GraphQLResult<Option<PublicUserType>> {
        let state = ctx.data::<AppState>()?;
        let user = state
            .user_store
            .find_by_id(id.as_str())
            .await
            .map_err(map_anyhow)?;

        Ok(user
            .as_ref()
            .filter(|record| !record.disabled)
            .map(PublicUserType::from_user))
    }

    #[graphql(name = "applyDocUpdates")]
    async fn apply_doc_updates(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: String,
        #[graphql(name = "docId")] doc_id: String,
        op: String,
        updates: String,
    ) -> GraphQLResult<String> {
        let empty_pair_error = || {
            map_app_error(AppError::bad_request(
                "workspaceId and docId must not be empty",
            ))
        };

        let workspace_id = normalize_non_empty_input("workspaceId", workspace_id)
            .map_err(|_| empty_pair_error())?;
        let doc_id = normalize_non_empty_input("docId", doc_id).map_err(|_| empty_pair_error())?;

        let _ = op;

        require_doc_permission(
            ctx,
            &workspace_id,
            &doc_id,
            |perms| perms.can_update_doc(),
            "Doc.Update permission required",
        )
        .await?;

        let state = ctx.data::<AppState>()?;
        ensure_workspace_exists(state, &workspace_id).await?;
        ensure_document_exists(state, &workspace_id, &doc_id).await?;

        Ok(updates.trim().to_string())
    }

    async fn document_snapshot(
        &self,
        ctx: &Context<'_>,
        workspace_id: ID,
        doc_id: ID,
    ) -> GraphQLResult<Option<DocumentSnapshotObject>> {
        let state = ctx.data::<AppState>()?;
        let (snapshot, _) =
            workspace_snapshot_or_not_found(&state, workspace_id.as_str(), doc_id.as_str())
                .await
                .map_err(map_app_error)?;
        Ok(Some(DocumentSnapshotObject {
            snapshot: BASE64.encode(snapshot),
        }))
    }

    async fn document_history(
        &self,
        ctx: &Context<'_>,
        workspace_id: ID,
        doc_id: ID,
        limit: Option<i32>,
    ) -> GraphQLResult<Vec<DocumentHistoryObject>> {
        let state = ctx.data::<AppState>()?;
        ensure_document_exists(&state, workspace_id.as_str(), doc_id.as_str()).await?;

        let history = history::fetch_history_records(
            &state,
            workspace_id.as_str(),
            doc_id.as_str(),
            limit.map(|value| value as i64),
        )
        .await
        .map_err(map_app_error)?;

        Ok(history
            .into_iter()
            .map(DocumentHistoryObject::from)
            .collect())
    }

    #[graphql(name = "queryWorkspaceEmbeddingStatus")]
    async fn query_workspace_embedding_status(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: ID,
    ) -> GraphQLResult<ContextWorkspaceEmbeddingStatus> {
        let workspace_id_str = workspace_id.as_str();
        require_workspace_permission(ctx, workspace_id_str, Permission::Collaborator).await?;

        let state = ctx.data::<AppState>()?;
        ensure_workspace_exists(state, workspace_id_str).await?;

        let total = state
            .workspace_embedding_files
            .get(workspace_id_str)
            .map(|files| files.len())
            .unwrap_or(0);

        let ignored = state
            .workspace_embedding_ignored_docs
            .get(workspace_id_str)
            .map(|docs| docs.len())
            .unwrap_or(0);

        let embedded = total.saturating_sub(ignored);

        Ok(ContextWorkspaceEmbeddingStatus::new(total, embedded))
    }

    #[graphql(
        name = "workspaceRolePermissions",
        deprecation = "use WorkspaceType[permissions] instead"
    )]
    async fn workspace_role_permissions(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> GraphQLResult<WorkspaceRolePermissions> {
        let request_user = require_request_user(ctx)?;
        let workspace_id = id.as_str();
        let state = ctx.data::<AppState>()?;

        let workspace = fetch_workspace_record(state, workspace_id).await?;

        if let Some(resolved) =
            workspace_permission_for_user(state, &workspace, &request_user.user_id)
                .await
                .map_err(map_app_error)?
        {
            return Ok(WorkspaceRolePermissions::new(resolved.permission));
        }

        Err(map_app_error(AppError::forbidden(
            "insufficient workspace permissions",
        )))
    }
}

fn server_name(state: &AppState) -> String {
    if let Some(name) = env::var("BARFFINE_SERVER_NAME")
        .ok()
        .and_then(non_empty_string)
    {
        return name;
    }

    if state
        .metadata
        .deployment_type
        .eq_ignore_ascii_case("selfhosted")
    {
        return "AFFiNE Selfhosted Cloud".to_string();
    }

    if state.metadata.message.trim().is_empty() {
        "AFFiNE Cloud".to_string()
    } else {
        state.metadata.message.clone()
    }
}

fn gather_server_features() -> Vec<ServerFeatureEnum> {
    env::var("BARFFINE_SERVER_FEATURES")
        .ok()
        .and_then(|raw| {
            let parsed = parse_list(&raw, |token| {
                if token.is_empty() {
                    None
                } else {
                    ServerFeatureEnum::from_str(token)
                }
            });
            if parsed.is_empty() {
                None
            } else {
                Some(parsed)
            }
        })
        .unwrap_or_default()
}

fn default_server_features() -> Vec<ServerFeatureEnum> {
    vec![
        ServerFeatureEnum::Indexer,
        ServerFeatureEnum::Comment,
        ServerFeatureEnum::LocalWorkspace,
    ]
}

fn allow_guest_demo_workspace_override() -> Option<bool> {
    parse_bool_env("BARFFINE_ALLOW_GUEST_DEMO_WORKSPACE")
}

fn password_limits_from_env() -> PasswordLimitsObject {
    let mut limits = PasswordLimitsObject::default();

    if let Some(min) = env::var("BARFFINE_PASSWORD_MIN")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
    {
        if min > 0 {
            limits.min_length = min;
        }
    }

    if let Some(max) = env::var("BARFFINE_PASSWORD_MAX")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
    {
        if max > limits.min_length {
            limits.max_length = max;
        }
    }

    if limits.min_length >= limits.max_length {
        PasswordLimitsObject::default()
    } else {
        limits
    }
}

fn oauth_providers_for_request(ctx: &Context<'_>, state: &AppState) -> Vec<OAuthProviderTypeEnum> {
    let providers = state.oauth.providers();
    let client_version = ctx.data_opt::<ClientVersion>().and_then(|cv| cv.parsed());
    filter_oauth_providers(providers, client_version)
}

fn map_oauth_provider(kind: OAuthProviderKind) -> OAuthProviderTypeEnum {
    match kind {
        OAuthProviderKind::Apple => OAuthProviderTypeEnum::Apple,
        OAuthProviderKind::GitHub => OAuthProviderTypeEnum::GitHub,
        OAuthProviderKind::Google => OAuthProviderTypeEnum::Google,
        OAuthProviderKind::Oidc => OAuthProviderTypeEnum::Oidc,
    }
}

fn meets_min_version(version: &Version, major: u64, minor: u64, patch: u64) -> bool {
    if version.major > major {
        return true;
    }
    if version.major < major {
        return false;
    }

    if version.minor > minor {
        return true;
    }
    if version.minor < minor {
        return false;
    }

    version.patch >= patch
}

fn filter_oauth_providers(
    providers: Vec<OAuthProviderKind>,
    client_version: Option<&Version>,
) -> Vec<OAuthProviderTypeEnum> {
    providers
        .into_iter()
        .filter(|kind| {
            if matches!(kind, OAuthProviderKind::Apple) {
                client_version
                    .filter(|version| meets_min_version(version, 0, 22, 0))
                    .is_some()
            } else {
                true
            }
        })
        .map(map_oauth_provider)
        .collect()
}

fn parse_list<T, F>(raw: &str, mut parser: F) -> Vec<T>
where
    T: Ord,
    F: FnMut(&str) -> Option<T>,
{
    let mut items: Vec<T> = raw
        .split(|ch| ch == ',' || ch == ';')
        .filter_map(|token| parser(token.trim()))
        .collect();

    items.sort_unstable();
    items.dedup();
    items
}

fn parse_bool_env(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .and_then(|value| parse_bool(value.trim()))
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{graphql::build_schema, test_support::setup_state};
    use async_graphql::Request;
    use once_cell::sync::Lazy;
    use tokio::sync::Mutex;

    static ENV_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    fn set_env_var(key: &str, value: &str) {
        // Nightly build is configured with `unsafe_op_in_unsafe_fn`, so keep the unsafe scope tiny.
        unsafe { std::env::set_var(key, value) };
    }

    fn remove_env_var(key: &str) {
        unsafe { std::env::remove_var(key) };
    }

    struct ProviderEnv;

    impl ProviderEnv {
        fn new() -> Self {
            set_env_var("BARFFINE_OAUTH_GOOGLE_CLIENT_ID", "google-client");
            set_env_var("BARFFINE_OAUTH_GOOGLE_CLIENT_SECRET", "google-secret");
            set_env_var("BARFFINE_OAUTH_APPLE_CLIENT_ID", "apple-client");
            set_env_var("BARFFINE_OAUTH_APPLE_CLIENT_SECRET", "apple-secret");
            Self
        }
    }

    impl Drop for ProviderEnv {
        fn drop(&mut self) {
            for key in [
                "BARFFINE_OAUTH_GOOGLE_CLIENT_ID",
                "BARFFINE_OAUTH_GOOGLE_CLIENT_SECRET",
                "BARFFINE_OAUTH_APPLE_CLIENT_ID",
                "BARFFINE_OAUTH_APPLE_CLIENT_SECRET",
            ] {
                remove_env_var(key);
            }
        }
    }

    #[tokio::test]
    async fn server_config_filters_apple_by_client_version() {
        let _guard = ENV_GUARD.lock().await;
        let _env = ProviderEnv::new();
        let (_tmp, _db, state) = setup_state().await;
        let schema = build_schema(state.clone());

        let request =
            Request::new("query ServerConfig { serverConfig { features oauthProviders } }").data(
                ClientVersion::new(Some(Version::parse("0.21.0").expect("parse version"))),
            );

        let older = schema.execute(request).await;
        assert!(older.errors.is_empty(), "{:?}", older.errors);
        let data = older.data.into_json().expect("json");
        let providers = data["serverConfig"]["oauthProviders"]
            .as_array()
            .expect("providers array")
            .iter()
            .map(|value| value.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(providers, vec!["Google"]);

        let request =
            Request::new("query ServerConfig { serverConfig { features oauthProviders } }").data(
                ClientVersion::new(Some(Version::parse("0.22.0").expect("parse version"))),
            );

        let newer = schema.execute(request).await;
        assert!(newer.errors.is_empty(), "{:?}", newer.errors);
        let data = newer.data.into_json().expect("json");
        let mut providers = data["serverConfig"]["oauthProviders"]
            .as_array()
            .expect("providers array")
            .iter()
            .map(|value| value.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        providers.sort();
        assert_eq!(providers, vec!["Apple", "Google"]);

        let features = data["serverConfig"]["features"]
            .as_array()
            .expect("features array");
        assert!(features.iter().any(|value| value.as_str() == Some("OAuth")));
    }

    #[test]
    fn filter_oauth_providers_hides_apple_without_version() {
        let providers = vec![OAuthProviderKind::Apple, OAuthProviderKind::Google];
        let filtered = filter_oauth_providers(providers, None);
        assert_eq!(filtered, vec![OAuthProviderTypeEnum::Google]);
    }

    #[test]
    fn filter_oauth_providers_hides_apple_for_old_clients() {
        let providers = vec![
            OAuthProviderKind::Apple,
            OAuthProviderKind::GitHub,
            OAuthProviderKind::Google,
        ];
        let version = Version::parse("0.21.9").ok();
        let filtered = filter_oauth_providers(providers, version.as_ref());
        assert_eq!(
            filtered,
            vec![OAuthProviderTypeEnum::GitHub, OAuthProviderTypeEnum::Google]
        );
    }

    #[test]
    fn filter_oauth_providers_keeps_apple_for_supported_clients() {
        let providers = vec![
            OAuthProviderKind::Google,
            OAuthProviderKind::Apple,
            OAuthProviderKind::Oidc,
        ];
        let version = Version::parse("0.22.0").ok();
        let filtered = filter_oauth_providers(providers, version.as_ref());
        assert_eq!(
            filtered,
            vec![
                OAuthProviderTypeEnum::Google,
                OAuthProviderTypeEnum::Apple,
                OAuthProviderTypeEnum::Oidc
            ]
        );
    }

    #[test]
    fn meets_min_version_handles_major_minor_patch() {
        let reference = Version::parse("0.22.0").unwrap();
        assert!(meets_min_version(&reference, 0, 22, 0));
        assert!(!meets_min_version(&reference, 0, 22, 1));
        assert!(meets_min_version(
            &Version::parse("1.0.0").unwrap(),
            0,
            22,
            0
        ));
        assert!(!meets_min_version(
            &Version::parse("0.21.9").unwrap(),
            0,
            22,
            0
        ));
    }
}

fn non_empty_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}
