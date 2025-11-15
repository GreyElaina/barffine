use std::collections::{HashMap, HashSet};

use async_graphql::{
    Context, Enum, ID, Json, Object, Result as GraphQLResult, SimpleObject, Union,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue, json};

use barffine_core::{
    feature::FeatureNamespace, notification::NotificationRecord, user,
    workspace::DEFAULT_WORKSPACE_NAME,
};

use crate::{
    AppError, AppState, user::helpers::display_name_for_user, workspace::service::WorkspaceService,
};

use super::types::user::{UserMembershipType, UserShareTokenType, UserType};
use super::{
    constants::*,
    copilot::CopilotType,
    cursors::{decode_cursor_payload_graphql, parse_timestamp_cursor_graphql},
    helpers::{
        format_bytes, format_history_period, map_anyhow, map_app_error, require_request_user,
        timestamp_to_datetime,
    },
    types::{PageInfo, PaginationInput, SubscriptionPlan, SubscriptionRecurring, SubscriptionType},
    workspace::workspace_role_from_str,
};

#[Object(name = "UserType")]
impl UserType {
    async fn id(&self) -> ID {
        ID(self.record.id.clone())
    }

    async fn email(&self) -> &str {
        &self.record.email
    }

    async fn name(&self) -> String {
        display_name_for_user(&self.record)
    }

    #[graphql(name = "avatarUrl")]
    async fn avatar_url(&self) -> Option<&str> {
        self.record.avatar_url.as_deref()
    }

    #[graphql(name = "createdAt")]
    async fn created_at(&self) -> DateTime<Utc> {
        timestamp_to_datetime(self.record.created_at)
    }

    async fn disabled(&self) -> bool {
        self.record.disabled
    }

    #[graphql(name = "emailVerified")]
    async fn email_verified(&self) -> bool {
        self.record.email_verified_at.is_some()
    }

    #[graphql(name = "hasPassword")]
    async fn has_password(&self) -> bool {
        !self.record.password_hash.is_empty()
    }

    async fn memberships(&self, ctx: &Context<'_>) -> GraphQLResult<Vec<UserMembershipType>> {
        let state = ctx.data::<AppState>()?;
        let records = state
            .workspace_store
            .list_memberships_for_user(&self.record.id)
            .await
            .map_err(map_anyhow)?;

        let mut memberships = Vec::with_capacity(records.len());
        for record in records {
            let role = workspace_role_from_str(&record.role);
            memberships.push(UserMembershipType::from_membership(
                record,
                &self.record.id,
                role,
            ));
        }

        Ok(memberships)
    }

    #[graphql(name = "shareTokens")]
    async fn share_tokens(&self, ctx: &Context<'_>) -> GraphQLResult<Vec<UserShareTokenType>> {
        let state = ctx.data::<AppState>()?;
        let tokens = state
            .document_store
            .list_share_tokens_for_user(&self.record.id)
            .await
            .map_err(map_anyhow)?;

        Ok(tokens.into_iter().map(UserShareTokenType::from).collect())
    }

    async fn features(&self, ctx: &Context<'_>) -> GraphQLResult<Vec<FeatureTypeEnum>> {
        let state = ctx.data::<AppState>()?;
        let mut flags: HashSet<FeatureTypeEnum> = HashSet::new();

        let namespaces = [
            FeatureNamespace::Global,
            FeatureNamespace::user(self.record.id.clone()),
        ];
        for namespace in namespaces {
            let snapshot = state
                .feature_service
                .snapshot(&namespace)
                .await
                .map_err(map_anyhow)?;
            for flag in snapshot.flags.iter().filter(|flag| flag.enabled) {
                if let Some(feature) = FeatureTypeEnum::from_flag_key(&flag.key) {
                    flags.insert(feature);
                }
            }
        }

        let mut features: Vec<FeatureTypeEnum> = flags.into_iter().collect();
        features.sort_by_key(|feature| feature.as_str());
        Ok(features)
    }

    async fn token(&self, ctx: &Context<'_>) -> GraphQLResult<TokenType> {
        let request_user = require_request_user(ctx)?;
        if request_user.user_id != self.record.id {
            return Err(map_app_error(AppError::unauthorized(
                "access to requested user is not permitted",
            )));
        }

        let state = ctx.data::<AppState>()?;
        let session = state
            .user_store
            .create_session(&self.record.id)
            .await
            .map_err(map_anyhow)?;

        Ok(TokenType {
            token: session.id.clone(),
            refresh: String::new(),
            session_token: Some(session.id),
        })
    }

    async fn copilot(
        &self,
        _ctx: &Context<'_>,
        #[graphql(name = "workspaceId")] workspace_id: Option<String>,
    ) -> GraphQLResult<CopilotType> {
        Ok(CopilotType::new(self.record.id.clone(), workspace_id))
    }

    #[graphql(name = "invoiceCount")]
    async fn invoice_count(&self) -> i32 {
        0
    }

    async fn invoices(
        &self,
        #[graphql(default)] _skip: Option<i32>,
        #[graphql(default)] _take: Option<i32>,
    ) -> GraphQLResult<Vec<InvoiceType>> {
        Ok(Vec::new())
    }

    async fn quota(&self) -> UserQuotaType {
        UserQuotaType::default()
    }

    #[graphql(name = "quotaUsage")]
    async fn quota_usage(&self) -> UserQuotaUsageType {
        UserQuotaUsageType::default()
    }

    #[graphql(name = "notificationCount")]
    async fn notification_count(&self, ctx: &Context<'_>) -> GraphQLResult<i32> {
        let state = self.ensure_notification_access(ctx).await?;

        let count = state
            .notification_center
            .count_unread(&self.record.id)
            .await
            .map_err(map_anyhow)?;

        Ok(count.min(i32::MAX as i64) as i32)
    }

    async fn notifications(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "pagination")] pagination: PaginationInput,
    ) -> GraphQLResult<PaginatedNotificationObjectType> {
        let state = self.ensure_notification_access(ctx).await?;

        let (limit, offset, after) = pagination.normalize().map_err(map_app_error)?;
        let cursor = after
            .as_deref()
            .map(decode_notification_cursor)
            .transpose()?;

        let fetch_limit = limit + 1;
        let mut notifications = state
            .notification_center
            .list_for_user(
                &self.record.id,
                fetch_limit,
                offset,
                cursor.clone().map(|c| (c.timestamp, c.id.clone())),
            )
            .await
            .map_err(map_anyhow)?;
        let total: i64 = state
            .notification_center
            .count_for_user(&self.record.id)
            .await
            .map_err(map_anyhow)?;

        let has_next_page = (notifications.len() as i64) > limit;
        if has_next_page {
            notifications.truncate(limit as usize);
        }

        let lookups = NotificationLookups::gather(state, &notifications).await?;

        let edges: Vec<NotificationObjectTypeEdge> = notifications
            .into_iter()
            .map(|record| {
                let cursor = encode_notification_cursor(&record);
                let notification_type = notification_type_from_kind(record.kind.as_str());
                let payload = enrich_notification_payload(&record, notification_type, &lookups);

                NotificationObjectTypeEdge {
                    cursor,
                    node: NotificationObjectType::from_record(record, notification_type, payload),
                }
            })
            .collect();

        let page_info = PageInfo {
            start_cursor: edges.first().map(|edge| edge.cursor.clone()),
            end_cursor: edges.last().map(|edge| edge.cursor.clone()),
            has_next_page,
            has_previous_page: cursor.is_some() || offset > 0,
        };

        Ok(PaginatedNotificationObjectType {
            edges,
            page_info,
            total_count: total.min(i32::MAX as i64) as i32,
        })
    }

    async fn subscriptions(&self) -> GraphQLResult<Vec<SubscriptionType>> {
        Ok(Vec::new())
    }

    async fn settings(&self, ctx: &Context<'_>) -> GraphQLResult<UserSettingsType> {
        let request_user = require_request_user(ctx)?;
        if request_user.user_id != self.record.id {
            return Err(map_app_error(AppError::unauthorized(
                "access to user settings is not permitted",
            )));
        }
        let state = ctx.data::<AppState>()?;
        let settings = state
            .user_settings
            .get(&self.record.id)
            .await
            .map_err(map_anyhow)?;
        Ok(UserSettingsType::from(settings))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        auth::generate_password_hash,
        graphql::{self, RequestUser},
        testing::{insert_document, seed_workspace, setup_state},
    };
    use async_graphql::{Request as GraphQLRequest, Value as GraphQLValue};
    use barffine_core::notification::NotificationRecord;
    use chrono::{Duration, Utc};
    use serde_json::json;
    use uuid::Uuid;

    #[tokio::test]
    async fn graphql_user_by_id_returns_authenticated_user() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("fetcher@example.com", &password_hash, None)
            .await
            .expect("create user");

        let schema = graphql::build_schema(state.clone());
        let query = format!("{{ userById(id: \"{}\") {{ id email name }} }}", user.id);
        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(user.id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("valid json");
        assert_eq!(data["userById"]["id"], user.id);
        assert_eq!(data["userById"]["email"], user.email);
        assert_eq!(data["userById"]["name"], "fetcher");
    }

    #[tokio::test]
    async fn graphql_user_profile_fields_reflect_database_state() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create(
                "profile@example.com",
                &password_hash,
                Some("  Display Name  "),
            )
            .await
            .expect("create user");

        state
            .user_store
            .update_profile(
                &user.id,
                None,
                Some(Some("https://example.com/avatar.png".to_string())),
            )
            .await
            .expect("update avatar");
        state
            .user_store
            .set_email_verified(&user.id, true)
            .await
            .expect("verify email");
        state
            .user_store
            .set_disabled(&user.id, true)
            .await
            .expect("disable user");

        let schema = graphql::build_schema(state.clone());
        let query = "{ currentUser { name avatarUrl emailVerified disabled } }";
        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(user.id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("valid json");
        let current_user = &data["currentUser"];
        assert_eq!(current_user["name"], "Display Name");
        assert_eq!(
            current_user["avatarUrl"].as_str(),
            Some("https://example.com/avatar.png")
        );
        assert_eq!(current_user["emailVerified"], true);
        assert_eq!(current_user["disabled"], true);
    }

    #[tokio::test]
    async fn graphql_user_memberships_and_share_tokens() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, user_id) = seed_workspace(&state).await;

        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;
        state
            .document_store
            .publish_doc(&workspace_id, &doc_id, "page")
            .await
            .expect("publish doc")
            .expect("doc metadata");

        let schema = graphql::build_schema(state.clone());
        let query = "{ currentUser { memberships { workspaceId workspaceName role isOwner } shareTokens { workspaceId docId token } } }";
        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(user_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("valid json");
        let memberships = data["currentUser"]["memberships"]
            .as_array()
            .expect("memberships");
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0]["workspaceId"], workspace_id);
        assert_eq!(memberships[0]["workspaceName"], "Test Workspace");
        assert_eq!(memberships[0]["role"], "Owner");
        assert!(memberships[0]["isOwner"].as_bool().unwrap());

        let share_tokens = data["currentUser"]["shareTokens"]
            .as_array()
            .expect("share tokens");
        assert_eq!(share_tokens.len(), 1);
        assert_eq!(share_tokens[0]["workspaceId"], workspace_id);
        assert_eq!(share_tokens[0]["docId"], doc_id);
        let token_value = share_tokens[0]["token"].as_str().expect("token string");
        assert!(!token_value.is_empty());
    }

    #[tokio::test]
    async fn graphql_user_memberships_excludes_pending() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let password_hash = generate_password_hash("secret").expect("hash password");
        let pending = state
            .user_store
            .create("pending@example.com", &password_hash, Some("Pending User"))
            .await
            .expect("create pending user");

        state
            .workspace_store
            .set_member_role_with_status(
                &workspace_id,
                &pending.id,
                "editor",
                "pending",
                Some(&owner_id),
                None,
                None,
            )
            .await
            .expect("invite pending member");

        let schema = graphql::build_schema(state.clone());
        let query = "{ currentUser { memberships { workspaceId role } } }";
        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(owner_id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("valid json");
        let memberships = data["currentUser"]["memberships"]
            .as_array()
            .expect("memberships");
        assert_eq!(memberships.len(), 1);
        assert_ne!(memberships[0]["workspaceId"], pending.id);
    }

    #[tokio::test]
    async fn graphql_user_by_id_rejects_other_users() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let requester = state
            .user_store
            .create("requester@example.com", &password_hash, Some("Requester"))
            .await
            .expect("create requester");
        let other = state
            .user_store
            .create("other@example.com", &password_hash, Some("Other"))
            .await
            .expect("create other");

        let schema = graphql::build_schema(state.clone());
        let query = format!("{{ userById(id: \"{}\") {{ id email }} }}", other.id);
        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(requester.id.clone())))
            .await;

        assert!(!response.errors.is_empty(), "expected authorization error");
        let error = &response.errors[0];
        assert_eq!(error.message, "administrator privileges required");
        let extensions = error.extensions.as_ref().expect("extensions present");
        assert_eq!(
            extensions.get("code"),
            Some(&GraphQLValue::from("FORBIDDEN"))
        );
    }

    #[tokio::test]
    async fn graphql_user_by_email_returns_user_for_admin() {
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
            .expect("promote admin");

        let target = state
            .user_store
            .create("target@example.com", &password_hash, Some("Target"))
            .await
            .expect("create target");

        let schema = graphql::build_schema(state.clone());
        let query = format!(
            "{{ userByEmail(email: \"{}\") {{ id email }} }}",
            target.email
        );
        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(admin.id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );

        let data = response.data.into_json().expect("valid json");
        assert_eq!(data["userByEmail"]["id"], target.id);
    }

    #[tokio::test]
    async fn graphql_user_by_email_requires_admin() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let requester = state
            .user_store
            .create("requester@example.com", &password_hash, Some("Requester"))
            .await
            .expect("create requester");
        let other = state
            .user_store
            .create("other@example.com", &password_hash, Some("Other User"))
            .await
            .expect("create other");

        let schema = graphql::build_schema(state.clone());
        let query = format!(
            "{{ userByEmail(email: \"{}\") {{ id email }} }}",
            other.email
        );
        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(requester.id.clone())))
            .await;

        assert!(!response.errors.is_empty(), "expected authorization error");
        let error = &response.errors[0];
        assert_eq!(error.message, "administrator privileges required");
        let extensions = error.extensions.as_ref().expect("extensions present");
        assert_eq!(
            extensions.get("code"),
            Some(&GraphQLValue::from("FORBIDDEN"))
        );
    }

    #[tokio::test]
    async fn graphql_user_by_email_returns_none_when_missing() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let admin = state
            .user_store
            .create("admin2@example.com", &password_hash, Some("Admin Two"))
            .await
            .expect("create admin");
        state
            .user_store
            .add_admin(&admin.id)
            .await
            .expect("promote admin");

        let schema = graphql::build_schema(state.clone());
        let query = "{ userByEmail(email: \"missing@example.com\") { id email } }".to_string();
        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(admin.id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        assert!(data["userByEmail"].is_null());
    }

    #[tokio::test]
    async fn graphql_user_token_requires_matching_user() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let target = state
            .user_store
            .create("token@example.com", &password_hash, Some("Token"))
            .await
            .expect("create user");
        let requester = state
            .user_store
            .create("requester@example.com", &password_hash, Some("Requester"))
            .await
            .expect("create requester");

        let schema = graphql::build_schema(state.clone());
        let query = format!(
            "{{ userById(id: \"{}\") {{ token {{ token }} }} }}",
            target.id
        );
        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(requester.id.clone())))
            .await;

        assert!(!response.errors.is_empty(), "expected unauthorized error");
        let error = &response.errors[0];
        assert_eq!(error.message, "administrator privileges required");
    }

    #[tokio::test]
    async fn graphql_user_query_returns_limited_profile_when_anonymous() {
        let (_temp_dir, _database, state) = setup_state().await;
        let schema = graphql::build_schema(state.clone());
        let response = schema
            .execute(GraphQLRequest::new("{ currentUser { id email name } }"))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        assert!(data["currentUser"].is_null());
    }

    #[tokio::test]
    async fn graphql_user_query_returns_full_profile_when_authenticated() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("profile@example.com", &password_hash, Some("Profile User"))
            .await
            .expect("create user");

        let schema = graphql::build_schema(state.clone());
        let response = schema
            .execute(
                GraphQLRequest::new("{ currentUser { id email name } }")
                    .data(RequestUser::new(user.id.clone())),
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        assert_eq!(data["currentUser"]["id"], user.id);
        assert_eq!(data["currentUser"]["email"], user.email);
    }

    #[tokio::test]
    async fn graphql_users_supports_query_filter() {
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
            .expect("mark admin");
        state
            .user_store
            .create("filter-one@example.com", &password_hash, Some("Filter One"))
            .await
            .expect("create user");
        state
            .user_store
            .create("other@example.com", &password_hash, Some("Other User"))
            .await
            .expect("create user");

        let schema = graphql::build_schema(state.clone());
        let query = "{ users(filter: { query: \"filter\" }) { email } }";
        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(admin.id.clone())))
            .await;

        assert!(
            response.errors.is_empty(),
            "unexpected errors: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("valid json");
        let users = data["users"].as_array().expect("users array");
        assert_eq!(users.len(), 1);
        assert_eq!(users[0]["email"], "filter-one@example.com");
    }

    #[tokio::test]
    async fn graphql_users_requires_admin() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let requester = state
            .user_store
            .create("requester@example.com", &password_hash, Some("Requester"))
            .await
            .expect("create requester");

        let schema = graphql::build_schema(state.clone());
        let query = "{ users(filter: { query: \"any\" }) { email } }";
        let response = schema
            .execute(GraphQLRequest::new(query).data(RequestUser::new(requester.id.clone())))
            .await;

        assert!(!response.errors.is_empty(), "expected admin requirement");
        let error = &response.errors[0];
        assert_eq!(error.message, "administrator privileges required");
    }

    #[tokio::test]
    async fn graphql_notifications_read_is_idempotent() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create(
                "notification-user@example.com",
                &password_hash,
                Some("Notifier"),
            )
            .await
            .expect("create user");

        let schema = graphql::build_schema(state.clone());

        let now = Utc::now();
        let record = NotificationRecord {
            id: Uuid::new_v4().to_string(),
            user_id: user.id.clone(),
            kind: "comment".to_string(),
            payload: json!({ "workspaceId": "ws", "docId": "doc" }),
            read: false,
            created_at: now,
            updated_at: now,
        };
        state
            .notification_center
            .enqueue(record.clone())
            .await
            .expect("enqueue notification");

        let mutation = format!("mutation {{ readNotification(id: \"{}\") }}", record.id);

        let first = schema
            .execute(GraphQLRequest::new(mutation.clone()).data(RequestUser::new(user.id.clone())))
            .await;
        assert!(
            first.errors.is_empty(),
            "first read errors: {:?}",
            first.errors
        );
        let first_json = first.data.into_json().expect("valid json");
        assert_eq!(first_json["readNotification"].as_bool(), Some(true));

        let second = schema
            .execute(GraphQLRequest::new(mutation.clone()).data(RequestUser::new(user.id.clone())))
            .await;
        assert!(
            second.errors.is_empty(),
            "second read errors: {:?}",
            second.errors
        );
        let second_json = second.data.into_json().expect("valid json");
        assert_eq!(second_json["readNotification"].as_bool(), Some(true));

        let count_query = "{ currentUser { notificationCount } }";
        let count_response = schema
            .execute(GraphQLRequest::new(count_query).data(RequestUser::new(user.id.clone())))
            .await;
        assert!(
            count_response.errors.is_empty(),
            "count errors: {:?}",
            count_response.errors
        );
        let count_json = count_response.data.into_json().expect("valid json");
        assert_eq!(
            count_json["currentUser"]["notificationCount"].as_i64(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn graphql_notifications_pagination_filters_unread() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create(
                "notification-pagination@example.com",
                &password_hash,
                Some("Notifier"),
            )
            .await
            .expect("create user");

        let schema = graphql::build_schema(state.clone());

        let now = Utc::now();
        let older_time = now - Duration::seconds(90);

        let older = NotificationRecord {
            id: Uuid::new_v4().to_string(),
            user_id: user.id.clone(),
            kind: "comment".to_string(),
            payload: json!({ "workspaceId": "ws", "docId": "older" }),
            read: false,
            created_at: older_time,
            updated_at: older_time,
        };
        let newer = NotificationRecord {
            id: Uuid::new_v4().to_string(),
            user_id: user.id.clone(),
            kind: "comment".to_string(),
            payload: json!({ "workspaceId": "ws", "docId": "newer" }),
            read: false,
            created_at: now,
            updated_at: now,
        };

        state
            .notification_center
            .enqueue(older.clone())
            .await
            .expect("enqueue older notification");
        state
            .notification_center
            .enqueue(newer.clone())
            .await
            .expect("enqueue newer notification");

        let first_query = r#"
    {
        currentUser {
            notifications(pagination: { first: 1 }) {
                edges { cursor node { id read } }
                pageInfo { hasNextPage endCursor }
                totalCount
            }
        }
    }
    "#;

        let first_page = schema
            .execute(GraphQLRequest::new(first_query).data(RequestUser::new(user.id.clone())))
            .await;
        assert!(
            first_page.errors.is_empty(),
            "first page errors: {:?}",
            first_page.errors
        );
        let first_json = first_page.data.into_json().expect("valid json");
        let first_edges = first_json["currentUser"]["notifications"]["edges"]
            .as_array()
            .expect("edges array");
        assert_eq!(first_edges.len(), 1);
        let first_id = first_edges[0]["node"]["id"].as_str().expect("edge id");
        assert_eq!(first_id, newer.id);
        assert_eq!(first_edges[0]["node"]["read"].as_bool(), Some(false));
        assert_eq!(
            first_json["currentUser"]["notifications"]["totalCount"].as_i64(),
            Some(2)
        );
        assert_eq!(
            first_json["currentUser"]["notifications"]["pageInfo"]["hasNextPage"].as_bool(),
            Some(true)
        );
        let cursor = first_json["currentUser"]["notifications"]["pageInfo"]["endCursor"]
            .as_str()
            .expect("end cursor")
            .to_string();

        let second_query = format!(
            r#"{{
            currentUser {{
                notifications(pagination: {{ first: 1, after: "{cursor}" }}) {{
                    edges {{ node {{ id }} }}
                    pageInfo {{ hasNextPage }}
                    totalCount
                }}
            }}
        }}"#,
        );

        let second_page = schema
            .execute(GraphQLRequest::new(second_query).data(RequestUser::new(user.id.clone())))
            .await;
        assert!(
            second_page.errors.is_empty(),
            "second page errors: {:?}",
            second_page.errors
        );
        let second_json = second_page.data.into_json().expect("valid json");
        let second_edges = second_json["currentUser"]["notifications"]["edges"]
            .as_array()
            .expect("edges array");
        assert_eq!(second_edges.len(), 1);
        let second_id = second_edges[0]["node"]["id"].as_str().expect("edge id");
        assert_eq!(second_id, older.id);
        assert_eq!(
            second_json["currentUser"]["notifications"]["pageInfo"]["hasNextPage"].as_bool(),
            Some(false)
        );

        let read_mutation = format!("mutation {{ readNotification(id: \"{}\") }}", newer.id);
        let read_response = schema
            .execute(GraphQLRequest::new(read_mutation).data(RequestUser::new(user.id.clone())))
            .await;
        assert!(
            read_response.errors.is_empty(),
            "read errors: {:?}",
            read_response.errors
        );

        let after_read_query = r#"
    {
        currentUser {
            notifications(pagination: { first: 10 }) {
                edges { node { id } }
                pageInfo { hasNextPage }
                totalCount
            }
            notificationCount
        }
    }
    "#;

        let after_read_response = schema
            .execute(GraphQLRequest::new(after_read_query).data(RequestUser::new(user.id.clone())))
            .await;
        assert!(
            after_read_response.errors.is_empty(),
            "after read errors: {:?}",
            after_read_response.errors
        );
        let after_json = after_read_response.data.into_json().expect("valid json");
        assert_eq!(
            after_json["currentUser"]["notifications"]["totalCount"].as_i64(),
            Some(1)
        );
        assert_eq!(
            after_json["currentUser"]["notificationCount"].as_i64(),
            Some(1)
        );
    }
}

impl UserType {
    async fn ensure_notification_access<'ctx>(
        &self,
        ctx: &'ctx Context<'_>,
    ) -> GraphQLResult<&'ctx AppState> {
        let request_user = require_request_user(ctx)?;
        let state = ctx.data::<AppState>()?;

        state
            .user_service
            .ensure_self_or_admin(
                &request_user.user_id,
                &self.record.id,
                "administrator privileges required",
            )
            .await
            .map_err(map_app_error)?;

        Ok(state)
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "LimitedUserType")]
pub(crate) struct LimitedUserType {
    email: String,
    #[graphql(name = "hasPassword")]
    has_password: bool,
}

impl From<&user::UserRecord> for LimitedUserType {
    fn from(record: &user::UserRecord) -> Self {
        Self {
            email: record.email.clone(),
            has_password: !record.password_hash.is_empty(),
        }
    }
}

#[derive(Union)]
#[graphql(name = "UserOrLimitedUser")]
pub(crate) enum UserOrLimitedUser {
    User(UserType),
    Limited(LimitedUserType),
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "tokenType")]
pub(crate) struct TokenType {
    token: String,
    refresh: String,
    #[graphql(name = "sessionToken")]
    session_token: Option<String>,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(name = "NotificationLevel", rename_items = "PascalCase")]
pub(crate) enum NotificationLevel {
    Default,
    High,
    Low,
    Min,
    None,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(name = "NotificationType", rename_items = "PascalCase")]
pub(crate) enum NotificationTypeEnum {
    Comment,
    CommentMention,
    Invitation,
    InvitationAccepted,
    InvitationBlocked,
    InvitationRejected,
    InvitationReviewApproved,
    InvitationReviewDeclined,
    InvitationReviewRequest,
    Mention,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "NotificationBodyPlaceholderType")]
pub(crate) struct NotificationBodyPlaceholderType {
    #[graphql(name = "kind")]
    kind: String,
}

impl Default for NotificationBodyPlaceholderType {
    fn default() -> Self {
        Self {
            kind: "empty".to_string(),
        }
    }
}

#[derive(Union, Clone)]
#[graphql(name = "UnionNotificationBodyType")]
pub(crate) enum UnionNotificationBodyType {
    Placeholder(NotificationBodyPlaceholderType),
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "NotificationObjectType")]
pub(crate) struct NotificationObjectType {
    #[graphql(name = "_placeholderForUnionNotificationBodyType")]
    placeholder: UnionNotificationBodyType,
    body: Json<JsonValue>,
    #[graphql(name = "createdAt")]
    created_at: DateTime<Utc>,
    id: ID,
    level: NotificationLevel,
    read: bool,
    #[graphql(name = "type")]
    notification_type: NotificationTypeEnum,
    #[graphql(name = "updatedAt")]
    updated_at: DateTime<Utc>,
}

impl Default for NotificationObjectType {
    fn default() -> Self {
        Self {
            placeholder: UnionNotificationBodyType::Placeholder(
                NotificationBodyPlaceholderType::default(),
            ),
            body: Json(JsonValue::Null),
            created_at: Utc::now(),
            id: ID::from("0"),
            level: NotificationLevel::Default,
            read: true,
            notification_type: NotificationTypeEnum::Comment,
            updated_at: Utc::now(),
        }
    }
}

impl NotificationObjectType {
    fn from_record(
        record: NotificationRecord,
        notification_type: NotificationTypeEnum,
        payload: JsonValue,
    ) -> Self {
        Self {
            placeholder: UnionNotificationBodyType::Placeholder(
                NotificationBodyPlaceholderType::default(),
            ),
            body: Json(payload),
            created_at: record.created_at,
            id: ID::from(record.id),
            level: NotificationLevel::Default,
            read: record.read,
            notification_type,
            updated_at: record.updated_at,
        }
    }
}

fn notification_type_from_kind(kind: &str) -> NotificationTypeEnum {
    match kind {
        "comment.mention" => NotificationTypeEnum::CommentMention,
        "comment" => NotificationTypeEnum::Comment,
        "invitation" => NotificationTypeEnum::Invitation,
        "invitation.accepted" => NotificationTypeEnum::InvitationAccepted,
        "invitation.blocked" => NotificationTypeEnum::InvitationBlocked,
        "invitation.rejected" => NotificationTypeEnum::InvitationRejected,
        "invitation.review.request" => NotificationTypeEnum::InvitationReviewRequest,
        "invitation.review.approved" => NotificationTypeEnum::InvitationReviewApproved,
        "invitation.review.declined" => NotificationTypeEnum::InvitationReviewDeclined,
        "mention" => NotificationTypeEnum::Mention,
        _ => NotificationTypeEnum::Comment,
    }
}

fn notification_type_str(value: NotificationTypeEnum) -> &'static str {
    match value {
        NotificationTypeEnum::Comment => "Comment",
        NotificationTypeEnum::CommentMention => "CommentMention",
        NotificationTypeEnum::Invitation => "Invitation",
        NotificationTypeEnum::InvitationAccepted => "InvitationAccepted",
        NotificationTypeEnum::InvitationBlocked => "InvitationBlocked",
        NotificationTypeEnum::InvitationRejected => "InvitationRejected",
        NotificationTypeEnum::InvitationReviewApproved => "InvitationReviewApproved",
        NotificationTypeEnum::InvitationReviewDeclined => "InvitationReviewDeclined",
        NotificationTypeEnum::InvitationReviewRequest => "InvitationReviewRequest",
        NotificationTypeEnum::Mention => "Mention",
    }
}

struct NotificationLookups {
    users: HashMap<String, JsonValue>,
    workspaces: HashMap<String, JsonValue>,
}

impl NotificationLookups {
    async fn gather(state: &AppState, notifications: &[NotificationRecord]) -> GraphQLResult<Self> {
        let mut user_ids: HashSet<String> = HashSet::new();
        let mut workspace_ids: HashSet<String> = HashSet::new();

        for record in notifications {
            if let JsonValue::Object(map) = &record.payload {
                if let Some(user_id) = map.get("createdByUserId").and_then(|value| value.as_str()) {
                    user_ids.insert(user_id.to_string());
                }

                if let Some(workspace_id) = map.get("workspaceId").and_then(|value| value.as_str())
                {
                    workspace_ids.insert(workspace_id.to_string());
                }
            }
        }

        let mut users = HashMap::new();
        if !user_ids.is_empty() {
            let ids: Vec<String> = user_ids.into_iter().collect();
            let records = state
                .user_store
                .find_by_ids(&ids)
                .await
                .map_err(map_anyhow)?;

            for record in records {
                let name = display_name_for_user(&record);
                users.insert(
                    record.id.clone(),
                    json!({
                        "id": record.id,
                        "name": name,
                        "avatarUrl": record.avatar_url,
                    }),
                );
            }
        }

        let mut workspaces = HashMap::new();
        if !workspace_ids.is_empty() {
            let ids: Vec<String> = workspace_ids.into_iter().collect();
            let records = state
                .workspace_store
                .find_by_ids(&ids)
                .await
                .map_err(map_anyhow)?;

            let base_url = state.base_url.trim_end_matches('/');
            for record in records {
                let workspace_id = record.id.clone();
                let display_name = WorkspaceService::normalized_workspace_title(&record.name)
                    .unwrap_or(DEFAULT_WORKSPACE_NAME)
                    .to_string();
                let url = format_workspace_url(base_url, &workspace_id);
                workspaces.insert(
                    workspace_id,
                    json!({
                        "id": record.id,
                        "name": display_name,
                        "url": url,
                    }),
                );
            }
        }

        Ok(Self { users, workspaces })
    }

    fn user(&self, user_id: &str) -> Option<&JsonValue> {
        self.users.get(user_id)
    }

    fn workspace(&self, workspace_id: &str) -> Option<&JsonValue> {
        self.workspaces.get(workspace_id)
    }
}

fn format_workspace_url(base_url: &str, workspace_id: &str) -> String {
    if base_url.is_empty() {
        format!("/workspace/{}", workspace_id)
    } else {
        format!("{}/workspace/{}", base_url, workspace_id)
    }
}

fn enrich_notification_payload(
    record: &NotificationRecord,
    notification_type: NotificationTypeEnum,
    lookups: &NotificationLookups,
) -> JsonValue {
    let mut payload = record.payload.clone();

    if let JsonValue::Object(ref mut map) = payload {
        map.entry("type".to_string()).or_insert_with(|| {
            JsonValue::String(notification_type_str(notification_type).to_string())
        });

        if let Some(workspace_id) = map.get("workspaceId").and_then(|value| value.as_str()) {
            if !map.contains_key("workspace") {
                if let Some(info) = lookups.workspace(workspace_id) {
                    map.insert("workspace".to_string(), info.clone());
                }
            }
        }

        if let Some(user_id) = map.get("createdByUserId").and_then(|value| value.as_str()) {
            if !map.contains_key("createdByUser") {
                if let Some(info) = lookups.user(user_id) {
                    map.insert("createdByUser".to_string(), info.clone());
                }
            }
        }

        enrich_notification_doc(map);
    } else {
        payload = json!({
            "type": notification_type_str(notification_type),
            "raw": payload,
        });
    }

    payload
}

fn enrich_notification_doc(map: &mut serde_json::Map<String, JsonValue>) {
    if map.contains_key("doc") {
        return;
    }

    let Some(doc_id) = map.get("docId").and_then(|value| value.as_str()) else {
        return;
    };

    let doc_title = map
        .get("docTitle")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let doc_mode = map
        .get("docMode")
        .and_then(|value| value.as_str())
        .unwrap_or("page");

    let mut doc = json!({
        "id": doc_id,
        "title": doc_title,
        "mode": doc_mode,
    });

    if let JsonValue::Object(ref mut doc_map) = doc {
        if let Some(block_id) = map
            .get("blockId")
            .or_else(|| map.get("docBlockId"))
            .and_then(|value| value.as_str())
        {
            doc_map.insert(
                "blockId".to_string(),
                JsonValue::String(block_id.to_string()),
            );
        }

        if let Some(element_id) = map
            .get("elementId")
            .or_else(|| map.get("docElementId"))
            .and_then(|value| value.as_str())
        {
            doc_map.insert(
                "elementId".to_string(),
                JsonValue::String(element_id.to_string()),
            );
        }
    }

    map.insert("doc".to_string(), doc);
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "NotificationObjectTypeEdge")]
pub(crate) struct NotificationObjectTypeEdge {
    cursor: String,
    node: NotificationObjectType,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "PaginatedNotificationObjectType")]
pub(crate) struct PaginatedNotificationObjectType {
    edges: Vec<NotificationObjectTypeEdge>,
    #[graphql(name = "pageInfo")]
    page_info: PageInfo,
    #[graphql(name = "totalCount")]
    total_count: i32,
}

#[derive(Clone, Debug)]
struct NotificationCursor {
    timestamp: i64,
    id: String,
}

fn encode_notification_cursor(record: &NotificationRecord) -> String {
    let payload = format!("{}:{}", record.created_at.timestamp(), record.id);
    BASE64.encode(payload)
}

fn decode_notification_cursor(cursor: &str) -> GraphQLResult<NotificationCursor> {
    let decoded = decode_cursor_payload_graphql(cursor)?;
    let (timestamp, id) = parse_timestamp_cursor_graphql(&decoded)?;

    Ok(NotificationCursor { timestamp, id })
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "InvoiceStatus")]
pub(crate) enum InvoiceStatus {
    Draft,
    Open,
    Paid,
    Uncollectible,
    Void,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvoiceType")]
pub(crate) struct InvoiceType {
    amount: i32,
    #[graphql(name = "createdAt")]
    created_at: DateTime<Utc>,
    currency: String,
    #[graphql(deprecation = "removed")]
    id: Option<String>,
    #[graphql(name = "lastPaymentError")]
    last_payment_error: Option<String>,
    link: Option<String>,
    #[graphql(deprecation = "removed", name = "plan")]
    plan: Option<SubscriptionPlan>,
    reason: String,
    #[graphql(deprecation = "removed", name = "recurring")]
    recurring: Option<SubscriptionRecurring>,
    status: InvoiceStatus,
    #[graphql(name = "updatedAt")]
    updated_at: DateTime<Utc>,
}

impl Default for InvoiceType {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            amount: 0,
            created_at: now,
            currency: "USD".to_string(),
            id: None,
            last_payment_error: None,
            link: None,
            plan: None,
            reason: "None".to_string(),
            recurring: None,
            status: InvoiceStatus::Draft,
            updated_at: now,
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "UserQuotaHumanReadableType")]
pub(crate) struct UserQuotaHumanReadableType {
    #[graphql(name = "blobLimit")]
    blob_limit: String,
    #[graphql(name = "copilotActionLimit")]
    copilot_action_limit: String,
    #[graphql(name = "historyPeriod")]
    history_period: String,
    #[graphql(name = "memberLimit")]
    member_limit: String,
    name: String,
    #[graphql(name = "storageQuota")]
    storage_quota: String,
    #[graphql(name = "usedStorageQuota")]
    used_storage_quota: String,
}

impl Default for UserQuotaHumanReadableType {
    fn default() -> Self {
        Self {
            blob_limit: format_bytes(FREE_BLOB_LIMIT),
            copilot_action_limit: "0".to_string(),
            history_period: format_history_period(FREE_HISTORY_PERIOD_MS),
            member_limit: FREE_MEMBER_LIMIT.to_string(),
            name: FREE_PLAN_NAME.to_string(),
            storage_quota: format_bytes(FREE_STORAGE_QUOTA),
            used_storage_quota: format_bytes(0),
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "UserQuotaType")]
pub(crate) struct UserQuotaType {
    #[graphql(name = "blobLimit")]
    blob_limit: i64,
    #[graphql(name = "copilotActionLimit")]
    copilot_action_limit: Option<i32>,
    #[graphql(name = "historyPeriod")]
    history_period: i64,
    #[graphql(name = "humanReadable")]
    human_readable: UserQuotaHumanReadableType,
    #[graphql(name = "memberLimit")]
    member_limit: i32,
    name: String,
    #[graphql(name = "storageQuota")]
    storage_quota: i64,
    #[graphql(name = "usedStorageQuota")]
    used_storage_quota: i64,
}

impl Default for UserQuotaType {
    fn default() -> Self {
        Self {
            blob_limit: FREE_BLOB_LIMIT,
            copilot_action_limit: Some(0),
            history_period: FREE_HISTORY_PERIOD_MS,
            human_readable: UserQuotaHumanReadableType::default(),
            member_limit: FREE_MEMBER_LIMIT,
            name: FREE_PLAN_NAME.to_string(),
            storage_quota: FREE_STORAGE_QUOTA,
            used_storage_quota: 0,
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "UserQuotaUsageType")]
pub(crate) struct UserQuotaUsageType {
    #[graphql(name = "storageQuota")]
    storage_quota: i64,
}

impl Default for UserQuotaUsageType {
    fn default() -> Self {
        Self { storage_quota: 0 }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "UserSettingsType")]
pub(crate) struct UserSettingsType {
    #[graphql(name = "receiveCommentEmail")]
    receive_comment_email: bool,
    #[graphql(name = "receiveInvitationEmail")]
    receive_invitation_email: bool,
    #[graphql(name = "receiveMentionEmail")]
    receive_mention_email: bool,
    #[graphql(name = "receiveDocActivityNotification")]
    receive_doc_activity_notification: bool,
    #[graphql(name = "receivePermissionChangeNotification")]
    receive_permission_change_notification: bool,
}

impl From<barffine_core::user_settings::UserNotificationSettings> for UserSettingsType {
    fn from(settings: barffine_core::user_settings::UserNotificationSettings) -> Self {
        Self {
            receive_comment_email: settings.receive_comment_email,
            receive_invitation_email: settings.receive_invitation_email,
            receive_mention_email: settings.receive_mention_email,
            receive_doc_activity_notification: settings.notify_doc_activity,
            receive_permission_change_notification: settings.notify_permission_change,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug, Hash)]
#[graphql(name = "FeatureType")]
pub enum FeatureTypeEnum {
    AIEarlyAccess,
    Admin,
    EarlyAccess,
    FreePlan,
    LifetimeProPlan,
    ProPlan,
    TeamPlan,
    UnlimitedCopilot,
    UnlimitedWorkspace,
}

impl FeatureTypeEnum {
    pub fn as_str(&self) -> &'static str {
        match self {
            FeatureTypeEnum::AIEarlyAccess => "AIEarlyAccess",
            FeatureTypeEnum::Admin => "Admin",
            FeatureTypeEnum::EarlyAccess => "EarlyAccess",
            FeatureTypeEnum::FreePlan => "FreePlan",
            FeatureTypeEnum::LifetimeProPlan => "LifetimeProPlan",
            FeatureTypeEnum::ProPlan => "ProPlan",
            FeatureTypeEnum::TeamPlan => "TeamPlan",
            FeatureTypeEnum::UnlimitedCopilot => "UnlimitedCopilot",
            FeatureTypeEnum::UnlimitedWorkspace => "UnlimitedWorkspace",
        }
    }

    pub fn from_flag_key(key: &str) -> Option<Self> {
        match key {
            "ai_early_access" | "aiEarlyAccess" => Some(Self::AIEarlyAccess),
            "administrator" | "admin" => Some(Self::Admin),
            "early_access" | "earlyAccess" => Some(Self::EarlyAccess),
            "free_plan" | "freePlan" | "free_plan_v1" => Some(Self::FreePlan),
            "lifetime_pro_plan" | "lifetimeProPlan" | "lifetime_pro_plan_v1" => {
                Some(Self::LifetimeProPlan)
            }
            "pro_plan" | "proPlan" | "pro_plan_v1" => Some(Self::ProPlan),
            "team_plan" | "teamPlan" | "team_plan_v1" => Some(Self::TeamPlan),
            "unlimited_copilot" | "unlimitedCopilot" => Some(Self::UnlimitedCopilot),
            "unlimited_workspace" | "unlimitedWorkspace" => Some(Self::UnlimitedWorkspace),
            _ => None,
        }
    }

    pub fn workspace_flag_key(&self) -> Option<&'static str> {
        match self {
            FeatureTypeEnum::TeamPlan => Some("team_plan_v1"),
            FeatureTypeEnum::UnlimitedWorkspace => Some("unlimited_workspace"),
            _ => None,
        }
    }
}
