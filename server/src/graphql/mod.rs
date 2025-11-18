mod constants;
mod context;
pub(crate) mod copilot;
mod cursors;
mod doc;
mod handlers;
mod helpers;
mod metrics;
mod mutation;
mod query;
mod schema;
mod types;
mod user;
mod workspace;

pub use context::RequestUser;
pub use handlers::{graphql_handler, graphql_options_handler, graphql_playground};
pub use mutation::MutationRoot;
pub use query::QueryRoot;
pub use schema::{BarffineSchema, build_schema};

pub(crate) use copilot::*;
pub(crate) use doc::*;
pub(crate) use helpers::*;
pub(crate) use types::*;
pub(crate) use user::*;
pub(crate) use workspace::*;

pub(crate) use crate::{
    auth::generate_password_hash,
    cookies::{build_session_cookie, build_user_cookie, clear_session_cookie, clear_user_cookie},
};
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Error as AnyError;
    use barffine_core::{
        doc_roles::DocumentRoleRecord, doc_store::DocumentMetadata, feature::FeatureNamespace,
    };
    // use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
    use super::constants::FREE_MEMBER_LIMIT;
    use crate::AppError;
    use serde_json::to_value;

    #[test]
    fn graphql_error_extensions_match_user_friendly_contract() {
        let gql_error = map_app_error(AppError::bad_request("invalid input"));
        assert_eq!(gql_error.message, "invalid input");

        let extensions = gql_error.extensions.clone().expect("extensions present");
        let ext_json = to_value(extensions).unwrap();

        assert_eq!(ext_json["status"], 400);
        assert_eq!(ext_json["code"], "BAD_REQUEST");
        assert_eq!(ext_json["reason"], "Bad Request");
        assert_eq!(ext_json["type"], "BAD_REQUEST");
        assert_eq!(ext_json["name"], "BAD_REQUEST");
        assert_eq!(ext_json["message"], "invalid input");
    }

    #[test]
    fn map_anyhow_masks_internal_error_details() {
        let gql_error = map_anyhow(AnyError::msg("boom"));
        assert_eq!(gql_error.message, "An internal error occurred.");

        let extensions = gql_error.extensions.clone().expect("extensions present");
        let ext_json = to_value(extensions).unwrap();

        assert_eq!(ext_json["status"], 500);
        assert_eq!(ext_json["code"], "INTERNAL_SERVER_ERROR");
        assert_eq!(ext_json["name"], "INTERNAL_SERVER_ERROR");
        assert_eq!(ext_json["type"], "INTERNAL_SERVER_ERROR");
        assert_eq!(ext_json["message"], "An internal error occurred.");
    }

    #[test]
    fn doc_not_found_extensions_include_doc_metadata() {
        let gql_error = map_app_error(AppError::doc_not_found("space-42", "doc-9"));

        assert_eq!(
            gql_error.message,
            "Doc doc-9 under Space space-42 not found."
        );

        let extensions = gql_error.extensions.clone().expect("extensions present");
        let ext_json = to_value(extensions).unwrap();

        assert_eq!(ext_json["status"], 404);
        assert_eq!(ext_json["code"], "NOT_FOUND");
        assert_eq!(ext_json["reason"], "Not Found");
        assert_eq!(ext_json["type"], "RESOURCE_NOT_FOUND");
        assert_eq!(ext_json["name"], "DOC_NOT_FOUND");
        assert_eq!(
            ext_json["message"],
            "Doc doc-9 under Space space-42 not found."
        );

        let data = ext_json["data"].as_object().expect("data present");
        assert_eq!(
            data.get("spaceId"),
            Some(&serde_json::Value::String("space-42".into()))
        );
        assert_eq!(
            data.get("docId"),
            Some(&serde_json::Value::String("doc-9".into()))
        );
    }

    #[test]
    fn workspace_not_found_extensions_include_workspace_metadata() {
        let gql_error = map_app_error(AppError::workspace_not_found("space-404"));

        assert_eq!(gql_error.message, "Space space-404 not found.");

        let extensions = gql_error.extensions.clone().expect("extensions present");
        let ext_json = to_value(extensions).unwrap();

        assert_eq!(ext_json["status"], 404);
        assert_eq!(ext_json["code"], "NOT_FOUND");
        assert_eq!(ext_json["reason"], "Not Found");
        assert_eq!(ext_json["type"], "RESOURCE_NOT_FOUND");
        assert_eq!(ext_json["name"], "SPACE_NOT_FOUND");
        assert_eq!(ext_json["message"], "Space space-404 not found.");

        let data = ext_json["data"].as_object().expect("data present");
        assert_eq!(
            data.get("spaceId"),
            Some(&serde_json::Value::String("space-404".into()))
        );
    }

    #[test]
    fn parse_feature_namespace_accepts_scopes() {
        assert!(matches!(
            parse_feature_namespace("global"),
            Ok(FeatureNamespace::Global)
        ));

        match parse_feature_namespace("workspace:ws-1").expect("workspace namespace") {
            FeatureNamespace::Workspace(id) => assert_eq!(id, "ws-1"),
            _ => panic!("expected workspace namespace"),
        }

        match parse_feature_namespace("user:user-1").expect("user namespace") {
            FeatureNamespace::User(id) => assert_eq!(id, "user-1"),
            _ => panic!("expected user namespace"),
        }

        let err = parse_feature_namespace("").expect_err("empty namespace invalid");
        let (_, payload) = err.into_payload();
        assert_eq!(payload.message, "invalid feature namespace");
    }

    #[test]
    fn workspace_role_mapping_respects_aliases() {
        assert_eq!(workspace_role_from_str("owner"), Permission::Owner);
        assert_eq!(workspace_role_from_str("ADMIN"), Permission::Admin);
        assert_eq!(workspace_role_from_str("member"), Permission::Collaborator);
        assert_eq!(workspace_role_from_str("viewer"), Permission::External);
        assert_eq!(workspace_role_from_str("unknown"), Permission::External);
    }

    #[test]
    fn list_user_input_applies_defaults_and_clamps() {
        let defaults = ListUserInput::default();
        let (limit, skip, keyword, cursor) = defaults.normalize().expect("defaults valid");
        assert_eq!(limit, 20);
        assert_eq!(skip, 0);
        assert!(keyword.is_none());
        assert!(cursor.is_none());

        let custom = ListUserInput::for_test(Some(5), Some(250), Some("  Search  ".into()), None);
        let (limit, skip, keyword, cursor) = custom.normalize().expect("custom input valid");
        assert_eq!(limit, 100);
        assert_eq!(skip, 5);
        assert_eq!(keyword.as_deref(), Some("Search"));
        assert!(cursor.is_none());
    }

    // #[test]
    // fn list_user_input_rejects_offset_with_after() {
    //     let cursor = BASE64.encode("123:abc");
    //     let input = ListUserInput::for_test(Some(1), Some(10), None, Some(cursor));
    //     let err = input.normalize().expect_err("offset + after invalid");
    //     assert_eq!(
    //         err.message,
    //         "offset pagination is not supported when using after cursors"
    //     );
    // }

    // #[test]
    // fn user_cursor_roundtrip() {
    //     let record = core_user::UserRecord {
    //         id: "user-123".into(),
    //         email: "user@example.com".into(),
    //         password_hash: "hash".into(),
    //         name: None,
    //         avatar_url: None,
    //         email_verified_at: None,
    //         disabled: false,
    //         created_at: 1_695_000,
    //     };

    //     let encoded = encode_user_cursor(&record);
    //     let decoded = decode_user_cursor(&encoded).expect("cursor decodes");
    //     assert_eq!(decoded.id, record.id);
    //     assert_eq!(decoded.created_at, record.created_at);
    // }

    // #[test]
    // fn decode_user_cursor_validates_payload() {
    //     let err = decode_user_cursor("%%%%").expect_err("invalid encoding");
    //     assert_eq!(err.message, "invalid cursor encoding");

    //     let err = decode_user_cursor("MTIz").expect_err("missing id");
    //     assert_eq!(err.message, "invalid cursor payload");
    // }

    #[test]
    fn list_user_input_rejects_negative_skip() {
        let input = ListUserInput::for_test(Some(-1), None, None, None);

        let err = input.normalize().expect_err("skip must be non-negative");
        let (_, payload) = err.into_payload();
        assert_eq!(payload.message, "skip must be non-negative");
    }

    #[test]
    fn workspace_permissions_for_external_are_read_only() {
        let perms = WorkspacePermissions::from_role(Permission::External);
        assert!(perms.workspace_read);
        assert!(perms.workspace_blobs_read);
        assert!(perms.workspace_organize_read);
        assert!(perms.workspace_properties_read);
        assert!(!perms.workspace_create_doc);
        assert!(!perms.workspace_users_read);
        assert!(!perms.workspace_settings_read);
    }

    #[test]
    fn workspace_permissions_for_collaborator_allow_creation() {
        let perms = WorkspacePermissions::from_role(Permission::Collaborator);
        assert!(perms.workspace_read);
        assert!(perms.workspace_create_doc);
        assert!(perms.workspace_sync);
        assert!(perms.workspace_users_read);
        assert!(perms.workspace_settings_read);
        assert!(!perms.workspace_users_manage);
        assert!(!perms.workspace_delete);
    }

    #[test]
    fn workspace_permissions_for_owner_enable_administration() {
        let perms = WorkspacePermissions::from_role(Permission::Owner);
        assert!(perms.workspace_users_manage);
        assert!(perms.workspace_delete);
        assert!(perms.workspace_transfer_owner);
        assert!(perms.workspace_administrators_manage);
        assert!(perms.workspace_payment_manage);
    }

    #[test]
    fn build_workspace_quota_uses_free_defaults() {
        let quota = super::build_workspace_quota(2);
        assert_eq!(quota.name, "Free");
        assert_eq!(quota.member_limit, FREE_MEMBER_LIMIT);
        assert_eq!(quota.member_count, 2);
        assert_eq!(quota.overcapacity_member_count, 0);
        assert_eq!(quota.human_readable.name, "Free");
        assert_eq!(
            quota.human_readable.member_limit,
            FREE_MEMBER_LIMIT.to_string()
        );
        assert_eq!(quota.used_storage_quota, 0);
    }

    #[test]
    fn build_workspace_quota_tracks_overcapacity() {
        let quota = super::build_workspace_quota(FREE_MEMBER_LIMIT + 4);
        assert_eq!(quota.overcapacity_member_count, 4);
        assert_eq!(quota.human_readable.overcapacity_member_count, "4");
    }

    #[test]
    fn doc_cursor_roundtrip() {
        let metadata = DocumentMetadata {
            id: "doc-1".to_string().into(),
            workspace_id: "ws-1".to_string().into(),
            created_at: 12_000,
            updated_at: 12_345,
            default_role: "manager".to_string(),
            public: true,
            blocked: false,
            mode: "page".to_string(),
            title: Some("Doc".to_string()),
            summary: Some("Summary".to_string()),
            creator_id: Some("user-1".to_string()),
            updater_id: Some("user-2".to_string()),
            share_token: Some("token-123".to_string()),
            trashed_at: None,
            trashed_by: None,
            snapshot: None,
        };

        let created_cursor = super::encode_doc_cursor(&metadata, DocCursorKind::CreatedAt);
        let created_decoded = super::decode_doc_cursor(&created_cursor).expect("cursor decodes");
        assert_eq!(created_decoded.timestamp, metadata.created_at);
        assert_eq!(created_decoded.id, metadata.id);

        let updated_cursor = super::encode_doc_cursor(&metadata, DocCursorKind::UpdatedAt);
        let updated_decoded = super::decode_doc_cursor(&updated_cursor).expect("cursor decodes");
        assert_eq!(updated_decoded.timestamp, metadata.updated_at);
        assert_eq!(updated_decoded.id, metadata.id);
    }

    #[test]
    fn granted_user_cursor_roundtrip() {
        use barffine_core::ids::{DocId, UserId, WorkspaceId};

        let record = DocumentRoleRecord {
            workspace_id: WorkspaceId::from("ws-1"),
            doc_id: DocId::from("doc-1"),
            user_id: UserId::from("user-9"),
            role: "editor".to_string(),
            created_at: 12_345,
        };

        let encoded = super::encode_granted_user_cursor(&record);
        let decoded = super::decode_granted_user_cursor(&encoded).expect("cursor decodes");

        assert_eq!(decoded.created_at, record.created_at);
        assert_eq!(decoded.user_id, record.user_id);
    }

    #[test]
    fn doc_cursor_invalid_encoding_returns_error() {
        let err = super::decode_doc_cursor("%%%%").expect_err("invalid cursor");
        let (_, payload) = err.into_payload();
        assert_eq!(payload.message, "invalid cursor encoding");

        let err = super::decode_doc_cursor("MTA6").expect_err("missing id");
        let (_, payload) = err.into_payload();
        assert_eq!(payload.message, "invalid cursor payload");
    }

    #[test]
    fn doc_role_from_str_maps_known_roles() {
        assert!(matches!(super::doc_role_from_str("owner"), DocRole::Owner));
        assert!(matches!(
            super::doc_role_from_str("admin"),
            DocRole::Manager
        ));
        assert!(matches!(
            super::doc_role_from_str("manager"),
            DocRole::Manager
        ));
        assert!(matches!(
            super::doc_role_from_str("viewer"),
            DocRole::Reader
        ));
        assert!(matches!(
            super::doc_role_from_str("commenter"),
            DocRole::Commenter
        ));
        assert!(matches!(
            super::doc_role_from_str("unknown"),
            DocRole::Editor
        ));
    }

    #[test]
    fn fixup_doc_role_escalates_workspace_owner() {
        let resolved = super::fixup_doc_role(Some(Permission::Owner), Some(DocRole::Editor));
        assert!(matches!(resolved, Some(DocRole::Owner)));
    }

    #[test]
    fn fixup_doc_role_clamps_external_roles() {
        let resolved = super::fixup_doc_role(Some(Permission::External), Some(DocRole::Manager));
        assert!(matches!(resolved, Some(DocRole::Editor)));
    }

    #[test]
    fn fixup_doc_role_promotes_admin_minimum_role() {
        let resolved = super::fixup_doc_role(Some(Permission::Admin), Some(DocRole::Editor));
        assert!(matches!(resolved, Some(DocRole::Manager)));
    }
}
