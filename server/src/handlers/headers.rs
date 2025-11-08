use crate::graphql::{DocRole, Permission};

pub(crate) const HEADER_USER_ID: &str = "x-affine-user-id";
pub(crate) const HEADER_WORKSPACE_ID: &str = "x-affine-workspace-id";
pub(crate) const HEADER_WORKSPACE_ROLE: &str = "x-affine-workspace-role";
pub(crate) const HEADER_DOC_ID: &str = "x-affine-doc-id";
pub(crate) const HEADER_DOC_ROLE: &str = "x-affine-doc-role";

pub(crate) const HEADER_PUBLISH_MODE: &str = "publish-mode";

pub(crate) fn permission_header_value(permission: Permission) -> &'static str {
    match permission {
        Permission::Owner => "OWNER",
        Permission::Admin => "ADMIN",
        Permission::Collaborator => "COLLABORATOR",
        Permission::External => "EXTERNAL",
    }
}

pub(crate) fn doc_role_header_value(role: DocRole) -> &'static str {
    match role {
        DocRole::Owner => "OWNER",
        DocRole::Manager => "MANAGER",
        DocRole::Editor => "EDITOR",
        DocRole::Commenter => "COMMENTER",
        DocRole::Reader => "READER",
        DocRole::External => "EXTERNAL",
        DocRole::None => "NONE",
    }
}
