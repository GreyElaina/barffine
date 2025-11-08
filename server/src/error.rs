use std::fmt;

use anyhow::Error as AnyError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use tracing::error;

#[derive(Debug, Clone, Copy)]
struct ErrorDescriptor {
    status: StatusCode,
    name: &'static str,
    error_type: &'static str,
    default_message: &'static str,
}

const BAD_REQUEST_DESCRIPTOR: ErrorDescriptor = ErrorDescriptor {
    status: StatusCode::BAD_REQUEST,
    name: "BAD_REQUEST",
    error_type: "BAD_REQUEST",
    default_message: "Bad request.",
};

const UNAUTHORIZED_DESCRIPTOR: ErrorDescriptor = ErrorDescriptor {
    status: StatusCode::UNAUTHORIZED,
    name: "AUTHENTICATION_REQUIRED",
    error_type: "AUTHENTICATION_REQUIRED",
    default_message: "You must sign in first to access this resource.",
};

const CONFLICT_DESCRIPTOR: ErrorDescriptor = ErrorDescriptor {
    status: StatusCode::CONFLICT,
    name: "RESOURCE_ALREADY_EXISTS",
    error_type: "RESOURCE_ALREADY_EXISTS",
    default_message: "Resource already exists.",
};

const NOT_FOUND_DESCRIPTOR: ErrorDescriptor = ErrorDescriptor {
    status: StatusCode::NOT_FOUND,
    name: "NOT_FOUND",
    error_type: "RESOURCE_NOT_FOUND",
    default_message: "Resource not found.",
};

const FORBIDDEN_DESCRIPTOR: ErrorDescriptor = ErrorDescriptor {
    status: StatusCode::FORBIDDEN,
    name: "ACTION_FORBIDDEN",
    error_type: "ACTION_FORBIDDEN",
    default_message: "Action forbidden.",
};

const INTERNAL_SERVER_ERROR_DESCRIPTOR: ErrorDescriptor = ErrorDescriptor {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    name: "INTERNAL_SERVER_ERROR",
    error_type: "INTERNAL_SERVER_ERROR",
    default_message: "An internal error occurred.",
};

#[derive(Debug)]
pub struct AppError {
    descriptor: &'static ErrorDescriptor,
    name: String,
    error_type: String,
    message: String,
    data: Option<JsonValue>,
    source: Option<AnyError>,
}

impl AppError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::from_descriptor(&BAD_REQUEST_DESCRIPTOR, Some(message.into()))
    }

    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self::from_descriptor(&UNAUTHORIZED_DESCRIPTOR, Some(message.into()))
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self::from_descriptor(&FORBIDDEN_DESCRIPTOR, Some(message.into()))
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self::from_descriptor(&CONFLICT_DESCRIPTOR, Some(message.into()))
    }

    #[allow(dead_code)]
    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::from_descriptor(&NOT_FOUND_DESCRIPTOR, Some(message.into()))
    }

    pub(crate) fn internal(error: AnyError) -> Self {
        error!(?error, "internal server error");
        Self::from_descriptor(&INTERNAL_SERVER_ERROR_DESCRIPTOR, None).with_source(error)
    }

    pub(crate) fn from_anyhow(error: AnyError) -> Self {
        Self::internal(error)
    }

    pub(crate) fn doc_not_found(space_id: &str, doc_id: &str) -> Self {
        let space_id = space_id.to_owned();
        let doc_id = doc_id.to_owned();
        let message = format!("Doc {doc_id} under Space {space_id} not found.");

        Self::from_descriptor(&NOT_FOUND_DESCRIPTOR, Some(message))
            .with_name("DOC_NOT_FOUND")
            .with_data(json!({ "spaceId": space_id, "docId": doc_id }))
    }

    pub(crate) fn workspace_not_found(space_id: &str) -> Self {
        let space_id = space_id.to_owned();
        let message = format!("Space {space_id} not found.");

        Self::from_descriptor(&NOT_FOUND_DESCRIPTOR, Some(message))
            .with_name("SPACE_NOT_FOUND")
            .with_data(json!({ "spaceId": space_id }))
    }

    pub(crate) fn blob_not_found(space_id: &str, blob_id: &str) -> Self {
        let space_id = space_id.to_owned();
        let blob_id = blob_id.to_owned();
        let message = format!("Blob {blob_id} not found in Space {space_id}.");

        Self::from_descriptor(&NOT_FOUND_DESCRIPTOR, Some(message))
            .with_name("BLOB_NOT_FOUND")
            .with_data(json!({ "spaceId": space_id, "blobId": blob_id }))
    }

    pub(crate) fn comment_attachment_not_found(space_id: &str, doc_id: &str, key: &str) -> Self {
        let space_id = space_id.to_owned();
        let doc_id = doc_id.to_owned();
        let key = key.to_owned();
        let message =
            format!("Comment attachment {key} not found for Doc {doc_id} under Space {space_id}.");

        Self::from_descriptor(&NOT_FOUND_DESCRIPTOR, Some(message))
            .with_name("COMMENT_ATTACHMENT_NOT_FOUND")
            .with_data(json!({ "spaceId": space_id, "docId": doc_id, "key": key }))
    }

    pub(crate) fn space_access_denied(space_id: &str) -> Self {
        let space_id = space_id.to_owned();
        let message = format!("You do not have permission to access Space {space_id}.");

        Self::from_descriptor(&FORBIDDEN_DESCRIPTOR, Some(message))
            .with_name("SPACE_ACCESS_DENIED")
            .with_error_type("NO_PERMISSION")
            .with_data(json!({ "spaceId": space_id }))
    }

    pub(crate) fn doc_update_blocked(space_id: &str, doc_id: &str) -> Self {
        let space_id = space_id.to_owned();
        let doc_id = doc_id.to_owned();
        let message = format!("Doc {doc_id} under Space {space_id} is blocked from updating.");

        Self::from_descriptor(&FORBIDDEN_DESCRIPTOR, Some(message))
            .with_name("DOC_UPDATE_BLOCKED")
            .with_error_type("DOC_UPDATE_BLOCKED")
            .with_data(json!({ "spaceId": space_id, "docId": doc_id }))
    }

    pub(crate) fn not_in_space(space_id: &str) -> Self {
        let space_id = space_id.to_owned();
        let message = format!("Socket must join space {space_id} before performing this action.");

        Self::from_descriptor(&FORBIDDEN_DESCRIPTOR, Some(message))
            .with_name("NOT_IN_SPACE")
            .with_error_type("NOT_IN_SPACE")
            .with_data(json!({ "spaceId": space_id }))
    }

    pub(crate) fn invalid_internal_request() -> Self {
        Self::from_descriptor(
            &FORBIDDEN_DESCRIPTOR,
            Some("Invalid internal request".to_string()),
        )
        .with_name("ACCESS_DENIED")
        .with_error_type("NO_PERMISSION")
    }

    pub(crate) fn into_payload(self) -> (StatusCode, UserFriendlyPayload) {
        let AppError {
            descriptor,
            name,
            error_type,
            message,
            data,
            source: _,
        } = self;

        let status = descriptor.status;
        let (code, reason) = code_and_reason(status);
        let payload = UserFriendlyPayload {
            status: status.as_u16(),
            code,
            reason,
            error_type,
            name,
            message,
            data,
        };

        (status, payload)
    }

    fn from_descriptor(descriptor: &'static ErrorDescriptor, message: Option<String>) -> Self {
        Self {
            descriptor,
            name: descriptor.name.to_owned(),
            error_type: descriptor.error_type.to_owned(),
            message: message.unwrap_or_else(|| descriptor.default_message.to_owned()),
            data: None,
            source: None,
        }
    }

    fn with_source(mut self, error: AnyError) -> Self {
        self.source = Some(error);
        self
    }

    pub(crate) fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub(crate) fn with_error_type(mut self, error_type: impl Into<String>) -> Self {
        self.error_type = error_type.into();
        self
    }

    pub(crate) fn with_data(mut self, data: JsonValue) -> Self {
        self.data = Some(data);
        self
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, payload) = self.into_payload();
        (status, Json(payload)).into_response()
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct UserFriendlyPayload {
    pub(crate) status: u16,
    pub(crate) code: String,
    pub(crate) reason: String,
    #[serde(rename = "type")]
    pub(crate) error_type: String,
    pub(crate) name: String,
    pub(crate) message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<JsonValue>,
}

fn code_and_reason(status: StatusCode) -> (String, String) {
    let reason = status
        .canonical_reason()
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("Status {}", status.as_u16()));

    let code = reason
        .chars()
        .map(|ch| match ch {
            'a'..='z' => ch.to_ascii_uppercase(),
            'A'..='Z' | '0'..='9' => ch,
            ' ' | '-' | '/' | '\\' => '_',
            _ => '_',
        })
        .collect::<String>();

    (code, reason)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn http_error_payload_matches_contract() {
        let response = AppError::bad_request("email must not be empty").into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(json["status"], 400);
        assert_eq!(json["code"], "BAD_REQUEST");
        assert_eq!(json["reason"], "Bad Request");
        assert_eq!(json["type"], "BAD_REQUEST");
        assert_eq!(json["name"], "BAD_REQUEST");
        assert_eq!(json["message"], "email must not be empty");
        assert!(json.get("data").is_none());
    }

    #[tokio::test]
    async fn doc_not_found_error_includes_domain_metadata() {
        let response = AppError::doc_not_found("space-123", "doc-456").into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(json["status"], 404);
        assert_eq!(json["code"], "NOT_FOUND");
        assert_eq!(json["reason"], "Not Found");
        assert_eq!(json["type"], "RESOURCE_NOT_FOUND");
        assert_eq!(json["name"], "DOC_NOT_FOUND");
        assert_eq!(
            json["message"],
            "Doc doc-456 under Space space-123 not found."
        );

        let data = json["data"].as_object().expect("data present");
        assert_eq!(
            data.get("spaceId"),
            Some(&serde_json::Value::String("space-123".into()))
        );
        assert_eq!(
            data.get("docId"),
            Some(&serde_json::Value::String("doc-456".into()))
        );
    }

    #[tokio::test]
    async fn space_access_denied_error_uses_forbidden_contract() {
        let response = AppError::space_access_denied("space-abc").into_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(json["status"], 403);
        assert_eq!(json["code"], "FORBIDDEN");
        assert_eq!(json["reason"], "Forbidden");
        assert_eq!(json["type"], "NO_PERMISSION");
        assert_eq!(json["name"], "SPACE_ACCESS_DENIED");
        assert_eq!(
            json["message"],
            "You do not have permission to access Space space-abc."
        );

        let data = json["data"].as_object().expect("data present");
        assert_eq!(
            data.get("spaceId"),
            Some(&serde_json::Value::String("space-abc".into()))
        );
    }
}
