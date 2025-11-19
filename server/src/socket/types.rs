use std::fmt;
use std::sync::Arc;

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use dashmap::DashMap;
use serde::Serialize;
use serde_json::Value as JsonValue;
use tracing::Span;
use uuid::Uuid;

use crate::{
    error::{AppError, UserFriendlyPayload},
    socket::rooms::SpaceType,
    types::{AuthenticatedRestSession, SessionUser},
};

#[derive(Clone)]
pub struct SocketUserContext {
    pub user: SessionUser,
    pub user_id: String,
    pub cookie_header: Option<String>,
    pub access_token: Option<String>,
    pub authorization_header: Option<String>,
}

impl SocketUserContext {
    pub fn new(
        auth: &AuthenticatedRestSession,
        cookie_header: Option<String>,
        access_token: Option<String>,
        authorization_header: Option<String>,
    ) -> Self {
        let user = SessionUser::from(&auth.user);

        Self {
            user,
            user_id: auth.user.id.clone(),
            cookie_header,
            access_token,
            authorization_header,
        }
    }

    pub fn header_map(&self) -> Result<HeaderMap, AppError> {
        let mut headers = HeaderMap::new();

        if let Some(cookie) = self.cookie_header.as_ref() {
            let value = HeaderValue::from_str(cookie)
                .map_err(|_| AppError::bad_request("invalid cookie header"))?;
            headers.insert(HeaderName::from_static("cookie"), value);
        }

        if let Some(token) = self.access_token.as_ref() {
            let header_value = HeaderValue::from_str(token)
                .map_err(|_| AppError::bad_request("invalid access token"))?;
            headers.insert(
                HeaderName::from_static("x-access-token"),
                header_value.clone(),
            );
        }

        if let Some(auth_header) = self.authorization_header.as_ref() {
            let auth_value = HeaderValue::from_str(auth_header)
                .map_err(|_| AppError::bad_request("invalid authorization header"))?;
            headers.insert(HeaderName::from_static("authorization"), auth_value);
        }

        Ok(headers)
    }
}

#[derive(Clone)]
pub struct SocketRequestContext {
    pub request_id: String,
}

impl SocketRequestContext {
    pub fn new(request_id: Option<&str>) -> Self {
        let id = request_id
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string())
            .unwrap_or_else(|| format!("ws-{}", Uuid::new_v4().simple()));

        Self { request_id: id }
    }
}

#[derive(Clone)]
pub struct SocketSpanRegistry {
    root_span: Span,
    spaces: Arc<DashMap<(SpaceType, String), Span>>,
}

impl SocketSpanRegistry {
    pub fn new(root_span: Span) -> Self {
        Self {
            root_span,
            spaces: Arc::new(DashMap::new()),
        }
    }

    pub fn root_span(&self) -> Span {
        self.root_span.clone()
    }

    pub fn space_span(&self, space_type: SpaceType, space_id: &str) -> Option<Span> {
        self.spaces
            .get(&(space_type, space_id.to_string()))
            .map(|span| span.clone())
    }

    pub fn insert_space_span(&self, space_type: SpaceType, space_id: &str, span: Span) {
        self.spaces.insert((space_type, space_id.to_string()), span);
    }

    pub fn remove_space_span(&self, space_type: SpaceType, space_id: &str) {
        self.spaces.remove(&(space_type, space_id.to_string()));
    }
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum SocketAck<T> {
    Data { data: T },
    Error { error: SocketAckError },
}

impl<T> SocketAck<T> {
    pub fn ok(data: T) -> Self {
        SocketAck::Data { data }
    }

    pub fn from_error(error: AppError, request_id: Option<&str>) -> Self {
        SocketAck::Error {
            error: SocketAckError::from_app_error(error, request_id),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SocketAckError {
    pub status: u16,
    pub code: String,
    #[serde(rename = "type")]
    pub error_type: String,
    pub name: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonValue>,
    #[serde(rename = "requestId", skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl SocketAckError {
    pub fn from_app_error(error: AppError, request_id: Option<&str>) -> Self {
        let (status, payload) = error.into_payload();
        Self::from_payload(payload, status.as_u16(), request_id)
    }

    fn from_payload(payload: UserFriendlyPayload, status: u16, request_id: Option<&str>) -> Self {
        Self {
            status,
            code: payload.code,
            error_type: payload.error_type,
            name: payload.name,
            message: payload.message,
            data: payload.data,
            request_id: request_id.map(|id| id.to_string()),
        }
    }
}

impl fmt::Display for SocketAckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
