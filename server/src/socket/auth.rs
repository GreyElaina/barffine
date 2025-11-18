use std::{borrow::Cow, str, sync::Arc};

use axum::http::{
    HeaderMap, HeaderName, HeaderValue,
    header::{AUTHORIZATION, COOKIE},
};
use serde_json::Value as JsonValue;
use socketioxide::SocketIo;
use socketioxide::adapter::Adapter;
use socketioxide::handler::{ConnectMiddleware, Value};
use socketioxide::layer::SocketIoLayer;
use tracing::{error, info, warn};

use crate::{
    auth::authenticate_rest_request,
    cookies::{SESSION_COOKIE_NAME, USER_COOKIE_NAME},
    error::AppError,
    socket::types::{SocketRequestContext, SocketSpanRegistry, SocketUserContext},
    state::{AppState, SocketRuntimeState},
};

pub(crate) fn build_socket(
    runtime: Arc<SocketRuntimeState>,
) -> (SocketIoLayer, SocketIo) {
    SocketIo::builder()
        .with_state(runtime.clone())
        .max_payload(100_000_000)
        .max_buffer_size(16_384)
        .build_layer()
}

#[derive(Clone)]
pub(crate) struct SocketAuthMiddleware {
    state: Arc<AppState>,
    runtime: Arc<SocketRuntimeState>,
}

impl SocketAuthMiddleware {
    pub fn new(state: Arc<AppState>, runtime: Arc<SocketRuntimeState>) -> Self {
        Self { state, runtime }
    }

    fn build_header_map(
        parts: &HeaderMap,
        cookies: Option<&str>,
        access_token: Option<&str>,
    ) -> Result<HeaderMap, AppError> {
        let mut headers = HeaderMap::new();

        for (name, value) in parts.iter() {
            if name == COOKIE {
                continue;
            }
            headers.append(name.clone(), value.clone());
        }

        if let Some(header_value) = cookies {
            let value = HeaderValue::from_str(header_value)
                .map_err(|_| AppError::bad_request("invalid cookie header"))?;
            headers.insert(HeaderName::from_static("cookie"), value);
        }

        if let Some(token) = access_token {
            let value = HeaderValue::from_str(token)
                .map_err(|_| AppError::bad_request("invalid access token"))?;
            headers.insert(HeaderName::from_static("x-access-token"), value);
        }

        Ok(headers)
    }

    fn parse_handshake_auth(auth: Option<&Value>, query: Option<&str>) -> HandshakeAuth {
        let mut payload = HandshakeAuth::default();

        if let Some(value) = auth {
            if let Some(as_str) = value.as_str() {
                payload.ingest_str(as_str.as_ref());
            } else if let Some(bytes) = value.as_bytes() {
                if let Ok(text) = str::from_utf8(bytes.as_ref()) {
                    payload.ingest_str(text);
                }
            }
        }

        if let Some(q) = query {
            payload.ingest_str(q);
        }

        payload
    }

    fn merge_cookies(a: Option<&str>, b: Option<&str>) -> Option<String> {
        match (a, b) {
            (Some(existing), Some(new)) if !existing.is_empty() && !new.is_empty() => {
                Some(format!("{existing}; {new}"))
            }
            (Some(existing), _) if !existing.is_empty() => Some(existing.to_string()),
            (_, Some(new)) if !new.is_empty() => Some(new.to_string()),
            _ => None,
        }
    }

    fn format_error(error: AppError, request_id: Option<&str>) -> String {
        let (status, payload) = error.into_payload();
        match serde_json::to_string(&serde_json::json!({
            "status": status.as_u16(),
            "code": payload.code,
            "type": payload.error_type,
            "name": payload.name,
            "message": payload.message,
            "data": payload.data,
            "requestId": request_id,
        })) {
            Ok(serialized) => serialized,
            Err(err) => {
                error!(?err, "failed to serialize websocket auth error");
                payload.message
            }
        }
    }
}

impl<A> ConnectMiddleware<A, ()> for SocketAuthMiddleware
where
    A: Adapter + 'static,
{
    fn call<'a>(
        &'a self,
        socket: Arc<socketioxide::socket::Socket<A>>,
        _auth: &'a Option<Value>,
    ) -> impl futures_util::Future<
        Output = Result<(), Box<dyn std::fmt::Display + std::marker::Send + 'static>>,
    > + std::marker::Send {
        let state = self.state.clone();
        let runtime = self.runtime.clone();

        Box::pin(async move {
            let parts = socket.req_parts();
            let cookie_header = parts
                .headers
                .get(COOKIE)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string());
            let access_token = parts
                .headers
                .get("x-access-token")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string());
            let query_params = parts.uri.query();
            let handshake_auth = Self::parse_handshake_auth(_auth.as_ref(), query_params);

            let handshake_cookie = handshake_auth.cookie_header();
            let merged_cookie =
                Self::merge_cookies(cookie_header.as_deref(), handshake_cookie.as_deref());

            let mut effective_access_token = access_token.clone();
            if effective_access_token.is_none() {
                effective_access_token = handshake_auth.doc_access_token();
            }

            let request_id_header = parts
                .headers
                .get("x-request-id")
                .and_then(|value| value.to_str().ok());

            let request_context = SocketRequestContext::new(request_id_header);

            let mut headers = match Self::build_header_map(
                &parts.headers,
                merged_cookie.as_deref(),
                effective_access_token.as_deref(),
            ) {
                Ok(map) => map,
                Err(err) => {
                    let formatted = Self::format_error(err, Some(&request_context.request_id));
                    return Err(Box::new(formatted) as Box<dyn std::fmt::Display + Send>);
                }
            };

            let authorization_header = handshake_auth.authorization_header().or_else(|| {
                merged_cookie
                    .as_ref()
                    .and_then(|cookie| extract_session_token_from_cookie(cookie))
                    .map(|token| format!("Bearer {}", token))
            });

            if let Some(ref auth_value) = authorization_header {
                match HeaderValue::from_str(auth_value) {
                    Ok(value) => {
                        headers.insert(AUTHORIZATION, value);
                    }
                    Err(_) => {
                        let formatted = Self::format_error(
                            AppError::bad_request("invalid authorization header"),
                            Some(&request_context.request_id),
                        );
                        return Err(Box::new(formatted) as Box<dyn std::fmt::Display + Send>);
                    }
                }
            }

            let auth = match authenticate_rest_request(state.as_ref(), &headers).await {
                Ok(session) => session,
                Err(err) => {
                    warn!(error = %err, "socket authenticate request refused");
                    let formatted = Self::format_error(err, Some(&request_context.request_id));
                    return Err(Box::new(formatted) as Box<dyn std::fmt::Display + Send>);
                }
            };

            let socket_ref = socketioxide::extract::SocketRef::from(socket.clone());
            socket_ref.extensions.insert(request_context.clone());
            socket_ref.extensions.insert(runtime.clone());
            socket_ref.extensions.insert(SocketUserContext::new(
                &auth,
                merged_cookie,
                effective_access_token,
                authorization_header.clone(),
            ));
            let socket_root_span = logfire::span!(
                "socket connection",
                socket_id = socket_ref.id.to_string(),
                user_id = auth.user.id.as_str(),
                request_id = request_context.request_id.as_str()
            );
            socket_ref
                .extensions
                .insert(SocketSpanRegistry::new(socket_root_span.clone()));

            socket_root_span.in_scope(|| {
                info!(
                    request_id = %request_context.request_id,
                    user_id = %auth.user.id,
                    "socket authenticated"
                );
            });

            runtime.socket_metrics.inc_connections();

            Ok(())
        })
    }
}

#[derive(Default, Debug)]
struct HandshakeAuth {
    session: Option<String>,
    user: Option<String>,
    cookie: Option<String>,
    bearer: Option<String>,
    access_token: Option<String>,
}

impl HandshakeAuth {
    fn ingest_str(&mut self, payload: &str) {
        let trimmed = payload.trim();
        if trimmed.is_empty() {
            return;
        }

        if let Ok(json) = serde_json::from_str::<JsonValue>(trimmed) {
            self.ingest_json(&json);
            return;
        }

        for pair in trimmed.split('&') {
            if pair.is_empty() {
                continue;
            }
            let mut iter = pair.splitn(2, '=');
            let key = iter.next().unwrap_or_default();
            let value = iter.next().unwrap_or_default();
            self.ingest_pair(key, value);
        }
    }

    fn ingest_json(&mut self, value: &JsonValue) {
        match value {
            JsonValue::Object(map) => {
                for (key, v) in map {
                    match v {
                        JsonValue::String(s) => self.ingest_pair(key, s),
                        JsonValue::Number(n) => self.ingest_pair(key, &n.to_string()),
                        JsonValue::Bool(b) => {
                            self.ingest_pair(key, if *b { "true" } else { "false" })
                        }
                        JsonValue::Object(inner) if key.eq_ignore_ascii_case("cookies") => {
                            for (cookie_key, cookie_value) in inner {
                                if let Some(cookie_value) = cookie_value.as_str() {
                                    let fragment = format!("{cookie_key}={cookie_value}");
                                    self.push_cookie(&fragment);
                                }
                            }
                        }
                        JsonValue::Object(inner) => {
                            self.ingest_json(&JsonValue::Object(inner.clone()));
                        }
                        JsonValue::Array(arr) if key.eq_ignore_ascii_case("cookies") => {
                            for entry in arr {
                                if let Some(s) = entry.as_str() {
                                    self.push_cookie(s);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            JsonValue::String(s) => self.ingest_str(s),
            _ => {}
        }
    }

    fn ingest_pair(&mut self, key: &str, value: &str) {
        let trimmed_value = value.trim();
        if trimmed_value.is_empty() {
            return;
        }

        let key_lower = key.to_ascii_lowercase();
        match key_lower.as_str() {
            "token" | "session" | "sessionid" | "sid" | "affine_session" => {
                if self.session.is_none() {
                    self.session = Some(trimmed_value.to_string());
                }
                if self.bearer.is_none() {
                    self.bearer = Self::normalize_token(trimmed_value);
                }
            }
            "authorization" | "auth" | "bearer" => {
                if self.bearer.is_none() {
                    self.bearer = Self::normalize_token(trimmed_value);
                }
            }
            "access_token" | "accesstoken" | "doc_token" | "doctoken" | "doc_access_token" => {
                if self.access_token.is_none() {
                    self.access_token = Self::normalize_token(trimmed_value);
                }
            }
            "cookie" | "cookies" => {
                self.push_cookie(trimmed_value);
            }
            "userid" | "user_id" | "affine_user_id" => {
                if self.user.is_none() {
                    self.user = Some(trimmed_value.to_string());
                }
            }
            _ => {}
        }
    }

    fn push_cookie(&mut self, fragment: &str) {
        if fragment.is_empty() {
            return;
        }

        if let Some(existing) = &mut self.cookie {
            if !existing.is_empty() {
                existing.push_str("; ");
            }
            existing.push_str(fragment);
        } else {
            self.cookie = Some(fragment.to_string());
        }
    }

    fn cookie_header(&self) -> Option<String> {
        let mut segments: Vec<String> = Vec::new();

        if let Some(cookie) = &self.cookie {
            if !cookie.is_empty() {
                segments.push(cookie.clone());
            }
        }

        if let Some(session) = &self.session {
            segments.push(format!("{}={}", SESSION_COOKIE_NAME, session));
        }

        if let Some(user) = &self.user {
            segments.push(format!("{}={}", USER_COOKIE_NAME, user));
        }

        if segments.is_empty() {
            None
        } else {
            Some(segments.join("; "))
        }
    }

    fn bearer_token(&self) -> Option<String> {
        self.bearer.clone()
    }

    fn doc_access_token(&self) -> Option<String> {
        self.access_token.clone()
    }

    fn authorization_header(&self) -> Option<String> {
        self.bearer
            .as_ref()
            .map(|token| format!("Bearer {}", token))
    }

    fn normalize_token(value: &str) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }

        if trimmed.len() > 7 && trimmed[0..7].eq_ignore_ascii_case("bearer ") && trimmed.len() > 7 {
            Some(trimmed[7..].trim().to_string())
        } else {
            Some(trimmed.to_string())
        }
    }
}

fn extract_session_token_from_cookie(cookie_header: &str) -> Option<String> {
    for part in cookie_header.split(';') {
        let mut iter = part.trim().splitn(2, '=');
        let name = iter.next()?.trim();
        if !name.eq_ignore_ascii_case(SESSION_COOKIE_NAME) {
            continue;
        }
        return iter.next().map(|value| value.trim().to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue, header::COOKIE};
    use serde_json::json;

    #[test]
    fn merge_cookies_combines_segments() {
        let merged = SocketAuthMiddleware::merge_cookies(Some("foo=bar"), Some("zip=zap")).unwrap();
        assert_eq!(merged, "foo=bar; zip=zap");

        let carry_first = SocketAuthMiddleware::merge_cookies(Some("foo=bar"), Some("")).unwrap();
        assert_eq!(carry_first, "foo=bar");

        let carry_second = SocketAuthMiddleware::merge_cookies(None, Some("zip=zap")).unwrap();
        assert_eq!(carry_second, "zip=zap");

        assert!(
            SocketAuthMiddleware::merge_cookies(Some(""), Some("")).is_none(),
            "empty fragments should be ignored"
        );
    }

    #[test]
    fn build_header_map_merges_new_sources() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-test"),
            HeaderValue::from_static("value"),
        );
        headers.insert(COOKIE, HeaderValue::from_static("stale=1"));

        let merged =
            SocketAuthMiddleware::build_header_map(&headers, Some("fresh=2"), Some("doc-token"))
                .expect("header map");

        assert_eq!(merged.get("x-test").unwrap(), "value");
        assert_eq!(merged.get(COOKIE).unwrap(), "fresh=2");
        assert_eq!(merged.get("x-access-token").unwrap(), "doc-token");
    }

    #[test]
    fn build_header_map_rejects_invalid_cookie_value() {
        let headers = HeaderMap::new();
        let err = SocketAuthMiddleware::build_header_map(&headers, Some("bad\r\nvalue"), None)
            .expect_err("invalid cookie header must error");
        assert_eq!(format!("{err}"), "invalid cookie header");
    }

    #[test]
    fn handshake_auth_ingests_query_pairs() {
        let mut auth = HandshakeAuth::default();
        auth.ingest_str("token=sid-123&userid=user-9&cookie=foo=bar");

        let cookie_header = auth.cookie_header().expect("cookie header");
        assert_eq!(
            cookie_header,
            format!(
                "foo=bar; {}=sid-123; {}=user-9",
                SESSION_COOKIE_NAME, USER_COOKIE_NAME
            )
        );
        assert_eq!(auth.authorization_header().unwrap(), "Bearer sid-123");
        assert_eq!(auth.bearer_token().unwrap(), "sid-123");
    }

    #[test]
    fn handshake_auth_understands_json_payloads() {
        let mut auth = HandshakeAuth::default();
        let payload = json!({
            "cookies": ["foo=bar"],
            "Cookies": { "baz": "qux" },
            "nested": { "cookies": { "deep": "value" } },
            "access_token": "  Bearer doc-token ",
            "authorization": "Bearer bearer-token"
        });
        auth.ingest_str(&payload.to_string());

        let cookie_header = auth.cookie_header().unwrap();
        let mut segments: Vec<_> = cookie_header.split("; ").collect();
        segments.sort();
        assert_eq!(segments, vec!["baz=qux", "deep=value", "foo=bar"]);
        assert_eq!(auth.doc_access_token().unwrap(), "doc-token");
        assert_eq!(auth.authorization_header().unwrap(), "Bearer bearer-token");
    }

    #[test]
    fn format_error_includes_request_id() {
        let json_text =
            SocketAuthMiddleware::format_error(AppError::bad_request("nope"), Some("req-42"));
        let json_value: serde_json::Value = serde_json::from_str(&json_text).expect("valid json");
        assert_eq!(json_value["status"], 400);
        assert_eq!(json_value["code"], "BAD_REQUEST");
        assert_eq!(json_value["message"], "nope");
        assert_eq!(json_value["requestId"], "req-42");
    }

    #[test]
    fn extract_session_token_prefers_affine_cookie() {
        let header = format!("foo=bar; {}=session-123; baz=qux", SESSION_COOKIE_NAME);
        assert_eq!(
            extract_session_token_from_cookie(&header),
            Some("session-123".to_string())
        );
        assert!(extract_session_token_from_cookie("foo=bar").is_none());
    }
}
