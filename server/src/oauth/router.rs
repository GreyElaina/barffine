use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::State,
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION},
    },
    response::{IntoResponse, Response},
    routing::post,
};
use barffine_core::user::{ConnectedAccountTokens, NewConnectedAccount};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::str::FromStr;
use url::Url;
use uuid::Uuid;

use crate::{
    AppError, AppState,
    cookies::{build_session_cookie, build_user_cookie},
    http::append_set_cookie_headers,
};

use super::{OAuthAccount, OAuthProviderKind, OAuthState};

pub fn oauth_router() -> Router<AppState> {
    Router::new()
        .route("/oauth/preflight", post(oauth_preflight_handler))
        .route("/oauth/callback", post(oauth_callback_handler))
}

#[derive(Deserialize)]
struct OAuthPreflightRequest {
    provider: String,
    #[serde(default)]
    redirect_uri: Option<String>,
    #[serde(default)]
    client: Option<String>,
    #[serde(default, rename = "client_nonce")]
    client_nonce: Option<String>,
}

#[derive(Serialize)]
pub struct OAuthPreflightResponse {
    pub url: String,
}

#[derive(Serialize)]
pub struct OAuthCallbackResponse {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "redirectUri")]
    pub redirect_uri: Option<String>,
}

async fn oauth_preflight_handler(
    State(state): State<AppState>,
    Json(payload): Json<OAuthPreflightRequest>,
) -> Result<Json<OAuthPreflightResponse>, AppError> {
    let provider = OAuthProviderKind::from_str(&payload.provider)
        .map_err(|_| unknown_oauth_provider(&payload.provider))?;

    let oauth = &state.oauth;
    let Some(service_provider) = oauth.provider(&provider) else {
        return Err(unknown_oauth_provider(&payload.provider));
    };

    let redirect_uri = sanitize_redirect(payload.redirect_uri.as_deref())?;
    let client = normalize_opt(payload.client);
    let client_nonce = normalize_opt(payload.client_nonce);

    let state_token = oauth
        .issue_state(OAuthState {
            provider,
            redirect_uri: redirect_uri.clone(),
            client: client.clone(),
            client_nonce: client_nonce.clone(),
        })
        .await;

    let encoded_state = json!({
        "state": state_token,
        "client": client.clone().unwrap_or_else(|| "web".to_string()),
        "provider": provider.as_str(),
    })
    .to_string();

    let url = service_provider
        .authorization_url(&encoded_state, client_nonce.as_deref())
        .await?;

    Ok(Json(OAuthPreflightResponse { url }))
}

async fn oauth_callback_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let payload = parse_callback_payload(&headers, &body)?;
    let parsed_state = parse_state_payload(&payload.state)?;

    let Some(stored_state) = state.oauth.get_state(&parsed_state.token).await else {
        return Err(AppError::bad_request("oauth state expired").with_name("OAUTH_STATE_EXPIRED"));
    };

    if stored_state.provider == OAuthProviderKind::Apple
        && parsed_state.raw_provider.is_some()
        && stored_state
            .client
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case("web"))
    {
        let redirect = build_open_app_redirect(
            &state.base_url,
            stored_state.client.as_deref().unwrap(),
            &payload.state,
            &payload.code,
            parsed_state
                .raw_provider
                .as_deref()
                .unwrap_or(stored_state.provider.as_str()),
        )?;
        return Ok(redirect.into_response());
    }

    if let Some(expected) = stored_state.client_nonce.as_deref() {
        if stored_state.provider != OAuthProviderKind::Apple
            && !payload
                .client_nonce
                .as_deref()
                .is_some_and(|value| value == expected)
        {
            return Err(AppError::bad_request("invalid oauth state")
                .with_name("INVALID_OAUTH_CALLBACK_STATE"));
        }
    }

    let Some(provider) = state.oauth.provider(&stored_state.provider) else {
        return Err(unknown_oauth_provider(stored_state.provider.as_str()));
    };

    let tokens = provider.exchange_code(&payload.code).await?;
    let account = provider.fetch_account(&tokens, &stored_state).await?;
    let user = upsert_oauth_user(&state, stored_state.provider, &account, &tokens).await?;
    let session = state
        .user_store
        .create_session(&user.id)
        .await
        .map_err(AppError::from_anyhow)?;

    let cookies = vec![
        build_session_cookie(&session.id, session.expires_at),
        build_user_cookie(&user.id, session.expires_at),
    ];

    if stored_state.provider == OAuthProviderKind::Apple
        && stored_state
            .client
            .as_deref()
            .map(|value| value.eq_ignore_ascii_case("web"))
            .unwrap_or(true)
    {
        let mut response =
            apple_web_redirect(&state.base_url, stored_state.redirect_uri.as_deref())?;
        append_set_cookie_headers(&mut response, &cookies)?;
        state.oauth.invalidate_state(&parsed_state.token).await;
        return Ok(response);
    }

    let mut response = Json(OAuthCallbackResponse {
        id: user.id.clone(),
        redirect_uri: stored_state.redirect_uri.clone(),
    })
    .into_response();

    append_set_cookie_headers(&mut response, &cookies)?;
    state.oauth.invalidate_state(&parsed_state.token).await;

    Ok(response)
}

fn normalize_opt(value: Option<String>) -> Option<String> {
    value.and_then(|val| {
        let trimmed = val.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn sanitize_redirect(value: Option<&str>) -> Result<Option<String>, AppError> {
    if let Some(raw) = value {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        if trimmed.starts_with('/') {
            return Ok(Some(trimmed.to_string()));
        }
        return Err(
            AppError::bad_request("redirect_uri must be a relative path")
                .with_name("INVALID_OAUTH_REDIRECT"),
        );
    }
    Ok(None)
}

fn unknown_oauth_provider(name: &str) -> AppError {
    AppError::bad_request(format!("unknown oauth provider {name}"))
        .with_name("UNKNOWN_OAUTH_PROVIDER")
}

#[derive(Deserialize)]
struct RawCallback {
    code: Option<String>,
    state: Option<String>,
    #[serde(default, rename = "client_nonce")]
    client_nonce: Option<String>,
}

struct CallbackPayload {
    code: String,
    state: String,
    client_nonce: Option<String>,
}

fn parse_callback_payload(headers: &HeaderMap, body: &[u8]) -> Result<CallbackPayload, AppError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_ascii_lowercase();

    let payload: RawCallback = if content_type.starts_with("application/x-www-form-urlencoded")
        || content_type.starts_with("application/octet-stream")
    {
        serde_urlencoded::from_bytes(body).map_err(|err| {
            AppError::bad_request(format!("invalid callback payload: {err}"))
                .with_name("INVALID_OAUTH_RESPONSE")
        })?
    } else {
        serde_json::from_slice(body).map_err(|err| {
            AppError::bad_request(format!("invalid callback payload: {err}"))
                .with_name("INVALID_OAUTH_RESPONSE")
        })?
    };

    let code = payload.code.ok_or_else(|| {
        AppError::bad_request("missing oauth code")
            .with_name("MISSING_OAUTH_QUERY_PARAMETER")
            .with_data(json!({ "name": "code" }))
    })?;
    let state = payload.state.ok_or_else(|| {
        AppError::bad_request("missing oauth state")
            .with_name("MISSING_OAUTH_QUERY_PARAMETER")
            .with_data(json!({ "name": "state" }))
    })?;

    Ok(CallbackPayload {
        code,
        state,
        client_nonce: payload.client_nonce.and_then(|value| {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        }),
    })
}

#[derive(Debug)]
struct ParsedState {
    token: String,
    raw_provider: Option<String>,
}

fn parse_state_payload(raw: &str) -> Result<ParsedState, AppError> {
    if raw.len() > 36 {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) {
            if let Some(token) = value
                .get("state")
                .and_then(|val| val.as_str())
                .map(|val| val.to_string())
            {
                return Ok(ParsedState {
                    token,
                    raw_provider: value
                        .get("provider")
                        .and_then(|val| val.as_str())
                        .map(|val| val.to_string()),
                });
            }
        }
    }

    if Uuid::parse_str(raw).is_err() {
        return Err(AppError::bad_request("invalid oauth state token")
            .with_name("INVALID_OAUTH_CALLBACK_STATE"));
    }

    Ok(ParsedState {
        token: raw.to_string(),
        raw_provider: None,
    })
}

fn build_open_app_redirect(
    base_url: &str,
    client: &str,
    state: &str,
    code: &str,
    provider: &str,
) -> Result<Response, AppError> {
    let mut client_url = Url::parse(&format!("{client}://authentication"))
        .map_err(|err| AppError::bad_request(err.to_string()))?;
    client_url
        .query_pairs_mut()
        .append_pair("method", "oauth")
        .append_pair(
            "payload",
            &json!({
                "state": state,
                "code": code,
                "provider": provider,
            })
            .to_string(),
        )
        .append_pair("server", base_url);

    let mut open_app = Url::parse(&join_base(base_url, "/open-app/url"))
        .map_err(|err| AppError::bad_request(err.to_string()))?;
    open_app
        .query_pairs_mut()
        .append_pair("url", &client_url.to_string());

    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::FOUND;
    let headers = response.headers_mut();
    headers.insert(
        LOCATION,
        HeaderValue::from_str(open_app.as_str())
            .map_err(|err| AppError::bad_request(err.to_string()))?,
    );
    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
    Ok(response)
}

fn apple_web_redirect(base_url: &str, redirect: Option<&str>) -> Result<Response, AppError> {
    let target = redirect.unwrap_or("/");
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::FOUND;
    response.headers_mut().insert(
        LOCATION,
        HeaderValue::from_str(&join_base(base_url, target))
            .map_err(|err| AppError::bad_request(err.to_string()))?,
    );
    response
        .headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
    Ok(response)
}

fn join_base(base: &str, path: &str) -> String {
    let mut normalized = base.trim_end_matches('/').to_string();
    normalized.push('/');
    normalized.push_str(path.trim_start_matches('/'));
    normalized
}

async fn upsert_oauth_user(
    state: &AppState,
    provider: OAuthProviderKind,
    account: &OAuthAccount,
    tokens: &super::OAuthTokens,
) -> Result<barffine_core::user::UserRecord, AppError> {
    let provider_key = provider.as_str();
    if let Some(connected) = state
        .user_store
        .get_connected_account(provider_key, &account.id)
        .await
        .map_err(AppError::from_anyhow)?
    {
        let updated = state
            .user_store
            .update_connected_account_tokens(
                &connected.id,
                ConnectedAccountTokens {
                    scope: tokens.scope.as_deref(),
                    access_token: Some(&tokens.access_token),
                    refresh_token: tokens.refresh_token.as_deref(),
                    expires_at: tokens.expires_at,
                },
            )
            .await
            .map_err(AppError::from_anyhow)?;

        let mut user = state
            .user_store
            .find_by_id(&updated.user_id)
            .await
            .map_err(AppError::from_anyhow)?
            .ok_or_else(|| AppError::internal(anyhow::anyhow!("user not found")))?;

        if user.email_verified_at.is_none() && email_eq(&user.email, &account.email) {
            user = state
                .user_store
                .set_email_verified(&user.id, true)
                .await
                .map_err(AppError::from_anyhow)?;
        }

        return Ok(user);
    }

    if !state.oauth.allow_signup() {
        return Err(AppError::forbidden("oauth sign up disabled").with_name("SIGN_UP_FORBIDDEN"));
    }

    let mut user = state
        .user_store
        .find_by_email(&account.email)
        .await
        .map_err(AppError::from_anyhow)?;

    if user.is_none() {
        user = Some(
            state
                .user_store
                .create(&account.email, "", account.name.as_deref())
                .await
                .map_err(AppError::from_anyhow)?,
        );
    }

    let mut user = user.expect("user present");

    if user.email_verified_at.is_none() {
        user = state
            .user_store
            .set_email_verified(&user.id, true)
            .await
            .map_err(AppError::from_anyhow)?;
    }

    state
        .user_store
        .create_connected_account(NewConnectedAccount {
            user_id: &user.id,
            provider: provider_key,
            provider_account_id: &account.id,
            scope: tokens.scope.as_deref(),
            access_token: Some(&tokens.access_token),
            refresh_token: tokens.refresh_token.as_deref(),
            expires_at: tokens.expires_at,
        })
        .await
        .map_err(AppError::from_anyhow)?;

    Ok(user)
}

fn email_eq(lhs: &str, rhs: &str) -> bool {
    lhs.eq_ignore_ascii_case(rhs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::LOCATION;
    use serde_json::json;

    #[test]
    fn parse_state_payload_supports_json_wrappers() {
        let token = Uuid::new_v4().to_string();
        let encoded = json!({
            "state": token,
            "provider": "apple"
        })
        .to_string();

        let parsed = parse_state_payload(&encoded).expect("state parsed");
        assert_eq!(parsed.token, token);
        assert_eq!(parsed.raw_provider.as_deref(), Some("apple"));
    }

    #[test]
    fn parse_state_payload_rejects_invalid_tokens() {
        let err = parse_state_payload("not-a-uuid").expect_err("invalid state rejected");
        assert_eq!(err.into_payload().1.name, "INVALID_OAUTH_CALLBACK_STATE");
    }

    #[test]
    fn sanitize_redirect_accepts_relative_paths() {
        let value = sanitize_redirect(Some("/welcome")).expect("sanitize ok");
        assert_eq!(value.as_deref(), Some("/welcome"));
    }

    #[test]
    fn sanitize_redirect_rejects_absolute_urls() {
        let err =
            sanitize_redirect(Some("https://example.com")).expect_err("absolute url rejected");
        assert_eq!(err.into_payload().1.name, "INVALID_OAUTH_REDIRECT");
    }

    #[test]
    fn build_open_app_redirect_sets_location_header() {
        let response = build_open_app_redirect(
            "https://cloud.local",
            "affine",
            "state-token",
            "code-1",
            "google",
        )
        .expect("build redirect");

        assert_eq!(response.status(), StatusCode::FOUND);
        let location = response
            .headers()
            .get(LOCATION)
            .expect("location header")
            .to_str()
            .unwrap();

        let outer = Url::parse(location).expect("outer url");
        assert_eq!(outer.path(), "/open-app/url");
        let inner = outer
            .query_pairs()
            .find(|(key, _)| key == "url")
            .map(|(_, value)| value.into_owned())
            .expect("embedded url");
        let app_url = Url::parse(&inner).expect("embedded affine url");
        assert_eq!(app_url.scheme(), "affine");
        assert_eq!(app_url.host_str(), Some("authentication"));
    }
}
