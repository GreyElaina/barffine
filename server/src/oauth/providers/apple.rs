use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use parking_lot::RwLock;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use url::Url;

use crate::AppError;

use super::super::{
    config::AppleProviderConfig,
    provider::{OAuthAccount, OAuthProvider, OAuthProviderKind, OAuthTokens},
    state_store::OAuthState,
};

const APPLE_AUTH_URL: &str = "https://appleid.apple.com/auth/authorize";
const APPLE_TOKEN_URL: &str = "https://appleid.apple.com/auth/token";
const APPLE_KEYS_URL: &str = "https://appleid.apple.com/auth/keys";

#[derive(Debug, Clone, Deserialize)]
struct AppleKeysResponse {
    keys: Vec<AppleJwk>,
}

#[derive(Debug, Clone, Deserialize)]
struct AppleJwk {
    kid: String,
    n: String,
    e: String,
}

#[derive(Debug, Clone, Deserialize)]
struct AppleTokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    id_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct AppleClaims {
    sub: String,
    email: Option<String>,
    nonce: Option<String>,
}

pub struct AppleOAuthProvider {
    client: Client,
    config: AppleProviderConfig,
    redirect_uri: String,
    key_cache: RwLock<Option<AppleKeyCache>>,
}

#[derive(Clone)]
struct AppleKeyCache {
    fetched_at: Instant,
    keys: Arc<Vec<AppleJwk>>,
}

impl AppleOAuthProvider {
    pub fn new(client: Client, config: AppleProviderConfig, redirect_uri: String) -> Self {
        Self {
            client,
            config,
            redirect_uri,
            key_cache: RwLock::new(None),
        }
    }

    async fn apple_keys(&self) -> Result<Arc<Vec<AppleJwk>>, AppError> {
        if let Some(cache) = self.key_cache.read().clone() {
            if cache.fetched_at.elapsed() < Duration::from_secs(3600) {
                return Ok(cache.keys);
            }
        }

        let keys = Arc::new(self.fetch_apple_keys().await?);

        let mut writer = self.key_cache.write();
        if let Some(cache) = writer.clone() {
            if cache.fetched_at.elapsed() < Duration::from_secs(3600) {
                return Ok(cache.keys);
            }
        }

        *writer = Some(AppleKeyCache {
            fetched_at: Instant::now(),
            keys: keys.clone(),
        });
        Ok(keys)
    }

    async fn fetch_apple_keys(&self) -> Result<Vec<AppleJwk>, AppError> {
        let response = self
            .client
            .get(APPLE_KEYS_URL)
            .send()
            .await
            .context("fetch apple signing keys")
            .map_err(AppError::from_anyhow)?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".to_string());
            return Err(AppError::internal(anyhow!(
                "apple keys endpoint error {}: {}",
                status,
                body
            )));
        }

        let payload = response
            .json::<AppleKeysResponse>()
            .await
            .context("decode apple signing keys")
            .map_err(AppError::from_anyhow)?;

        Ok(payload.keys)
    }
}

#[async_trait]
impl OAuthProvider for AppleOAuthProvider {
    fn kind(&self) -> OAuthProviderKind {
        OAuthProviderKind::Apple
    }

    async fn authorization_url(
        &self,
        encoded_state: &str,
        client_nonce: Option<&str>,
    ) -> Result<String, AppError> {
        let mut url = Url::parse(APPLE_AUTH_URL).expect("apple auth url valid");
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("client_id", &self.config.client_id);
            query.append_pair("redirect_uri", &self.redirect_uri);
            query.append_pair("response_type", "code");
            query.append_pair("response_mode", "form_post");
            query.append_pair("scope", "name email");
            query.append_pair("state", encoded_state);
            if let Some(nonce) = client_nonce {
                query.append_pair("nonce", nonce);
            }
        }

        Ok(url.into())
    }

    async fn exchange_code(&self, code: &str) -> Result<OAuthTokens, AppError> {
        let body = serde_urlencoded::to_string([
            ("code", code),
            ("client_id", self.config.client_id.as_str()),
            ("client_secret", self.config.client_secret.as_str()),
            ("redirect_uri", self.redirect_uri.as_str()),
            ("grant_type", "authorization_code"),
        ])
        .expect("serialize apple token body");

        let response = self
            .client
            .post(APPLE_TOKEN_URL)
            .header("accept", "application/json")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
            .context("request apple token")
            .map_err(AppError::from_anyhow)?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".to_string());
            if status.is_client_error() {
                return Err(AppError::bad_request("invalid OAuth callback code")
                    .with_name("INVALID_OAUTH_CALLBACK_CODE")
                    .with_data(json!({ "status": status.as_u16(), "body": body })));
            }
            return Err(AppError::internal(anyhow!(
                "apple token endpoint error {}: {}",
                status,
                body
            )));
        }

        let payload = response
            .json::<AppleTokenResponse>()
            .await
            .context("decode apple token response")
            .map_err(AppError::from_anyhow)?;

        Ok(OAuthTokens {
            access_token: payload.access_token,
            refresh_token: payload.refresh_token,
            id_token: Some(payload.id_token),
            scope: None,
            expires_at: payload
                .expires_in
                .map(|seconds| Utc::now().timestamp() + seconds as i64),
        })
    }

    async fn fetch_account(
        &self,
        tokens: &OAuthTokens,
        state: &OAuthState,
    ) -> Result<OAuthAccount, AppError> {
        let id_token = tokens.id_token.as_deref().ok_or_else(|| {
            AppError::bad_request("missing Apple id_token").with_name("INVALID_OAUTH_RESPONSE")
        })?;

        let header = decode_header(id_token).map_err(|err| AppError::internal(err.into()))?;
        let kid = header.kid.ok_or_else(|| {
            AppError::bad_request("missing Apple key id").with_name("INVALID_OAUTH_RESPONSE")
        })?;

        let keys = self.apple_keys().await?;
        let key = keys.iter().find(|key| key.kid == kid).ok_or_else(|| {
            AppError::bad_request("Apple signing key not found").with_name("INVALID_OAUTH_RESPONSE")
        })?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[self.config.client_id.as_str()]);
        validation.set_issuer(&["https://appleid.apple.com"]);

        let decoding_key = DecodingKey::from_rsa_components(&key.n, &key.e).map_err(|err| {
            AppError::bad_request(err.to_string()).with_name("INVALID_OAUTH_RESPONSE")
        })?;

        let decoded =
            decode::<AppleClaims>(id_token, &decoding_key, &validation).map_err(|err| {
                AppError::bad_request(err.to_string()).with_name("INVALID_OAUTH_RESPONSE")
            })?;

        let claims = decoded.claims;
        if let Some(expected) = state.client_nonce.as_deref() {
            if claims.nonce.as_deref() != Some(expected) {
                return Err(AppError::bad_request("mismatched Apple nonce")
                    .with_name("INVALID_OAUTH_CALLBACK_STATE"));
            }
        }

        let email = claims.email.ok_or_else(|| {
            AppError::bad_request("Apple response missing email")
                .with_name("INVALID_OAUTH_RESPONSE")
        })?;

        Ok(OAuthAccount {
            id: claims.sub,
            email,
            name: None,
            avatar_url: None,
        })
    }
}
