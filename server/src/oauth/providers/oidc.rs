use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use parking_lot::RwLock;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use url::Url;

use crate::AppError;

use super::super::{
    config::OidcProviderConfig,
    provider::{OAuthAccount, OAuthProvider, OAuthProviderKind, OAuthTokens},
    state_store::OAuthState,
};

#[derive(Debug, Clone, Deserialize)]
struct OidcConfiguration {
    authorization_endpoint: String,
    token_endpoint: String,
    userinfo_endpoint: String,
}

pub struct OidcOAuthProvider {
    client: Client,
    config: OidcProviderConfig,
    redirect_uri: String,
    endpoints: RwLock<Option<Arc<OidcConfiguration>>>,
}

impl OidcOAuthProvider {
    pub fn new(client: Client, config: OidcProviderConfig, redirect_uri: String) -> Self {
        Self {
            client,
            config,
            redirect_uri,
            endpoints: RwLock::new(None),
        }
    }

    async fn configuration(&self) -> Result<Arc<OidcConfiguration>, AppError> {
        if let Some(current) = self.endpoints.read().clone() {
            return Ok(current);
        }

        let fetched = Arc::new(self.fetch_configuration().await?);
        let mut writer = self.endpoints.write();
        if let Some(current) = writer.clone() {
            return Ok(current);
        }
        *writer = Some(fetched.clone());
        Ok(fetched)
    }

    async fn fetch_configuration(&self) -> Result<OidcConfiguration, AppError> {
        let mut issuer = self.config.issuer.clone();
        if !issuer.ends_with('/') {
            issuer.push('/');
        }
        let discovery = format!("{issuer}.well-known/openid-configuration");

        let response = self
            .client
            .get(&discovery)
            .header("accept", "application/json")
            .send()
            .await
            .with_context(|| format!("fetch oidc discovery {}", discovery))
            .map_err(AppError::from_anyhow)?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".to_string());
            return Err(AppError::bad_request("invalid OIDC issuer")
                .with_name("INVALID_OAUTH_RESPONSE")
                .with_data(json!({ "status": status.as_u16(), "body": body })));
        }

        let config = response
            .json::<OidcConfiguration>()
            .await
            .context("decode oidc discovery document")
            .map_err(AppError::from_anyhow)?;

        Ok(config)
    }

    fn resolve_claim<'a>(
        claims: &'a JsonMap<String, JsonValue>,
        key: Option<&str>,
        fallback: &str,
    ) -> Option<&'a JsonValue> {
        if let Some(overridden) = key {
            if let Some(value) = claims.get(overridden) {
                return Some(value);
            }
        }
        claims.get(fallback)
    }
}

#[async_trait]
impl OAuthProvider for OidcOAuthProvider {
    fn kind(&self) -> OAuthProviderKind {
        OAuthProviderKind::Oidc
    }

    async fn authorization_url(
        &self,
        encoded_state: &str,
        _client_nonce: Option<&str>,
    ) -> Result<String, AppError> {
        let config = self.configuration().await?;
        let mut url = Url::parse(&config.authorization_endpoint)
            .map_err(|err| AppError::internal(err.into()))?;

        {
            let mut query = url.query_pairs_mut();
            query.append_pair("client_id", &self.config.client_id);
            query.append_pair("redirect_uri", &self.redirect_uri);
            query.append_pair("response_type", "code");
            query.append_pair(
                "scope",
                self.config
                    .scope
                    .as_deref()
                    .unwrap_or("openid profile email"),
            );
            query.append_pair("state", encoded_state);
        }

        Ok(url.into())
    }

    async fn exchange_code(&self, code: &str) -> Result<OAuthTokens, AppError> {
        let config = self.configuration().await?;
        let body = serde_urlencoded::to_string([
            ("code", code),
            ("client_id", self.config.client_id.as_str()),
            ("client_secret", self.config.client_secret.as_str()),
            ("redirect_uri", self.redirect_uri.as_str()),
            ("grant_type", "authorization_code"),
        ])
        .expect("serialize oidc token body");

        let response = self
            .client
            .post(&config.token_endpoint)
            .header("content-type", "application/x-www-form-urlencoded")
            .header("accept", "application/json")
            .body(body)
            .send()
            .await
            .context("request oidc token")
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
                "oidc token endpoint error {}: {}",
                status,
                body
            )));
        }

        let payload = response
            .json::<OidcTokenResponse>()
            .await
            .context("decode oidc token response")
            .map_err(AppError::from_anyhow)?;

        Ok(OAuthTokens {
            access_token: payload.access_token,
            refresh_token: payload.refresh_token,
            scope: payload.scope,
            expires_at: payload
                .expires_in
                .map(|seconds| Utc::now().timestamp() + seconds as i64),
            id_token: None,
        })
    }

    async fn fetch_account(
        &self,
        tokens: &OAuthTokens,
        _state: &OAuthState,
    ) -> Result<OAuthAccount, AppError> {
        let config = self.configuration().await?;

        let response = self
            .client
            .get(&config.userinfo_endpoint)
            .bearer_auth(&tokens.access_token)
            .header("accept", "application/json")
            .send()
            .await
            .context("request oidc userinfo")
            .map_err(AppError::from_anyhow)?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".to_string());
            if status.is_client_error() {
                return Err(AppError::bad_request("invalid OAuth response")
                    .with_name("INVALID_OAUTH_RESPONSE")
                    .with_data(json!({ "status": status.as_u16(), "body": body })));
            }
            return Err(AppError::internal(anyhow!(
                "oidc userinfo error {}: {}",
                status,
                body
            )));
        }

        let payload = response
            .json::<JsonValue>()
            .await
            .context("decode oidc userinfo")
            .map_err(AppError::from_anyhow)?;

        let claims = payload
            .as_object()
            .ok_or_else(|| AppError::bad_request("invalid OIDC userinfo payload"))?;

        let id_claim = Self::resolve_claim(
            claims,
            self.config.claim_id.as_deref(),
            "preferred_username",
        );
        let email_claim = Self::resolve_claim(claims, self.config.claim_email.as_deref(), "email");

        let id = id_claim.and_then(|value| value.as_str()).ok_or_else(|| {
            AppError::bad_request("OIDC response missing user identifier")
                .with_name("INVALID_OAUTH_RESPONSE")
        })?;
        let email = email_claim
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                AppError::bad_request("OIDC response missing email")
                    .with_name("INVALID_OAUTH_RESPONSE")
            })?;

        let name = Self::resolve_claim(claims, self.config.claim_name.as_deref(), "name")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string());

        Ok(OAuthAccount {
            id: id.to_string(),
            email: email.to_string(),
            name,
            avatar_url: None,
        })
    }
}

#[derive(Debug, Deserialize)]
struct OidcTokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    scope: Option<String>,
}
