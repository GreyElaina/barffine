use anyhow::Context;
use async_trait::async_trait;
use chrono::Utc;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use url::Url;

use crate::AppError;

use super::super::{
    config::GoogleProviderConfig,
    provider::{OAuthAccount, OAuthProvider, OAuthProviderKind, OAuthTokens},
    state_store::OAuthState,
};

const GOOGLE_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const GOOGLE_USERINFO_URL: &str = "https://www.googleapis.com/oauth2/v2/userinfo";

pub struct GoogleOAuthProvider {
    client: Client,
    config: GoogleProviderConfig,
    redirect_uri: String,
}

impl GoogleOAuthProvider {
    pub fn new(client: Client, config: GoogleProviderConfig, redirect_uri: String) -> Self {
        Self {
            client,
            config,
            redirect_uri,
        }
    }
}

#[async_trait]
impl OAuthProvider for GoogleOAuthProvider {
    fn kind(&self) -> OAuthProviderKind {
        OAuthProviderKind::Google
    }

    async fn authorization_url(
        &self,
        encoded_state: &str,
        _client_nonce: Option<&str>,
    ) -> Result<String, AppError> {
        let mut url = Url::parse(GOOGLE_AUTH_URL).expect("google auth url valid");

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
                    .unwrap_or("openid email profile"),
            );
            query.append_pair(
                "prompt",
                self.config.prompt.as_deref().unwrap_or("select_account"),
            );
            query.append_pair(
                "access_type",
                self.config.access_type.as_deref().unwrap_or("offline"),
            );
            query.append_pair("state", encoded_state);
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
        .expect("serialize google token body");

        let response = self
            .client
            .post(GOOGLE_TOKEN_URL)
            .header("content-type", "application/x-www-form-urlencoded")
            .header("accept", "application/json")
            .body(body)
            .send()
            .await
            .context("request google token")
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
            return Err(AppError::internal(anyhow::anyhow!(
                "google token endpoint error {}: {}",
                status,
                body
            )));
        }

        let payload = response
            .json::<GoogleTokenResponse>()
            .await
            .context("decode google token response")
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
        let response = self
            .client
            .get(GOOGLE_USERINFO_URL)
            .bearer_auth(&tokens.access_token)
            .send()
            .await
            .context("request google userinfo")
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

            return Err(AppError::internal(anyhow::anyhow!(
                "google userinfo error {}: {}",
                status,
                body
            )));
        }

        let user = response
            .json::<GoogleUserInfo>()
            .await
            .context("decode google userinfo")
            .map_err(AppError::from_anyhow)?;

        Ok(OAuthAccount {
            id: user.id,
            email: user.email,
            name: user.name,
            avatar_url: user.picture,
        })
    }
}

#[derive(Debug, Deserialize)]
struct GoogleTokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GoogleUserInfo {
    id: String,
    email: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    picture: Option<String>,
}
