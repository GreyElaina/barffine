use anyhow::{Context, anyhow};
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use url::Url;

use crate::AppError;

use super::super::{
    config::GithubProviderConfig,
    provider::{OAuthAccount, OAuthProvider, OAuthProviderKind, OAuthTokens},
    state_store::OAuthState,
};

const GITHUB_AUTH_URL: &str = "https://github.com/login/oauth/authorize";
const GITHUB_TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
const GITHUB_USER_URL: &str = "https://api.github.com/user";

pub struct GithubOAuthProvider {
    client: Client,
    config: GithubProviderConfig,
    redirect_uri: String,
}

impl GithubOAuthProvider {
    pub fn new(client: Client, config: GithubProviderConfig, redirect_uri: String) -> Self {
        Self {
            client,
            config,
            redirect_uri,
        }
    }
}

#[async_trait]
impl OAuthProvider for GithubOAuthProvider {
    fn kind(&self) -> OAuthProviderKind {
        OAuthProviderKind::GitHub
    }

    async fn authorization_url(
        &self,
        encoded_state: &str,
        _client_nonce: Option<&str>,
    ) -> Result<String, AppError> {
        let mut url = Url::parse(GITHUB_AUTH_URL).expect("github auth url valid");
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("client_id", &self.config.client_id);
            query.append_pair("redirect_uri", &self.redirect_uri);
            query.append_pair("scope", self.config.scope.as_deref().unwrap_or("user"));
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
        ])
        .expect("serialize github token body");

        let response = self
            .client
            .post(GITHUB_TOKEN_URL)
            .header("accept", "application/json")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
            .context("request github token")
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
                "github token endpoint error {}: {}",
                status,
                body
            )));
        }

        let payload = response
            .json::<GithubTokenResponse>()
            .await
            .context("decode github token response")
            .map_err(AppError::from_anyhow)?;

        Ok(OAuthTokens {
            access_token: payload.access_token,
            refresh_token: None,
            scope: payload.scope,
            expires_at: None,
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
            .get(GITHUB_USER_URL)
            .bearer_auth(&tokens.access_token)
            .header("accept", "application/json")
            .header("user-agent", "barffine-oauth")
            .send()
            .await
            .context("request github userinfo")
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
                "github userinfo error {}: {}",
                status,
                body
            )));
        }

        let user = response
            .json::<GithubUser>()
            .await
            .context("decode github userinfo")
            .map_err(AppError::from_anyhow)?;

        let email = user.email.ok_or_else(|| {
            AppError::bad_request("GitHub account does not expose an email address")
                .with_name("INVALID_OAUTH_RESPONSE")
        })?;

        Ok(OAuthAccount {
            id: user.login,
            email,
            name: user.name,
            avatar_url: user.avatar_url,
        })
    }
}

#[derive(Debug, Deserialize)]
struct GithubTokenResponse {
    access_token: String,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GithubUser {
    login: String,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    avatar_url: Option<String>,
}
