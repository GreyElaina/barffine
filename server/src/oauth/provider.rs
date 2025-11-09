use std::{fmt, str::FromStr};

use async_trait::async_trait;

use crate::AppError;

use super::state_store::OAuthState;

#[derive(Clone, Debug)]
pub struct OAuthAccount {
    pub id: String,
    pub email: String,
    pub name: Option<String>,
    pub avatar_url: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OAuthTokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub scope: Option<String>,
    pub expires_at: Option<i64>,
    pub id_token: Option<String>,
}

#[async_trait]
pub trait OAuthProvider: Send + Sync {
    fn kind(&self) -> OAuthProviderKind;

    async fn authorization_url(
        &self,
        encoded_state: &str,
        client_nonce: Option<&str>,
    ) -> Result<String, AppError>;

    async fn exchange_code(&self, code: &str) -> Result<OAuthTokens, AppError>;

    async fn fetch_account(
        &self,
        tokens: &OAuthTokens,
        state: &OAuthState,
    ) -> Result<OAuthAccount, AppError>;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum OAuthProviderKind {
    Apple,
    GitHub,
    Google,
    Oidc,
}

impl OAuthProviderKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Apple => "apple",
            Self::GitHub => "github",
            Self::Google => "google",
            Self::Oidc => "oidc",
        }
    }
}

impl fmt::Display for OAuthProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OAuthProviderKind {
    type Err = ();

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.to_ascii_lowercase().as_str() {
            "apple" => Ok(Self::Apple),
            "github" | "git_hub" => Ok(Self::GitHub),
            "google" => Ok(Self::Google),
            "oidc" | "openid" | "open_id" => Ok(Self::Oidc),
            _ => Err(()),
        }
    }
}
