use async_graphql::{Enum, SimpleObject};
use chrono::{DateTime, Utc};

use crate::graphql::FeatureTypeEnum;

#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[graphql(name = "ServerFeature")]
pub enum ServerFeatureEnum {
    Captcha,
    Comment,
    Copilot,
    CopilotEmbedding,
    Indexer,
    LocalWorkspace,
    OAuth,
    Payment,
}

impl ServerFeatureEnum {
    pub fn from_str(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "captcha" => Some(Self::Captcha),
            "comment" => Some(Self::Comment),
            "copilot" => Some(Self::Copilot),
            "copilot_embedding" | "copilot-embedding" | "copilotembedding" => {
                Some(Self::CopilotEmbedding)
            }
            "indexer" => Some(Self::Indexer),
            "local_workspace" | "local-workspace" | "localworkspace" => Some(Self::LocalWorkspace),
            "oauth" | "o_auth" => Some(Self::OAuth),
            "payment" => Some(Self::Payment),
            _ => None,
        }
    }
}

#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq)]
#[graphql(name = "ServerDeploymentType")]
pub enum ServerDeploymentTypeEnum {
    Affine,
    #[graphql(name = "Selfhosted")]
    Selfhosted,
}

impl ServerDeploymentTypeEnum {
    pub fn from_metadata(value: &str) -> Self {
        if value.eq_ignore_ascii_case("affine") {
            Self::Affine
        } else {
            Self::Selfhosted
        }
    }
}

#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[graphql(name = "OAuthProviderType")]
pub enum OAuthProviderTypeEnum {
    Apple,
    GitHub,
    Google,
    #[graphql(name = "OIDC")]
    Oidc,
}

impl OAuthProviderTypeEnum {
    pub fn from_str(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "apple" => Some(Self::Apple),
            "github" | "git_hub" => Some(Self::GitHub),
            "google" => Some(Self::Google),
            "oidc" | "openid" | "open_id" => Some(Self::Oidc),
            _ => None,
        }
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "PasswordLimitsType")]
pub struct PasswordLimitsObject {
    #[graphql(name = "minLength")]
    pub min_length: i32,
    #[graphql(name = "maxLength")]
    pub max_length: i32,
}

impl PasswordLimitsObject {
    pub const fn new(min_length: i32, max_length: i32) -> Self {
        Self {
            min_length,
            max_length,
        }
    }
}

impl Default for PasswordLimitsObject {
    fn default() -> Self {
        Self::new(8, 32)
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "CredentialsRequirementType")]
pub struct CredentialsRequirementObject {
    pub password: PasswordLimitsObject,
}

impl Default for CredentialsRequirementObject {
    fn default() -> Self {
        Self {
            password: PasswordLimitsObject::default(),
        }
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "ReleaseVersionType")]
pub struct ReleaseVersionObject {
    pub version: String,
    pub url: String,
    #[graphql(name = "publishedAt")]
    pub published_at: DateTime<Utc>,
    pub changelog: String,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(name = "ServerConfigType")]
pub struct ServerConfigObject {
    pub name: String,
    pub version: String,
    #[graphql(name = "baseUrl")]
    pub base_url: String,
    #[graphql(name = "type")]
    pub deployment_type: ServerDeploymentTypeEnum,
    pub features: Vec<ServerFeatureEnum>,
    #[graphql(name = "allowGuestDemoWorkspace")]
    pub allow_guest_demo_workspace: bool,
    #[graphql(name = "availableUpgrade")]
    pub available_upgrade: Option<ReleaseVersionObject>,
    #[graphql(name = "availableUserFeatures")]
    pub available_user_features: Vec<FeatureTypeEnum>,
    #[graphql(name = "credentialsRequirement")]
    pub credentials_requirement: CredentialsRequirementObject,
    pub initialized: bool,
    #[graphql(name = "oauthProviders")]
    pub oauth_providers: Vec<OAuthProviderTypeEnum>,
}

impl ServerConfigObject {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        version: String,
        base_url: String,
        deployment_type: ServerDeploymentTypeEnum,
        features: Vec<ServerFeatureEnum>,
        allow_guest_demo_workspace: bool,
        available_upgrade: Option<ReleaseVersionObject>,
        available_user_features: Vec<FeatureTypeEnum>,
        credentials_requirement: CredentialsRequirementObject,
        initialized: bool,
        oauth_providers: Vec<OAuthProviderTypeEnum>,
    ) -> Self {
        Self {
            name,
            version,
            base_url,
            deployment_type,
            features,
            allow_guest_demo_workspace,
            available_upgrade,
            available_user_features,
            credentials_requirement,
            initialized,
            oauth_providers,
        }
    }
}
