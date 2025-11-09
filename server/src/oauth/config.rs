use std::env;

#[derive(Clone, Debug)]
pub(crate) enum ProviderConfig {
    Google(GoogleProviderConfig),
    GitHub(GithubProviderConfig),
    Apple(AppleProviderConfig),
    Oidc(OidcProviderConfig),
}

#[derive(Clone, Debug)]
pub(crate) struct GoogleProviderConfig {
    pub client_id: String,
    pub client_secret: String,
    pub scope: Option<String>,
    pub prompt: Option<String>,
    pub access_type: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct GithubProviderConfig {
    pub client_id: String,
    pub client_secret: String,
    pub scope: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct AppleProviderConfig {
    pub client_id: String,
    pub client_secret: String,
}

#[derive(Clone, Debug)]
pub(crate) struct OidcProviderConfig {
    pub client_id: String,
    pub client_secret: String,
    pub issuer: String,
    pub scope: Option<String>,
    pub claim_id: Option<String>,
    pub claim_email: Option<String>,
    pub claim_name: Option<String>,
}

pub(crate) fn load_provider_configs() -> Vec<ProviderConfig> {
    let mut configs = Vec::new();

    if let (Some(client_id), Some(client_secret)) = (
        first_non_empty(&[
            "BARFFINE_OAUTH_GOOGLE_CLIENT_ID",
            "AFFINE_OAUTH_GOOGLE_CLIENT_ID",
        ]),
        first_non_empty(&[
            "BARFFINE_OAUTH_GOOGLE_CLIENT_SECRET",
            "AFFINE_OAUTH_GOOGLE_CLIENT_SECRET",
        ]),
    ) {
        configs.push(ProviderConfig::Google(GoogleProviderConfig {
            client_id,
            client_secret,
            scope: first_non_empty(&["BARFFINE_OAUTH_GOOGLE_SCOPE"]),
            prompt: first_non_empty(&["BARFFINE_OAUTH_GOOGLE_PROMPT"]),
            access_type: first_non_empty(&["BARFFINE_OAUTH_GOOGLE_ACCESS_TYPE"]),
        }));
    }

    if let (Some(client_id), Some(client_secret)) = (
        first_non_empty(&[
            "BARFFINE_OAUTH_GITHUB_CLIENT_ID",
            "AFFINE_OAUTH_GITHUB_CLIENT_ID",
        ]),
        first_non_empty(&[
            "BARFFINE_OAUTH_GITHUB_CLIENT_SECRET",
            "AFFINE_OAUTH_GITHUB_CLIENT_SECRET",
        ]),
    ) {
        configs.push(ProviderConfig::GitHub(GithubProviderConfig {
            client_id,
            client_secret,
            scope: first_non_empty(&["BARFFINE_OAUTH_GITHUB_SCOPE"]),
        }));
    }

    if let (Some(client_id), Some(client_secret)) = (
        first_non_empty(&[
            "BARFFINE_OAUTH_APPLE_CLIENT_ID",
            "AFFINE_OAUTH_APPLE_CLIENT_ID",
        ]),
        first_non_empty(&[
            "BARFFINE_OAUTH_APPLE_CLIENT_SECRET",
            "AFFINE_OAUTH_APPLE_CLIENT_SECRET",
        ]),
    ) {
        configs.push(ProviderConfig::Apple(AppleProviderConfig {
            client_id,
            client_secret,
        }));
    }

    if let (Some(client_id), Some(client_secret), Some(issuer)) = (
        first_non_empty(&[
            "BARFFINE_OAUTH_OIDC_CLIENT_ID",
            "AFFINE_OAUTH_OIDC_CLIENT_ID",
        ]),
        first_non_empty(&[
            "BARFFINE_OAUTH_OIDC_CLIENT_SECRET",
            "AFFINE_OAUTH_OIDC_CLIENT_SECRET",
        ]),
        first_non_empty(&["BARFFINE_OAUTH_OIDC_ISSUER", "AFFINE_OAUTH_OIDC_ISSUER"]),
    ) {
        configs.push(ProviderConfig::Oidc(OidcProviderConfig {
            client_id,
            client_secret,
            issuer,
            scope: first_non_empty(&["BARFFINE_OAUTH_OIDC_SCOPE"]),
            claim_id: first_non_empty(&["BARFFINE_OAUTH_OIDC_CLAIM_ID"]),
            claim_email: first_non_empty(&["BARFFINE_OAUTH_OIDC_CLAIM_EMAIL"]),
            claim_name: first_non_empty(&["BARFFINE_OAUTH_OIDC_CLAIM_NAME"]),
        }));
    }

    configs
}

pub(crate) fn oauth_signup_allowed() -> bool {
    first_bool(&[
        "BARFFINE_ALLOW_OAUTH_SIGNUP",
        "AFFINE_ALLOW_OAUTH_SIGNUP",
        "AFFINE_ALLOW_SIGNUP_FOR_OAUTH",
        "BARFFINE_ALLOW_SIGNUP",
    ])
    .unwrap_or(true)
}

fn first_non_empty(vars: &[&str]) -> Option<String> {
    vars.iter().find_map(|name| {
        env::var(name).ok().and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        })
    })
}

fn first_bool(vars: &[&str]) -> Option<bool> {
    vars.iter().find_map(|name| {
        env::var(name).ok().and_then(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" | "yes" | "on" => Some(true),
                "0" | "false" | "no" | "off" => Some(false),
                _ => None,
            }
        })
    })
}
