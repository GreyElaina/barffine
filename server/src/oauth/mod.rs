mod config;
mod provider;
mod providers;
mod router;
mod state_store;

use std::{collections::HashMap, sync::Arc, time::Duration};

use once_cell::sync::Lazy;
use reqwest::Client;

pub use provider::{OAuthAccount, OAuthProvider, OAuthProviderKind, OAuthTokens};
pub use router::{OAuthCallbackResponse, OAuthPreflightResponse, oauth_router};
pub use state_store::OAuthState;

use self::{
    config::{ProviderConfig, load_provider_configs, oauth_signup_allowed},
    providers::{
        apple::AppleOAuthProvider, github::GithubOAuthProvider, google::GoogleOAuthProvider,
        oidc::OidcOAuthProvider,
    },
    state_store::OAuthStateStore,
};

const OAUTH_STATE_TTL: Duration = Duration::from_secs(3 * 60 * 60);

static HTTP_CLIENT: Lazy<Client> = Lazy::new(|| {
    Client::builder()
        .user_agent(format!("barffine-server/{}", env!("CARGO_PKG_VERSION")))
        .build()
        .expect("build oauth http client")
});

#[derive(Clone)]
pub struct OAuthService {
    providers: HashMap<OAuthProviderKind, Arc<dyn OAuthProvider>>,
    state_store: OAuthStateStore,
    allow_signup: bool,
}

impl OAuthService {
    pub fn new(base_url: &str) -> Self {
        let configs = load_provider_configs();
        let providers = instantiate_providers(&configs, base_url);

        Self {
            providers,
            state_store: OAuthStateStore::new(OAUTH_STATE_TTL),
            allow_signup: oauth_signup_allowed(),
        }
    }

    pub fn has_providers(&self) -> bool {
        !self.providers.is_empty()
    }

    pub fn allow_signup(&self) -> bool {
        self.allow_signup
    }

    pub fn providers(&self) -> Vec<OAuthProviderKind> {
        let mut keys: Vec<_> = self.providers.keys().copied().collect();
        keys.sort_by_key(|kind| kind.as_str());
        keys
    }

    pub fn provider(&self, kind: &OAuthProviderKind) -> Option<Arc<dyn OAuthProvider>> {
        self.providers.get(kind).cloned()
    }

    pub async fn issue_state(&self, state: OAuthState) -> String {
        self.state_store.insert(state).await
    }

    pub async fn get_state(&self, token: &str) -> Option<OAuthState> {
        self.state_store.get(token).await
    }

    pub async fn invalidate_state(&self, token: &str) {
        self.state_store.invalidate(token).await;
    }
}

fn instantiate_providers(
    configs: &[ProviderConfig],
    base_url: &str,
) -> HashMap<OAuthProviderKind, Arc<dyn OAuthProvider>> {
    let mut providers: HashMap<OAuthProviderKind, Arc<dyn OAuthProvider>> = HashMap::new();

    for config in configs {
        match config {
            ProviderConfig::Google(cfg) => {
                let provider = GoogleOAuthProvider::new(
                    HTTP_CLIENT.clone(),
                    cfg.clone(),
                    redirect_url(base_url, "/oauth/callback"),
                );
                providers.insert(
                    OAuthProviderKind::Google,
                    Arc::new(provider) as Arc<dyn OAuthProvider>,
                );
            }
            ProviderConfig::GitHub(cfg) => {
                let provider = GithubOAuthProvider::new(
                    HTTP_CLIENT.clone(),
                    cfg.clone(),
                    redirect_url(base_url, "/oauth/callback"),
                );
                providers.insert(
                    OAuthProviderKind::GitHub,
                    Arc::new(provider) as Arc<dyn OAuthProvider>,
                );
            }
            ProviderConfig::Apple(cfg) => {
                let provider = AppleOAuthProvider::new(
                    HTTP_CLIENT.clone(),
                    cfg.clone(),
                    redirect_url(base_url, "/api/oauth/callback"),
                );
                providers.insert(
                    OAuthProviderKind::Apple,
                    Arc::new(provider) as Arc<dyn OAuthProvider>,
                );
            }
            ProviderConfig::Oidc(cfg) => {
                let provider = OidcOAuthProvider::new(
                    HTTP_CLIENT.clone(),
                    cfg.clone(),
                    redirect_url(base_url, "/oauth/callback"),
                );
                providers.insert(
                    OAuthProviderKind::Oidc,
                    Arc::new(provider) as Arc<dyn OAuthProvider>,
                );
            }
        }
    }

    providers
}

fn redirect_url(base_url: &str, path: &str) -> String {
    let mut base = base_url.trim_end_matches('/').to_string();
    if !path.starts_with('/') {
        base.push('/');
    }
    base.push_str(path);
    base
}
