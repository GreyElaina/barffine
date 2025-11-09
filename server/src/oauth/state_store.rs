use std::time::Duration;

use moka::future::Cache;
use uuid::Uuid;

use super::provider::OAuthProviderKind;

#[derive(Clone)]
pub(crate) struct OAuthStateStore {
    cache: Cache<String, OAuthState>,
}

impl OAuthStateStore {
    pub fn new(ttl: Duration) -> Self {
        let cache = Cache::builder()
            .time_to_live(ttl)
            .max_capacity(1024)
            .build();
        Self { cache }
    }

    pub async fn insert(&self, state: OAuthState) -> String {
        let token = Uuid::new_v4().to_string();
        self.cache.insert(token.clone(), state).await;
        token
    }

    pub async fn get(&self, token: &str) -> Option<OAuthState> {
        self.cache.get(token).await
    }

    pub async fn invalidate(&self, token: &str) {
        self.cache.invalidate(token).await;
    }
}

#[derive(Clone, Debug)]
pub struct OAuthState {
    pub provider: OAuthProviderKind,
    pub redirect_uri: Option<String>,
    pub client: Option<String>,
    pub client_nonce: Option<String>,
}
