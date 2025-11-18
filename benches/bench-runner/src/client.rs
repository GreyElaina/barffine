use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use barffine_server::cookies::parse_set_cookie;
use parking_lot::Mutex;
use reqwest::{
    Client, StatusCode,
    header::{CONTENT_TYPE, COOKIE, HeaderMap, HeaderValue},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::instrument;

use crate::config::TargetConfig;

const USER_AGENT: &str = "barffine-bench-runner";

pub struct BenchClient {
    http: Client,
    graphql_endpoint: String,
    rest_endpoint: String,
    cookies: Mutex<CookieStore>,
}

impl BenchClient {
    pub fn new(target: &TargetConfig) -> Result<Arc<Self>> {
        let http = Client::builder()
            .user_agent(USER_AGENT)
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Arc::new(Self {
            http,
            graphql_endpoint: target.graphql_endpoint(),
            rest_endpoint: target.rest_endpoint(),
            cookies: Mutex::new(CookieStore::default()),
        }))
    }

    #[instrument(skip(self, password), fields(email = %email))]
    pub async fn authenticate(self: &Arc<Self>, email: &str, password: &str) -> Result<()> {
        let payload = json!({
            "query": CREATE_SESSION_MUTATION,
            "variables": {
                "input": {
                    "email": email,
                    "password": password,
                }
            }
        });
        let response = self
            .post_json(&self.graphql_endpoint, payload)
            .await
            .context("failed to perform createSession mutation")?;
        let envelope: GraphqlEnvelope =
            serde_json::from_str(&response).context("failed to parse createSession response")?;
        if let Some(errors) = envelope.errors {
            return Err(anyhow!("createSession failed: {}", errors[0].message));
        }
        if envelope
            .data
            .as_ref()
            .and_then(|data| data["createSession"]["userId"].as_str())
            .is_none()
        {
            return Err(anyhow!("createSession returned no user id"));
        }
        Ok(())
    }

    pub async fn graphql(&self, query: &str, variables: Value) -> Result<Value> {
        let body = json!({
            "query": query,
            "variables": variables,
        });
        let raw = self.post_json(&self.graphql_endpoint, body).await?;
        let envelope: GraphqlEnvelope =
            serde_json::from_str(&raw).context("failed to parse GraphQL response")?;
        if let Some(errors) = envelope.errors {
            return Err(anyhow!("GraphQL error: {}", errors[0].message));
        }
        envelope
            .data
            .ok_or_else(|| anyhow!("GraphQL response missing data"))
    }

    pub async fn get_rest(
        &self,
        path: &str,
        query: Option<&[(&str, &str)]>,
    ) -> Result<(StatusCode, Vec<u8>)> {
        let mut url = self.rest_endpoint.clone();
        url.push_str(path);
        let mut req = self.http.get(url);
        if let Some(query) = query {
            req = req.query(query);
        }
        if let Some(cookie_header) = self.cookie_header()? {
            req = req.header(COOKIE, cookie_header);
        }
        let res = req.send().await?;
        self.capture_cookies(res.headers())?;
        let status = res.status();
        let body = res.bytes().await?.to_vec();
        Ok((status, body))
    }

    #[instrument(skip(self, payload))]
    async fn post_json(&self, url: &str, payload: Value) -> Result<String> {
        let mut req = self
            .http
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(payload.to_string());
        if let Some(cookie_header) = self.cookie_header()? {
            req = req.header(COOKIE, cookie_header);
        }
        let res = req.send().await?;
        self.capture_cookies(res.headers())?;
        Ok(res.text().await?)
    }

    fn cookie_header(&self) -> Result<Option<HeaderValue>> {
        let store = self.cookies.lock();
        if let Some(value) = store.serialize() {
            match HeaderValue::from_str(&value) {
                Ok(header) => Ok(Some(header)),
                Err(err) => {
                    tracing::warn!(%err, "failed to encode cookie header");
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    fn capture_cookies(&self, headers: &HeaderMap) -> Result<()> {
        let mut store = self.cookies.lock();
        store.ingest(headers);
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct GraphqlEnvelope {
    data: Option<Value>,
    errors: Option<Vec<GraphqlError>>,
}

#[derive(Debug, Deserialize)]
struct GraphqlError {
    message: String,
}

#[derive(Default)]
struct CookieStore {
    entries: Vec<(String, String)>,
}

impl CookieStore {
    fn serialize(&self) -> Option<String> {
        if self.entries.is_empty() {
            return None;
        }
        Some(
            self.entries
                .iter()
                .map(|(name, value)| format!("{}={}", name, value))
                .collect::<Vec<_>>()
                .join("; "),
        )
    }

    fn ingest(&mut self, headers: &HeaderMap) {
        for value in headers.get_all("set-cookie").iter() {
            if let Ok(text) = value.to_str() {
                if let Some((name, cookie_value)) = parse_set_cookie(text) {
                    if let Some(entry) = self.entries.iter_mut().find(|(n, _)| *n == name) {
                        entry.1 = cookie_value;
                    } else {
                        self.entries.push((name, cookie_value));
                    }
                }
            }
        }
    }
}

const CREATE_SESSION_MUTATION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../graphql/operations/create_session.graphql"
));
