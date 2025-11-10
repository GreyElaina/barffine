use std::{
    env, fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use axum::{
    extract::MatchedPath,
    http::{HeaderValue, Request, Response, header::HeaderName},
};
use once_cell::sync::OnceCell;
use tower::{Layer, Service};
use tower_http::trace::{MakeSpan, OnResponse};
use tracing::{Level, Span, event, field};
use tracing_subscriber::{
    Registry,
    registry::{LookupSpan, SpanRef},
};
use uuid::Uuid;

type SharedSampling = &'static SamplingConfig;

static SAMPLING_CONFIG: OnceCell<SamplingConfig> = OnceCell::new();
static OTEL_LAYERS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Initialise sampling configuration from environment, returning the shared config.
pub(crate) fn init_sampling() -> SharedSampling {
    SAMPLING_CONFIG.get_or_init(SamplingConfig::from_env)
}

/// Log a concise summary of the currently active sampling rules.
pub fn log_sampling_summary() {
    let config = init_sampling();
    event!(
        Level::INFO,
        default_rate = config.default_rate,
        force_all = config.force_all,
        rules = %config.rule_summary(),
        "HTTP trace sampling configured"
    );
}

/// Function used by `OtelAxumLayer::filter` to decide whether to record a span.
pub fn should_sample_path(path: &str) -> bool {
    let config = init_sampling();
    let decision = config.decision_for(path);
    if decision.rate >= 1.0 {
        if config.debug_logs {
            event!(
                Level::DEBUG,
                route = path,
                rule = decision.label,
                "forcing trace span for route"
            );
        }
        return true;
    }
    if decision.rate <= 0.0 {
        if config.debug_logs {
            event!(
                Level::DEBUG,
                route = path,
                rule = decision.label,
                "dropping trace span for route"
            );
        }
        return false;
    }

    let sampled = fastrand::f64() < decision.rate;
    if config.debug_logs {
        event!(
            Level::DEBUG,
            route = path,
            rule = decision.label,
            sample_rate = decision.rate,
            sampled,
            "trace sampling decision"
        );
    }
    sampled
}

/// Custom response logger that escalates log level for 4xx/5xx responses.
pub fn response_logger() -> ResponseLogger {
    ResponseLogger
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ResponseLogger;

impl<B> OnResponse<B> for ResponseLogger {
    fn on_response(self, response: &Response<B>, latency: Duration, span: &Span) {
        let status = response.status();
        let level = if status.is_server_error() {
            Level::ERROR
        } else if status.is_client_error() {
            Level::WARN
        } else {
            Level::INFO
        };

        span.record(
            "http.response.status_code",
            &field::display(status.as_u16()),
        );

        match level {
            Level::ERROR => event!(
                parent: span,
                Level::ERROR,
                http.response.status_code = status.as_u16(),
                latency_ms = latency.as_millis() as u64,
                "request completed"
            ),
            Level::WARN => event!(
                parent: span,
                Level::WARN,
                http.response.status_code = status.as_u16(),
                latency_ms = latency.as_millis() as u64,
                "request completed"
            ),
            _ => event!(
                parent: span,
                Level::INFO,
                http.response.status_code = status.as_u16(),
                latency_ms = latency.as_millis() as u64,
                "request completed"
            ),
        }
    }
}

static REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const CLIENT_VERSION_HEADER: &str = "x-affine-version";

#[derive(Clone, Debug)]
pub struct RequestContext {
    inner: Arc<RequestContextInner>,
}

#[derive(Debug)]
struct RequestContextInner {
    request_id: String,
    client_version: Option<String>,
}

impl RequestContext {
    fn new(request_id: String, client_version: Option<String>) -> Self {
        Self {
            inner: Arc::new(RequestContextInner {
                request_id,
                client_version,
            }),
        }
    }

    pub fn request_id(&self) -> &str {
        &self.inner.request_id
    }

    pub fn client_version(&self) -> Option<&str> {
        self.inner.client_version.as_deref()
    }
}

pub fn request_context_layer() -> RequestContextLayer {
    RequestContextLayer::default()
}

pub fn otel_layers_enabled() -> bool {
    OTEL_LAYERS_ENABLED.load(Ordering::Relaxed)
}

pub fn set_otel_layers_enabled(enabled: bool) {
    OTEL_LAYERS_ENABLED.store(enabled, Ordering::Relaxed);
}

#[derive(Clone, Default)]
pub struct RequestContextLayer;

#[derive(Clone)]
pub struct RequestContextMiddleware<S> {
    inner: S,
}

impl<S> Layer<S> for RequestContextLayer {
    type Service = RequestContextMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestContextMiddleware { inner }
    }
}

impl<S, B> Service<Request<B>> for RequestContextMiddleware<S>
where
    S: Service<Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<B>) -> Self::Future {
        let (request_id, missing_header) = request
            .headers()
            .get(&REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(|value| (value.to_string(), false))
            .unwrap_or_else(|| (Uuid::new_v4().to_string(), true));

        if missing_header {
            if let Ok(header_value) = HeaderValue::from_str(&request_id) {
                request
                    .headers_mut()
                    .insert(REQUEST_ID_HEADER.clone(), header_value);
            }
        }

        let client_version = request
            .headers()
            .get(CLIENT_VERSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());

        let context = RequestContext::new(request_id, client_version);
        request.extensions_mut().insert(context);

        self.inner.call(request)
    }
}

pub fn http_make_span() -> HttpMakeSpan {
    HttpMakeSpan
}

#[derive(Clone, Default)]
pub struct HttpMakeSpan;

impl<B> MakeSpan<B> for HttpMakeSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        let context = request.extensions().get::<RequestContext>().cloned();
        let request_id = context
            .as_ref()
            .map(|ctx| ctx.request_id().to_owned())
            .unwrap_or_else(|| "unknown".to_string());
        let client_version = context
            .as_ref()
            .and_then(|ctx| ctx.client_version())
            .unwrap_or("unknown")
            .to_string();
        let method = request.method().to_string();
        let target = request
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| request.uri().to_string());
        let matched_route = request
            .extensions()
            .get::<MatchedPath>()
            .map(|mp| mp.as_str().to_string());
        let route_for_title = matched_route.clone().unwrap_or_else(|| target.clone());

        let method_ref: &str = method.as_ref();
        let route_ref: &str = route_for_title.as_ref();
        let target_ref: &str = target.as_ref();
        let request_id_ref: &str = request_id.as_ref();
        let client_version_ref: &str = client_version.as_ref();

        let span = logfire::span!(
            "HTTP {method} {route}",
            method = method_ref,
            route = route_ref,
            request_id = request_id_ref,
            client_version = client_version_ref,
            http.request.method = method_ref,
            http.route = route_ref,
            http.target = target_ref,
            http.response.status_code = field::Empty,
            user_id = field::Empty,
            session_id = field::Empty
        );

        if let Some(ctx) = context {
            with_request_span_ref(&span, |span_ref| {
                span_ref.extensions_mut().insert(ctx.clone());
            });
        }

        span
    }
}

pub fn record_authenticated_identity(user_id: Option<&str>, session_id: Option<&str>) {
    let span = Span::current();
    if span.is_disabled() {
        return;
    }

    if let Some(user_id) = user_id {
        span.record("user_id", &field::display(user_id));
    }

    if let Some(session_id) = session_id {
        span.record("session_id", &field::display(session_id));
    }
}

pub fn current_request_context() -> Option<RequestContext> {
    let span = Span::current();
    if span.is_disabled() {
        return None;
    }

    let mut captured = None;
    with_request_span_ref(&span, |span_ref| {
        if let Some(existing) = span_ref.extensions().get::<RequestContext>() {
            captured = Some(existing.clone());
        }
    });
    captured
}

fn with_request_span_ref<F>(span: &Span, mut apply: F)
where
    F: FnMut(SpanRef<'_, Registry>),
{
    span.with_subscriber(|(id, dispatch)| {
        if let Some(registry) = dispatch.downcast_ref::<Registry>() {
            if let Some(span_ref) = registry.span(id) {
                apply(span_ref);
            }
        }
    });
}

#[derive(Debug)]
struct SamplingConfig {
    default_rate: f64,
    rules: Vec<RouteRule>,
    force_all: bool,
    debug_logs: bool,
}

impl SamplingConfig {
    fn from_env() -> Self {
        let default_rate = env::var("BARFFINE_TRACE_DEFAULT_RATE")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .map_or(DEFAULT_RATE, clamp_rate);

        let force_all = env_bool("BARFFINE_TRACE_FORCE_ALL");
        let debug_logs = env_bool("BARFFINE_TRACE_DEBUG");

        let mut rules = Vec::new();

        // Highest priority: explicit force/drop rules.
        if let Ok(value) = env::var("BARFFINE_TRACE_FORCE_PATHS") {
            rules.extend(parse_prefix_list(&value, 1.0, "force"));
        }

        if let Ok(value) = env::var("BARFFINE_TRACE_DROP_PATHS") {
            rules.extend(parse_prefix_list(&value, 0.0, "drop"));
        }

        if let Ok(value) = env::var("BARFFINE_TRACE_RULES") {
            rules.extend(parse_rule_list(&value));
        }

        // Baseline defaults tuned for Barffine traffic profile.
        rules.extend(default_rules());

        SamplingConfig {
            default_rate,
            rules,
            force_all,
            debug_logs,
        }
    }

    fn decision_for<'a>(&'a self, path: &str) -> Decision<'a> {
        if self.force_all {
            return Decision {
                label: "force-all",
                rate: 1.0,
            };
        }

        for rule in &self.rules {
            if path.starts_with(rule.prefix.as_str()) {
                return Decision {
                    label: rule.label.as_str(),
                    rate: rule.rate,
                };
            }
        }

        Decision {
            label: "default",
            rate: self.default_rate,
        }
    }

    fn rule_summary(&self) -> SamplingSummary<'_> {
        SamplingSummary {
            default_rate: self.default_rate,
            rules: &self.rules,
        }
    }
}

#[derive(Debug)]
struct RouteRule {
    prefix: String,
    rate: f64,
    label: String,
}

impl RouteRule {
    fn new(prefix: impl Into<String>, rate: f64, label: impl Into<String>) -> Self {
        RouteRule {
            prefix: prefix.into(),
            rate: clamp_rate(rate),
            label: label.into(),
        }
    }
}

struct Decision<'a> {
    label: &'a str,
    rate: f64,
}

struct SamplingSummary<'a> {
    default_rate: f64,
    rules: &'a [RouteRule],
}

impl fmt::Display for SamplingSummary<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "default={:.3}", self.default_rate)?;
        if !self.rules.is_empty() {
            write!(f, " rules=[")?;
            for (idx, rule) in self.rules.iter().enumerate() {
                if idx > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}:{}={:.3}", rule.label, rule.prefix, rule.rate)?;
            }
            write!(f, "]")?;
        }
        Ok(())
    }
}

const DEFAULT_RATE: f64 = 0.1;

fn default_rules() -> Vec<RouteRule> {
    vec![
        RouteRule::new("/health", 0.0, "health"),
        RouteRule::new("/feature", 0.0, "feature"),
        RouteRule::new("/socket.io", 0.02, "socket"),
        RouteRule::new("/api/setup", 1.0, "setup"),
        RouteRule::new("/api/auth", 1.0, "auth"),
        RouteRule::new("/sign-in", 1.0, "auth"),
        RouteRule::new("/session", 1.0, "auth"),
        RouteRule::new("/api/workspaces", 0.3, "workspace"),
        RouteRule::new("/workspaces", 0.2, "workspace"),
        RouteRule::new("/api/users", 0.4, "user"),
        RouteRule::new("/users", 0.2, "user"),
        RouteRule::new("/api/blobs", 0.15, "blob"),
        RouteRule::new("/rpc/", 0.3, "rpc"),
        RouteRule::new("/graphql", 0.5, "graphql"),
    ]
}

fn parse_prefix_list(input: &str, rate: f64, label: &str) -> Vec<RouteRule> {
    input
        .split(',')
        .filter_map(|raw| {
            let prefix = raw.trim();
            if prefix.is_empty() {
                None
            } else {
                Some(RouteRule::new(prefix.trim_end_matches('*'), rate, label))
            }
        })
        .collect()
}

fn parse_rule_list(input: &str) -> Vec<RouteRule> {
    input
        .split(',')
        .filter_map(|raw| parse_rule_entry(raw.trim()))
        .collect()
}

fn parse_rule_entry(entry: &str) -> Option<RouteRule> {
    if entry.is_empty() {
        return None;
    }

    let (prefix_part, rate_part) = entry.split_once('=').or_else(|| entry.split_once(':'))?;

    let rate = rate_part.trim().parse::<f64>().ok()?;

    let (label, prefix) = if let Some((label, prefix)) = prefix_part.trim().split_once('@') {
        (label.trim(), prefix.trim())
    } else {
        ("custom", prefix_part.trim())
    };

    if prefix.is_empty() {
        return None;
    }

    Some(RouteRule::new(prefix, rate, label))
}

fn clamp_rate(rate: f64) -> f64 {
    rate.clamp(0.0, 1.0)
}

fn env_bool(var: &str) -> bool {
    env::var(var)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}
