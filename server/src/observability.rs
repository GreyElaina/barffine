use std::{env, fmt, time::Duration};

use axum::http::Response;
use once_cell::sync::OnceCell;
use tower_http::trace::OnResponse;
use tracing::{Level, Span, event};

type SharedSampling = &'static SamplingConfig;

static SAMPLING_CONFIG: OnceCell<SamplingConfig> = OnceCell::new();

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

        match level {
            Level::ERROR => event!(
                parent: span,
                Level::ERROR,
                status = status.as_u16(),
                latency_ms = latency.as_millis() as u64,
                "request completed"
            ),
            Level::WARN => event!(
                parent: span,
                Level::WARN,
                status = status.as_u16(),
                latency_ms = latency.as_millis() as u64,
                "request completed"
            ),
            _ => event!(
                parent: span,
                Level::INFO,
                status = status.as_u16(),
                latency_ms = latency.as_millis() as u64,
                "request completed"
            ),
        }
    }
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
