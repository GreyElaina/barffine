use once_cell::sync::Lazy;
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
};

use crate::observability::RequestContext;

pub(crate) static GRAPHQL_METRICS: Lazy<GraphqlMetrics> = Lazy::new(GraphqlMetrics::new);

pub(crate) struct GraphqlMetrics {
    duration_ms: Histogram<f64>,
    errors: Counter<u64>,
}

impl GraphqlMetrics {
    fn new() -> Self {
        let meter = global::meter("barffine.graphql");
        let duration_ms = meter
            .f64_histogram("graphql.duration.ms")
            .with_description("GraphQL operation execution time in milliseconds")
            .build();
        let errors = meter
            .u64_counter("graphql.errors")
            .with_description("GraphQL operations that returned at least one error")
            .build();

        Self {
            duration_ms,
            errors,
        }
    }

    pub fn observe(
        &self,
        operation: &str,
        duration_ms: f64,
        request_context: &RequestContext,
        client_version: Option<&str>,
        authenticated: bool,
        has_errors: bool,
    ) {
        let attributes = build_attributes(
            operation,
            request_context,
            client_version,
            authenticated,
            has_errors,
        );

        self.duration_ms.record(duration_ms, &attributes);
        if has_errors {
            self.errors.add(1, &attributes);
        }
    }
}

fn build_attributes(
    operation: &str,
    request_context: &RequestContext,
    client_version: Option<&str>,
    authenticated: bool,
    has_errors: bool,
) -> Vec<KeyValue> {
    let mut attributes = Vec::with_capacity(5);
    attributes.push(KeyValue::new("operation", operation.to_string()));
    attributes.push(KeyValue::new("route", "/graphql"));
    attributes.push(KeyValue::new(
        "status",
        if has_errors {
            "error".to_string()
        } else {
            "ok".to_string()
        },
    ));
    attributes.push(KeyValue::new(
        "auth_state",
        if authenticated {
            "authenticated".to_string()
        } else {
            "anonymous".to_string()
        },
    ));
    attributes.push(KeyValue::new(
        "client_version",
        client_version
            .or_else(|| request_context.client_version())
            .unwrap_or("unknown")
            .to_string(),
    ));

    attributes
}
