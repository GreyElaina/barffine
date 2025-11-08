use async_graphql::{Json, SimpleObject};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use barffine_core::feature::{FeatureFlag, FeatureNamespace, FeatureSnapshot};

use crate::AppError;

#[derive(SimpleObject)]
#[graphql(name = "FeatureFlagObject")]
pub struct FeatureFlagObject {
    pub key: String,
    pub enabled: bool,
    pub namespace: String,
    #[graphql(name = "expiresAt")]
    pub expires_at: Option<DateTime<Utc>>,
    pub metadata: Json<JsonValue>,
}

impl From<FeatureFlag> for FeatureFlagObject {
    fn from(flag: FeatureFlag) -> Self {
        Self {
            key: flag.key,
            enabled: flag.enabled,
            namespace: namespace_to_string(&flag.namespace),
            expires_at: flag.expires_at,
            metadata: Json(flag.metadata),
        }
    }
}

#[derive(SimpleObject)]
#[graphql(name = "FeatureSnapshotObject")]
pub struct FeatureSnapshotObject {
    pub flags: Vec<FeatureFlagObject>,
    #[graphql(name = "generatedAt")]
    pub generated_at: DateTime<Utc>,
}

impl From<FeatureSnapshot> for FeatureSnapshotObject {
    fn from(snapshot: FeatureSnapshot) -> Self {
        Self {
            flags: snapshot
                .flags
                .into_iter()
                .map(FeatureFlagObject::from)
                .collect(),
            generated_at: snapshot.generated_at,
        }
    }
}

pub(crate) fn parse_feature_namespace(input: &str) -> Result<FeatureNamespace, AppError> {
    if input.eq_ignore_ascii_case("global") {
        return Ok(FeatureNamespace::Global);
    }

    if let Some(rest) = input.strip_prefix("workspace:") {
        if rest.is_empty() {
            return Err(AppError::bad_request("workspace namespace requires an id"));
        }
        return Ok(FeatureNamespace::workspace(rest));
    }

    if let Some(rest) = input.strip_prefix("user:") {
        if rest.is_empty() {
            return Err(AppError::bad_request("user namespace requires an id"));
        }
        return Ok(FeatureNamespace::user(rest));
    }

    Err(AppError::bad_request("invalid feature namespace"))
}

fn namespace_to_string(namespace: &FeatureNamespace) -> String {
    match namespace {
        FeatureNamespace::Global => "global".to_string(),
        FeatureNamespace::Workspace(id) => format!("workspace:{id}"),
        FeatureNamespace::User(id) => format!("user:{id}"),
    }
}
