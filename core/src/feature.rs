use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Namespaces for feature flags mirroring the AFFiNE contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FeatureNamespace {
    Global,
    Workspace(String),
    User(String),
}

impl FeatureNamespace {
    pub fn workspace(id: impl Into<String>) -> Self {
        Self::Workspace(id.into())
    }

    pub fn user(id: impl Into<String>) -> Self {
        Self::User(id.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeatureFlag {
    pub key: String,
    pub enabled: bool,
    pub namespace: FeatureNamespace,
    pub expires_at: Option<DateTime<Utc>>,
    pub metadata: JsonValue,
}

impl FeatureFlag {
    pub fn new(
        key: impl Into<String>,
        namespace: FeatureNamespace,
        enabled: bool,
        metadata: JsonValue,
    ) -> Self {
        Self {
            key: key.into(),
            enabled,
            namespace,
            expires_at: None,
            metadata,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSnapshot {
    pub flags: Vec<FeatureFlag>,
    pub generated_at: DateTime<Utc>,
}

impl FeatureSnapshot {
    pub fn empty() -> Self {
        Self {
            flags: Vec::new(),
            generated_at: Utc::now(),
        }
    }

    pub fn from_flags(flags: Vec<FeatureFlag>) -> Self {
        Self {
            flags,
            generated_at: Utc::now(),
        }
    }
}

#[async_trait]
pub trait FeatureStore: Send + Sync {
    async fn list(&self, namespace: &FeatureNamespace) -> Result<Vec<FeatureFlag>>;

    async fn get(&self, namespace: &FeatureNamespace, key: &str) -> Result<Option<FeatureFlag>>;

    async fn upsert(&self, flag: FeatureFlag) -> Result<()>;
}

/// Deterministic adapter that returns static feature snapshots for resource-limited deployments.
///
/// This mirrors the legacy Node service behaviour where flavours without AI/billing simply
/// advertise disabled feature flags so the clients can gracefully hide UI affordances.
#[derive(Debug, Default, Clone)]
pub struct DeterministicFeatureStore {
    snapshots: HashMap<FeatureNamespace, FeatureSnapshot>,
}

impl DeterministicFeatureStore {
    pub fn from_snapshots(snapshots: HashMap<FeatureNamespace, FeatureSnapshot>) -> Self {
        Self { snapshots }
    }

    pub fn with_global_defaults() -> Self {
        let defaults = vec![
            FeatureFlag::new("copilot", FeatureNamespace::Global, false, JsonValue::Null),
            FeatureFlag::new("billing", FeatureNamespace::Global, false, JsonValue::Null),
            FeatureFlag::new(
                "analytics",
                FeatureNamespace::Global,
                false,
                JsonValue::Null,
            ),
        ];

        let mut store = Self::default();
        store.snapshots.insert(
            FeatureNamespace::Global,
            FeatureSnapshot::from_flags(defaults),
        );
        store
    }

    pub fn snapshot(&self, namespace: &FeatureNamespace) -> FeatureSnapshot {
        self.snapshots
            .get(namespace)
            .cloned()
            .unwrap_or_else(FeatureSnapshot::empty)
    }

    fn namespace_flags(&self, namespace: &FeatureNamespace) -> Vec<FeatureFlag> {
        self.snapshot(namespace).flags
    }
}

#[async_trait]
impl FeatureStore for DeterministicFeatureStore {
    async fn list(&self, namespace: &FeatureNamespace) -> Result<Vec<FeatureFlag>> {
        Ok(self.namespace_flags(namespace))
    }

    async fn get(&self, namespace: &FeatureNamespace, key: &str) -> Result<Option<FeatureFlag>> {
        Ok(self
            .snapshots
            .get(namespace)
            .and_then(|snapshot| snapshot.flags.iter().find(|flag| flag.key == key).cloned()))
    }

    async fn upsert(&self, _flag: FeatureFlag) -> Result<()> {
        // Deterministic snapshot stores are read-only; callers expect no-op success when running
        // in constrained deployments that do not persist feature toggles.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_flag_builder() {
        let flag = FeatureFlag::new(
            "copilot",
            FeatureNamespace::workspace("ws"),
            false,
            JsonValue::Null,
        );

        assert_eq!(flag.key, "copilot");
        assert!(!flag.enabled);
    }

    #[tokio::test]
    async fn deterministic_store_returns_disabled_defaults() {
        let store = DeterministicFeatureStore::with_global_defaults();
        let flags = store
            .list(&FeatureNamespace::Global)
            .await
            .expect("list succeeds");

        assert_eq!(flags.len(), 3);
        assert!(flags.iter().all(|flag| !flag.enabled));
    }

    #[tokio::test]
    async fn deterministic_store_get_returns_specific_flag() {
        let store = DeterministicFeatureStore::with_global_defaults();

        let copilot = store
            .get(&FeatureNamespace::Global, "copilot")
            .await
            .expect("get succeeds");

        assert!(copilot.is_some());
        assert_eq!(copilot.unwrap().key, "copilot");
    }
}
