use std::collections::HashMap;

use anyhow::Result;
use chrono::Utc;
use serde_json::Value as JsonValue;

use barffine_core::{
    feature::{
        DeterministicFeatureStore, FeatureFlag, FeatureNamespace, FeatureSnapshot, FeatureStore,
    },
    workspace_feature_store::WorkspaceFeatureStore,
};

#[derive(Clone)]
pub struct FeatureService {
    deterministic: DeterministicFeatureStore,
    workspace: WorkspaceFeatureStore,
}

impl FeatureService {
    pub fn new(deterministic: DeterministicFeatureStore, workspace: WorkspaceFeatureStore) -> Self {
        Self {
            deterministic,
            workspace,
        }
    }

    pub fn deterministic(&self) -> &DeterministicFeatureStore {
        &self.deterministic
    }

    pub async fn snapshot(&self, namespace: &FeatureNamespace) -> Result<FeatureSnapshot> {
        match namespace {
            FeatureNamespace::Workspace(_) => {
                let base_flags = self.deterministic.snapshot(&FeatureNamespace::Global).flags;

                let mut merged: HashMap<String, FeatureFlag> = base_flags
                    .into_iter()
                    .map(|flag| (flag.key.clone(), flag))
                    .collect();

                for flag in self.workspace.list(namespace).await? {
                    merged.insert(flag.key.clone(), flag);
                }

                let mut flags: Vec<FeatureFlag> = merged.into_values().collect();
                flags.sort_by(|a, b| a.key.cmp(&b.key));

                Ok(FeatureSnapshot {
                    flags,
                    generated_at: Utc::now(),
                })
            }
            _ => Ok(self.deterministic.snapshot(namespace)),
        }
    }

    pub async fn enable_workspace_feature(&self, workspace_id: &str, key: &str) -> Result<()> {
        let flag = FeatureFlag {
            key: key.to_owned(),
            enabled: true,
            namespace: FeatureNamespace::workspace(workspace_id),
            expires_at: None,
            metadata: JsonValue::Null,
        };

        self.workspace.upsert(flag).await
    }

    pub async fn disable_workspace_feature(&self, workspace_id: &str, key: &str) -> Result<()> {
        self.workspace.remove(workspace_id, key).await
    }
}
