use anyhow::Result;
use async_trait::async_trait;

use crate::{
    db::{Database, workspace_feature_repo::WorkspaceFeatureRepositoryRef},
    feature::{FeatureFlag, FeatureNamespace, FeatureStore},
};

/// Mutable feature storage scoped to workspace namespaces.
#[derive(Clone)]
pub struct WorkspaceFeatureStore {
    repo: WorkspaceFeatureRepositoryRef,
}

impl WorkspaceFeatureStore {
    pub fn new(database: &Database) -> Self {
        Self {
            repo: database.repositories().workspace_feature_repo(),
        }
    }

    pub async fn remove(&self, workspace_id: &str, key: &str) -> Result<()> {
        self.repo.remove(workspace_id, key).await
    }
}

#[async_trait]
impl FeatureStore for WorkspaceFeatureStore {
    async fn list(&self, namespace: &FeatureNamespace) -> Result<Vec<FeatureFlag>> {
        self.repo.list(namespace).await
    }

    async fn get(&self, namespace: &FeatureNamespace, key: &str) -> Result<Option<FeatureFlag>> {
        self.repo.get(namespace, key).await
    }

    async fn upsert(&self, flag: FeatureFlag) -> Result<()> {
        self.repo.upsert(flag).await
    }
}
