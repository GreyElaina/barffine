use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::feature::{FeatureFlag, FeatureNamespace};

#[async_trait]
pub trait WorkspaceFeatureRepository: Send + Sync {
    async fn list(&self, namespace: &FeatureNamespace) -> Result<Vec<FeatureFlag>>;
    async fn get(&self, namespace: &FeatureNamespace, key: &str) -> Result<Option<FeatureFlag>>;
    async fn upsert(&self, flag: FeatureFlag) -> Result<()>;
    async fn remove(&self, workspace_id: &str, key: &str) -> Result<()>;
}

pub type WorkspaceFeatureRepositoryRef = Arc<dyn WorkspaceFeatureRepository>;
