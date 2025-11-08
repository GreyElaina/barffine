use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value as JsonValue;
use sqlx::{Pool, Row, Sqlite};

use crate::{
    db::Database,
    feature::{FeatureFlag, FeatureNamespace, FeatureStore},
};

/// Mutable feature storage scoped to workspace namespaces.
#[derive(Clone)]
pub struct WorkspaceFeatureStore {
    pool: Arc<Pool<Sqlite>>,
}

impl WorkspaceFeatureStore {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: Arc::new(database.pool().clone()),
        }
    }

    fn namespace_parts(namespace: &FeatureNamespace) -> Option<&str> {
        match namespace {
            FeatureNamespace::Workspace(id) => Some(id.as_str()),
            _ => None,
        }
    }

    pub async fn remove(&self, workspace_id: &str, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM workspace_feature_flags WHERE workspace_id = ? AND key = ?")
            .bind(workspace_id)
            .bind(key)
            .execute(&*self.pool)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl FeatureStore for WorkspaceFeatureStore {
    async fn list(&self, namespace: &FeatureNamespace) -> Result<Vec<FeatureFlag>> {
        let Some(workspace_id) = Self::namespace_parts(namespace) else {
            return Ok(Vec::new());
        };

        let rows =
            sqlx::query("SELECT key, enabled FROM workspace_feature_flags WHERE workspace_id = ?")
                .bind(workspace_id)
                .fetch_all(&*self.pool)
                .await?;

        Ok(rows
            .into_iter()
            .map(|row| FeatureFlag {
                key: row.get("key"),
                enabled: row.get::<i64, _>("enabled") != 0,
                namespace: FeatureNamespace::workspace(workspace_id),
                expires_at: None,
                metadata: JsonValue::Null,
            })
            .collect())
    }

    async fn get(&self, namespace: &FeatureNamespace, key: &str) -> Result<Option<FeatureFlag>> {
        let Some(workspace_id) = Self::namespace_parts(namespace) else {
            return Ok(None);
        };

        let row = sqlx::query(
            "SELECT key, enabled FROM workspace_feature_flags WHERE workspace_id = ? AND key = ?",
        )
        .bind(workspace_id)
        .bind(key)
        .fetch_optional(&*self.pool)
        .await?;

        Ok(row.map(|row| FeatureFlag {
            key: row.get("key"),
            enabled: row.get::<i64, _>("enabled") != 0,
            namespace: FeatureNamespace::workspace(workspace_id),
            expires_at: None,
            metadata: JsonValue::Null,
        }))
    }

    async fn upsert(&self, flag: FeatureFlag) -> Result<()> {
        let Some(workspace_id) = Self::namespace_parts(&flag.namespace) else {
            return Ok(());
        };

        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO workspace_feature_flags (workspace_id, key, enabled, updated_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(workspace_id, key)
             DO UPDATE SET enabled = excluded.enabled, updated_at = excluded.updated_at",
        )
        .bind(workspace_id)
        .bind(&flag.key)
        .bind(if flag.enabled { 1 } else { 0 })
        .bind(now)
        .execute(&*self.pool)
        .await?;

        Ok(())
    }
}
