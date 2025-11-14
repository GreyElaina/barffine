use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use deadpool_libsql::{
    Pool,
    libsql::{self, params},
};
use serde_json::Value as JsonValue;

use crate::db::libsql::row_ext;
use crate::db::workspace_feature_repo::WorkspaceFeatureRepository;
use crate::feature::{FeatureFlag, FeatureNamespace};

fn workspace_id(namespace: &FeatureNamespace) -> Option<&str> {
    match namespace {
        FeatureNamespace::Workspace(id) => Some(id),
        _ => None,
    }
}

pub struct LibsqlWorkspaceFeatureRepository {
    pool: Pool,
}

impl LibsqlWorkspaceFeatureRepository {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn row_to_flag(row: libsql::Row, workspace_id: &str) -> Result<FeatureFlag> {
        Ok(FeatureFlag {
            key: row_ext::get(&row, "key")?,
            enabled: row_ext::get::<i64>(&row, "enabled")? != 0,
            namespace: FeatureNamespace::workspace(workspace_id),
            expires_at: None,
            metadata: JsonValue::Null,
        })
    }
}

#[async_trait]
impl WorkspaceFeatureRepository for LibsqlWorkspaceFeatureRepository {
    async fn list(&self, namespace: &FeatureNamespace) -> Result<Vec<FeatureFlag>> {
        let Some(workspace_id) = workspace_id(namespace) else {
            return Ok(Vec::new());
        };

        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT key, enabled FROM workspace_feature_flags WHERE workspace_id = ?",
                params![workspace_id],
            )
            .await?;

        let mut flags = Vec::new();
        while let Some(row) = rows.next().await? {
            flags.push(Self::row_to_flag(row, workspace_id)?);
        }
        Ok(flags)
    }

    async fn get(&self, namespace: &FeatureNamespace, key: &str) -> Result<Option<FeatureFlag>> {
        let Some(workspace_id) = workspace_id(namespace) else {
            return Ok(None);
        };

        let conn = self.pool.get().await?;
        let mut rows = conn
            .query(
                "SELECT key, enabled FROM workspace_feature_flags \
                 WHERE workspace_id = ? AND key = ?",
                params![workspace_id, key],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            Ok(Some(Self::row_to_flag(row, workspace_id)?))
        } else {
            Ok(None)
        }
    }

    async fn upsert(&self, flag: FeatureFlag) -> Result<()> {
        let Some(workspace_id) = workspace_id(&flag.namespace) else {
            return Ok(());
        };

        let conn = self.pool.get().await?;
        conn.execute(
            "INSERT INTO workspace_feature_flags (workspace_id, key, enabled, updated_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(workspace_id, key)
             DO UPDATE SET enabled = excluded.enabled, updated_at = excluded.updated_at",
            params![
                workspace_id,
                flag.key,
                if flag.enabled { 1 } else { 0 },
                Utc::now().timestamp()
            ],
        )
        .await?;
        Ok(())
    }

    async fn remove(&self, workspace_id: &str, key: &str) -> Result<()> {
        let conn = self.pool.get().await?;
        conn.execute(
            "DELETE FROM workspace_feature_flags WHERE workspace_id = ? AND key = ?",
            params![workspace_id, key],
        )
        .await?;
        Ok(())
    }
}
