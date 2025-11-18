use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{DocId, WorkspaceId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchDocument {
    pub doc_id: DocId,
    pub workspace_id: WorkspaceId,
    pub title: Option<String>,
    pub content: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResults {
    pub total: u64,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    pub doc_id: DocId,
    pub workspace_id: WorkspaceId,
    pub score: f32,
    pub snippet: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub workspace_ids: Vec<WorkspaceId>,
    pub term: String,
    pub limit: u32,
    pub offset: u32,
}

impl SearchQuery {
    pub fn new(term: impl Into<String>) -> Self {
        Self {
            workspace_ids: Vec::new(),
            term: term.into(),
            limit: 20,
            offset: 0,
        }
    }

    pub fn with_workspace(mut self, workspace_id: impl Into<WorkspaceId>) -> Self {
        self.workspace_ids.push(workspace_id.into());
        self
    }
}

#[async_trait]
pub trait SearchIndex: Send + Sync {
    async fn upsert(&self, doc: SearchDocument) -> Result<()>;
    async fn remove(&self, workspace_id: &str, doc_id: &str) -> Result<()>;
    async fn query(&self, query: SearchQuery) -> Result<SearchResults>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_api() {
        let query = SearchQuery::new("affine").with_workspace(WorkspaceId::from("workspace"));
        assert_eq!(query.term, "affine");
        assert_eq!(
            query.workspace_ids,
            &[WorkspaceId::from("workspace")]
        );
    }
}
