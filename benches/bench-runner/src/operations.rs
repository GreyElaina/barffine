use std::sync::Arc;

use anyhow::{Result, anyhow};
use parking_lot::RwLock;
use rand::rng;
use serde_json::json;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    client::BenchClient,
    config::OperationKind,
    dataset::{Dataset, DocCoordinate},
};

pub struct WorkspaceCache {
    inner: RwLock<Vec<String>>,
}

impl WorkspaceCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(Vec::new()),
        })
    }

    pub fn register(&self, id: String) {
        let mut guard = self.inner.write();
        if !guard.iter().any(|existing| existing == &id) {
            guard.push(id);
        }
    }

    pub async fn random_or_fetch(
        &self,
        client: &Arc<BenchClient>,
    ) -> Result<String, OperationError> {
        {
            let guard = self.inner.read();
            if !guard.is_empty() {
                let idx = fastrand::usize(..guard.len());
                return Ok(guard[idx].clone());
            }
        }

        let ids = list_workspace_ids(client)
            .await
            .map_err(OperationError::Transport)?;
        if ids.is_empty() {
            return Err(OperationError::MissingWorkspace);
        }
        {
            let mut guard = self.inner.write();
            guard.clear();
            guard.extend(ids.iter().cloned());
        }
        let idx = fastrand::usize(..ids.len());
        Ok(ids[idx].clone())
    }
}

#[derive(Debug)]
pub enum Completion {
    Success,
    Skipped { reason: &'static str },
}

#[derive(thiserror::Error, Debug)]
pub enum OperationError {
    #[error("workspace list empty")]
    MissingWorkspace,
    #[error(transparent)]
    Transport(#[from] anyhow::Error),
}

pub struct OperationContext {
    pub client: Arc<BenchClient>,
    pub cache: Arc<WorkspaceCache>,
    pub dataset: Option<Arc<Dataset>>,
}

#[instrument(skip(ctx))]
pub async fn execute(
    kind: &OperationKind,
    ctx: &OperationContext,
) -> Result<Completion, OperationError> {
    match kind {
        OperationKind::Health => {
            let (status, _) = ctx.client.get_rest("/health", None).await?;
            if !status.is_success() {
                return Err(OperationError::Transport(anyhow!(
                    "healthcheck returned status {}",
                    status
                )));
            }
            Ok(Completion::Success)
        }
        OperationKind::GraphqlWorkspaces => {
            let _ = ctx
                .client
                .graphql(WORKSPACES_QUERY, serde_json::Value::Null)
                .await?;
            Ok(Completion::Success)
        }
        OperationKind::GraphqlWorkspaceMeta => {
            let workspace_id = ctx.cache.random_or_fetch(&ctx.client).await?;
            let _ = ctx
                .client
                .graphql(WORKSPACE_META_QUERY, json!({"id": workspace_id}))
                .await?;
            Ok(Completion::Success)
        }
        OperationKind::GraphqlCreateWorkspace => {
            let name = format!("bench-{}", Uuid::new_v4().simple());
            let data = ctx
                .client
                .graphql(
                    CREATE_WORKSPACE_MUTATION,
                    json!({ "input": { "name": name } }),
                )
                .await?;
            if let Some(id) = data["createWorkspace"]["id"].as_str() {
                ctx.cache.register(id.to_string());
            }
            Ok(Completion::Success)
        }
        OperationKind::GraphqlWorkspaceDocs => {
            // 选一个 workspace，拉取一页文档列表，压测 workspace.docs + 文档元数据链路
            let workspace_id = ctx.cache.random_or_fetch(&ctx.client).await?;
            let _ = ctx
                .client
                .graphql(
                    WORKSPACE_DOCS_QUERY,
                    json!({
                        "workspaceId": workspace_id,
                        "first": 50,
                        "after": serde_json::Value::Null,
                    }),
                )
                .await?;
            Ok(Completion::Success)
        }
        OperationKind::GraphqlDuplicateDoc => {
            let Some(doc) = pick_doc(&ctx.dataset) else {
                return Ok(Completion::Skipped {
                    reason: "doc_dataset",
                });
            };
            let target_doc_id = Uuid::new_v4().to_string();
            let _ = duplicate_doc(
                &ctx.client,
                &doc.workspace_id,
                &doc.doc_id,
                &target_doc_id,
                Some("bench-duplicate"),
            )
            .await
            .map_err(OperationError::Transport)?;
            Ok(Completion::Success)
        }
        OperationKind::GraphqlListComments => {
            let Some(doc) = pick_doc(&ctx.dataset) else {
                return Ok(Completion::Skipped {
                    reason: "doc_dataset",
                });
            };
            let _ = ctx
                .client
                .graphql(
                    LIST_COMMENTS_QUERY,
                    json!({
                        "workspaceId": doc.workspace_id,
                        "docId": doc.doc_id,
                        "pagination": { "first": 10 }
                    }),
                )
                .await?;
            Ok(Completion::Success)
        }
        OperationKind::RestDocContent => {
            let Some(doc) = pick_doc(&ctx.dataset) else {
                return Ok(Completion::Skipped {
                    reason: "doc_dataset",
                });
            };
            let path = format!(
                "/api/workspaces/{}/docs/{}/content",
                doc.workspace_id, doc.doc_id
            );
            let (status, _) = ctx.client.get_rest(&path, Some(&[("full", "1")])).await?;
            if !status.is_success() {
                return Err(OperationError::Transport(anyhow!(
                    "doc content returned status {}",
                    status
                )));
            }
            Ok(Completion::Success)
        }
        OperationKind::RestDocMarkdown => {
            let Some(doc) = pick_doc(&ctx.dataset) else {
                return Ok(Completion::Skipped {
                    reason: "doc_dataset",
                });
            };
            let path = format!(
                "/api/workspaces/{}/docs/{}/markdown",
                doc.workspace_id, doc.doc_id
            );
            let (status, _) = ctx.client.get_rest(&path, None).await?;
            if !status.is_success() {
                return Err(OperationError::Transport(anyhow!(
                    "doc markdown returned status {}",
                    status
                )));
            }
            Ok(Completion::Success)
        }
    }
}

fn pick_doc(dataset: &Option<Arc<Dataset>>) -> Option<DocCoordinate> {
    let dataset = dataset.as_ref()?;
    let mut rng = rng();
    dataset.random_doc(&mut rng)
}

pub async fn list_workspace_ids(client: &Arc<BenchClient>) -> Result<Vec<String>> {
    let data = client
        .graphql(WORKSPACES_QUERY, serde_json::Value::Null)
        .await?;
    let nodes = data["workspaces"]
        .as_array()
        .ok_or_else(|| anyhow!("workspaces query returned non-array"))?;
    let ids = nodes
        .iter()
        .filter_map(|node| node["id"].as_str().map(|s| s.to_string()))
        .collect();
    Ok(ids)
}

pub async fn list_workspace_doc_ids(
    client: &Arc<BenchClient>,
    workspace_id: &str,
    max_docs: usize,
    page_size: usize,
) -> Result<Vec<String>> {
    let mut collected = Vec::new();
    let mut after: Option<String> = None;
    let page_size = page_size.clamp(1, 100) as i32;

    loop {
        let data = client
            .graphql(
                WORKSPACE_DOCS_QUERY,
                json!({
                    "workspaceId": workspace_id,
                    "first": page_size,
                    "after": after,
                }),
            )
            .await?;

        let workspace = data
            .get("workspace")
            .ok_or_else(|| anyhow!("workspace query missing workspace field"))?;

        if workspace.is_null() {
            return Err(anyhow!("workspace {workspace_id} not found"));
        }

        let docs = workspace
            .get("docs")
            .and_then(|value| value.as_object())
            .ok_or_else(|| anyhow!("workspace {workspace_id} docs response malformed"))?;

        if let Some(edges) = docs.get("edges").and_then(|value| value.as_array()) {
            for edge in edges {
                if let Some(id) = edge
                    .get("node")
                    .and_then(|node| node.get("id"))
                    .and_then(|value| value.as_str())
                {
                    collected.push(id.to_string());
                }
            }
        }

        let has_next = docs
            .get("pageInfo")
            .and_then(|value| value.get("hasNextPage"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false);

        if !has_next {
            break;
        }

        after = docs
            .get("pageInfo")
            .and_then(|value| value.get("endCursor"))
            .and_then(|value| value.as_str())
            .map(|cursor| cursor.to_string());

        if after.is_none() {
            break;
        }
    }

    if max_docs > 0 && collected.len() > max_docs {
        fastrand::shuffle(&mut collected);
        collected.truncate(max_docs);
    }

    Ok(collected)
}

pub async fn duplicate_doc(
    client: &Arc<BenchClient>,
    workspace_id: &str,
    source_doc_id: &str,
    target_doc_id: &str,
    title: Option<&str>,
) -> Result<String> {
    let mut payload = json!({
        "workspaceId": workspace_id,
        "docId": source_doc_id,
        "targetDocId": target_doc_id,
    });
    if let Some(title) = title {
        payload
            .as_object_mut()
            .expect("payload object")
            .insert("title".into(), json!(title));
    }

    let data = client
        .graphql(DUPLICATE_DOC_MUTATION, json!({ "input": payload }))
        .await?;
    let doc = data
        .get("duplicateDoc")
        .and_then(|value| value.as_object())
        .ok_or_else(|| anyhow!("duplicateDoc response missing payload"))?;
    let id = doc
        .get("id")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("duplicateDoc response missing id"))?
        .to_string();
    Ok(id)
}

const WORKSPACES_QUERY: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../graphql/operations/workspaces.graphql"
));

const WORKSPACE_META_QUERY: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../graphql/operations/workspace_meta.graphql"
));

const CREATE_WORKSPACE_MUTATION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../graphql/operations/create_workspace.graphql"
));

const LIST_COMMENTS_QUERY: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../graphql/operations/list_comments.graphql"
));

const WORKSPACE_DOCS_QUERY: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../graphql/operations/workspace_docs.graphql"
));

const DUPLICATE_DOC_MUTATION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../graphql/operations/duplicate_doc.graphql"
));
