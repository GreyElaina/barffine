use std::{collections::HashMap, fs, path::Path};

use anyhow::{Context, Result};
use rand::{Rng, seq::IndexedRandom};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatasetFile {
    #[serde(default)]
    pub workspaces: Vec<WorkspaceDocs>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceDocs {
    pub workspace_id: String,
    #[serde(default)]
    pub doc_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Dataset {
    file: DatasetFile,
    cache: Vec<DocCoordinate>,
}

impl Dataset {
    pub fn load(path: &Path) -> Result<Self> {
        let data = if path.exists() {
            let raw = fs::read_to_string(path)
                .with_context(|| format!("failed to read dataset file {path:?}"))?;
            serde_json::from_str::<DatasetFile>(&raw)
                .with_context(|| format!("failed to parse dataset file {path:?}"))?
        } else {
            DatasetFile::default()
        };
        Ok(Self::from_file(data))
    }

    pub fn from_file(file: DatasetFile) -> Self {
        let cache = file
            .workspaces
            .iter()
            .flat_map(|ws| {
                ws.doc_ids.iter().map(move |doc| DocCoordinate {
                    workspace_id: ws.workspace_id.clone(),
                    doc_id: doc.clone(),
                })
            })
            .collect();
        Self { file, cache }
    }

    pub fn to_file(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create dataset dir {parent:?}"))?;
        }
        let raw = serde_json::to_string_pretty(&self.file)?;
        fs::write(path, raw).with_context(|| format!("failed to write dataset file {path:?}"))?;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn workspaces(&self) -> &[WorkspaceDocs] {
        &self.file.workspaces
    }

    pub fn sync_workspaces(&mut self, ids: &[String]) {
        let mut existing: HashMap<String, WorkspaceDocs> = self
            .file
            .workspaces
            .iter()
            .cloned()
            .map(|ws| (ws.workspace_id.clone(), ws))
            .collect();

        let mut desired: Vec<String> = ids.iter().cloned().collect();
        desired.sort();
        desired.dedup();

        let mut updated = Vec::with_capacity(desired.len());
        for id in desired {
            if let Some(ws) = existing.remove(&id) {
                updated.push(ws);
            } else {
                updated.push(WorkspaceDocs {
                    workspace_id: id,
                    doc_ids: vec![],
                });
            }
        }

        self.file.workspaces = updated;
        self.refresh_cache();
    }

    pub fn set_doc_ids(&mut self, workspace_id: &str, doc_ids: Vec<String>) {
        let mut normalized: Vec<String> = doc_ids
            .into_iter()
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty())
            .collect();
        normalized.sort();
        normalized.dedup();

        if let Some(workspace) = self
            .file
            .workspaces
            .iter_mut()
            .find(|ws| ws.workspace_id == workspace_id)
        {
            workspace.doc_ids = normalized;
        } else {
            self.file.workspaces.push(WorkspaceDocs {
                workspace_id: workspace_id.to_string(),
                doc_ids: normalized,
            });
        }
        self.refresh_cache();
    }

    #[allow(dead_code)]
    pub fn push_doc(&mut self, workspace_id: &str, doc_id: &str) {
        if let Some(workspace) = self
            .file
            .workspaces
            .iter_mut()
            .find(|ws| ws.workspace_id == workspace_id)
        {
            if !workspace.doc_ids.iter().any(|d| d == doc_id) {
                workspace.doc_ids.push(doc_id.to_string());
            }
        } else {
            self.file.workspaces.push(WorkspaceDocs {
                workspace_id: workspace_id.to_string(),
                doc_ids: vec![doc_id.to_string()],
            });
        }
        self.refresh_cache();
    }

    pub fn random_doc<'a>(&'a self, rng: &mut impl Rng) -> Option<DocCoordinate> {
        if self.cache.is_empty() {
            return None;
        }
        self.cache.choose(rng).cloned()
    }

    fn refresh_cache(&mut self) {
        self.cache = self
            .file
            .workspaces
            .iter()
            .flat_map(|ws| {
                ws.doc_ids.iter().map(move |doc| DocCoordinate {
                    workspace_id: ws.workspace_id.clone(),
                    doc_id: doc.clone(),
                })
            })
            .collect();
    }
}

#[derive(Debug, Clone)]
pub struct DocCoordinate {
    pub workspace_id: String,
    pub doc_id: String,
}
