use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, Eq, PartialEq, clap::ValueEnum)]
pub enum ScenarioKind {
    Low,
    Balanced,
    High,
    ReadHeavy,
    WriteHeavy,
}

impl ScenarioKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScenarioKind::Low => "low",
            ScenarioKind::Balanced => "balanced",
            ScenarioKind::High => "high",
            ScenarioKind::ReadHeavy => "read-heavy",
            ScenarioKind::WriteHeavy => "write-heavy",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BenchConfig {
    pub target: TargetConfig,
    #[serde(default)]
    pub dataset: DatasetConfig,
    #[serde(default)]
    pub scenarios: HashMap<String, ScenarioSpec>,
    #[serde(default = "default_output_dir")]
    pub output_dir: PathBuf,
    #[serde(default = "default_sample_interval_ms")]
    pub sample_interval_ms: u64,
}

fn default_output_dir() -> PathBuf {
    PathBuf::from("bench-results")
}

fn default_sample_interval_ms() -> u64 {
    500
}

impl BenchConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {path:?}"))?;
        let mut cfg: BenchConfig = toml::from_str(&raw)
            .with_context(|| format!("failed to parse config file {path:?}"))?;
        if cfg.output_dir.as_os_str().is_empty() {
            cfg.output_dir = default_output_dir();
        }
        Ok(cfg)
    }

    pub fn scenario(&self, kind: ScenarioKind) -> ScenarioSpec {
        if let Some(spec) = self.scenarios.get(kind.as_str()) {
            return spec.clone();
        }
        ScenarioSpec::preset(kind)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TargetConfig {
    pub base_url: String,
    pub admin_email: String,
    pub admin_password: String,
    #[serde(default)]
    pub graphql_endpoint: Option<String>,
    #[serde(default)]
    pub rest_endpoint: Option<String>,
    #[serde(default)]
    pub pid: Option<i32>,
    #[serde(default)]
    pub pid_file: Option<PathBuf>,
}

impl TargetConfig {
    pub fn graphql_endpoint(&self) -> String {
        self.graphql_endpoint
            .as_deref()
            .map(|s| s.to_owned())
            .unwrap_or_else(|| format!("{}/graphql", self.base_url.trim_end_matches('/')))
    }

    pub fn rest_endpoint(&self) -> String {
        self.rest_endpoint
            .as_deref()
            .map(|s| s.to_owned())
            .unwrap_or_else(|| self.base_url.trim_end_matches('/').to_owned())
    }

    pub fn resolve_pid(&self) -> Option<i32> {
        if let Some(pid) = self.pid {
            return Some(pid);
        }
        let path = self.pid_file.as_ref()?;
        match fs::read_to_string(path) {
            Ok(contents) => contents
                .trim()
                .parse::<i32>()
                .map_err(|err| {
                    tracing::warn!(?path, %err, "failed to parse pid file");
                })
                .ok(),
            Err(err) => {
                tracing::warn!(?path, %err, "failed to read pid file");
                None
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetConfig {
    #[serde(default = "default_dataset_path")]
    pub path: PathBuf,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        Self {
            path: default_dataset_path(),
        }
    }
}

fn default_dataset_path() -> PathBuf {
    PathBuf::from("bench-results/dataset.json")
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioSpec {
    pub concurrency: usize,
    #[serde(default)]
    pub think_time_ms: u64,
    #[serde(default)]
    pub duration_secs: Option<u64>,
    #[serde(default)]
    pub operations: Vec<OperationSpec>,
}

impl ScenarioSpec {
    pub fn preset(kind: ScenarioKind) -> Self {
        match kind {
            ScenarioKind::Low => ScenarioSpec {
                concurrency: 2,
                think_time_ms: 750,
                duration_secs: Some(10),
                operations: vec![
                    OperationSpec::new(OperationKind::Health, 1),
                    OperationSpec::new(OperationKind::GraphqlWorkspaces, 2),
                    OperationSpec::new(OperationKind::GraphqlWorkspaceMeta, 1),
                ],
            },
            ScenarioKind::Balanced => ScenarioSpec {
                concurrency: 32,
                think_time_ms: 0,
                duration_secs: Some(30),
                operations: vec![
                    OperationSpec::new(OperationKind::GraphqlWorkspaces, 2),
                    OperationSpec::new(OperationKind::GraphqlWorkspaceMeta, 3),
                    OperationSpec::new(OperationKind::GraphqlCreateWorkspace, 1),
                    OperationSpec::new(OperationKind::GraphqlListComments, 1),
                    OperationSpec::new(OperationKind::RestDocContent, 2),
                ],
            },
            ScenarioKind::High => ScenarioSpec {
                concurrency: 64,
                think_time_ms: 0,
                duration_secs: Some(30),
                operations: vec![
                    OperationSpec::new(OperationKind::GraphqlWorkspaceMeta, 3),
                    OperationSpec::new(OperationKind::GraphqlListComments, 2),
                    OperationSpec::new(OperationKind::RestDocContent, 3),
                    OperationSpec::new(OperationKind::RestDocMarkdown, 1),
                ],
            },
            ScenarioKind::ReadHeavy => ScenarioSpec {
                concurrency: 64,
                think_time_ms: 0,
                duration_secs: Some(60),
                operations: vec![
                    OperationSpec::new(OperationKind::GraphqlWorkspaces, 1),
                    OperationSpec::new(OperationKind::GraphqlWorkspaceMeta, 3),
                    OperationSpec::new(OperationKind::GraphqlWorkspaceDocs, 4),
                    OperationSpec::new(OperationKind::RestDocContent, 4),
                    OperationSpec::new(OperationKind::RestDocMarkdown, 2),
                ],
            },
            ScenarioKind::WriteHeavy => ScenarioSpec {
                concurrency: 32,
                think_time_ms: 100,
                duration_secs: Some(20),
                operations: vec![
                    // 持续创建新的 workspace，驱动 workspace 相关写路径
                    OperationSpec::new(OperationKind::GraphqlCreateWorkspace, 1),
                    // 在现有 workspace 内高频复制文档，压测 DocStore/DocRepository 写路径
                    OperationSpec::new(OperationKind::GraphqlDuplicateDoc, 4),
                    // 混入少量读取，避免纯写导致极端热点
                    OperationSpec::new(OperationKind::RestDocContent, 2),
                ],
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OperationSpec {
    pub kind: OperationKind,
    #[serde(default = "default_weight")]
    pub weight: u32,
}

impl OperationSpec {
    pub fn new(kind: OperationKind, weight: u32) -> Self {
        Self { kind, weight }
    }
}

fn default_weight() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Health,
    GraphqlWorkspaces,
    GraphqlWorkspaceMeta,
    GraphqlCreateWorkspace,
    GraphqlWorkspaceDocs,
    GraphqlDuplicateDoc,
    GraphqlListComments,
    RestDocContent,
    RestDocMarkdown,
}

impl OperationKind {
    pub fn requires_docs(&self) -> bool {
        matches!(
            self,
            OperationKind::GraphqlListComments
                | OperationKind::GraphqlDuplicateDoc
                | OperationKind::RestDocContent
                | OperationKind::RestDocMarkdown
        )
    }
}
