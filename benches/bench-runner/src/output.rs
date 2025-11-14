use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;

use crate::runner::{LatencyStats, ResourceSummary, RunArtifacts, RunSummary};

pub fn persist_results(base_dir: &Path, artifacts: &RunArtifacts) -> Result<PathBuf> {
    let timestamp = Utc::now().format("%Y%m%d-%H%M%S");
    let dir = base_dir.join(format!("{}-{}", timestamp, artifacts.summary.scenario_name));
    fs::create_dir_all(&dir).with_context(|| format!("failed to create result dir {dir:?}"))?;

    let summary_path = dir.join("summary.json");
    let summary_record = SummaryRecord::from(&artifacts.summary);
    let json = serde_json::to_string_pretty(&summary_record)?;
    fs::write(&summary_path, json)
        .with_context(|| format!("failed to write summary {summary_path:?}"))?;

    let samples_path = dir.join("memory.csv");
    write_samples(&samples_path, &artifacts)?;

    Ok(dir)
}

fn write_samples(path: &Path, artifacts: &RunArtifacts) -> Result<()> {
    if artifacts.samples.is_empty() {
        return Ok(());
    }
    let mut writer = String::from("timestamp_ms,rss_mb,virtual_mb,cpu_percent\n");
    for sample in &artifacts.samples {
        writer.push_str(&format!(
            "{},{:.2},{:.2},{:.2}\n",
            sample.timestamp_ms,
            sample.rss_bytes as f64 / (1024.0 * 1024.0),
            sample.virtual_bytes as f64 / (1024.0 * 1024.0),
            sample.cpu_percent,
        ));
    }
    fs::write(path, writer).with_context(|| format!("failed to write memory samples {path:?}"))?;
    Ok(())
}

#[derive(Serialize)]
struct SummaryRecord {
    scenario: String,
    duration_secs: f64,
    total_operations: u64,
    successes: u64,
    failures: u64,
    skipped: u64,
    throughput_per_sec: f64,
    latency: Option<LatencyRecord>,
    resources: Option<ResourceRecord>,
}

#[derive(Serialize)]
struct LatencyRecord {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Serialize)]
struct ResourceRecord {
    sample_count: usize,
    rss_peak_mb: f64,
    rss_p95_mb: f64,
    virtual_peak_mb: f64,
    cpu_avg_percent: f64,
}

impl From<&RunSummary> for SummaryRecord {
    fn from(summary: &RunSummary) -> Self {
        Self {
            scenario: summary.scenario_name.clone(),
            duration_secs: summary.duration.as_secs_f64(),
            total_operations: summary.total_operations,
            successes: summary.successes,
            failures: summary.failures,
            skipped: summary.skipped,
            throughput_per_sec: summary.throughput_per_sec,
            latency: summary.latency.as_ref().map(LatencyRecord::from),
            resources: summary.resources.as_ref().map(ResourceRecord::from),
        }
    }
}

impl From<&LatencyStats> for LatencyRecord {
    fn from(stats: &LatencyStats) -> Self {
        Self {
            p50_ms: stats.p50_ms,
            p95_ms: stats.p95_ms,
            p99_ms: stats.p99_ms,
            max_ms: stats.max_ms,
        }
    }
}

impl From<&ResourceSummary> for ResourceRecord {
    fn from(res: &ResourceSummary) -> Self {
        Self {
            sample_count: res.sample_count,
            rss_peak_mb: res.rss_peak_mb,
            rss_p95_mb: res.rss_p95_mb,
            virtual_peak_mb: res.virtual_peak_mb,
            cpu_avg_percent: res.cpu_avg_percent,
        }
    }
}
