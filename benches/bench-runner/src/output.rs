use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;

use crate::runner::{LatencyStats, ResourceSummary, RunArtifacts, RunSummary};
use crate::sampler::ProcessSample;

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
    let mut writer = String::from(
        "timestamp_ms,rss_mb,virtual_mb,cpu_percent,read_total_mb,write_total_mb,read_mb_per_s,write_mb_per_s\n",
    );
    let mut prev: Option<&ProcessSample> = None;
    for sample in &artifacts.samples {
        let (read_rate, write_rate) = if let Some(prev) = prev {
            let dt_ms = sample.timestamp_ms.saturating_sub(prev.timestamp_ms).max(1);
            (
                bytes_per_second(
                    sample
                        .total_read_bytes
                        .saturating_sub(prev.total_read_bytes),
                    dt_ms,
                ),
                bytes_per_second(
                    sample
                        .total_written_bytes
                        .saturating_sub(prev.total_written_bytes),
                    dt_ms,
                ),
            )
        } else {
            (0.0, 0.0)
        };
        writer.push_str(&format!(
            "{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}\n",
            sample.timestamp_ms,
            sample.rss_bytes as f64 / (1024.0 * 1024.0),
            sample.virtual_bytes as f64 / (1024.0 * 1024.0),
            sample.cpu_percent,
            sample.total_read_bytes as f64 / (1024.0 * 1024.0),
            sample.total_written_bytes as f64 / (1024.0 * 1024.0),
            read_rate,
            write_rate,
        ));
        prev = Some(sample);
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
    io_read_total_mb: f64,
    io_write_total_mb: f64,
    io_read_p95_mb_s: f64,
    io_write_p95_mb_s: f64,
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
            io_read_total_mb: res.io_read_total_mb,
            io_write_total_mb: res.io_write_total_mb,
            io_read_p95_mb_s: res.io_read_p95_mb_s,
            io_write_p95_mb_s: res.io_write_p95_mb_s,
        }
    }
}

fn bytes_per_second(delta_bytes: u64, delta_ms: u64) -> f64 {
    if delta_ms == 0 {
        return 0.0;
    }
    let per_second = delta_bytes as f64 * 1000.0 / delta_ms as f64;
    per_second / (1024.0 * 1024.0)
}
