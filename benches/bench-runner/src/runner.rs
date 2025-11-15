use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use hdrhistogram::Histogram;
use std::io::{self, Write};
use tokio::{sync::oneshot, task::JoinHandle};
use tracing::{debug, warn};

use crate::{
    client::BenchClient,
    config::{OperationKind, ScenarioSpec},
    dataset::Dataset,
    operations::{Completion, OperationContext, OperationError, WorkspaceCache, execute},
    sampler::{self, ProcessSample},
};

#[derive(Debug)]
pub struct RunOptions {
    pub scenario_name: String,
    pub spec: ScenarioSpec,
    pub duration_override: Option<Duration>,
    pub sample_interval: Duration,
    pub pid: Option<i32>,
    pub dataset: Option<Arc<Dataset>>,
}

#[derive(Debug)]
pub struct RunArtifacts {
    pub summary: RunSummary,
    pub samples: Vec<ProcessSample>,
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub scenario_name: String,
    pub duration: Duration,
    pub total_operations: u64,
    pub successes: u64,
    pub failures: u64,
    pub skipped: u64,
    pub throughput_per_sec: f64,
    pub latency: Option<LatencyStats>,
    pub resources: Option<ResourceSummary>,
}

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

#[derive(Debug, Clone)]
pub struct ResourceSummary {
    pub sample_count: usize,
    pub rss_peak_mb: f64,
    pub rss_p95_mb: f64,
    pub virtual_peak_mb: f64,
    pub cpu_avg_percent: f64,
}

pub async fn run_scenario(client: Arc<BenchClient>, options: RunOptions) -> Result<RunArtifacts> {
    if options.spec.operations.is_empty() {
        return Err(anyhow!("scenario has no operations"));
    }

    let duration = options
        .duration_override
        .or_else(|| options.spec.duration_secs.map(Duration::from_secs))
        .unwrap_or_else(|| Duration::from_secs(120));

    let plan = OperationPlan::new(&options.spec.operations);
    let cache = WorkspaceCache::new();
    if let Some(dataset) = options.dataset.as_ref() {
        for workspace in dataset.workspaces() {
            cache.register(workspace.workspace_id.clone());
        }
    }

    let ctx = OperationContext {
        client: client.clone(),
        cache: cache.clone(),
        dataset: options.dataset.clone(),
    };

    let start = Instant::now();
    let progress = ProgressReporter::spawn(options.scenario_name.clone(), duration, start);
    let deadline = start + duration;
    let think_time = Duration::from_millis(options.spec.think_time_ms);

    let sampler_handle = options
        .pid
        .map(|pid| sampler::spawn(pid, options.sample_interval, start));

    let mut handles: Vec<JoinHandle<WorkerStats>> = Vec::with_capacity(options.spec.concurrency);
    for worker_id in 0..options.spec.concurrency {
        let ctx = OperationContext {
            client: ctx.client.clone(),
            cache: ctx.cache.clone(),
            dataset: ctx.dataset.clone(),
        };
        let plan = plan.clone();
        let worker_deadline = deadline;
        let think_time = think_time;
        handles.push(tokio::spawn(async move {
            worker_loop(worker_id, ctx, plan, think_time, worker_deadline).await
        }));
    }

    let mut aggregate = WorkerStats::new();
    for handle in handles {
        let stats = handle.await.context("worker task panicked")?;
        aggregate.merge(stats);
    }

    let samples = if let Some(handle) = sampler_handle {
        handle.stop().await?
    } else {
        Vec::new()
    };

    let total_duration = start.elapsed().min(duration);
    let total_ops = aggregate.success + aggregate.failures;
    let throughput = if total_duration.as_secs_f64() > 0.0 {
        total_ops as f64 / total_duration.as_secs_f64()
    } else {
        0.0
    };

    let latency = if aggregate.histogram.len() > 0 {
        Some(LatencyStats {
            p50_ms: histogram_value_ms(&aggregate.histogram, 50.0),
            p95_ms: histogram_value_ms(&aggregate.histogram, 95.0),
            p99_ms: histogram_value_ms(&aggregate.histogram, 99.0),
            max_ms: aggregate.histogram.max() as f64 / 1_000.0,
        })
    } else {
        None
    };

    let resource_summary = if samples.is_empty() {
        None
    } else {
        Some(compute_resource_summary(&samples))
    };

    let summary = RunSummary {
        scenario_name: options.scenario_name,
        duration: total_duration,
        total_operations: total_ops,
        successes: aggregate.success,
        failures: aggregate.failures,
        skipped: aggregate.skipped,
        throughput_per_sec: throughput,
        latency,
        resources: resource_summary.clone(),
    };

    if let Some(reporter) = progress {
        reporter.stop().await;
    }

    Ok(RunArtifacts { summary, samples })
}

fn histogram_value_ms(hist: &Histogram<u64>, percentile: f64) -> f64 {
    hist.value_at_percentile(percentile) as f64 / 1_000.0
}

fn compute_resource_summary(samples: &[ProcessSample]) -> ResourceSummary {
    let rss_peak = samples.iter().map(|s| s.rss_bytes).max().unwrap_or(0);
    let virtual_peak = samples.iter().map(|s| s.virtual_bytes).max().unwrap_or(0);
    let mut rss_values: Vec<u64> = samples.iter().map(|s| s.rss_bytes).collect();
    rss_values.sort();
    let rss_p95 = percentile_u64(&rss_values, 95.0);
    let cpu_avg = if samples.is_empty() {
        0.0
    } else {
        samples
            .iter()
            .map(|s| f64::from(s.cpu_percent))
            .sum::<f64>()
            / samples.len() as f64
    };

    ResourceSummary {
        sample_count: samples.len(),
        rss_peak_mb: bytes_to_mb(rss_peak),
        rss_p95_mb: bytes_to_mb(rss_p95),
        virtual_peak_mb: bytes_to_mb(virtual_peak),
        cpu_avg_percent: cpu_avg,
    }
}

fn bytes_to_mb(value: u64) -> f64 {
    value as f64 / (1024.0 * 1024.0)
}

fn percentile_u64(values: &[u64], percentile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let rank = ((percentile / 100.0) * (values.len() as f64 - 1.0)).round() as usize;
    values[rank]
}

struct ProgressReporter {
    stop_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
}

impl ProgressReporter {
    fn spawn(name: String, duration: Duration, start: Instant) -> Option<Self> {
        if duration.is_zero() {
            return None;
        }
        let (stop_tx, stop_rx) = oneshot::channel();
        let handle = tokio::spawn(run_progress(name, duration, start, stop_rx));
        Some(Self {
            stop_tx: Some(stop_tx),
            handle,
        })
    }

    async fn stop(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.handle.await;
    }
}

async fn run_progress(
    scenario: String,
    duration: Duration,
    start: Instant,
    mut stop_rx: oneshot::Receiver<()>,
) {
    let mut ticker = tokio::time::interval(tokio::time::Duration::from_millis(250));
    loop {
        tokio::select! {
            _ = &mut stop_rx => {
                break;
            }
            _ = ticker.tick() => {
                render_progress_line(&scenario, duration, start);
            }
        }
    }
    render_progress_line(&scenario, duration, start);
    println!();
}

fn render_progress_line(scenario: &str, duration: Duration, start: Instant) {
    if duration.is_zero() {
        return;
    }
    let elapsed = Instant::now().saturating_duration_since(start);
    let progress = (elapsed.as_secs_f64() / duration.as_secs_f64()).clamp(0.0, 1.0);
    let bar = progress_bar(progress, 30);
    let percent = progress * 100.0;
    let _ = write!(
        io::stdout(),
        "\r[{}] {:>6.1}% {:>6.1}s/{:>6.1}s {}",
        bar,
        percent,
        elapsed.as_secs_f64().min(duration.as_secs_f64()),
        duration.as_secs_f64(),
        scenario
    );
    let _ = io::stdout().flush();
}

fn progress_bar(progress: f64, width: usize) -> String {
    let mut filled = (progress * width as f64).round() as usize;
    if filled > width {
        filled = width;
    }
    let mut bar = String::with_capacity(width);
    for idx in 0..width {
        if idx < filled {
            bar.push('#');
        } else {
            bar.push('-');
        }
    }
    bar
}

#[derive(Clone)]
struct OperationPlan {
    ranges: Vec<(u32, OperationKind)>,
    total: u32,
}

impl OperationPlan {
    fn new(specs: &[crate::config::OperationSpec]) -> Self {
        let mut ranges = Vec::new();
        let mut cursor = 0;
        for spec in specs {
            cursor += spec.weight.max(1);
            ranges.push((cursor, spec.kind.clone()));
        }
        Self {
            ranges,
            total: cursor,
        }
    }

    fn pick(&self) -> OperationKind {
        if self.total == 0 {
            return self.ranges[0].1.clone();
        }
        let roll = fastrand::u32(0..self.total);
        for (end, kind) in &self.ranges {
            if roll < *end {
                return kind.clone();
            }
        }
        self.ranges
            .last()
            .map(|(_, kind)| kind.clone())
            .expect("operation ranges not empty")
    }
}

async fn worker_loop(
    worker_id: usize,
    ctx: OperationContext,
    plan: OperationPlan,
    think_time: Duration,
    deadline: Instant,
) -> WorkerStats {
    let mut stats = WorkerStats::new();
    while Instant::now() < deadline {
        let op = plan.pick();
        let start = Instant::now();
        match execute(&op, &ctx).await {
            Ok(Completion::Success) => {
                let elapsed = start.elapsed();
                if let Err(err) = stats.histogram.record(elapsed.as_micros() as u64) {
                    warn!(?err, "failed to record latency");
                }
                stats.success += 1;
            }
            Ok(Completion::Skipped { reason }) => {
                stats.skipped += 1;
                debug!(%worker_id, %reason, "operation skipped");
            }
            Err(OperationError::MissingWorkspace) => {
                stats.failures += 1;
                warn!("workspace cache empty; skipping");
            }
            Err(OperationError::Transport(err)) => {
                stats.failures += 1;
                warn!(%worker_id, operation = ?op, %err, "operation failed");
            }
        }
        if !think_time.is_zero() {
            tokio::time::sleep(think_time).await;
        }
    }
    stats
}

fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 600_000_000, 3).expect("histogram bounds")
}

struct WorkerStats {
    success: u64,
    failures: u64,
    skipped: u64,
    histogram: Histogram<u64>,
}

impl WorkerStats {
    fn new() -> Self {
        Self {
            success: 0,
            failures: 0,
            skipped: 0,
            histogram: new_histogram(),
        }
    }

    fn merge(&mut self, other: WorkerStats) {
        self.success += other.success;
        self.failures += other.failures;
        self.skipped += other.skipped;
        if let Err(err) = self.histogram.add(&other.histogram) {
            warn!(?err, "failed to merge histograms");
        }
    }
}
