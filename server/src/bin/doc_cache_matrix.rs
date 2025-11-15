use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use barffine_core::{
    config::{AppConfig, BlobStoreBackend, DocDataBackend},
    db::Database,
};
use barffine_server::{
    build_state_with_config,
    doc::cache::{DocCacheAdaptiveConfig, DocCacheConfig},
    socket::rooms::SpaceType,
    state::{AppState, StateBuildConfig},
};
use clap::{Parser, ValueEnum};
use pprof::ProfilerGuardBuilder;
use tokio::task::JoinSet;
use tracing::{info, warn};
use uuid::Uuid;
use yrs::{Doc, ReadTxn, StateVector, Text, Transact};

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "Benchmark DocCache parameter combinations under stress"
)]
struct MatrixOptions {
    /// Number of iterations executed by each worker
    #[arg(long, default_value_t = 200)]
    iterations: usize,

    /// Workload profile for per-iteration update volume
    #[arg(long, value_enum, default_value_t = LoadProfile::High)]
    load_profile: LoadProfile,

    /// Overwrite the load profile with a custom updates-per-iteration value
    #[arg(long)]
    updates_per_iteration: Option<usize>,

    /// Concurrency profile controlling worker count
    #[arg(long, value_enum, default_value_t = ConcurrencyProfile::High)]
    concurrency_profile: ConcurrencyProfile,

    /// Overwrite the concurrency profile with a custom worker count
    #[arg(long)]
    workers: Option<usize>,

    /// Milliseconds to wait after the workload to allow async flushes to finish
    #[arg(long, default_value_t = 250)]
    flush_grace_ms: u64,

    /// Emit progress every N iterations (per worker); set to 0 to disable
    #[arg(long, default_value_t = 250)]
    log_every: usize,

    /// Static (non-adaptive) flush delays in milliseconds
    #[arg(
        long,
        value_delimiter = ',',
        default_values_t = vec![50, 250, 750, 2000]
    )]
    static_flush_ms: Vec<u64>,

    /// Static (non-adaptive) pending byte thresholds
    #[arg(
        long,
        value_delimiter = ',',
        default_values_t = vec![64 * 1024, 256 * 1024, 1024 * 1024]
    )]
    static_pending_bytes: Vec<usize>,

    /// Target flush interval (ms) for adaptive mode
    #[arg(
        long,
        value_delimiter = ',',
        default_values_t = vec![150, 500, 1000, 2000]
    )]
    adaptive_target_ms: Vec<u64>,

    /// Target pending bytes for adaptive mode
    #[arg(
        long,
        value_delimiter = ',',
        default_values_t = vec![64 * 1024, 256 * 1024, 1024 * 1024]
    )]
    adaptive_target_bytes: Vec<usize>,

    /// Minimum flush delay (ms) enforced by adaptive mode
    #[arg(long, default_value_t = 25)]
    adaptive_min_flush_ms: u64,

    /// Maximum flush delay (ms) enforced by adaptive mode
    #[arg(long, default_value_t = 5000)]
    adaptive_max_flush_ms: u64,

    /// Minimum pending bytes enforced by adaptive mode
    #[arg(long, default_value_t = 32 * 1024)]
    adaptive_min_pending_bytes: usize,

    /// Maximum pending bytes enforced by adaptive mode
    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    adaptive_max_pending_bytes: usize,

    /// EMA alpha used by adaptive statistics
    #[arg(long, default_value_t = 0.25)]
    adaptive_alpha: f64,

    /// Skip static (non-adaptive) combinations
    #[arg(long, default_value_t = false)]
    skip_static: bool,

    /// Skip adaptive combinations
    #[arg(long, default_value_t = false)]
    skip_adaptive: bool,

    /// Optional flamegraph path (set empty to disable)
    #[arg(long)]
    flamegraph: Option<PathBuf>,
}

#[derive(Clone)]
struct WorkloadContext {
    state: AppState,
    workspace_id: String,
    doc_id: String,
    owner_id: String,
    _db_guard: Arc<tempfile::TempDir>,
}

#[derive(Clone)]
struct CaseConfig {
    label: String,
    mode: &'static str,
    doc_cache: DocCacheConfig,
}

struct CaseResult {
    label: String,
    mode: String,
    elapsed_secs: f64,
    throughput: f64,
    flush_count: u64,
    avg_flush_ms: f64,
    pending_bytes: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let opts = MatrixOptions::parse();
    validate_options(&opts)?;
    let workload = opts.resolve_workload();
    validate_workload(&workload)?;

    let cases = build_cases(&opts);
    if cases.is_empty() {
        return Err(anyhow!(
            "no parameter combinations available (check skip flags and lists)"
        ));
    }

    info!(cases = cases.len(), "starting doc cache matrix benchmark");
    let mut results = Vec::with_capacity(cases.len());
    for case in cases {
        info!(label = %case.label, mode = case.mode, "running case");
        let result = run_case(&opts, &workload, &case).await?;
        info!(
            label = %result.label,
            mode = %result.mode,
            elapsed_secs = result.elapsed_secs,
            throughput = result.throughput,
            flushes = result.flush_count,
            "case complete"
        );
        results.push(result);
    }

    print_summary(&results);
    Ok(())
}

fn validate_workload(workload: &WorkloadSettings) -> Result<()> {
    if workload.iterations == 0 {
        return Err(anyhow!("--iterations must be greater than 0"));
    }
    if workload.updates_per_iteration == 0 {
        return Err(anyhow!("--updates-per-iteration must be greater than 0"));
    }
    if workload.workers == 0 {
        return Err(anyhow!("--workers must be greater than 0"));
    }
    Ok(())
}

impl MatrixOptions {
    fn resolve_workload(&self) -> WorkloadSettings {
        let updates = self
            .updates_per_iteration
            .unwrap_or(match self.load_profile {
                LoadProfile::Low => 2,
                LoadProfile::High => 16,
            });
        let workers = self.workers.unwrap_or(match self.concurrency_profile {
            ConcurrencyProfile::Low => 2,
            ConcurrencyProfile::High => 8,
        });
        WorkloadSettings {
            iterations: self.iterations,
            updates_per_iteration: updates,
            workers,
        }
    }
}

fn validate_options(opts: &MatrixOptions) -> Result<()> {
    if opts.adaptive_alpha <= 0.0 || opts.adaptive_alpha >= 1.0 {
        return Err(anyhow!("--adaptive-alpha must be in (0,1)"));
    }
    Ok(())
}

fn build_cases(opts: &MatrixOptions) -> Vec<CaseConfig> {
    let mut cases = Vec::new();

    if !opts.skip_static {
        for &flush_ms in &opts.static_flush_ms {
            for &pending in &opts.static_pending_bytes {
                let mut config = DocCacheConfig::default();
                config.flush_delay = Duration::from_millis(flush_ms);
                config.max_pending_bytes = pending;
                config.adaptive = None;
                let label = format!("static:f{flush_ms}ms:p{}k", pending / 1024);
                cases.push(CaseConfig {
                    label,
                    mode: "static",
                    doc_cache: config,
                });
            }
        }
    }

    if !opts.skip_adaptive {
        for &target_ms in &opts.adaptive_target_ms {
            for &target_bytes in &opts.adaptive_target_bytes {
                let adaptive = DocCacheAdaptiveConfig {
                    target_flush_interval: Duration::from_millis(target_ms),
                    min_flush_delay: Duration::from_millis(opts.adaptive_min_flush_ms),
                    max_flush_delay: Duration::from_millis(opts.adaptive_max_flush_ms),
                    target_pending_bytes: target_bytes,
                    min_pending_bytes: opts.adaptive_min_pending_bytes,
                    max_pending_bytes: opts.adaptive_max_pending_bytes,
                    ema_alpha: opts.adaptive_alpha,
                };
                let mut config = DocCacheConfig::default();
                config.flush_delay = adaptive.target_flush_interval;
                config.max_pending_bytes = adaptive.target_pending_bytes;
                config.adaptive = Some(adaptive);
                let label = format!("adaptive:tf{target_ms}ms:tp{}k", target_bytes / 1024);
                cases.push(CaseConfig {
                    label,
                    mode: "adaptive",
                    doc_cache: config,
                });
            }
        }
    }

    cases
}

async fn run_case(
    opts: &MatrixOptions,
    workload: &WorkloadSettings,
    case: &CaseConfig,
) -> Result<CaseResult> {
    let ctx = prepare_state_with_config(case.doc_cache.clone()).await?;

    ctx.state
        .doc_cache
        .clone()
        .snapshot(SpaceType::Workspace, &ctx.workspace_id, &ctx.doc_id)
        .await
        .context("prime doc cache snapshot")?;

    let metrics = ctx.state.doc_cache.metrics();
    let base_flush = metrics.flush_success_count();
    let base_flush_duration = metrics.total_flush_duration_ms();

    let profiler = if let Some(dir) = opts.flamegraph.as_ref() {
        if let Err(err) = fs::create_dir_all(dir) {
            warn!(
                path = %dir.display(),
                error = %err,
                "failed to create flamegraph directory"
            );
            None
        } else {
            Some(
                ProfilerGuardBuilder::default()
                    .frequency(999)
                    .build()
                    .context("start profiler")?,
            )
        }
    } else {
        None
    };

    let total_updates = workload.iterations * workload.updates_per_iteration * workload.workers;
    let workload_start = Instant::now();

    let mut join_set = JoinSet::new();
    for worker_id in 0..workload.workers {
        let worker_ctx = ctx.clone();
        let worker_opts = WorkerOpts {
            iterations: workload.iterations,
            updates_per_iteration: workload.updates_per_iteration,
            log_every: opts.log_every,
        };
        join_set.spawn(async move { run_worker(worker_id, worker_ctx, worker_opts).await });
    }

    while let Some(result) = join_set.join_next().await {
        result.context("worker join error")??;
    }

    if opts.flush_grace_ms > 0 {
        tokio::time::sleep(Duration::from_millis(opts.flush_grace_ms)).await;
    }

    if let (Some(guard), Some(dir)) = (profiler, opts.flamegraph.as_ref()) {
        match guard.report().build() {
            Ok(report) => {
                if let Err(err) = write_flamegraph(dir, case, report) {
                    warn!(error = %err, "failed to write flamegraph");
                }
            }
            Err(err) => {
                warn!(error = %err, "unable to build flamegraph report (insufficient samples?)");
            }
        }
    }

    let elapsed = workload_start.elapsed();
    let flush_count = metrics.flush_success_count().saturating_sub(base_flush);
    let flush_duration = metrics
        .total_flush_duration_ms()
        .saturating_sub(base_flush_duration);
    let avg_flush_ms = if flush_count == 0 {
        0.0
    } else {
        flush_duration as f64 / flush_count as f64
    };

    Ok(CaseResult {
        label: case.label.clone(),
        mode: case.mode.to_string(),
        elapsed_secs: elapsed.as_secs_f64(),
        throughput: total_updates as f64 / elapsed.as_secs_f64(),
        flush_count,
        avg_flush_ms,
        pending_bytes: metrics.pending_bytes(),
    })
}

#[derive(Clone)]
struct WorkerOpts {
    iterations: usize,
    updates_per_iteration: usize,
    log_every: usize,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum LoadProfile {
    Low,
    High,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum ConcurrencyProfile {
    Low,
    High,
}

#[derive(Clone, Copy)]
struct WorkloadSettings {
    iterations: usize,
    updates_per_iteration: usize,
    workers: usize,
}

async fn run_worker(worker_id: usize, ctx: WorkloadContext, opts: WorkerOpts) -> Result<()> {
    let mut generator = UpdateGenerator::new(worker_id);
    let mut last_log = Instant::now();

    for iteration in 0..opts.iterations {
        let updates = generator
            .make_batch(opts.updates_per_iteration, iteration)
            .context("generate yjs updates")?;

        ctx.state
            .doc_cache
            .clone()
            .apply_updates(
                SpaceType::Workspace,
                &ctx.workspace_id,
                &ctx.doc_id,
                updates,
                Some(ctx.owner_id.as_str()),
            )
            .await
            .context("apply updates via cache")?;

        if opts.log_every > 0 && (iteration + 1) % opts.log_every == 0 {
            info!(
                worker_id,
                iteration = iteration + 1,
                elapsed_ms = last_log.elapsed().as_millis(),
                "worker progress"
            );
            last_log = Instant::now();
        }
    }

    Ok(())
}

fn print_summary(results: &[CaseResult]) {
    println!(
        "{:<24} {:<10} {:>10} {:>14} {:>12} {:>14}",
        "case", "mode", "secs", "updates/sec", "flushes", "avg_flush_ms"
    );
    for result in results {
        println!(
            "{:<24} {:<10} {:>10.3} {:>14.0} {:>12} {:>14.2} (pending={}k)",
            result.label,
            result.mode,
            result.elapsed_secs,
            result.throughput,
            result.flush_count,
            result.avg_flush_ms,
            result.pending_bytes / 1024
        );
    }
}

fn write_flamegraph(base_dir: &Path, case: &CaseConfig, report: pprof::Report) -> Result<()> {
    if base_dir.as_os_str().is_empty() {
        return Ok(());
    }
    let mut path = base_dir.to_path_buf();
    let sanitized = sanitize_label(&case.label);
    path.push(format!("{}-{}.svg", case.mode, sanitized));
    let mut file = File::create(&path).context("create flamegraph file")?;
    report.flamegraph(&mut file).context("render flamegraph")?;
    info!(path = %path.display(), "flamegraph written");
    Ok(())
}

fn sanitize_label(label: &str) -> String {
    label
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

async fn prepare_state_with_config(doc_cache_config: DocCacheConfig) -> Result<WorkloadContext> {
    let tmp_dir = Arc::new(tempfile::tempdir().context("create temp dir")?);
    let mut app_config = AppConfig::default();
    app_config.database_path = tmp_dir
        .path()
        .join("profile.db")
        .to_string_lossy()
        .to_string();
    app_config.doc_data_backend = DocDataBackend::Sqlite;
    app_config.doc_data_path = tmp_dir.path().join("doc-kv").to_string_lossy().to_string();
    app_config.blob_store_backend = BlobStoreBackend::Sql;
    app_config.blob_store_path = tmp_dir
        .path()
        .join("blob-store")
        .to_string_lossy()
        .to_string();

    let database = Database::connect(&app_config)
        .await
        .context("connect sqlite database")?;
    let state = build_state_with_config(
        &database,
        &app_config,
        StateBuildConfig {
            doc_cache: Some(doc_cache_config),
        },
    );
    state
        .workspace_store
        .normalize_member_statuses()
        .await
        .context("normalize member statuses")?;

    let password_hash = "$argon2id$v=19$m=16,t=2,p=1$profiler$payload";
    let owner = state
        .user_store
        .create("matrix@example.com", password_hash, Some("Matrix Owner"))
        .await
        .context("create matrix user")?;

    let workspace = state
        .workspace_store
        .create(
            &owner.id,
            Some("Matrix Workspace"),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
        )
        .await
        .context("create matrix workspace")?;

    let doc_id = format!("doc-{}", Uuid::new_v4());
    state
        .document_store
        .ensure_doc_record(&workspace.id, &doc_id, &owner.id, Some("Matrix Doc"))
        .await
        .context("ensure document skeleton")?;

    Ok(WorkloadContext {
        state,
        workspace_id: workspace.id,
        doc_id,
        owner_id: owner.id,
        _db_guard: tmp_dir,
    })
}

struct UpdateGenerator {
    doc: Doc,
    state_vector: StateVector,
    text_name: String,
}

impl UpdateGenerator {
    fn new(worker_id: usize) -> Self {
        Self {
            doc: Doc::new(),
            state_vector: StateVector::default(),
            text_name: format!("matrix-content-{worker_id}"),
        }
    }

    fn make_batch(&mut self, batch_size: usize, iteration: usize) -> Result<Vec<Vec<u8>>> {
        let mut updates = Vec::with_capacity(batch_size);
        for offset in 0..batch_size {
            let line = format!(
                "worker:{} iteration:{} offset:{}\n",
                self.text_name, iteration, offset
            );
            updates.push(self.next_update(&line)?);
        }
        Ok(updates)
    }

    fn next_update(&mut self, content: &str) -> Result<Vec<u8>> {
        let text = self
            .doc
            .get_or_insert_text(*Arc::new(self.text_name.as_str()));
        let mut txn = self.doc.transact_mut();
        let len = text.len(&txn);
        text.insert(&mut txn, len, content);
        let update = txn.encode_state_as_update_v1(&self.state_vector);
        self.state_vector = txn.state_vector();
        Ok(update)
    }
}
