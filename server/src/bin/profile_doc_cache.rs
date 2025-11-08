use std::{
    fs::File,
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use barffine_core::{config::AppConfig, db::Database};
use barffine_server::{
    build_state, socket::rooms::SpaceType, state::AppState, utils::db::run_migrations,
};
use clap::Parser;
use pprof::ProfilerGuardBuilder;
use tokio::task::JoinSet;
use tracing::{info, warn};
use y_octo::{Doc, StateVector};

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "Profile Barffine's collaborative document pipeline"
)]
struct Options {
    /// Number of iterations executed by each worker
    #[arg(long, default_value = "100")]
    iterations: usize,

    /// Number of Yjs updates generated per iteration
    #[arg(long, default_value = "4")]
    updates_per_iteration: usize,

    /// Concurrent workers issuing updates against the same document
    #[arg(long, default_value = "1")]
    workers: usize,

    /// Flamegraph output path (SVG)
    #[arg(long, default_value = "flamegraph.svg")]
    flamegraph: PathBuf,

    /// Milliseconds to wait after the workload to allow async flushes to finish
    #[arg(long, default_value = "100")]
    flush_grace_ms: u64,

    /// Emit progress every N iterations (per worker); set to 0 to disable
    #[arg(long, default_value = "100")]
    log_every: usize,
}

#[derive(Clone)]
struct WorkloadContext {
    state: AppState,
    workspace_id: String,
    doc_id: String,
    owner_id: String,
    _db_guard: Arc<tempfile::TempDir>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let opts = Options::parse();
    if opts.updates_per_iteration == 0 {
        return Err(anyhow!("--updates-per-iteration must be greater than 0"));
    }
    if opts.iterations == 0 {
        return Err(anyhow!("--iterations must be greater than 0"));
    }
    if opts.workers == 0 {
        return Err(anyhow!("--workers must be greater than 0"));
    }

    let setup_start = Instant::now();
    let ctx = prepare_state().await?;
    info!(
        duration_ms = setup_start.elapsed().as_millis(),
        "state initialized"
    );

    ctx.state
        .doc_cache
        .clone()
        .snapshot(SpaceType::Workspace, &ctx.workspace_id, &ctx.doc_id)
        .await
        .context("prime doc cache snapshot")?;

    let profiler = ProfilerGuardBuilder::default()
        .frequency(999)
        .build()
        .context("start profiler")?;

    let total_updates = opts.iterations * opts.updates_per_iteration * opts.workers;
    info!(
        iterations = opts.iterations,
        updates_per_iteration = opts.updates_per_iteration,
        workers = opts.workers,
        total_updates,
        "starting workload"
    );

    let workload_start = Instant::now();
    let mut join_set = JoinSet::new();
    for worker_id in 0..opts.workers {
        let worker_ctx = ctx.clone();
        let worker_opts = opts.clone();
        join_set.spawn(async move { run_worker(worker_id, worker_ctx, worker_opts).await });
    }

    while let Some(result) = join_set.join_next().await {
        result.context("worker join error")??;
    }

    let workload_elapsed = workload_start.elapsed();
    info!(
        elapsed_secs = workload_elapsed.as_secs_f64(),
        total_updates, "workload complete"
    );

    if opts.flush_grace_ms > 0 {
        info!(wait_ms = opts.flush_grace_ms, "waiting for cache flush");
        tokio::time::sleep(Duration::from_millis(opts.flush_grace_ms)).await;
    }

    match catch_unwind(AssertUnwindSafe(|| profiler.report().build())) {
        Ok(Ok(report)) => {
            if let Err(err) = write_flamegraph(&opts, report) {
                warn!(error = %err, "failed to write profiling artifacts");
            }
        }
        Ok(Err(err)) => {
            warn!(error = %err, "pprof report unavailable (insufficient samples?)");
        }
        Err(_) => {
            warn!("pprof report generation panicked; skipping profiling artifacts");
        }
    }

    Ok(())
}

async fn prepare_state() -> Result<WorkloadContext> {
    let tmp_dir = Arc::new(tempfile::tempdir().context("create temp dir")?);
    let mut config = AppConfig::default();
    config.database_path = tmp_dir
        .path()
        .join("profile.db")
        .to_string_lossy()
        .to_string();

    let database = Database::connect(&config)
        .await
        .context("connect sqlite database")?;
    run_migrations(database.pool())
        .await
        .context("apply migrations")?;

    let state = build_state(&database);
    state
        .workspace_store
        .normalize_member_statuses()
        .await
        .context("normalize member statuses")?;

    let password_hash = "$argon2id$v=19$m=16,t=2,p=1$profiler$payload";
    let owner = state
        .user_store
        .create("profiler@example.com", password_hash, Some("Profiler"))
        .await
        .context("create profiler user")?;

    let workspace = state
        .workspace_store
        .create(
            &owner.id,
            Some("Profiler Workspace"),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
        )
        .await
        .context("create profiler workspace")?;

    let doc_id = format!("doc-{}", uuid::Uuid::new_v4());
    state
        .document_store
        .ensure_doc_record(&workspace.id, &doc_id, &owner.id, Some("Profiler Doc"))
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

async fn run_worker(worker_id: usize, ctx: WorkloadContext, opts: Options) -> Result<()> {
    let mut generator = UpdateGenerator::new(worker_id);
    let log_every = opts.log_every;
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

        if log_every > 0 && (iteration + 1) % log_every == 0 {
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

fn write_flamegraph(opts: &Options, report: pprof::Report) -> Result<()> {
    let mut flame = File::create(&opts.flamegraph).context("create flamegraph file")?;
    report.flamegraph(&mut flame).context("render flamegraph")?;
    info!(path = %opts.flamegraph.display(), "flamegraph written");
    Ok(())
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
            text_name: format!("content-{}", worker_id),
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
        let mut text = self
            .doc
            .get_or_create_text(&self.text_name)
            .context("get text type")?;
        let len = text.len();
        text.insert(len, content).context("insert text")?;

        let update = self
            .doc
            .encode_state_as_update_v1(&self.state_vector)
            .context("encode update")?;
        self.state_vector = self.doc.get_state_vector();
        Ok(update)
    }
}
