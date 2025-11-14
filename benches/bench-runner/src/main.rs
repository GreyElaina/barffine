mod client;
mod config;
mod dataset;
mod operations;
mod output;
mod runner;
mod sampler;

use std::{env, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use logfire::{ShutdownGuard, config::SendToLogfire};
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use uuid::Uuid;

use crate::{
    client::BenchClient,
    config::{BenchConfig, ScenarioKind},
    dataset::Dataset,
    operations::{duplicate_doc, list_workspace_doc_ids, list_workspace_ids},
    output::persist_results,
    runner::{RunOptions, run_scenario},
};

#[derive(Parser)]
#[command(name = "bench-runner", version, about = "Barffine performance harness")]
struct Cli {
    #[arg(long, default_value = "bench-runner.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a benchmark scenario
    Run {
        #[arg(value_enum)]
        scenario: ScenarioKind,
        /// Override scenario duration (seconds)
        #[arg(long)]
        duration: Option<u64>,
        /// Override output directory
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Refresh dataset metadata (workspace/doc ids)
    Seed {
        #[arg(long)]
        dataset: Option<PathBuf>,
    },
    /// Generate a randomized dataset with doc ids for each workspace
    Generate(GenerateCommandArgs),
}

#[derive(Args, Debug, Clone)]
struct GenerateCommandArgs {
    /// Override dataset output path
    #[arg(long)]
    dataset: Option<PathBuf>,
    /// Limit the number of workspaces included in the dataset
    #[arg(long)]
    workspace_limit: Option<usize>,
    /// Number of docs to sample per workspace
    #[arg(long, default_value_t = 8)]
    docs_per_workspace: usize,
    /// Page size for GraphQL doc pagination (1-100)
    #[arg(long, default_value_t = 50)]
    page_size: usize,
    /// Skip creating additional docs to reach docs_per_workspace
    #[arg(long, default_value_t = false)]
    skip_populate: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _logfire_guard = init_observability();
    let cli = Cli::parse();
    let config = BenchConfig::load(&cli.config)?;

    match cli.command {
        Command::Run {
            scenario,
            duration,
            output,
        } => {
            run_command(&config, scenario, duration, output).await?;
        }
        Command::Seed { dataset } => {
            seed_command(&config, dataset).await?;
        }
        Command::Generate(args) => {
            generate_command(&config, &args).await?;
        }
    }

    Ok(())
}

fn init_observability() -> Option<ShutdownGuard> {
    match env::var("LOGFIRE_TOKEN") {
        Ok(token) if !token.trim().is_empty() => {}
        _ => {
            init_tracing_fallback();
            return None;
        }
    }

    let mut builder = logfire::configure()
        .send_to_logfire(SendToLogfire::IfTokenPresent)
        .with_service_name("barffine-bench-runner")
        .with_service_version(env!("CARGO_PKG_VERSION"));

    if let Some(environment) = pick_non_empty_env(&[
        "BARFFINE_BENCH_ENVIRONMENT",
        "BARFFINE_ENVIRONMENT",
        "BARFFINE_ENV",
    ]) {
        builder = builder.with_environment(environment);
    }

    match builder.finish() {
        Ok(logfire) => Some(logfire.shutdown_guard()),
        Err(error) => {
            eprintln!(
                "failed to initialize logfire for bench-runner: {error:?}; falling back to tracing_subscriber"
            );
            init_tracing_fallback();
            tracing::error!(
                ?error,
                "failed to initialize logfire; using tracing_subscriber fallback for bench-runner"
            );
            None
        }
    }
}

fn pick_non_empty_env(vars: &[&str]) -> Option<String> {
    for var in vars {
        if let Ok(value) = env::var(var) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }
    None
}

fn init_tracing_fallback() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_target(false)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

async fn run_command(
    config: &BenchConfig,
    scenario: ScenarioKind,
    duration_override: Option<u64>,
    output_override: Option<PathBuf>,
) -> Result<()> {
    let client = BenchClient::new(&config.target)?;
    info!("authenticating as {}", config.target.admin_email);
    client
        .authenticate(&config.target.admin_email, &config.target.admin_password)
        .await?;

    let dataset_path = config.dataset.path.clone();
    let dataset = Dataset::load(&dataset_path)?;
    let dataset_arc = Arc::new(dataset);
    let spec = config.scenario(scenario);
    if spec.operations.iter().any(|op| op.kind.requires_docs()) && dataset_arc.is_empty() {
        println!(
            "⚠️ Scenario {} includes doc operations but dataset {} has no doc_ids; those steps will be skipped.",
            scenario.as_str(),
            dataset_path.display()
        );
    }

    let options = RunOptions {
        scenario_name: scenario.as_str().to_string(),
        spec: spec.clone(),
        duration_override: duration_override.map(Duration::from_secs),
        sample_interval: Duration::from_millis(config.sample_interval_ms),
        pid: config.target.resolve_pid(),
        dataset: Some(dataset_arc.clone()),
    };

    let artifacts = run_scenario(client.clone(), options).await?;

    println!(
        "Scenario {} completed: {:.2} req/s (success: {}, failure: {}, skipped: {})",
        artifacts.summary.scenario_name,
        artifacts.summary.throughput_per_sec,
        artifacts.summary.successes,
        artifacts.summary.failures,
        artifacts.summary.skipped,
    );

    if let Some(latency) = &artifacts.summary.latency {
        println!(
            "Latency p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms",
            latency.p50_ms, latency.p95_ms, latency.p99_ms, latency.max_ms
        );
    }

    if let Some(resource) = &artifacts.summary.resources {
        println!(
            "RSS peak {:.1} MB (p95 {:.1} MB); CPU avg {:.1}%",
            resource.rss_peak_mb, resource.rss_p95_mb, resource.cpu_avg_percent
        );
    } else {
        println!("Process sampling disabled (PID not configured)");
    }

    let output_dir = output_override.unwrap_or_else(|| config.output_dir.clone());
    let result_dir = persist_results(&output_dir, &artifacts)?;
    println!("Artifacts written to {}", result_dir.display());

    Ok(())
}

async fn seed_command(config: &BenchConfig, dataset_override: Option<PathBuf>) -> Result<()> {
    let client = BenchClient::new(&config.target)?;
    client
        .authenticate(&config.target.admin_email, &config.target.admin_password)
        .await?;
    let workspaces = list_workspace_ids(&client)
        .await
        .context("failed to list workspaces")?;
    if workspaces.is_empty() {
        println!("No workspaces found; create one via AFFiNE client before seeding");
        return Ok(());
    }
    let dataset_path = dataset_override.unwrap_or_else(|| config.dataset.path.clone());
    let mut dataset = Dataset::load(&dataset_path)?;
    dataset.sync_workspaces(&workspaces);
    dataset.to_file(&dataset_path)?;
    println!(
        "Dataset updated at {} ({} workspaces). Add doc_ids manually for REST operations.",
        dataset_path.display(),
        dataset.workspaces().len()
    );
    Ok(())
}

async fn generate_command(config: &BenchConfig, args: &GenerateCommandArgs) -> Result<()> {
    let client = BenchClient::new(&config.target)?;
    client
        .authenticate(&config.target.admin_email, &config.target.admin_password)
        .await?;

    let mut workspaces = list_workspace_ids(&client)
        .await
        .context("failed to list workspaces")?;
    if workspaces.is_empty() {
        println!("No workspaces found; create one via AFFiNE client before generating a dataset");
        return Ok(());
    }

    fastrand::shuffle(&mut workspaces);
    if let Some(limit) = args.workspace_limit {
        if limit < workspaces.len() {
            workspaces.truncate(limit);
        }
    }

    let dataset_path = args
        .dataset
        .clone()
        .unwrap_or_else(|| config.dataset.path.clone());
    let mut dataset = Dataset::load(&dataset_path)?;
    dataset.sync_workspaces(&workspaces);

    let doc_limit = args.docs_per_workspace.max(1);
    let page_size = args.page_size.clamp(1, 100);
    let mut total_created_docs = 0usize;

    for workspace_id in &workspaces {
        let mut doc_ids = list_workspace_doc_ids(&client, workspace_id, 0, page_size)
            .await
            .with_context(|| format!("failed to list docs for workspace {workspace_id}"))?;

        if !args.skip_populate && doc_ids.len() < doc_limit {
            let template_doc = doc_ids
                .iter()
                .find(|doc| doc.as_str() != workspace_id)
                .cloned();
            if let Some(template) = template_doc {
                let missing = doc_limit - doc_ids.len();
                let new_docs = populate_workspace_docs(&client, workspace_id, &template, missing)
                    .await
                    .with_context(|| {
                        format!("failed to populate docs for workspace {workspace_id}")
                    })?;
                total_created_docs += new_docs.len();
                doc_ids.extend(new_docs);
            } else {
                println!(
                    "[bench] workspace {} has no duplicable docs; skipping populate",
                    workspace_id
                );
            }
        }

        fastrand::shuffle(&mut doc_ids);
        if doc_ids.len() > doc_limit {
            doc_ids.truncate(doc_limit);
        }

        dataset.set_doc_ids(workspace_id, doc_ids);
    }

    dataset.to_file(&dataset_path)?;
    println!(
        "Dataset generated at {} ({} workspaces, up to {} docs each, created {} docs)",
        dataset_path.display(),
        workspaces.len(),
        doc_limit,
        total_created_docs
    );

    Ok(())
}

async fn populate_workspace_docs(
    client: &Arc<BenchClient>,
    workspace_id: &str,
    template_doc_id: &str,
    missing: usize,
) -> Result<Vec<String>> {
    let mut created = Vec::with_capacity(missing);
    for _ in 0..missing {
        let doc_id = Uuid::new_v4().to_string();
        let title = format!("Bench Doc {}", &doc_id[..8]);
        duplicate_doc(client, workspace_id, template_doc_id, &doc_id, Some(&title))
            .await
            .with_context(|| format!("failed to duplicate doc in workspace {workspace_id}"))?;
        created.push(doc_id);
    }
    Ok(created)
}
