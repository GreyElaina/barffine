// Barffine Server - Main Entry Point
//
// This file contains only the application bootstrap logic, CLI commands,
// and initialization. All handlers, routes, and business logic are in separate modules.

pub use barffine_server::*;

use anyhow::{Context, anyhow, bail};
use barffine_core::{config::AppConfig, db::Database, user::UserStore};
use barffine_server::utils::db::is_unique_violation;
use barffine_server::workspace::service::WorkspaceCreationParams;
use clap::{Args, Parser, Subcommand};
#[cfg(debug_assertions)]
use console_subscriber::ConsoleLayer;
use dotenvy::{Error as DotenvError, dotenv, from_filename};
use logfire::config::SendToLogfire;
use pprof::{ProfilerGuard, protos::Message as _};
use std::{
    env,
    fs::File,
    path::{Path, PathBuf},
    sync::OnceLock,
};
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use tracing_appender::non_blocking;
use tracing_subscriber::EnvFilter;
#[cfg(debug_assertions)]
use tracing_subscriber::registry::Registry;
use uuid::Uuid;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

static TRACING_FALLBACK_GUARD: OnceLock<non_blocking::WorkerGuard> = OnceLock::new();

#[derive(Parser, Debug)]
#[command(author, version, about = "Barffine server", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the HTTP server
    Serve,
    /// Run database migrations
    Migrate,
    /// Create or update an administrator account
    CreateAdmin {
        /// Email for the administrator account
        email: String,
        /// Password for the administrator account
        password: String,
    },
    /// Create a workspace owned by an existing user
    CreateWorkspace(CreateWorkspaceArgs),
    /// Seed documents into an existing workspace
    SeedWorkspace(SeedWorkspaceArgs),
}

#[derive(Args, Debug)]
struct CreateWorkspaceArgs {
    /// Owner user ID to associate with the workspace
    #[arg(
        long = "owner-id",
        value_name = "ID",
        required_unless_present = "owner_email"
    )]
    owner_id: Option<String>,
    /// Owner email (looked up before creation)
    #[arg(
        long = "owner-email",
        value_name = "EMAIL",
        required_unless_present = "owner_id"
    )]
    owner_email: Option<String>,
    /// Display name for the workspace
    #[arg(long, value_name = "NAME")]
    name: String,
    /// Override the workspace public flag
    #[arg(long, value_name = "true|false")]
    public: Option<bool>,
    /// Override AI features
    #[arg(long = "enable-ai", value_name = "true|false")]
    enable_ai: Option<bool>,
    /// Override embedding configuration
    #[arg(long = "enable-doc-embedding", value_name = "true|false")]
    enable_doc_embedding: Option<bool>,
    /// Override URL preview flag
    #[arg(long = "enable-url-preview", value_name = "true|false")]
    enable_url_preview: Option<bool>,
}

#[derive(Args, Debug)]
struct SeedWorkspaceArgs {
    /// Workspace ID to seed
    #[arg(long = "workspace-id", value_name = "ID")]
    workspace_id: String,
    /// Number of docs to ensure (default: 1)
    #[arg(long = "doc-count", default_value_t = 1)]
    doc_count: usize,
    /// Optional prefix for generated doc IDs
    #[arg(long = "doc-prefix", value_name = "PREFIX")]
    doc_prefix: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tokio_console();
    let env_status = load_env_file();
    let _observability_guard = init_observability();
    observability::log_sampling_summary();
    report_env_status(&env_status);

    let cli = Cli::parse();
    let config = AppConfig::load()?;

    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => run_serve(config).await,
        Command::Migrate => run_migrate(config).await,
        Command::CreateAdmin { email, password } => run_create_admin(config, email, password).await,
        Command::CreateWorkspace(args) => run_create_workspace(config, args).await,
        Command::SeedWorkspace(args) => run_seed_workspace(config, args).await,
    }
}

fn init_tokio_console() {
    #[cfg(debug_assertions)]
    {
        if env::var("TOKIO_CONSOLE_ADDR").is_ok()
            || env::var("TOKIO_CONSOLE_BIND").is_ok()
            || env::var("TOKIO_CONSOLE_RETENTION").is_ok()
        {
            let _ = ConsoleLayer::builder()
                .with_default_env()
                .spawn::<Registry>();
            warn!(
                "tokio-console enabled (set RUSTFLAGS=\"--cfg tokio_unstable\" and run `tokio-console` to attach)"
            );
        }
    }
}

async fn run_serve(config: AppConfig) -> anyhow::Result<()> {
    let profiler = HttpProfiler::maybe_start();
    info!(
        database_backend = ?config.database_backend,
        database_path = %config.database_path,
        database_max_connections = config.database_max_connections,
        doc_data_backend = ?config.doc_data_backend,
        doc_data_path = %config.doc_data_path,
        doc_store_backend = ?config.doc_store_backend,
        blob_store_backend = ?config.blob_store_backend,
        blob_store_path = %config.blob_store_path,
        "Starting server with database configuration"
    );
    let database = Database::connect(&config).await?;
    let state = build_state(&database, &config);
    state
        .workspace_store
        .normalize_member_statuses()
        .await
        .context("normalize workspace member statuses")?;
    info!(
        compatibility = %state.metadata.compatibility,
        deployment_type = %state.metadata.deployment_type,
        flavor = %state.metadata.flavor,
        base_url = %state.base_url,
        server_path = %state.server_path.as_deref().unwrap_or("/"),
        "Loaded server metadata"
    );

    let app = router::build_router(state);

    let listener = TcpListener::bind(config.bind_address)
        .await
        .context("failed to bind socket")?;
    let actual_addr = listener
        .local_addr()
        .context("failed to read local address")?;

    info!("listening on {actual_addr}");

    if let Err(error) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        error!(?error, "server terminated with error");
    }

    if let Some(profiler) = profiler {
        profiler.finish();
    }

    Ok(())
}

async fn run_migrate(config: AppConfig) -> anyhow::Result<()> {
    let _database = Database::connect(&config).await?;
    info!("migrations completed");
    Ok(())
}

async fn run_create_admin(
    config: AppConfig,
    email: String,
    password: String,
) -> anyhow::Result<()> {
    if email.trim().is_empty() {
        anyhow::bail!("email must not be empty");
    }

    if password.is_empty() {
        anyhow::bail!("password must not be empty");
    }

    let database = Database::connect(&config).await?;
    let user_store = UserStore::new(&database);
    let password_hash = auth::generate_password_hash(&password)?;

    let admin = match user_store.create(&email, &password_hash, None).await {
        Ok(record) => {
            info!("created admin user {email}");
            record
        }
        Err(err) => {
            if is_unique_violation(&err) {
                if let Some(existing) = user_store.find_by_email(&email).await? {
                    user_store
                        .update_password(&existing.id, &password_hash)
                        .await?;
                    info!("updated password for admin user {email}");
                    existing
                } else {
                    return Err(err.into());
                }
            } else {
                return Err(err.into());
            }
        }
    };

    user_store.add_admin(&admin.id).await?;
    info!("ensured administrator privileges for {email}");

    Ok(())
}

async fn run_create_workspace(config: AppConfig, args: CreateWorkspaceArgs) -> anyhow::Result<()> {
    let CreateWorkspaceArgs {
        owner_id,
        owner_email,
        name,
        public,
        enable_ai,
        enable_doc_embedding,
        enable_url_preview,
    } = args;

    let trimmed_name = name.trim();
    if trimmed_name.is_empty() {
        bail!("workspace name must not be empty");
    }

    let database = Database::connect(&config).await?;
    let state = build_state(&database, &config);
    let user_store = state.user_store.clone();
    let workspace_service = state.workspace_service.clone();
    let owner_id = resolve_owner_id(&user_store, owner_id, owner_email).await?;

    let workspace = workspace_service
        .create_workspace_with_defaults(WorkspaceCreationParams {
            owner_id: &owner_id,
            name: Some(trimmed_name),
            public,
            enable_ai,
            enable_doc_embedding,
            enable_url_preview,
        })
        .await
        .map_err(|err| anyhow!("failed to create workspace: {err}"))?;

    info!(
        workspace_id = %workspace.id,
        owner_id = %workspace.owner_id,
        "created workspace"
    );
    println!(
        "Created workspace '{}' ({}) for owner {}",
        workspace.name, workspace.id, workspace.owner_id
    );

    Ok(())
}

async fn run_seed_workspace(config: AppConfig, args: SeedWorkspaceArgs) -> anyhow::Result<()> {
    if args.doc_count == 0 {
        println!(
            "Doc count is 0; nothing to seed for workspace {}",
            args.workspace_id
        );
        return Ok(());
    }

    let database = Database::connect(&config).await?;
    let state = build_state(&database, &config);
    let workspace = state
        .workspace_service
        .fetch_workspace(&args.workspace_id)
        .await
        .map_err(|err| anyhow!("failed to fetch workspace {}: {err}", args.workspace_id))?;

    for index in 0..args.doc_count {
        let doc_id = match &args.doc_prefix {
            Some(prefix) => format!("{prefix}-{index}"),
            None => Uuid::new_v4().to_string(),
        };
        state
            .document_store
            .ensure_doc_record(
                &workspace.id,
                &doc_id,
                &workspace.owner_id,
                Some("Bench Sample"),
            )
            .await?;
        state
            .doc_role_store
            .upsert(&workspace.id, &doc_id, &workspace.owner_id, "owner")
            .await?;
    }

    println!(
        "Seeded {} docs in workspace {}",
        args.doc_count, args.workspace_id
    );

    Ok(())
}

async fn resolve_owner_id(
    user_store: &UserStore,
    owner_id: Option<String>,
    owner_email: Option<String>,
) -> anyhow::Result<String> {
    if let Some(id) = owner_id {
        let normalized = id.trim().to_owned();
        if normalized.is_empty() {
            bail!("owner-id must not be empty");
        }

        user_store
            .find_by_id(&normalized)
            .await?
            .with_context(|| format!("no user found with id {normalized}"))?;

        return Ok(normalized);
    }

    if let Some(email) = owner_email {
        let normalized = email.trim().to_owned();
        if normalized.is_empty() {
            bail!("owner-email must not be empty");
        }

        let user = user_store
            .find_by_email(&normalized)
            .await?
            .with_context(|| format!("no user found with email {normalized}"))?;

        return Ok(user.id);
    }

    bail!("either --owner-id or --owner-email must be provided");
}

fn init_observability() -> Option<logfire::ShutdownGuard> {
    // Check if LOGFIRE_TOKEN is empty/missing, if so, directly use fallback
    if let Ok(token) = std::env::var("LOGFIRE_TOKEN") {
        if token.trim().is_empty() {
            observability::set_otel_layers_enabled(false);
            init_tracing_fallback();
            return None;
        }
    } else {
        observability::set_otel_layers_enabled(false);
        init_tracing_fallback();
        return None;
    }

    let mut builder = logfire::configure()
        .send_to_logfire(SendToLogfire::IfTokenPresent)
        .with_service_name("barffine-server")
        .with_service_version(env!("CARGO_PKG_VERSION"));

    if let Ok(environment) =
        std::env::var("BARFFINE_ENVIRONMENT").or_else(|_| std::env::var("BARFFINE_ENV"))
    {
        builder = builder.with_environment(environment);
    }

    match builder.finish() {
        Ok(logfire) => {
            observability::set_otel_layers_enabled(true);
            Some(logfire.shutdown_guard())
        }
        Err(error) => {
            eprintln!(
                "failed to initialize logfire: {error:?}; falling back to tracing_subscriber"
            );
            init_tracing_fallback();
            tracing::error!(
                ?error,
                "failed to initialize logfire; using tracing_subscriber fallback"
            );
            observability::set_otel_layers_enabled(false);
            None
        }
    }
}

fn init_tracing_fallback() {
    // Fallback logger: emit compact JSON to a rolling file, not stdout.
    // This avoids terminal ANSI formatting costs and reduces contention on stdout
    // (which showed up as a hotspot in profiles). Use RUST_LOG to control level.
    use std::fs;
    observability::set_otel_layers_enabled(false);
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Optional override: when BARFFINE_LOG_TO_STDOUT is set (and not "0"),
    // send logs to stdout instead of a file. This is useful for local
    // debugging or bench scripts that capture server logs via redirection.
    let log_to_stdout = std::env::var("BARFFINE_LOG_TO_STDOUT")
        .map(|v| !v.trim().is_empty() && v.trim() != "0")
        .unwrap_or(false);

    if log_to_stdout {
        if tracing_subscriber::fmt()
            .with_env_filter(env_filter.clone())
            .with_ansi(false)
            .json()
            .with_writer(std::io::stdout)
            .try_init()
            .is_ok()
        {
            return;
        }
    }

    let log_dir = std::env::var("BARFFINE_LOG_DIR").unwrap_or_else(|_| "logs".to_string());
    if let Err(err) = fs::create_dir_all(&log_dir) {
        eprintln!("failed to create log dir '{}': {err}", log_dir);
        std::process::exit(1);
    }
    let file_appender = tracing_appender::rolling::daily(&log_dir, "server.log");
    let (writer, guard) = non_blocking(file_appender);

    if tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_ansi(false)
        .json()
        .with_writer(writer)
        .try_init()
        .is_ok()
    {
        let _ = TRACING_FALLBACK_GUARD.set(guard);
    }
}

struct HttpProfiler {
    guard: ProfilerGuard<'static>,
    output_path: PathBuf,
    format: ProfileOutputFormat,
    frequency: i32,
}

impl HttpProfiler {
    fn maybe_start() -> Option<Self> {
        let path = env::var_os("BARFFINE_PROFILE_PPROF")?;
        let frequency = env::var("BARFFINE_PROFILE_PPROF_FREQ")
            .ok()
            .and_then(|freq| freq.parse::<i32>().ok())
            .unwrap_or(99)
            .max(1);
        let format = ProfileOutputFormat::from_env();

        match pprof::ProfilerGuardBuilder::default()
            .frequency(frequency)
            .build()
        {
            Ok(guard) => {
                let profiler = Self {
                    guard,
                    output_path: PathBuf::from(path),
                    format,
                    frequency,
                };
                info!(
                    path = %profiler.output_path.display(),
                    frequency = profiler.frequency,
                    format = %profiler.format.as_str(),
                    "pprof profiler enabled"
                );
                Some(profiler)
            }
            Err(err) => {
                warn!(?err, "failed to start pprof profiler");
                None
            }
        }
    }

    fn finish(self) {
        match self.guard.report().build() {
            Ok(report) => {
                if let Err(err) = self.format.write(report, &self.output_path) {
                    warn!(
                        ?err,
                        path = %self.output_path.display(),
                        "failed to write pprof report"
                    );
                } else {
                    info!(
                        path = %self.output_path.display(),
                        format = %self.format.as_str(),
                        "pprof report written"
                    );
                }
            }
            Err(err) => warn!(?err, "failed to build pprof report"),
        }
    }
}

enum ProfileOutputFormat {
    Flamegraph,
    Protobuf,
}

impl ProfileOutputFormat {
    fn from_env() -> Self {
        match env::var("BARFFINE_PROFILE_PPROF_FORMAT")
            .unwrap_or_else(|_| "flamegraph".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "proto" | "protobuf" | "pprof" | "perfetto" => Self::Protobuf,
            _ => Self::Flamegraph,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Flamegraph => "flamegraph",
            Self::Protobuf => "protobuf",
        }
    }

    fn write(&self, report: pprof::Report, path: &Path) -> anyhow::Result<()> {
        match self {
            Self::Flamegraph => {
                let mut file = File::create(path)?;
                report.flamegraph(&mut file)?;
            }
            Self::Protobuf => {
                let profile = report.pprof()?;
                let mut file = File::create(path)?;
                profile.write_to_writer(&mut file)?;
            }
        }
        Ok(())
    }
}

enum EnvLoadStatus {
    Loaded(PathBuf),
    NotFound,
    Failed(DotenvError),
}

fn load_env_file() -> EnvLoadStatus {
    if let Ok(env_file) = std::env::var("BARFFINE_ENV_FILE") {
        let trimmed = env_file.trim();
        if !trimmed.is_empty() {
            let path = PathBuf::from(trimmed);
            return match from_filename(&path) {
                Ok(_) => {
                    let display_path = make_relative(&path).unwrap_or_else(|| path.clone());
                    EnvLoadStatus::Loaded(display_path)
                }
                Err(err) => EnvLoadStatus::Failed(err),
            };
        }
    }

    match dotenv() {
        Ok(path) => {
            let display_path = make_relative(&path).unwrap_or_else(|| path.clone());
            EnvLoadStatus::Loaded(display_path)
        }
        Err(DotenvError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            EnvLoadStatus::NotFound
        }
        Err(err) => EnvLoadStatus::Failed(err),
    }
}

fn report_env_status(status: &EnvLoadStatus) {
    match status {
        EnvLoadStatus::Loaded(path) => {
            info!("Loaded environment variables from {}", path.display());
        }
        EnvLoadStatus::NotFound => {
            info!("No .env file found; using process environment only");
        }
        EnvLoadStatus::Failed(err) => {
            warn!("Failed to load .env file: {err:?}");
        }
    }
}

fn make_relative(path: &Path) -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    path.strip_prefix(&cwd).map(|p| p.to_path_buf()).ok()
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut term = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");

        tokio::select! {
            _ = term.recv() => {},
            _ = int.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
