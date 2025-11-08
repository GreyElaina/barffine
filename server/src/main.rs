// Barffine Server - Main Entry Point
//
// This file contains only the application bootstrap logic, CLI commands,
// and initialization. All handlers, routes, and business logic are in separate modules.

pub use barffine_server::*;

use anyhow::Context;
use barffine_core::{config::AppConfig, db::Database, user::UserStore};
use barffine_server::utils::db::{is_unique_violation, run_migrations};
use clap::{Parser, Subcommand};
use dotenvy::{Error as DotenvError, dotenv};
use logfire::config::SendToLogfire;
use std::path::{Path, PathBuf};
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(author, version, about = "Barffine server", long_about = None)]
struct Cli {
    /// Optional path to a configuration file
    #[arg(long)]
    config: Option<PathBuf>,

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_status = load_env_file();
    let _observability_guard = init_observability();
    observability::log_sampling_summary();
    report_env_status(&env_status);

    let cli = Cli::parse();
    let config = AppConfig::load_with(cli.config.clone())?;

    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => run_serve(config).await,
        Command::Migrate => run_migrate(config).await,
        Command::CreateAdmin { email, password } => run_create_admin(config, email, password).await,
    }
}

async fn run_serve(config: AppConfig) -> anyhow::Result<()> {
    let database = Database::connect(&config).await?;
    run_migrations(database.pool()).await?;
    let state = build_state(&database);
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

    Ok(())
}

async fn run_migrate(config: AppConfig) -> anyhow::Result<()> {
    let database = Database::connect(&config).await?;
    run_migrations(database.pool()).await?;
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
    run_migrations(database.pool()).await?;

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

fn init_observability() -> Option<logfire::ShutdownGuard> {
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
        Ok(logfire) => Some(logfire.shutdown_guard()),
        Err(error) => {
            eprintln!(
                "failed to initialize logfire: {error:?}; falling back to tracing_subscriber"
            );
            init_tracing_fallback();
            tracing::error!(
                ?error,
                "failed to initialize logfire; using tracing_subscriber fallback"
            );
            None
        }
    }
}

fn init_tracing_fallback() {
    if tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .is_err()
    {
        // already initialised
    }
}

enum EnvLoadStatus {
    Loaded(PathBuf),
    NotFound,
    Failed(DotenvError),
}

fn load_env_file() -> EnvLoadStatus {
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
