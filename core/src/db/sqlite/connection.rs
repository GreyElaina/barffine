use std::{path::Path, time::Duration};

use anyhow::{Context, Result};
use sqlx::{
    Pool, Sqlite,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

pub type SqlitePool = Pool<Sqlite>;

pub async fn create_pool(path: &Path, max_connections: u32) -> Result<SqlitePool> {
    let max_connections = max_connections.max(1);
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .pragma("busy_timeout", "5000")
        .busy_timeout(Duration::from_secs(5));

    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await
        .with_context(|| format!("failed to connect to database: {}", path.display()))
}

pub async fn run_migrations(pool: &SqlitePool) -> Result<()> {
    sqlx::migrate!("../server/migrations/sqlite")
        .run(pool)
        .await
        .context("failed to run sqlite migrations")
}
