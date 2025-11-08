use std::{fs, fs::File, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use sqlx::{
    Pool, Sqlite,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

use crate::config::AppConfig;

#[derive(Clone)]
pub struct Database {
    pool: Pool<Sqlite>,
    path: PathBuf,
}

impl Database {
    pub async fn connect(config: &AppConfig) -> Result<Self> {
        let path = Self::resolve_db_path(&config.database_path)?;

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create database directory: {}", parent.display())
            })?;
        }

        if !path.exists() {
            File::create(&path)
                .with_context(|| format!("failed to create database file: {}", path.display()))?;
        }

        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to connect to database: {}", path.display()))?;

        Ok(Self { pool, path })
    }

    pub fn pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }

    pub fn database_path(&self) -> &PathBuf {
        &self.path
    }

    fn resolve_db_path(path: &str) -> Result<PathBuf> {
        let path = PathBuf::from(path);
        if path.is_absolute() {
            Ok(path)
        } else {
            let cwd = std::env::current_dir().context("failed to obtain current directory")?;
            Ok(cwd.join(path))
        }
    }
}
