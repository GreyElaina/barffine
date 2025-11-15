use std::{
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use crate::db::Database;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::str::FromStr;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
    #[serde(default = "default_database_path")]
    pub database_path: String,
    #[serde(default)]
    pub database_url: Option<String>,
    #[serde(default = "DatabaseBackend::default")]
    pub database_backend: DatabaseBackend,
    #[serde(default = "default_database_max_connections")]
    pub database_max_connections: u32,
    #[serde(default = "default_doc_data_path")]
    pub doc_data_path: String,
    #[serde(default = "DocDataBackend::default")]
    pub doc_data_backend: DocDataBackend,
    /// Controls the primary storage backend for document data.
    ///
    /// Currently only `Sql` is fully supported; `RocksDb` is reserved
    /// for future doc store implementations that use Rocks as the
    /// authoritative snapshot/log store.
    #[serde(default = "DocStoreBackend::default")]
    pub doc_store_backend: DocStoreBackend,
    #[serde(default = "default_blob_store_path")]
    pub blob_store_path: String,
    #[serde(default = "BlobStoreBackend::default")]
    pub blob_store_backend: BlobStoreBackend,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
            database_path: default_database_path(),
            database_url: None,
            database_backend: DatabaseBackend::default(),
            database_max_connections: default_database_max_connections(),
            doc_data_path: default_doc_data_path(),
            doc_data_backend: DocDataBackend::default(),
            doc_store_backend: DocStoreBackend::default(),
            blob_store_path: default_blob_store_path(),
            blob_store_backend: BlobStoreBackend::default(),
        }
    }
}

impl AppConfig {
    const BIND_ADDRESS_ENV: &'static str = "BARFFINE_BIND_ADDRESS";
    const DATABASE_PATH_ENV: &'static str = "BARFFINE_DATABASE_PATH";
    const DATABASE_URL_ENV: &'static str = "BARFFINE_DATABASE_URL";
    const DATABASE_BACKEND_ENV: &'static str = "BARFFINE_DATABASE_BACKEND";
    const DATABASE_MAX_CONNECTIONS_ENV: &'static str = "BARFFINE_DATABASE_MAX_CONNECTIONS";
    const DOC_DATA_PATH_ENV: &'static str = "BARFFINE_DOC_DATA_PATH";
    const DOC_DATA_BACKEND_ENV: &'static str = "BARFFINE_DOC_DATA_BACKEND";
    const DOC_STORE_BACKEND_ENV: &'static str = "BARFFINE_DOC_STORE_BACKEND";
    const BLOB_STORE_PATH_ENV: &'static str = "BARFFINE_BLOB_STORE_PATH";
    const BLOB_STORE_BACKEND_ENV: &'static str = "BARFFINE_BLOB_STORE_BACKEND";

    pub fn load() -> Result<Self> {
        let mut config = Self::default();

        if let Ok(addr) = env::var(Self::BIND_ADDRESS_ENV) {
            config.bind_address = addr
                .parse()
                .with_context(|| format!("invalid {name}", name = Self::BIND_ADDRESS_ENV))?;
        }

        if let Ok(path) = env::var(Self::DATABASE_PATH_ENV) {
            config.database_path = path;
        }

        if let Ok(url) = env::var(Self::DATABASE_URL_ENV) {
            let trimmed = url.trim().to_owned();
            if !trimmed.is_empty() {
                config.database_url = Some(trimmed);
            }
        }

        if let Ok(backend) = env::var(Self::DATABASE_BACKEND_ENV) {
            config.database_backend = DatabaseBackend::from_str(&backend)?;
        }

        if let Ok(value) = env::var(Self::DATABASE_MAX_CONNECTIONS_ENV) {
            config.database_max_connections = value.parse().with_context(|| {
                format!(
                    "invalid {} (expected positive integer)",
                    Self::DATABASE_MAX_CONNECTIONS_ENV
                )
            })?;
        }

        let mut doc_data_overridden = false;
        if let Ok(path) = env::var(Self::DOC_DATA_PATH_ENV) {
            config.doc_data_path = path;
            doc_data_overridden = true;
        }

        if let Ok(backend) = env::var(Self::DOC_DATA_BACKEND_ENV) {
            config.doc_data_backend = DocDataBackend::from_str(&backend)?;
        }

        if let Ok(backend) = env::var(Self::DOC_STORE_BACKEND_ENV) {
            config.doc_store_backend = DocStoreBackend::from_str(&backend)?;
        }

        let mut blob_path_overridden = false;
        if let Ok(path) = env::var(Self::BLOB_STORE_PATH_ENV) {
            config.blob_store_path = path;
            blob_path_overridden = true;
        }

        if let Ok(backend) = env::var(Self::BLOB_STORE_BACKEND_ENV) {
            config.blob_store_backend = BlobStoreBackend::from_str(&backend)?;
        }

        if !doc_data_overridden {
            config.doc_data_path = default_child_path_for_database(&config.database_path, "doc-kv");
        }

        if !blob_path_overridden {
            config.blob_store_path =
                default_child_path_for_database(&config.database_path, "blob-store");
        }

        Ok(config)
    }

    pub async fn init_database(&self) -> Result<Database> {
        Database::connect(self).await
    }
}

fn default_bind_address() -> SocketAddr {
    "127.0.0.1:8081"
        .parse()
        .expect("default bind address must be valid")
}

fn default_database_path() -> String {
    "./data".to_owned()
}

fn default_database_max_connections() -> u32 {
    16
}

fn default_doc_data_path() -> String {
    let base = default_database_path();
    default_child_path_for_database(&base, "doc-kv")
}

fn default_blob_store_path() -> String {
    let base = default_database_path();
    default_child_path_for_database(&base, "blob-store")
}

fn default_child_path_for_database(base: &str, child: &str) -> String {
    let root = database_root_dir(base);
    root.join(child).to_string_lossy().into_owned()
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseBackend {
    Sqlite,
    Postgres,
}

impl Default for DatabaseBackend {
    fn default() -> Self {
        DatabaseBackend::Sqlite
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DocDataBackend {
    Sqlite,
    RocksDb,
}

impl Default for DocDataBackend {
    fn default() -> Self {
        DocDataBackend::Sqlite
    }
}

impl FromStr for DocDataBackend {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "sqlite" => Ok(DocDataBackend::Sqlite),
            "rocksdb" | "rocks" => Ok(DocDataBackend::RocksDb),
            other => Err(anyhow::anyhow!(
                "unsupported doc data backend '{other}' (expected 'sqlite' or 'rocksdb')"
            )),
        }
    }
}

/// Controls which backend is used for document data access at the
/// application layer. This is distinct from `DatabaseBackend` and
/// `DocDataBackend`:
///
/// - `Sql` means documents are loaded from the SQL doc repositories
///   (current behavior for all database backends).
/// - `RocksDb` switches to a Rocks-backed doc store only when the
///   database backend is SQLite and `DocDataBackend::RocksDb` is
///   configured; for Postgres backends it is treated as an
///   alias for `Sql` in core, and only affects doc cache behavior
///   at the server layer.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DocStoreBackend {
    Sql,
    RocksDb,
}

impl Default for DocStoreBackend {
    fn default() -> Self {
        DocStoreBackend::Sql
    }
}

impl FromStr for DocStoreBackend {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "sql" => Ok(DocStoreBackend::Sql),
            "rocksdb" | "rocks" => Ok(DocStoreBackend::RocksDb),
            other => Err(anyhow::anyhow!(
                "unsupported doc store backend '{other}' (expected 'sql' or 'rocksdb')"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
pub enum BlobStoreBackend {
    #[serde(
        rename = "sql",
        alias = "sqlite",
        alias = "postgres",
        alias = "postgresql",
        alias = "pg"
    )]
    Sql,
    #[serde(rename = "rocks", alias = "rocksdb")]
    Rocks,
}

impl Default for BlobStoreBackend {
    fn default() -> Self {
        BlobStoreBackend::Rocks
    }
}

impl FromStr for BlobStoreBackend {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "sql" | "sqlite" | "postgres" | "postgresql" | "pg" => Ok(BlobStoreBackend::Sql),
            "rocks" | "rocksdb" => Ok(BlobStoreBackend::Rocks),
            other => Err(anyhow::anyhow!(
                "unsupported blob store backend '{other}' (expected 'sql' or 'rocks')"
            )),
        }
    }
}

impl FromStr for DatabaseBackend {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "sqlite" => Ok(DatabaseBackend::Sqlite),
            "postgres" | "postgresql" | "pg" => Ok(DatabaseBackend::Postgres),
            other => Err(anyhow::anyhow!(
                "unsupported database backend '{other}' (expected 'sqlite' or 'postgres')"
            )),
        }
    }
}

const DEFAULT_DB_FILE_NAME: &str = "barffine.db";
const DB_FILE_EXTENSIONS: &[&str] = &["db", "sqlite", "sqlite3"];

pub(crate) fn database_path_is_file(path: &str) -> bool {
    let path = Path::new(path);
    if let Some(ext) = path.extension().and_then(|ext| ext.to_str()) {
        let ext_lower = ext.to_ascii_lowercase();
        if DB_FILE_EXTENSIONS
            .iter()
            .any(|candidate| candidate == &ext_lower.as_str())
        {
            return true;
        }
    }

    if let Some(file_name) = path.file_name().and_then(|name| name.to_str()) {
        if file_name.eq_ignore_ascii_case(DEFAULT_DB_FILE_NAME) {
            return true;
        }
    }

    false
}

pub(crate) fn database_root_dir(path: &str) -> PathBuf {
    if database_path_is_file(path) {
        Path::new(path).parent().map_or_else(
            || PathBuf::from("."),
            |parent| {
                if parent.as_os_str().is_empty() {
                    PathBuf::from(".")
                } else {
                    parent.to_path_buf()
                }
            },
        )
    } else {
        Path::new(path).to_path_buf()
    }
}
