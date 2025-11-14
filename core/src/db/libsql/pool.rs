use std::{fs, path::Path, time::Duration};

use anyhow::{Context, Result};
use deadpool_libsql::{ConnectionError, Hook, HookError, Manager, Pool};

use super::transaction::LibsqlTransaction;
use std::sync::Arc;

#[derive(Clone)]
pub struct LibsqlPool {
    inner: Arc<Pool>,
}

impl LibsqlPool {
    pub fn new(inner: Pool) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn pool(&self) -> &Pool {
        &self.inner
    }

    pub async fn begin_transaction(&self) -> Result<LibsqlTransaction> {
        LibsqlTransaction::begin_from_pool(self).await
    }
}

pub async fn create_local_pool(path: &Path, max_connections: u32) -> Result<LibsqlPool> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create parent directory for libsql database: {}",
                parent.display()
            )
        })?;
    }

    let mut builder = deadpool_libsql::libsql::Builder::new_local(path.to_string_lossy().as_ref());
    // Skip the builder's serialized-threading assertion because sqlx (SQLite) may have already
    // initialized the global sqlite3 runtime before we get here.
    unsafe {
        builder = builder.skip_safety_assert(true);
    }
    let database = builder
        .build()
        .await
        .with_context(|| format!("failed to open libsql database at {}", path.display()))?;

    let manager = Manager::from_libsql_database(database);
    let busy_timeout = Duration::from_secs(5);
    build_pool(manager, max_connections, busy_timeout)
}

pub async fn create_remote_pool(
    url: &str,
    auth_token: Option<&str>,
    max_connections: u32,
) -> Result<LibsqlPool> {
    let token = auth_token.unwrap_or_default().to_string();
    let builder = deadpool_libsql::libsql::Builder::new_remote(url.to_string(), token);
    let database = builder
        .build()
        .await
        .with_context(|| format!("failed to connect to remote libsql endpoint {url}"))?;
    let manager = Manager::from_libsql_database(database);
    let busy_timeout = Duration::from_secs(5);
    build_pool(manager, max_connections, busy_timeout)
}

fn build_pool(
    manager: Manager,
    max_connections: u32,
    busy_timeout: Duration,
) -> Result<LibsqlPool> {
    let pool = Pool::builder(manager)
        .max_size(max_connections.max(1) as usize)
        .post_create(Hook::sync_fn(move |conn, _metrics| {
            conn.busy_timeout(busy_timeout)
                .map_err(|err| HookError::Backend(ConnectionError::from(err)))
        }))
        .build()
        .context("failed to build libsql connection pool")?;

    Ok(LibsqlPool::new(pool))
}
