use anyhow::{Context, Result};
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};

pub type PostgresPool = Pool<Postgres>;

pub async fn create_pool(url: &str, max_connections: u32) -> Result<PostgresPool> {
    let max_connections = max_connections.max(1);

    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await
        .with_context(|| format!("failed to connect to postgres database: {url}"))
}

pub async fn run_migrations(pool: &PostgresPool) -> Result<()> {
    sqlx::migrate!("../server/migrations/postgres")
        .run(pool)
        .await
        .context("failed to run postgres migrations")
}
