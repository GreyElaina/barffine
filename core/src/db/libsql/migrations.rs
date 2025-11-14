use std::{collections::HashMap, time::Instant};

use anyhow::{Context, Result, bail};
use deadpool_libsql::{
    Pool,
    libsql::{self, params},
};
use sqlx::migrate::{Migration, Migrator};

use super::row_ext;

static SQLITE_MIGRATOR: Migrator = sqlx::migrate!("../server/migrations/sqlite");

const CREATE_MIGRATIONS_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS _sqlx_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    checksum BLOB NOT NULL,
    execution_time BIGINT NOT NULL
);
"#;

pub async fn run_remote_migrations(pool: &Pool) -> Result<()> {
    let conn = pool.get().await?;
    conn.execute(CREATE_MIGRATIONS_TABLE_SQL, params![])
        .await
        .context("failed to ensure _sqlx_migrations table")?;
    ensure_not_dirty(&conn).await?;

    let mut applied = load_applied_migrations(&conn).await?;
    validate_applied(&applied)?;

    for migration in SQLITE_MIGRATOR.iter() {
        if migration.migration_type.is_down_migration() {
            continue;
        }
        if applied.remove(&migration.version).is_some() {
            continue;
        }
        apply_migration(&conn, migration).await?;
    }

    Ok(())
}

async fn ensure_not_dirty(conn: &libsql::Connection) -> Result<()> {
    let mut rows = conn
        .query(
            "SELECT version FROM _sqlx_migrations WHERE success = FALSE ORDER BY version LIMIT 1",
            params![],
        )
        .await?;
    if let Some(row) = rows.next().await? {
        let version = row_ext::get::<i64>(&row, "version")?;
        bail!(
            "database contains unfinished migration version {version}; resolve manually before continuing"
        );
    }
    Ok(())
}

async fn load_applied_migrations(conn: &libsql::Connection) -> Result<HashMap<i64, Vec<u8>>> {
    let mut rows = conn
        .query(
            "SELECT version, checksum FROM _sqlx_migrations ORDER BY version",
            params![],
        )
        .await?;
    let mut applied = HashMap::new();
    while let Some(row) = rows.next().await? {
        let version = row_ext::get::<i64>(&row, "version")?;
        let checksum = row_ext::get::<Vec<u8>>(&row, "checksum")?;
        applied.insert(version, checksum);
    }
    Ok(applied)
}

fn validate_applied(applied: &HashMap<i64, Vec<u8>>) -> Result<()> {
    for (version, checksum) in applied {
        let Some(expected) = SQLITE_MIGRATOR.iter().find(|m| m.version == *version) else {
            bail!("database contains unknown migration version {version}");
        };
        if checksum.as_slice() != expected.checksum.as_ref() {
            bail!(
                "migration {version} checksum mismatch (database does not match compiled migrations)"
            );
        }
    }
    Ok(())
}

async fn apply_migration(conn: &libsql::Connection, migration: &Migration) -> Result<()> {
    let tx = conn.transaction().await?;
    let start = Instant::now();
    tx.execute_batch(migration.sql.as_ref())
        .await
        .with_context(|| format!("failed to run migration {}", migration.version))?;
    tx.execute(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
         VALUES (?, ?, TRUE, ?, -1)",
        params![
            migration.version,
            migration.description.as_ref(),
            migration.checksum.as_ref()
        ],
    )
    .await
    .with_context(|| format!("failed to record migration {}", migration.version))?;
    tx.commit().await?;

    let elapsed = start.elapsed();
    #[allow(clippy::cast_possible_truncation)]
    let elapsed_ns = elapsed.as_nanos().min(i64::MAX as u128) as i64;
    conn.execute(
        "UPDATE _sqlx_migrations SET execution_time = ? WHERE version = ?",
        params![elapsed_ns, migration.version],
    )
    .await
    .ok(); // best-effort tracking

    Ok(())
}
