use anyhow::Error as AnyError;
use sqlx::migrate::MigrateError;
use sqlx::{Pool, Sqlite};

pub fn is_unique_violation(err: &AnyError) -> bool {
    if let Some(sqlx_err) = err.downcast_ref::<sqlx::Error>() {
        if let sqlx::Error::Database(db_error) = sqlx_err {
            return db_error.message().contains("UNIQUE constraint failed");
        }
    }

    false
}

pub async fn run_migrations(pool: &Pool<Sqlite>) -> Result<(), MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}
