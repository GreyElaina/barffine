use anyhow::{Context, Result};
use sqlx::{Pool, Row, Sqlite, Transaction, sqlite::SqliteRow};

#[derive(Debug, Clone)]
pub struct DocUpdateRecord {
    pub update: Vec<u8>,
    pub created_at: i64,
    pub editor_id: Option<String>,
}

pub(crate) async fn insert_doc_updates(
    tx: &mut Transaction<'_, Sqlite>,
    space_type: &str,
    space_id: &str,
    doc_id: &str,
    updates: &[Vec<u8>],
    editor_id: Option<&str>,
    timestamp: i64,
) -> Result<()> {
    for update in updates {
        sqlx::query(
            "INSERT INTO doc_updates (space_type, space_id, doc_id, editor_id, update_blob, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(space_type)
        .bind(space_id)
        .bind(doc_id)
        .bind(editor_id)
        .bind(update)
        .bind(timestamp)
        .execute(tx.as_mut())
        .await?;
    }
    Ok(())
}

pub(crate) async fn trim_doc_updates(
    tx: &mut Transaction<'_, Sqlite>,
    space_type: &str,
    space_id: &str,
    doc_id: &str,
    limit: i64,
) -> Result<()> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM doc_updates WHERE space_type = ? AND space_id = ? AND doc_id = ?",
    )
    .bind(space_type)
    .bind(space_id)
    .bind(doc_id)
    .fetch_one(tx.as_mut())
    .await?;

    if count > limit {
        let delete_count = count - limit;
        sqlx::query(
            "DELETE FROM doc_updates
             WHERE id IN (
                 SELECT id FROM doc_updates
                 WHERE space_type = ? AND space_id = ? AND doc_id = ?
                 ORDER BY id ASC
                 LIMIT ?
             )",
        )
        .bind(space_type)
        .bind(space_id)
        .bind(doc_id)
        .bind(delete_count)
        .execute(tx.as_mut())
        .await?;
    }

    Ok(())
}

pub(crate) async fn fetch_doc_logs(
    pool: Pool<Sqlite>,
    space_type: &str,
    space_id: &str,
    doc_id: &str,
) -> Result<Vec<DocUpdateRecord>> {
    let rows = sqlx::query(
        "SELECT id, update_blob, created_at, editor_id
         FROM doc_updates
         WHERE space_type = ? AND space_id = ? AND doc_id = ?
         ORDER BY id ASC",
    )
    .bind(space_type)
    .bind(space_id)
    .bind(doc_id)
    .fetch_all(&pool)
    .await?;

    parse_doc_log_rows(rows)
}

pub(crate) async fn fetch_doc_logs_via_executor<'e, E>(
    executor: E,
    space_type: &str,
    space_id: &str,
    doc_id: &str,
) -> Result<Vec<DocUpdateRecord>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let rows = sqlx::query(
        "SELECT id, update_blob, created_at, editor_id
         FROM doc_updates
         WHERE space_type = ? AND space_id = ? AND doc_id = ?
         ORDER BY id ASC",
    )
    .bind(space_type)
    .bind(space_id)
    .bind(doc_id)
    .fetch_all(executor)
    .await?;

    parse_doc_log_rows(rows)
}

fn parse_doc_log_rows(rows: Vec<SqliteRow>) -> Result<Vec<DocUpdateRecord>> {
    let mut logs = Vec::with_capacity(rows.len());
    for row in rows {
        logs.push(DocUpdateRecord {
            update: row
                .try_get::<Vec<u8>, _>("update_blob")
                .context("read doc update blob")?,
            created_at: row.try_get("created_at")?,
            editor_id: row
                .try_get::<Option<String>, _>("editor_id")
                .unwrap_or(None),
        });
    }
    Ok(logs)
}
