use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use sqlx::{Pool, Row, Sqlite, sqlite::SqliteRow};

use crate::{
    blob::{BlobMetadata, BlobScope, ListedBlobRecord},
    db::blob_repo::{BlobRecord, BlobRepository},
};

pub struct SqliteBlobRepository {
    pool: Pool<Sqlite>,
}

impl SqliteBlobRepository {
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self { pool }
    }

    fn encode_last_modified(meta: &BlobMetadata) -> Option<i64> {
        meta.last_modified.map(|value| value.timestamp())
    }

    fn decode_last_modified(value: Option<i64>) -> Option<DateTime<Utc>> {
        value.and_then(|ts| Utc.timestamp_opt(ts, 0).single())
    }

    fn clamp_length(len: u64) -> i64 {
        (len.min(i64::MAX as u64)) as i64
    }

    fn build_metadata(row: &SqliteRow) -> Result<BlobMetadata> {
        let content_type = row.try_get::<Option<String>, _>("content_type")?;
        let content_length = row
            .try_get::<Option<i64>, _>("content_length")?
            .map(|len| len.max(0) as u64);
        let etag = row.try_get::<Option<String>, _>("etag")?;
        let last_modified =
            Self::decode_last_modified(row.try_get::<Option<i64>, _>("last_modified")?);

        Ok(BlobMetadata {
            content_type,
            content_length,
            etag,
            last_modified,
        })
    }
}

#[async_trait]
impl BlobRepository for SqliteBlobRepository {
    async fn upsert_blob(
        &self,
        scope: &BlobScope,
        key: &str,
        content: &[u8],
        metadata: &BlobMetadata,
        now: i64,
    ) -> Result<()> {
        let table = BlobTable::table_for(scope);
        let identifier = scope.identifier();
        let content_length = metadata
            .content_length
            .unwrap_or_else(|| content.len() as u64);
        let last_modified = Self::encode_last_modified(metadata);

        sqlx::query(table.upsert_sql())
            .bind(identifier)
            .bind(key)
            .bind(content)
            .bind(metadata.content_type.clone())
            .bind(Self::clamp_length(content_length))
            .bind(metadata.etag.clone())
            .bind(last_modified)
            .bind(now)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn fetch_blob(&self, scope: &BlobScope, key: &str) -> Result<Option<BlobRecord>> {
        let table = BlobTable::table_for(scope);
        let identifier = scope.identifier();
        let row = sqlx::query(table.select_sql())
            .bind(identifier)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let metadata = Self::build_metadata(&row)?;
        let bytes: Vec<u8> = row.try_get("content")?;
        Ok(Some(BlobRecord { metadata, bytes }))
    }

    async fn delete_blob(
        &self,
        scope: &BlobScope,
        key: &str,
        permanently: bool,
        timestamp: i64,
    ) -> Result<()> {
        let table = BlobTable::table_for(scope);
        let identifier = scope.identifier();
        if permanently {
            sqlx::query(table.hard_delete_sql())
                .bind(identifier)
                .bind(key)
                .execute(&self.pool)
                .await?;
        } else {
            sqlx::query(table.soft_delete_sql())
                .bind(timestamp)
                .bind(identifier)
                .bind(key)
                .execute(&self.pool)
                .await?;
        }

        Ok(())
    }

    async fn release_deleted(&self, scope: &BlobScope) -> Result<()> {
        let table = BlobTable::table_for(scope);
        let identifier = scope.identifier();
        sqlx::query(table.release_deleted_sql())
            .bind(identifier)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_blobs(&self, scope: &BlobScope) -> Result<Vec<ListedBlobRecord>> {
        let table = BlobTable::table_for(scope);
        let identifier = scope.identifier();
        let rows = sqlx::query(table.list_sql())
            .bind(identifier)
            .fetch_all(&self.pool)
            .await?;

        let mut blobs = Vec::with_capacity(rows.len());
        for row in rows {
            let key: String = row.try_get("blob_key")?;
            let mime = row
                .try_get::<Option<String>, _>("content_type")?
                .filter(|value| !value.is_empty());
            let size = row
                .try_get::<Option<i64>, _>("content_length")?
                .unwrap_or(0);
            let created_at = row.try_get::<Option<i64>, _>("created_at")?;
            blobs.push(ListedBlobRecord {
                key,
                mime,
                size,
                created_at,
            });
        }

        blobs.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(blobs)
    }

    async fn total_size(&self, scope: &BlobScope) -> Result<i64> {
        let table = BlobTable::table_for(scope);
        let identifier = scope.identifier();
        let row = sqlx::query(table.total_size_sql())
            .bind(identifier)
            .fetch_one(&self.pool)
            .await?;

        Ok(row.try_get::<i64, _>("total")?)
    }
}

#[derive(Copy, Clone)]
enum BlobTable {
    Workspace,
    Namespace,
}

impl BlobTable {
    fn table_for(scope: &BlobScope) -> Self {
        match scope {
            BlobScope::Workspace { .. } => BlobTable::Workspace,
            BlobScope::Namespace { .. } => BlobTable::Namespace,
        }
    }

    fn upsert_sql(self) -> &'static str {
        match self {
            BlobTable::Workspace => UPSERT_WORKSPACE_BLOB_SQL,
            BlobTable::Namespace => UPSERT_NAMESPACE_BLOB_SQL,
        }
    }

    fn select_sql(self) -> &'static str {
        match self {
            BlobTable::Workspace => SELECT_WORKSPACE_BLOB_SQL,
            BlobTable::Namespace => SELECT_NAMESPACE_BLOB_SQL,
        }
    }

    fn hard_delete_sql(self) -> &'static str {
        match self {
            BlobTable::Workspace => HARD_DELETE_WORKSPACE_BLOB_SQL,
            BlobTable::Namespace => HARD_DELETE_NAMESPACE_BLOB_SQL,
        }
    }

    fn soft_delete_sql(self) -> &'static str {
        match self {
            BlobTable::Workspace => SOFT_DELETE_WORKSPACE_BLOB_SQL,
            BlobTable::Namespace => SOFT_DELETE_NAMESPACE_BLOB_SQL,
        }
    }

    fn release_deleted_sql(self) -> &'static str {
        match self {
            BlobTable::Workspace => RELEASE_DELETED_WORKSPACE_BLOBS_SQL,
            BlobTable::Namespace => RELEASE_DELETED_NAMESPACE_BLOBS_SQL,
        }
    }

    fn list_sql(self) -> &'static str {
        match self {
            BlobTable::Workspace => LIST_WORKSPACE_BLOBS_SQL,
            BlobTable::Namespace => LIST_NAMESPACE_BLOBS_SQL,
        }
    }

    fn total_size_sql(self) -> &'static str {
        match self {
            BlobTable::Workspace => TOTAL_SIZE_WORKSPACE_BLOBS_SQL,
            BlobTable::Namespace => TOTAL_SIZE_NAMESPACE_BLOBS_SQL,
        }
    }
}

const UPSERT_WORKSPACE_BLOB_SQL: &str = r#"
    INSERT INTO workspace_blobs (
        workspace_id, blob_key, content, content_type, content_length,
        etag, last_modified, created_at, deleted_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
    ON CONFLICT(workspace_id, blob_key) DO UPDATE SET
        content = excluded.content,
        content_type = excluded.content_type,
        content_length = excluded.content_length,
        etag = excluded.etag,
        last_modified = excluded.last_modified,
        deleted_at = NULL,
        created_at = CASE
            WHEN workspace_blobs.deleted_at IS NULL THEN workspace_blobs.created_at
            ELSE excluded.created_at
        END
"#;

const UPSERT_NAMESPACE_BLOB_SQL: &str = r#"
    INSERT INTO namespace_blobs (
        namespace, blob_key, content, content_type, content_length,
        etag, last_modified, created_at, deleted_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
    ON CONFLICT(namespace, blob_key) DO UPDATE SET
        content = excluded.content,
        content_type = excluded.content_type,
        content_length = excluded.content_length,
        etag = excluded.etag,
        last_modified = excluded.last_modified,
        deleted_at = NULL,
        created_at = CASE
            WHEN namespace_blobs.deleted_at IS NULL THEN namespace_blobs.created_at
            ELSE excluded.created_at
        END
"#;

const SELECT_WORKSPACE_BLOB_SQL: &str = r#"
    SELECT content, content_type, content_length, etag, last_modified
    FROM workspace_blobs
    WHERE workspace_id = ? AND blob_key = ? AND deleted_at IS NULL
"#;

const SELECT_NAMESPACE_BLOB_SQL: &str = r#"
    SELECT content, content_type, content_length, etag, last_modified
    FROM namespace_blobs
    WHERE namespace = ? AND blob_key = ? AND deleted_at IS NULL
"#;

const HARD_DELETE_WORKSPACE_BLOB_SQL: &str = r#"
    DELETE FROM workspace_blobs
    WHERE workspace_id = ? AND blob_key = ?
"#;

const HARD_DELETE_NAMESPACE_BLOB_SQL: &str = r#"
    DELETE FROM namespace_blobs
    WHERE namespace = ? AND blob_key = ?
"#;

const SOFT_DELETE_WORKSPACE_BLOB_SQL: &str = r#"
    UPDATE workspace_blobs
    SET deleted_at = ?
    WHERE workspace_id = ? AND blob_key = ?
"#;

const SOFT_DELETE_NAMESPACE_BLOB_SQL: &str = r#"
    UPDATE namespace_blobs
    SET deleted_at = ?
    WHERE namespace = ? AND blob_key = ?
"#;

const RELEASE_DELETED_WORKSPACE_BLOBS_SQL: &str = r#"
    DELETE FROM workspace_blobs
    WHERE workspace_id = ? AND deleted_at IS NOT NULL
"#;

const RELEASE_DELETED_NAMESPACE_BLOBS_SQL: &str = r#"
    DELETE FROM namespace_blobs
    WHERE namespace = ? AND deleted_at IS NOT NULL
"#;

const LIST_WORKSPACE_BLOBS_SQL: &str = r#"
    SELECT blob_key, content_type, content_length, last_modified, created_at
    FROM workspace_blobs
    WHERE workspace_id = ? AND deleted_at IS NULL
    ORDER BY blob_key
"#;

const LIST_NAMESPACE_BLOBS_SQL: &str = r#"
    SELECT blob_key, content_type, content_length, last_modified, created_at
    FROM namespace_blobs
    WHERE namespace = ? AND deleted_at IS NULL
    ORDER BY blob_key
"#;

const TOTAL_SIZE_WORKSPACE_BLOBS_SQL: &str = r#"
    SELECT COALESCE(SUM(content_length), 0) AS total
    FROM workspace_blobs
    WHERE workspace_id = ? AND deleted_at IS NULL
"#;

const TOTAL_SIZE_NAMESPACE_BLOBS_SQL: &str = r#"
    SELECT COALESCE(SUM(content_length), 0) AS total
    FROM namespace_blobs
    WHERE namespace = ? AND deleted_at IS NULL
"#;
