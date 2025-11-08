use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use serde_json::Value as JsonValue;
use sqlx::{Pool, Row, Sqlite, sqlite::SqliteRow};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    db::Database,
    doc::DocEngine,
    doc_update_log::{
        DocUpdateRecord, fetch_doc_logs, fetch_doc_logs_via_executor, insert_doc_updates,
        trim_doc_updates,
    },
};

pub const DOC_UPDATE_LOG_LIMIT: i64 = 200;
const SPACE_TYPE_WORKSPACE: &str = "workspace";
const WORKSPACE_DB_DOC_SUFFIXES: &[&str] = &[
    "folders",
    "docProperties",
    "docCustomPropertyInfo",
    "pinnedCollections",
    "explorerIcon",
];

const WORKSPACE_USERDATA_DOC_SUFFIXES: &[&str] = &["favorite", "settings", "docIntegrationRef"];

#[derive(Debug, PartialEq, Eq)]
enum SystemDocKind<'a> {
    WorkspaceRoot,
    WorkspaceDb { suffix: &'a str },
    Userdata { user_id: &'a str, suffix: &'a str },
}

fn classify_system_doc<'a>(workspace_id: &'a str, doc_id: &'a str) -> Option<SystemDocKind<'a>> {
    if doc_id == workspace_id {
        return Some(SystemDocKind::WorkspaceRoot);
    }

    let parts: Vec<&str> = doc_id.split('$').collect();
    match parts.as_slice() {
        ["db", ws_id, suffix]
            if *ws_id == workspace_id && WORKSPACE_DB_DOC_SUFFIXES.contains(suffix) =>
        {
            Some(SystemDocKind::WorkspaceDb { suffix })
        }
        ["userdata", user_id, ws_id, suffix]
            if *ws_id == workspace_id && WORKSPACE_USERDATA_DOC_SUFFIXES.contains(suffix) =>
        {
            Some(SystemDocKind::Userdata { user_id, suffix })
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct DocumentHistoryRecord {
    pub id: i64,
    pub snapshot: Vec<u8>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct DocumentSnapshot {
    pub snapshot: Vec<u8>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DocumentMetadata {
    pub id: String,
    pub workspace_id: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub default_role: String,
    pub public: bool,
    pub blocked: bool,
    pub mode: String,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub creator_id: Option<String>,
    pub updater_id: Option<String>,
    pub share_token: Option<String>,
    pub trashed_at: Option<i64>,
    pub trashed_by: Option<String>,
    pub snapshot: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct UserShareTokenRecord {
    pub workspace_id: String,
    pub doc_id: String,
    pub token: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct DocumentCursor {
    pub timestamp: i64,
    pub id: String,
}

#[derive(Clone)]
pub struct DocumentStore {
    pool: Pool<Sqlite>,
}

impl DocumentStore {
    pub fn new(database: &Database) -> Self {
        Self {
            pool: database.pool().clone(),
        }
    }

    fn now_millis() -> i64 {
        Utc::now().timestamp_millis()
    }

    fn metadata_from_row(row: SqliteRow) -> DocumentMetadata {
        let public = row.get::<i64, _>("public") != 0;
        let blocked = row.try_get::<i64, _>("blocked").unwrap_or(0) != 0;
        let share_token = row
            .try_get::<Option<String>, _>("share_token")
            .unwrap_or(None)
            .filter(|_| public);

        let trashed_at = row.try_get::<Option<i64>, _>("trashed_at").unwrap_or(None);
        let trashed_by = row
            .try_get::<Option<String>, _>("trashed_by")
            .unwrap_or(None);

        DocumentMetadata {
            id: row.get("id"),
            workspace_id: row.get("workspace_id"),
            created_at: row.get::<i64, _>("created_at"),
            updated_at: row.get::<i64, _>("updated_at"),
            default_role: row.get("default_role"),
            public,
            blocked,
            mode: row.get("mode"),
            title: row.try_get::<Option<String>, _>("title").unwrap_or(None),
            summary: row.try_get::<Option<String>, _>("summary").unwrap_or(None),
            creator_id: row
                .try_get::<Option<String>, _>("creator_id")
                .unwrap_or(None),
            updater_id: row
                .try_get::<Option<String>, _>("updater_id")
                .unwrap_or(None),
            share_token,
            trashed_at,
            trashed_by,
            snapshot: None,
        }
    }

    fn metadata_needs_backfill(metadata: &DocumentMetadata) -> bool {
        metadata.title.is_none()
            || metadata.summary.is_none()
            || metadata.creator_id.is_none()
            || metadata.updater_id.is_none()
            || metadata.created_at == 0
            || metadata.updated_at == 0
    }

    fn merge_parsed_metadata(metadata: &mut DocumentMetadata, parsed: ParsedDocMeta) {
        let ParsedDocMeta {
            title,
            summary,
            created_at,
            updated_at,
            creator_id,
            updater_id,
        } = parsed;

        if metadata.title.is_none() {
            metadata.title = title;
        }

        if metadata.summary.is_none() {
            metadata.summary = summary;
        }

        if metadata.creator_id.is_none() {
            metadata.creator_id = creator_id;
        }

        if metadata.updater_id.is_none() {
            metadata.updater_id = updater_id;
        }

        if let Some(parsed_created_at) = created_at {
            if metadata.created_at == 0 || parsed_created_at < metadata.created_at {
                metadata.created_at = parsed_created_at;
            }
        }

        if let Some(parsed_updated_at) = updated_at {
            if parsed_updated_at > metadata.updated_at {
                metadata.updated_at = parsed_updated_at;
            }
        }
    }

    pub async fn hydrate_metadata(&self, metadata: DocumentMetadata) -> Result<DocumentMetadata> {
        if !Self::metadata_needs_backfill(&metadata) {
            return Ok(metadata);
        }

        let snapshot = if let Some(existing) = metadata.snapshot.clone() {
            Some(existing)
        } else {
            self.fetch_snapshot(&metadata.workspace_id, &metadata.id)
                .await?
        };

        let Some(snapshot) = snapshot else {
            return Ok(metadata);
        };

        let parsed = extract_doc_meta(&snapshot, &metadata.id);
        let mut updated = metadata.clone();
        updated.snapshot = Some(snapshot.clone());
        Self::merge_parsed_metadata(&mut updated, parsed);

        if updated == metadata {
            return Ok(metadata);
        }

        sqlx::query(
            "UPDATE documents
             SET title = ?, summary = ?, creator_id = ?, updater_id = ?, created_at = ?, updated_at = ?
             WHERE workspace_id = ? AND id = ?",
        )
        .bind(updated.title.as_deref())
        .bind(updated.summary.as_deref())
        .bind(updated.creator_id.as_deref())
        .bind(updated.updater_id.as_deref())
        .bind(updated.created_at)
        .bind(updated.updated_at)
        .bind(&updated.workspace_id)
        .bind(&updated.id)
        .execute(&self.pool)
        .await?;

        Ok(updated)
    }

    fn generate_share_token() -> String {
        Uuid::new_v4().to_string().replace('-', "")
    }

    pub async fn apply_updates(
        &self,
        workspace_id: &str,
        doc_id: &str,
        updates: &[Vec<u8>],
    ) -> Result<Vec<u8>> {
        if updates.is_empty() {
            return Err(anyhow!("updates payload must not be empty"));
        }

        let mut tx = self.pool.begin().await?;
        let snapshot_row =
            sqlx::query("SELECT snapshot FROM documents WHERE workspace_id = ? AND id = ?")
                .bind(workspace_id)
                .bind(doc_id)
                .fetch_optional(&mut *tx)
                .await?;

        let existing_snapshot: Option<Vec<u8>> = snapshot_row
            .as_ref()
            .map(|row| row.try_get("snapshot"))
            .transpose()
            .context("failed to read snapshot column")?;

        let SnapshotComputation {
            snapshot: new_snapshot,
            meta,
            created_at,
            updated_at,
        } = build_snapshot_from_updates(
            doc_id,
            existing_snapshot.as_deref(),
            updates,
            Self::now_millis,
        )?;

        if existing_snapshot.is_none() {
            sqlx::query(
                "INSERT INTO documents (id, workspace_id, snapshot, created_at, updated_at, default_role, public, blocked, mode, title, summary, creator_id, updater_id, trashed_at, trashed_by)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)",
            )
            .bind(doc_id)
            .bind(workspace_id)
            .bind(&new_snapshot)
            .bind(created_at)
            .bind(updated_at)
            .bind("manager")
            .bind(0_i64)
            .bind(0_i64)
            .bind("page")
            .bind(meta.title.as_deref())
            .bind(meta.summary.as_deref())
            .bind(meta.creator_id.as_deref())
            .bind(meta.updater_id.as_deref())
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query(
                "UPDATE documents SET snapshot = ?, updated_at = ?, title = ?, summary = ?, updater_id = COALESCE(?, updater_id), creator_id = COALESCE(?, creator_id) WHERE id = ? AND workspace_id = ?",
            )
            .bind(&new_snapshot)
            .bind(updated_at)
            .bind(meta.title.as_deref())
            .bind(meta.summary.as_deref())
            .bind(meta.updater_id.as_deref())
            .bind(meta.creator_id.as_deref())
            .bind(doc_id)
            .bind(workspace_id)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(previous) = existing_snapshot {
            sqlx::query(
                "INSERT INTO document_history (doc_id, workspace_id, snapshot, created_at) VALUES (?, ?, ?, ?)",
            )
            .bind(doc_id)
            .bind(workspace_id)
            .bind(previous)
            .bind(updated_at)
            .execute(&mut *tx)
            .await?;
        }

        insert_doc_updates(
            &mut tx,
            SPACE_TYPE_WORKSPACE,
            workspace_id,
            doc_id,
            updates,
            meta.updater_id.as_deref(),
            updated_at,
        )
        .await?;
        trim_doc_updates(
            &mut tx,
            SPACE_TYPE_WORKSPACE,
            workspace_id,
            doc_id,
            DOC_UPDATE_LOG_LIMIT,
        )
        .await?;

        tx.commit().await?;

        Ok(new_snapshot)
    }

    pub async fn append_doc_updates(
        &self,
        workspace_id: &str,
        doc_id: &str,
        updates: &[Vec<u8>],
        editor_id: Option<&str>,
        timestamp: i64,
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE documents
             SET updated_at = ?, updater_id = COALESCE(?, updater_id)
             WHERE workspace_id = ? AND id = ?",
        )
        .bind(timestamp)
        .bind(editor_id)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.rollback().await?;
            anyhow::bail!("document not found: {workspace_id}/{doc_id}");
        }

        insert_doc_updates(
            &mut tx,
            SPACE_TYPE_WORKSPACE,
            workspace_id,
            doc_id,
            updates,
            editor_id,
            timestamp,
        )
        .await?;
        trim_doc_updates(
            &mut tx,
            SPACE_TYPE_WORKSPACE,
            workspace_id,
            doc_id,
            DOC_UPDATE_LOG_LIMIT,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn compact_doc(&self, workspace_id: &str, doc_id: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT snapshot FROM documents WHERE workspace_id = ? AND id = ?")
            .bind(workspace_id)
            .bind(doc_id)
            .fetch_optional(&mut *tx)
            .await?;

        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(());
        };

        let base_snapshot: Vec<u8> = row.try_get("snapshot")?;
        let logs =
            fetch_doc_logs_via_executor(tx.as_mut(), SPACE_TYPE_WORKSPACE, workspace_id, doc_id)
                .await?;
        if logs.is_empty() {
            tx.commit().await?;
            return Ok(());
        }

        let updates = logs
            .iter()
            .map(|log| log.update.clone())
            .collect::<Vec<_>>();
        let merged_snapshot = DocEngine::apply_updates_to_snapshot(Some(&base_snapshot), &updates)
            .context("merge doc updates during compaction")?;
        let meta = extract_doc_meta(&merged_snapshot, doc_id);
        let updated_at = meta
            .updated_at
            .or_else(|| logs.last().map(|log| log.created_at))
            .unwrap_or_else(Self::now_millis);

        sqlx::query(
            "UPDATE documents
             SET snapshot = ?, updated_at = ?, title = ?, summary = ?,
                 updater_id = COALESCE(?, updater_id), creator_id = COALESCE(?, creator_id)
             WHERE workspace_id = ? AND id = ?",
        )
        .bind(&merged_snapshot)
        .bind(updated_at)
        .bind(meta.title.as_deref())
        .bind(meta.summary.as_deref())
        .bind(meta.updater_id.as_deref())
        .bind(meta.creator_id.as_deref())
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query("DELETE FROM doc_updates WHERE space_type = ? AND space_id = ? AND doc_id = ?")
            .bind(SPACE_TYPE_WORKSPACE)
            .bind(workspace_id)
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn ensure_doc_record(
        &self,
        workspace_id: &str,
        doc_id: &str,
        owner_id: &str,
        title: Option<&str>,
    ) -> Result<DocumentMetadata> {
        if let Some(metadata) = self.find_metadata(workspace_id, doc_id).await? {
            return Ok(metadata);
        }

        let snapshot = DocEngine::new().snapshot()?;
        let now_ts = Self::now_millis();
        let title_value = title
            .map(str::trim)
            .filter(|trimmed| !trimmed.is_empty())
            .map(ToOwned::to_owned);

        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO documents (
                 id,
                 workspace_id,
                 snapshot,
                 created_at,
                 updated_at,
                 default_role,
                 public,
                 blocked,
                 mode,
                 title,
                 summary,
                 creator_id,
                 updater_id,
                 trashed_at,
                 trashed_by
             ) VALUES (?, ?, ?, ?, ?, 'manager', 0, 0, 'page', ?, NULL, ?, ?, NULL, NULL)",
        )
        .bind(doc_id)
        .bind(workspace_id)
        .bind(&snapshot)
        .bind(now_ts)
        .bind(now_ts)
        .bind(title_value.as_deref())
        .bind(owner_id)
        .bind(owner_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        let mut metadata = self
            .find_metadata(workspace_id, doc_id)
            .await?
            .ok_or_else(|| anyhow!("workspace root doc inserted but not found"))?;
        metadata.snapshot = Some(snapshot);
        Ok(metadata)
    }

    pub async fn fetch_snapshot(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<Vec<u8>>> {
        let snapshot = self
            .fetch_snapshot_with_timestamp(workspace_id, doc_id)
            .await?
            .map(|doc| doc.snapshot);
        Ok(snapshot)
    }

    pub async fn fetch_snapshot_with_timestamp(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentSnapshot>> {
        let row = sqlx::query(
            "SELECT snapshot, updated_at FROM documents WHERE workspace_id = ? AND id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let mut snapshot: Vec<u8> = row.try_get("snapshot").context("failed to read snapshot")?;
        let mut updated_at: i64 = row.try_get("updated_at")?;

        let logs = self.fetch_doc_logs(workspace_id, doc_id).await?;
        if !logs.is_empty() {
            let log_updates = logs
                .iter()
                .map(|log| log.update.clone())
                .collect::<Vec<_>>();
            snapshot = DocEngine::apply_updates_to_snapshot(Some(&snapshot), &log_updates)
                .context("apply doc updates from log to snapshot")?;
            if let Some(last) = logs.last() {
                updated_at = updated_at.max(last.created_at);
            }
        }

        Ok(Some(DocumentSnapshot {
            snapshot,
            updated_at,
        }))
    }

    pub async fn list_doc_timestamps(
        &self,
        workspace_id: &str,
        after: Option<i64>,
    ) -> Result<HashMap<String, i64>> {
        let rows = if let Some(threshold) = after {
            sqlx::query(
                "SELECT id, updated_at FROM documents WHERE workspace_id = ? AND updated_at > ?",
            )
            .bind(workspace_id)
            .bind(threshold)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query("SELECT id, updated_at FROM documents WHERE workspace_id = ?")
                .bind(workspace_id)
                .fetch_all(&self.pool)
                .await?
        };

        let mut map = HashMap::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")?;
            let updated_at: i64 = row.try_get("updated_at")?;
            map.insert(id, updated_at);
        }

        Ok(map)
    }

    pub async fn list_history(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
    ) -> Result<Vec<DocumentHistoryRecord>> {
        let rows = sqlx::query(
            "SELECT id, snapshot, created_at FROM document_history
             WHERE workspace_id = ? AND doc_id = ?
             ORDER BY created_at DESC, id DESC
             LIMIT ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let records = rows
            .into_iter()
            .map(|row| -> Result<_> {
                Ok(DocumentHistoryRecord {
                    id: row.try_get("id").context("failed to read history id")?,
                    snapshot: row.try_get("snapshot").context("failed to read snapshot")?,
                    created_at: row.try_get("created_at")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(records)
    }

    pub async fn fetch_history_as_of(
        &self,
        workspace_id: &str,
        doc_id: &str,
        timestamp: i64,
    ) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query(
            "SELECT snapshot FROM document_history
             WHERE workspace_id = ? AND doc_id = ? AND created_at <= ?
             ORDER BY created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(timestamp)
        .fetch_optional(&self.pool)
        .await?;

        let snapshot = match row {
            Some(row) => Some(row.try_get("snapshot").context("failed to read snapshot")?),
            None => None,
        };

        Ok(snapshot)
    }

    pub async fn find_metadata(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let row = sqlx::query(
            "SELECT
                 d.id,
                 d.workspace_id,
                 d.created_at,
                 d.updated_at,
                 d.default_role,
                 d.public,
                 d.blocked,
                 d.mode,
                 d.title,
                 d.summary,
                 d.creator_id,
                 d.updater_id,
                 d.trashed_at,
                 d.trashed_by,
                 l.token AS share_token
             FROM documents d
             LEFT JOIN doc_public_links l
               ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
             WHERE d.workspace_id = ? AND d.id = ?",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let metadata = Self::metadata_from_row(row);
            let hydrated = self.hydrate_metadata(metadata).await?;
            Ok(Some(hydrated))
        } else {
            Ok(None)
        }
    }

    pub async fn list_by_workspace_created(
        &self,
        workspace_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentCursor>,
    ) -> Result<Vec<DocumentMetadata>> {
        let rows = if let Some(cursor) = cursor {
            sqlx::query(
                "SELECT
                     d.id,
                     d.workspace_id,
                     d.created_at,
                     d.updated_at,
                     d.default_role,
                     d.public,
                     d.blocked,
                     d.mode,
                     d.title,
                     d.summary,
                     d.creator_id,
                     d.updater_id,
                                     d.trashed_at,
                                     d.trashed_by,
                                     l.token AS share_token
                 FROM documents d
                 LEFT JOIN doc_public_links l
                   ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
                 WHERE d.workspace_id = ?
                                 AND d.trashed_at IS NULL
                   AND (d.created_at > ? OR (d.created_at = ? AND d.id > ?))
                 ORDER BY d.created_at ASC, d.id ASC
                 LIMIT ?",
            )
            .bind(workspace_id)
            .bind(cursor.timestamp)
            .bind(cursor.timestamp)
            .bind(&cursor.id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT
                     d.id,
                     d.workspace_id,
                     d.created_at,
                     d.updated_at,
                     d.default_role,
                     d.public,
                     d.blocked,
                     d.mode,
                     d.title,
                     d.summary,
                     d.creator_id,
                     d.updater_id,
                                     d.trashed_at,
                                     d.trashed_by,
                                     l.token AS share_token
                 FROM documents d
                 LEFT JOIN doc_public_links l
                   ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
                 WHERE d.workspace_id = ?
                                 AND d.trashed_at IS NULL
                 ORDER BY d.created_at ASC, d.id ASC
                 LIMIT ? OFFSET ?",
            )
            .bind(workspace_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?
        };

        let mut records: Vec<_> = rows.into_iter().map(Self::metadata_from_row).collect();
        for record in &mut records {
            if Self::metadata_needs_backfill(record) {
                *record = self.hydrate_metadata(record.clone()).await?;
            }
        }

        Ok(records)
    }

    pub async fn list_by_workspace_recent(
        &self,
        workspace_id: &str,
        limit: i64,
        offset: i64,
        cursor: Option<&DocumentCursor>,
    ) -> Result<Vec<DocumentMetadata>> {
        let rows = if let Some(cursor) = cursor {
            sqlx::query(
                "SELECT
                     d.id,
                     d.workspace_id,
                     d.created_at,
                     d.updated_at,
                     d.default_role,
                     d.public,
                     d.blocked,
                     d.mode,
                     d.title,
                     d.summary,
                     d.creator_id,
                     d.updater_id,
                                     d.trashed_at,
                                     d.trashed_by,
                                     l.token AS share_token
                 FROM documents d
                 LEFT JOIN doc_public_links l
                   ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
                 WHERE d.workspace_id = ?
                                 AND d.trashed_at IS NULL
                   AND (d.updated_at < ? OR (d.updated_at = ? AND d.id < ?))
                 ORDER BY d.updated_at DESC, d.id DESC
                 LIMIT ?",
            )
            .bind(workspace_id)
            .bind(cursor.timestamp)
            .bind(cursor.timestamp)
            .bind(&cursor.id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT
                     d.id,
                     d.workspace_id,
                     d.created_at,
                     d.updated_at,
                     d.default_role,
                     d.public,
                     d.blocked,
                     d.mode,
                     d.title,
                     d.summary,
                     d.creator_id,
                     d.updater_id,
                                     d.trashed_at,
                                     d.trashed_by,
                                     l.token AS share_token
                 FROM documents d
                 LEFT JOIN doc_public_links l
                   ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
                 WHERE d.workspace_id = ?
                                 AND d.trashed_at IS NULL
                 ORDER BY d.updated_at DESC, d.id DESC
                 LIMIT ? OFFSET ?",
            )
            .bind(workspace_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?
        };

        let mut records: Vec<_> = rows.into_iter().map(Self::metadata_from_row).collect();
        for record in &mut records {
            if Self::metadata_needs_backfill(record) {
                *record = self.hydrate_metadata(record.clone()).await?;
            }
        }

        Ok(records)
    }

    pub async fn count_by_workspace(&self, workspace_id: &str) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) as count FROM documents WHERE workspace_id = ? AND trashed_at IS NULL",
        )
        .bind(workspace_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    pub async fn list_share_tokens_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<UserShareTokenRecord>> {
        let rows = sqlx::query(
            "SELECT l.workspace_id, l.doc_id, l.token, l.created_at
             FROM doc_public_links l
             JOIN workspace_members wm
               ON wm.workspace_id = l.workspace_id
             WHERE wm.user_id = ?
             ORDER BY l.created_at DESC, l.workspace_id ASC, l.doc_id ASC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| UserShareTokenRecord {
                workspace_id: row.get("workspace_id"),
                doc_id: row.get("doc_id"),
                token: row.get("token"),
                created_at: row.get::<i64, _>("created_at"),
            })
            .collect())
    }

    pub async fn publish_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        mode: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let now = Self::now_millis();
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE documents
             SET public = 1, mode = ?, updated_at = ?
             WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
        )
        .bind(mode)
        .bind(now)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(None);
        }

        sqlx::query(
            "INSERT INTO doc_public_links (workspace_id, doc_id, token, created_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(workspace_id, doc_id) DO NOTHING",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(Self::generate_share_token())
        .bind(now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        self.find_metadata(workspace_id, doc_id).await
    }

    pub async fn unpublish_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let now = Self::now_millis();
        let result = sqlx::query(
            "UPDATE documents
             SET public = 0, updated_at = ?
             WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
        )
        .bind(now)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        self.find_metadata(workspace_id, doc_id).await
    }

    pub async fn update_default_role(
        &self,
        workspace_id: &str,
        doc_id: &str,
        role: &str,
    ) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE documents SET default_role = ? WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
        )
        .bind(role)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn duplicate_doc(
        &self,
        source_workspace_id: &str,
        source_doc_id: &str,
        target_workspace_id: &str,
        new_doc_id: &str,
        creator_id: &str,
        title_override: Option<&str>,
    ) -> Result<Option<DocumentMetadata>> {
        let mut tx = self.pool.begin().await?;

        let source_row = sqlx::query(
            "SELECT snapshot, default_role, mode, title, summary FROM documents WHERE workspace_id = ? AND id = ?",
        )
        .bind(source_workspace_id)
        .bind(source_doc_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = source_row else {
            tx.rollback().await?;
            return Ok(None);
        };

        let snapshot: Vec<u8> = row.try_get("snapshot")?;
        let default_role: String = row.try_get("default_role")?;
        let mode: String = row.try_get("mode")?;
        let original_title: Option<String> = row.try_get("title").unwrap_or(None);
        let summary: Option<String> = row.try_get("summary").unwrap_or(None);

        let sanitized_title = title_override
            .and_then(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .or_else(|| original_title.clone());

        let now = Self::now_millis();
        sqlx::query(
            "INSERT INTO documents (
                 id,
                 workspace_id,
                 snapshot,
                 created_at,
             updated_at,
             default_role,
             public,
             blocked,
             mode,
                 title,
                 summary,
                 creator_id,
                 updater_id,
                 trashed_at,
             trashed_by
         ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, NULL, NULL)",
        )
        .bind(new_doc_id)
        .bind(target_workspace_id)
        .bind(&snapshot)
        .bind(now)
        .bind(now)
        .bind(&default_role)
        .bind(&mode)
        .bind(sanitized_title.as_deref())
        .bind(summary.as_deref())
        .bind(creator_id)
        .bind(creator_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        let metadata = self.find_metadata(target_workspace_id, new_doc_id).await?;

        Ok(metadata)
    }

    pub async fn ensure_workspace_defaults(
        &self,
        workspace_id: &str,
        owner_id: &str,
        workspace_name: Option<&str>,
    ) -> Result<Vec<DocumentMetadata>> {
        let mut ensured = Vec::with_capacity(1 + WORKSPACE_DB_DOC_SUFFIXES.len());
        ensured.push(
            self.ensure_doc_record(workspace_id, workspace_id, owner_id, workspace_name)
                .await?,
        );

        for suffix in WORKSPACE_DB_DOC_SUFFIXES {
            let doc_id = format!("db${workspace_id}${suffix}");
            ensured.push(
                self.ensure_doc_record(workspace_id, &doc_id, owner_id, None)
                    .await?,
            );
        }

        Ok(ensured)
    }

    pub async fn ensure_system_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        owner_id: &str,
        workspace_name: Option<&str>,
    ) -> Result<Option<DocumentMetadata>> {
        match classify_system_doc(workspace_id, doc_id) {
            Some(SystemDocKind::WorkspaceRoot) => self
                .ensure_doc_record(workspace_id, doc_id, owner_id, workspace_name)
                .await
                .map(Some),
            Some(SystemDocKind::WorkspaceDb { .. }) => self
                .ensure_doc_record(workspace_id, doc_id, owner_id, None)
                .await
                .map(Some),
            Some(SystemDocKind::Userdata { .. }) => self
                .ensure_doc_record(workspace_id, doc_id, owner_id, None)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    pub async fn trash_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        trashed_by: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let now = Self::now_millis();
        let result = sqlx::query(
            "UPDATE documents
             SET trashed_at = ?, trashed_by = ?, updated_at = ?, updater_id = ?
             WHERE workspace_id = ? AND id = ? AND trashed_at IS NULL",
        )
        .bind(now)
        .bind(trashed_by)
        .bind(now)
        .bind(trashed_by)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        self.find_metadata(workspace_id, doc_id).await
    }

    pub async fn restore_doc(
        &self,
        workspace_id: &str,
        doc_id: &str,
        restored_by: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let now = Self::now_millis();
        let result = sqlx::query(
            "UPDATE documents
             SET trashed_at = NULL, trashed_by = NULL, updated_at = ?, updater_id = ?
             WHERE workspace_id = ? AND id = ? AND trashed_at IS NOT NULL",
        )
        .bind(now)
        .bind(restored_by)
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        self.find_metadata(workspace_id, doc_id).await
    }

    pub async fn delete_doc(&self, workspace_id: &str, doc_id: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query("DELETE FROM documents WHERE workspace_id = ? AND id = ?")
            .bind(workspace_id)
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM doc_updates WHERE space_type = ? AND space_id = ? AND doc_id = ?")
            .bind(SPACE_TYPE_WORKSPACE)
            .bind(workspace_id)
            .bind(doc_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn restore_doc_history(
        &self,
        workspace_id: &str,
        doc_id: &str,
        history_id: i64,
        updater_id: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let mut tx = self.pool.begin().await?;

        let history_row = sqlx::query(
            "SELECT snapshot FROM document_history WHERE id = ? AND workspace_id = ? AND doc_id = ?",
        )
        .bind(history_id)
        .bind(workspace_id)
        .bind(doc_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(history_row) = history_row else {
            tx.rollback().await?;
            return Ok(None);
        };

        let target_snapshot: Vec<u8> = history_row.try_get("snapshot")?;

        let doc_row =
            sqlx::query("SELECT snapshot FROM documents WHERE workspace_id = ? AND id = ?")
                .bind(workspace_id)
                .bind(doc_id)
                .fetch_optional(&mut *tx)
                .await?;

        let Some(doc_row) = doc_row else {
            tx.rollback().await?;
            return Ok(None);
        };

        let previous_snapshot: Vec<u8> = doc_row.try_get("snapshot")?;
        let now = Utc::now().timestamp();

        sqlx::query(
            "INSERT INTO document_history (doc_id, workspace_id, snapshot, created_at)
             VALUES (?, ?, ?, ?)",
        )
        .bind(doc_id)
        .bind(workspace_id)
        .bind(previous_snapshot)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        let mut parsed = extract_doc_meta(&target_snapshot, doc_id);
        if parsed.updater_id.is_none() {
            parsed.updater_id = Some(updater_id.to_string());
        }

        let updated_at = now;

        sqlx::query(
            "UPDATE documents SET snapshot = ?, updated_at = ?, title = ?, summary = ?,
             updater_id = COALESCE(?, updater_id), creator_id = COALESCE(?, creator_id)
             WHERE workspace_id = ? AND id = ?",
        )
        .bind(&target_snapshot)
        .bind(updated_at)
        .bind(parsed.title.as_deref())
        .bind(parsed.summary.as_deref())
        .bind(parsed.updater_id.as_deref())
        .bind(parsed.creator_id.as_deref())
        .bind(workspace_id)
        .bind(doc_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        let metadata = self.find_metadata(workspace_id, doc_id).await?;
        Ok(metadata.map(|mut value| {
            value.snapshot = Some(target_snapshot);
            value
        }))
    }

    pub async fn restore_doc_at_timestamp(
        &self,
        workspace_id: &str,
        doc_id: &str,
        timestamp: i64,
        updater_id: &str,
    ) -> Result<Option<DocumentMetadata>> {
        let history_id: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM document_history
             WHERE workspace_id = ? AND doc_id = ? AND created_at <= ?
             ORDER BY created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(workspace_id)
        .bind(doc_id)
        .bind(timestamp)
        .fetch_optional(&self.pool)
        .await?;

        let Some(history_id) = history_id else {
            return Ok(None);
        };

        self.restore_doc_history(workspace_id, doc_id, history_id, updater_id)
            .await
    }

    pub async fn is_public(&self, workspace_id: &str, doc_id: &str) -> Result<bool> {
        let value: Option<i64> =
            sqlx::query_scalar("SELECT public FROM documents WHERE workspace_id = ? AND id = ?")
                .bind(workspace_id)
                .bind(doc_id)
                .fetch_optional(&self.pool)
                .await?;

        Ok(value.unwrap_or(0) != 0)
    }

    pub async fn list_public(&self, workspace_id: &str) -> Result<Vec<DocumentMetadata>> {
        let rows = sqlx::query(
            "SELECT
                 d.id,
                 d.workspace_id,
                 d.created_at,
                 d.updated_at,
                 d.default_role,
                 d.public,
                 d.blocked,
                 d.mode,
                 d.title,
                 d.summary,
                 d.creator_id,
                 d.updater_id,
                                 d.trashed_at,
                                 d.trashed_by,
                                 l.token AS share_token
             FROM documents d
             LEFT JOIN doc_public_links l
               ON l.workspace_id = d.workspace_id AND l.doc_id = d.id
                         WHERE d.workspace_id = ? AND d.public = 1 AND d.trashed_at IS NULL
             ORDER BY d.created_at ASC, d.id ASC",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;

        let mut records: Vec<_> = rows.into_iter().map(Self::metadata_from_row).collect();
        for record in &mut records {
            if Self::metadata_needs_backfill(record) {
                *record = self.hydrate_metadata(record.clone()).await?;
            }
        }

        Ok(records)
    }

    pub async fn docs_requiring_compaction(
        &self,
        threshold: i64,
        limit: i64,
    ) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT space_id, doc_id
             FROM doc_updates
             WHERE space_type = ?
             GROUP BY space_id, doc_id
             HAVING COUNT(*) > ?
             LIMIT ?",
        )
        .bind(SPACE_TYPE_WORKSPACE)
        .bind(threshold)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut docs = Vec::with_capacity(rows.len());
        for row in rows {
            docs.push((row.try_get("space_id")?, row.try_get("doc_id")?));
        }
        Ok(docs)
    }
}

impl DocumentStore {
    pub async fn fetch_doc_logs(
        &self,
        workspace_id: &str,
        doc_id: &str,
    ) -> Result<Vec<DocUpdateRecord>> {
        fetch_doc_logs(
            self.pool.clone(),
            SPACE_TYPE_WORKSPACE,
            workspace_id,
            doc_id,
        )
        .await
    }
}

#[derive(Default, Clone)]
pub(crate) struct ParsedDocMeta {
    pub(crate) title: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) created_at: Option<i64>,
    pub(crate) updated_at: Option<i64>,
    pub(crate) creator_id: Option<String>,
    pub(crate) updater_id: Option<String>,
}

pub(crate) fn extract_doc_meta(snapshot: &[u8], doc_id: &str) -> ParsedDocMeta {
    use y_octo::Doc;

    let doc = match Doc::try_from_binary_v1(snapshot) {
        Ok(doc) => doc,
        Err(_) => return ParsedDocMeta::default(),
    };

    let map = match doc.get_map("meta") {
        Ok(map) => map,
        Err(_) => return ParsedDocMeta::default(),
    };

    let Some(pages_value) = map.get("pages") else {
        return ParsedDocMeta::default();
    };

    match serde_json::to_value(&pages_value) {
        Ok(json) => parse_meta_from_json(json, doc_id),
        Err(_) => ParsedDocMeta::default(),
    }
}

fn number_to_millis(value: &JsonValue) -> Option<i64> {
    if let Some(int) = value.as_i64() {
        return Some(int);
    }

    value.as_f64().map(|float| float.round() as i64)
}

fn parse_meta_from_json(json: JsonValue, doc_id: &str) -> ParsedDocMeta {
    let mut meta = ParsedDocMeta::default();

    let JsonValue::Array(entries) = json else {
        return meta;
    };

    for entry in entries {
        let JsonValue::Object(obj) = entry else {
            continue;
        };

        let Some(id) = obj.get("id").and_then(JsonValue::as_str) else {
            continue;
        };

        if id != doc_id {
            continue;
        }

        meta.title = obj
            .get("title")
            .and_then(JsonValue::as_str)
            .map(|s| s.to_owned());
        meta.summary = obj
            .get("summary")
            .and_then(JsonValue::as_str)
            .map(|s| s.to_owned());
        meta.created_at = obj.get("createDate").and_then(number_to_millis);
        meta.updated_at = obj.get("updatedDate").and_then(number_to_millis);
        meta.creator_id = obj
            .get("creator")
            .and_then(JsonValue::as_str)
            .map(|s| s.to_owned());
        meta.updater_id = obj
            .get("lastUpdatedBy")
            .and_then(JsonValue::as_str)
            .map(|s| s.to_owned());
        break;
    }

    meta
}

pub(crate) struct SnapshotComputation {
    pub snapshot: Vec<u8>,
    pub meta: ParsedDocMeta,
    pub created_at: i64,
    pub updated_at: i64,
}

pub(crate) fn build_snapshot_from_updates<F>(
    doc_id: &str,
    existing_snapshot: Option<&[u8]>,
    updates: &[Vec<u8>],
    now_fn: F,
) -> Result<SnapshotComputation>
where
    F: FnOnce() -> i64,
{
    let snapshot = DocEngine::apply_updates_to_snapshot(existing_snapshot, updates)?;
    let meta = extract_doc_meta(&snapshot, doc_id);
    let fallback_timestamp = now_fn();
    let updated_at = meta.updated_at.unwrap_or(fallback_timestamp);
    let created_at = meta.created_at.unwrap_or(updated_at);

    Ok(SnapshotComputation {
        snapshot,
        meta,
        created_at,
        updated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_doc_meta_reads_title_and_timestamps() {
        let json = serde_json::json!([
            {
                "id": "doc-1",
                "title": "Hello",
                "summary": "Summary",
                "createDate": 1_234_000,
                "updatedDate": 5_678_000,
                "creator": "user-1",
                "lastUpdatedBy": "user-2"
            },
            {
                "id": "other",
                "title": "Other",
                "createDate": 9_000_000,
                "updatedDate": 9_000_000
            }
        ]);

        let parsed = parse_meta_from_json(json, "doc-1");

        assert_eq!(parsed.title.as_deref(), Some("Hello"));
        assert_eq!(parsed.summary.as_deref(), Some("Summary"));
        assert_eq!(parsed.created_at, Some(1234000));
        assert_eq!(parsed.updated_at, Some(5678000));
        assert_eq!(parsed.creator_id.as_deref(), Some("user-1"));
        assert_eq!(parsed.updater_id.as_deref(), Some("user-2"));
    }

    #[test]
    fn parse_meta_from_json_gracefully_skips_missing_doc() {
        let json = serde_json::json!([
            {
                "id": "doc-2",
                "title": "Other",
                "createDate": 9_000_000,
                "updatedDate": 9_000_000
            }
        ]);

        let parsed = parse_meta_from_json(json, "doc-1");

        assert!(parsed.title.is_none());
        assert!(parsed.summary.is_none());
        assert!(parsed.created_at.is_none());
        assert!(parsed.updated_at.is_none());
        assert!(parsed.creator_id.is_none());
        assert!(parsed.updater_id.is_none());
    }

    #[test]
    fn parse_meta_from_json_allows_missing_fields() {
        let json = serde_json::json!([
            {
                "id": "doc-1",
                "title": "Hello"
            }
        ]);

        let parsed = parse_meta_from_json(json, "doc-1");

        assert_eq!(parsed.title.as_deref(), Some("Hello"));
        assert!(parsed.summary.is_none());
        assert!(parsed.created_at.is_none());
        assert!(parsed.updated_at.is_none());
        assert!(parsed.creator_id.is_none());
        assert!(parsed.updater_id.is_none());
    }

    #[test]
    fn merge_parsed_metadata_fills_missing_fields() {
        let mut metadata = DocumentMetadata {
            id: "doc-1".to_string(),
            workspace_id: "ws-1".to_string(),
            created_at: 0,
            updated_at: 0,
            default_role: "manager".to_string(),
            public: false,
            blocked: false,
            mode: "page".to_string(),
            title: None,
            summary: None,
            creator_id: None,
            updater_id: None,
            share_token: None,
            trashed_at: None,
            trashed_by: None,
            snapshot: None,
        };

        let parsed = ParsedDocMeta {
            title: Some("Title".to_string()),
            summary: Some("Summary".to_string()),
            created_at: Some(1_000),
            updated_at: Some(2_000),
            creator_id: Some("user-1".to_string()),
            updater_id: Some("user-2".to_string()),
        };

        DocumentStore::merge_parsed_metadata(&mut metadata, parsed);

        assert_eq!(metadata.title.as_deref(), Some("Title"));
        assert_eq!(metadata.summary.as_deref(), Some("Summary"));
        assert_eq!(metadata.creator_id.as_deref(), Some("user-1"));
        assert_eq!(metadata.updater_id.as_deref(), Some("user-2"));
        assert_eq!(metadata.created_at, 1_000);
        assert_eq!(metadata.updated_at, 2_000);
    }

    #[test]
    fn merge_parsed_metadata_preserves_existing_values() {
        let mut metadata = DocumentMetadata {
            id: "doc-2".to_string(),
            workspace_id: "ws-2".to_string(),
            created_at: 5_000,
            updated_at: 6_000,
            default_role: "manager".to_string(),
            public: true,
            blocked: false,
            mode: "page".to_string(),
            title: Some("Existing".to_string()),
            summary: Some("Present".to_string()),
            creator_id: Some("author".to_string()),
            updater_id: Some("editor".to_string()),
            share_token: Some("token".to_string()),
            trashed_at: None,
            trashed_by: None,
            snapshot: None,
        };

        let parsed = ParsedDocMeta {
            title: Some("New Title".to_string()),
            summary: Some("New Summary".to_string()),
            created_at: Some(4_000),
            updated_at: Some(7_000),
            creator_id: Some("other-author".to_string()),
            updater_id: Some("other-editor".to_string()),
        };

        DocumentStore::merge_parsed_metadata(&mut metadata, parsed);

        assert_eq!(metadata.title.as_deref(), Some("Existing"));
        assert_eq!(metadata.summary.as_deref(), Some("Present"));
        assert_eq!(metadata.creator_id.as_deref(), Some("author"));
        assert_eq!(metadata.updater_id.as_deref(), Some("editor"));
        assert_eq!(metadata.created_at, 4_000);
        assert_eq!(metadata.updated_at, 7_000);
        assert_eq!(metadata.share_token.as_deref(), Some("token"));
    }
}
