#![allow(dead_code)]

use std::{fs, path::Path, str::FromStr};

use crate::{
    auth::generate_password_hash,
    state::{AppState, build_state},
};
use anyhow::{Context, Result};
use barffine_core::{
    config::{AppConfig, BlobStoreBackend, DatabaseBackend, DocDataBackend, DocStoreBackend},
    db::Database,
};
use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;

pub(crate) async fn setup_state() -> (TempDir, Database, AppState) {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut config = AppConfig::default();
    let db_path = temp_dir.path().join("test.db");
    config.database_path = db_path.to_string_lossy().into_owned();
    configure_test_database_backend(&mut config).await;
    config.doc_data_backend = DocDataBackend::RocksDb;
    config.doc_data_path = temp_dir
        .path()
        .join("doc-kv")
        .to_string_lossy()
        .into_owned();
    config.blob_store_backend = BlobStoreBackend::Rocks;
    config.blob_store_path = temp_dir
        .path()
        .join("blob-store")
        .to_string_lossy()
        .into_owned();

    let database = Database::connect(&config).await.expect("connect database");

    let state = build_state(&database, &config);
    state
        .workspace_store
        .normalize_member_statuses()
        .await
        .expect("normalize member statuses");

    (temp_dir, database, state)
}

/// Variant of `setup_state` that uses RocksDb as the primary doc store
/// backend. This exercises the Rocks-backed `DocRepository` and related
/// services (history, roles, public links) in server-level tests.
pub(crate) async fn setup_state_with_rocks_doc_store() -> (TempDir, Database, AppState) {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut config = AppConfig::default();
    let db_path = temp_dir.path().join("test.db");
    config.database_path = db_path.to_string_lossy().into_owned();
    configure_test_database_backend(&mut config).await;
    config.doc_data_backend = DocDataBackend::RocksDb;
    config.doc_data_path = temp_dir
        .path()
        .join("doc-kv")
        .to_string_lossy()
        .into_owned();
    config.doc_store_backend = DocStoreBackend::RocksDb;
    config.blob_store_backend = BlobStoreBackend::Rocks;
    config.blob_store_path = temp_dir
        .path()
        .join("blob-store")
        .to_string_lossy()
        .into_owned();

    let database = Database::connect(&config).await.expect("connect database");
    let state = build_state(&database, &config);
    state
        .workspace_store
        .normalize_member_statuses()
        .await
        .expect("normalize member statuses");

    (temp_dir, database, state)
}

pub(crate) async fn seed_workspace(state: &AppState) -> (String, String) {
    let password_hash = generate_password_hash("password").expect("hash password");
    let user = state
        .user_store
        .create("tester@example.com", &password_hash, None)
        .await
        .expect("create user");
    let workspace = state
        .workspace_store
        .create(&user.id, Some("Test Workspace"), None, None, None, None)
        .await
        .expect("create workspace");
    (workspace.id, user.id)
}

pub(crate) async fn insert_document(
    database: &Database,
    workspace_id: &str,
    doc_id: &str,
    public: bool,
    mode: &str,
) {
    use barffine_core::doc_store::DocumentStore;

    let doc_store = DocumentStore::new(database);
    let workspace_repo = database.repositories().workspace_repo();
    let workspace = workspace_repo
        .fetch_workspace(workspace_id)
        .await
        .expect("fetch workspace")
        .unwrap_or_else(|| panic!("workspace {workspace_id} not found"));
    doc_store
        .ensure_doc_record(workspace_id, doc_id, &workspace.owner_id, None)
        .await
        .expect("ensure doc");

    let doc_repo = database.repositories().doc_repo();
    if mode != "page" {
        doc_repo
            .update_doc_mode(workspace_id, doc_id, mode)
            .await
            .expect("update doc mode");
    }

    if public {
        let timestamp = Utc::now().timestamp_millis();
        let token = format!("test-token-{doc_id}");
        doc_repo
            .publish_doc_entry(workspace_id, doc_id, mode, timestamp, &token)
            .await
            .expect("publish doc");
    }
}

pub(crate) fn fixture_snapshot(name: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../AFFiNE/packages/backend/server/src/__tests__/__fixtures__")
        .join(name);
    fs::read(path).expect("load fixture snapshot")
}

pub(crate) async fn persist_snapshot(
    database: &Database,
    workspace_id: &str,
    doc_id: &str,
    snapshot: Vec<u8>,
) {
    use barffine_core::db::doc_repo::ReplaceDocSnapshotParams;

    let doc_repo = database.repositories().doc_repo();
    doc_repo
        .replace_doc_snapshot(ReplaceDocSnapshotParams {
            workspace_id: workspace_id.to_owned(),
            doc_id: doc_id.to_owned(),
            snapshot,
            updated_at: Utc::now().timestamp_millis(),
            title: None,
            summary: None,
            creator_id: None,
            updater_id: None,
            history_entry: None,
        })
        .await
        .expect("persist snapshot");
}

async fn configure_test_database_backend(config: &mut AppConfig) {
    if let Some(requested) = requested_test_backend() {
        if configure_backend_choice(config, requested).await {
            return;
        }
        eprintln!(
            "warning: BARFFINE_TEST_BACKEND={requested:?} could not be provisioned; falling back to defaults"
        );
    }

    if configure_backend_choice(config, DatabaseBackend::Sqlite).await {
        return;
    }

    set_sqlite_backend(config);
}

fn requested_test_backend() -> Option<DatabaseBackend> {
    let value = std::env::var("BARFFINE_TEST_BACKEND").ok()?;
    match DatabaseBackend::from_str(&value) {
        Ok(backend) => Some(backend),
        Err(err) => {
            eprintln!("warning: invalid BARFFINE_TEST_BACKEND='{value}': {err}; ignoring override");
            None
        }
    }
}

async fn configure_backend_choice(config: &mut AppConfig, backend: DatabaseBackend) -> bool {
    match backend {
        DatabaseBackend::Sqlite => {
            set_sqlite_backend(config);
            true
        }
        DatabaseBackend::Postgres => configure_postgres_backend(config).await,
    }
}

async fn configure_postgres_backend(config: &mut AppConfig) -> bool {
    let Some(base_url) = test_postgres_base_url() else {
        return false;
    };

    match provision_postgres_database(&base_url).await {
        Ok(connection_url) => {
            config.database_backend = DatabaseBackend::Postgres;
            config.database_url = Some(connection_url);
            true
        }
        Err(err) => {
            eprintln!(
                "warning: failed to prepare postgres test database: {err:?}\nfalling back to sqlite"
            );
            false
        }
    }
}

fn set_sqlite_backend(config: &mut AppConfig) {
    config.database_backend = DatabaseBackend::Sqlite;
    config.database_url = None;
}

fn test_postgres_base_url() -> Option<String> {
    if std::env::var("BARFFINE_TEST_DISABLE_POSTGRES").is_ok() {
        return None;
    }

    if let Ok(url) = std::env::var("BARFFINE_TEST_PG_URL") {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_owned());
        }
    }

    Some("postgres://barffine:barffine@127.0.0.1:5433/barffine".to_owned())
}

async fn provision_postgres_database(base_url: &str) -> Result<String> {
    let mut parsed =
        Url::parse(base_url).with_context(|| format!("invalid postgres url '{base_url}'"))?;

    let base_db = parsed.path().trim_start_matches('/').to_string();
    if base_db.is_empty() {
        anyhow::bail!("postgres url must include a database name");
    }

    let db_name = format!("barffine_test_{}", Uuid::new_v4().simple());
    let admin_db = std::env::var("BARFFINE_TEST_PG_ADMIN_DB").unwrap_or_else(|_| "postgres".into());
    let mut admin_url = parsed.clone();
    admin_url.set_path(&format!("/{admin_db}"));

    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url.as_str())
        .await
        .with_context(|| format!("failed to connect to postgres admin url {}", admin_url))?;

    let drop_sql = format!("DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)");
    if let Err(err) = sqlx::query(&drop_sql).execute(&admin_pool).await {
        // Retry without FORCE for servers that do not support it.
        let fallback_sql = format!("DROP DATABASE IF EXISTS \"{db_name}\"");
        sqlx::query(&fallback_sql)
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to drop postgres database '{db_name}': {err}"))?;
    }

    let mut create_sql =
        format!("CREATE DATABASE \"{db_name}\" TEMPLATE template0 ENCODING 'UTF8'");
    let owner = parsed.username();
    if !owner.is_empty() {
        create_sql.push_str(&format!(" OWNER \"{owner}\""));
    }

    sqlx::query(&create_sql)
        .execute(&admin_pool)
        .await
        .with_context(|| format!("failed to create postgres database '{db_name}'"))?;

    admin_pool.close().await;

    parsed.set_path(&format!("/{db_name}"));
    Ok(parsed.to_string())
}
