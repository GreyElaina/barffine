#![allow(dead_code)]

use std::{fs, path::Path};

use barffine_core::{config::AppConfig, db::Database};
use chrono::Utc;
use tempfile::TempDir;

use crate::{
    auth::generate_password_hash,
    state::{AppState, build_state},
    utils::db::run_migrations,
};

pub(crate) async fn setup_state() -> (TempDir, Database, AppState) {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut config = AppConfig::default();
    let db_path = temp_dir.path().join("test.db");
    config.database_path = db_path.to_string_lossy().into_owned();

    let database = Database::connect(&config).await.expect("connect database");
    run_migrations(database.pool())
        .await
        .expect("apply migrations");

    let state = build_state(&database);
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
    let now = Utc::now().timestamp();
    sqlx::query(
            "INSERT INTO documents (id, workspace_id, snapshot, created_at, updated_at, default_role, public, blocked, mode, title, summary, creator_id, updater_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)",
        )
        .bind(doc_id)
        .bind(workspace_id)
        .bind(Vec::<u8>::new())
        .bind(now)
        .bind(now)
        .bind("manager")
        .bind(if public { 1_i64 } else { 0_i64 })
        .bind(mode)
        .bind::<Option<String>>(None)
        .bind::<Option<String>>(None)
        .bind::<Option<String>>(None)
        .bind::<Option<String>>(None)
        .execute(database.pool())
        .await
        .expect("insert document");
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
    sqlx::query(
        "UPDATE documents SET snapshot = ?, title = NULL, summary = NULL WHERE workspace_id = ? AND id = ?",
    )
    .bind(snapshot)
    .bind(workspace_id)
    .bind(doc_id)
    .execute(database.pool())
    .await
    .expect("persist snapshot");
}
