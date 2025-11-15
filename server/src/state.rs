use std::collections::{HashMap, HashSet};
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::Result as AnyResult;
use crossbeam_channel::unbounded;
use dashmap::{DashMap, mapref::entry::Entry};
use futures_util::stream::{self, StreamExt};
use once_cell::sync::OnceCell;
use rayon::prelude::*;
use serde::Serialize;
use socketioxide::SocketIo;
use tokio::{
    spawn,
    sync::broadcast,
    task::spawn_blocking,
    time::{Duration, sleep},
};
use tracing::{info, warn};

use barffine_core::{
    access_token::AccessTokenStore,
    blob::BlobStorage,
    comment_attachment::CommentAttachmentStore,
    comment_store::RepositoryCommentStore,
    config::{AppConfig, BlobStoreBackend, DatabaseBackend, DocStoreBackend},
    db::Database,
    db::{
        postgres::notification_center::PostgresNotificationCenter,
        rocks::{blob_store::RocksBlobStorage, notification_center::RocksNotificationCenter},
        sql::blob_store::SqlBlobStorage,
        sqlite::notification_center::SqliteNotificationCenter,
    },
    doc_data::DocDataBackend,
    doc_roles::DocumentRoleStore,
    doc_store::{DOC_UPDATE_LOG_LIMIT, DocumentStore},
    feature::{DeterministicFeatureStore, FeatureFlag, FeatureNamespace, FeatureSnapshot},
    notification::{CommentStore as CommentStoreTrait, NotificationCenter},
    user::UserStore,
    user_doc_store::UserDocStore,
    user_settings::UserSettingsStore,
    workspace::WorkspaceStore,
    workspace_feature_store::WorkspaceFeatureStore,
};

use crate::oauth::OAuthService;
use crate::{
    crypto::DocTokenSigner,
    doc::{
        cache::{DocCache, DocCacheBuilder, DocCacheConfig},
        service::DocAccessService,
    },
    feature_service::FeatureService,
    graphql::copilot::CopilotSessionRecord,
    socket::{
        rooms::{RoomKind, SpaceType, space_room_name},
        types::SocketRequestDeduper,
    },
    types::SessionUser,
    user::service::UserService,
    workspace::service::WorkspaceService,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Value as JsonValue, json};

#[derive(Clone, Debug)]
pub struct WorkspaceEmbeddingEvent {
    pub workspace_id: String,
    pub enable_doc_embedding: Option<bool>,
}

#[derive(Clone)]
pub struct AppState {
    pub user_store: UserStore,
    pub user_settings: UserSettingsStore,
    pub workspace_store: WorkspaceStore,
    pub document_store: DocumentStore,
    pub user_doc_store: UserDocStore,
    pub doc_role_store: DocumentRoleStore,
    pub access_token_store: AccessTokenStore,
    pub comment_store: Arc<dyn CommentStoreTrait>,
    pub comment_attachment_store: CommentAttachmentStore,
    pub notification_center: Arc<dyn NotificationCenter>,
    pub sync_hub: SyncHub,
    pub doc_sessions: Arc<DocSessionManager>,
    pub doc_cache: Arc<DocCache>,
    pub metadata: ServerMetadata,
    pub feature_service: FeatureService,
    pub user_service: Arc<UserService>,
    pub workspace_service: Arc<WorkspaceService>,
    pub doc_access_service: Arc<DocAccessService>,
    pub blob_store: Arc<dyn BlobStorage>,
    pub workspace_embedding_files:
        Arc<DashMap<String, HashMap<String, WorkspaceEmbeddingFileRecord>>>,
    pub workspace_embedding_ignored_docs: Arc<DashMap<String, HashSet<String>>>,
    pub copilot_sessions: Arc<DashMap<String, CopilotSessionRecord>>,
    pub doc_token_signer: Arc<DocTokenSigner>,
    pub server_path: Option<String>,
    pub base_url: String,
    pub socket_io: Arc<OnceCell<Arc<SocketIo>>>,
    pub socket_metrics: Arc<SocketMetrics>,
    pub workspace_embedding_events: broadcast::Sender<WorkspaceEmbeddingEvent>,
    pub oauth: Arc<OAuthService>,
    pub request_deduper: Arc<SocketRequestDeduper>,
    pub socket_runtime: Arc<SocketRuntimeState>,
}

#[derive(Clone, Default)]
pub struct StateBuildConfig {
    pub doc_cache: Option<DocCacheConfig>,
}

#[derive(Clone)]
pub(crate) struct WorkspaceEmbeddingFileRecord {
    pub(crate) file_name: String,
    pub(crate) blob_id: String,
}

#[derive(Clone)]
pub(crate) struct SyncHub {
    channels: Arc<DashMap<String, broadcast::Sender<SyncEvent>>>,
    socket_io: Arc<OnceCell<Arc<SocketIo>>>,
    socket_metrics: Arc<SocketMetrics>,
}

#[derive(Clone)]
pub struct SocketRuntimeState {
    pub doc_cache: Arc<DocCache>,
    pub doc_sessions: Arc<DocSessionManager>,
    pub sync_hub: SyncHub,
    pub request_deduper: Arc<SocketRequestDeduper>,
    pub socket_metrics: Arc<SocketMetrics>,
    pub workspace_embedding_files:
        Arc<DashMap<String, HashMap<String, WorkspaceEmbeddingFileRecord>>>,
    pub workspace_embedding_ignored_docs: Arc<DashMap<String, HashSet<String>>>,
    pub workspace_embedding_events: broadcast::Sender<WorkspaceEmbeddingEvent>,
    pub user_service: Arc<UserService>,
    pub workspace_service: Arc<WorkspaceService>,
    pub doc_access_service: Arc<DocAccessService>,
    pub workspace_store: WorkspaceStore,
    pub document_store: DocumentStore,
    pub user_doc_store: UserDocStore,
}

impl SocketRuntimeState {
    pub fn new(
        doc_cache: Arc<DocCache>,
        doc_sessions: Arc<DocSessionManager>,
        sync_hub: SyncHub,
        request_deduper: Arc<SocketRequestDeduper>,
        socket_metrics: Arc<SocketMetrics>,
        workspace_embedding_files: Arc<
            DashMap<String, HashMap<String, WorkspaceEmbeddingFileRecord>>,
        >,
        workspace_embedding_ignored_docs: Arc<DashMap<String, HashSet<String>>>,
        workspace_embedding_events: broadcast::Sender<WorkspaceEmbeddingEvent>,
        user_service: Arc<UserService>,
        workspace_service: Arc<WorkspaceService>,
        doc_access_service: Arc<DocAccessService>,
        workspace_store: WorkspaceStore,
        document_store: DocumentStore,
        user_doc_store: UserDocStore,
    ) -> Self {
        Self {
            doc_cache,
            doc_sessions,
            sync_hub,
            request_deduper,
            socket_metrics,
            workspace_embedding_files,
            workspace_embedding_ignored_docs,
            workspace_embedding_events,
            user_service,
            workspace_service,
            doc_access_service,
            workspace_store,
            document_store,
            user_doc_store,
        }
    }

    pub fn emit_workspace_embedding(&self, workspace_id: &str, enable_doc_embedding: Option<bool>) {
        let event = WorkspaceEmbeddingEvent {
            workspace_id: workspace_id.to_string(),
            enable_doc_embedding,
        };

        if let Err(err) = self.workspace_embedding_events.send(event) {
            warn!(
                %workspace_id,
                ?err,
                "failed to emit workspace embedding event"
            );
        }
    }

    pub fn subscribe_workspace_embedding(&self) -> broadcast::Receiver<WorkspaceEmbeddingEvent> {
        self.workspace_embedding_events.subscribe()
    }
}

impl SyncHub {
    pub(crate) fn new(
        socket_io: Arc<OnceCell<Arc<SocketIo>>>,
        socket_metrics: Arc<SocketMetrics>,
    ) -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            socket_io,
            socket_metrics,
        }
    }

    pub(crate) fn subscribe(&self, key: &str) -> broadcast::Receiver<SyncEvent> {
        let sender = self.ensure_sender(key);
        sender.subscribe()
    }

    pub(crate) fn publish_snapshot(&self, key: &str, snapshot: Vec<u8>) {
        let sender = self.ensure_sender(key);
        let event = SyncEvent::snapshot(snapshot);
        let _ = sender.send(event);
    }

    pub(crate) fn publish_updates(
        &self,
        key: &str,
        updates: &[Vec<u8>],
        meta: Option<SocketBroadcastMeta>,
    ) {
        let sender = self.ensure_sender(key);
        for update in updates {
            let _ = sender.send(SyncEvent::update(update.clone()));
        }

        if !updates.is_empty() {
            self.broadcast_socket_updates(key, updates, meta.as_ref());
        }
    }

    fn ensure_sender(&self, key: &str) -> broadcast::Sender<SyncEvent> {
        self.channels
            .entry(key.to_string())
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(128);
                tx
            })
            .clone()
    }

    pub(crate) fn remove_channel(&self, key: &str) {
        self.channels.remove(key);
    }

    fn broadcast_socket_updates(
        &self,
        key: &str,
        updates: &[Vec<u8>],
        meta: Option<&SocketBroadcastMeta>,
    ) {
        let Some(io) = self.socket_io.get() else {
            return;
        };

        let (space_type, space_id, doc_id, editor_id, editor_user, timestamp) =
            if let Some(meta) = meta {
                (
                    meta.space_type,
                    meta.space_id.clone(),
                    meta.doc_id.clone(),
                    meta.editor_id.clone(),
                    meta.editor_user.clone(),
                    meta.timestamp,
                )
            } else if let Some((space_id, doc_id)) = parse_doc_channel(key) {
                (SpaceType::Workspace, space_id, doc_id, None, None, None)
            } else {
                return;
            };

        let base_updates: Vec<String> = updates.iter().map(|bytes| BASE64.encode(bytes)).collect();
        if base_updates.is_empty() {
            return;
        }

        self.socket_metrics
            .broadcast_updates
            .fetch_add(1, Ordering::Relaxed);

        let mut payload = json!({
            "spaceType": space_type,
            "spaceId": space_id,
            "docId": doc_id,
            "update": base_updates.last().cloned().unwrap_or_default(),
            "timestamp": timestamp,
        });

        if let JsonValue::Object(ref mut map) = payload {
            if let Some(editor_user) = editor_user.clone() {
                if let Ok(value) = serde_json::to_value(editor_user.clone()) {
                    map.insert("editorUser".to_string(), value);
                }
                map.entry("editor".to_string())
                    .or_insert_with(|| json!(editor_user.id));
            } else if let Some(editor_id) = editor_id.clone() {
                map.insert("editor".to_string(), json!(editor_id));
            }
        }

        let room_sync = space_room_name(space_type, &space_id, RoomKind::Sync);

        let io_for_sync = io.clone();
        let payload_sync = payload.clone();
        spawn(async move {
            if let Some(ns) = io_for_sync.of("/") {
                if let Err(err) = ns
                    .to(room_sync)
                    .emit("space:broadcast-doc-update", &payload_sync)
                    .await
                {
                    warn!(?err, "failed to emit socket doc update");
                }
            }
        });
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DocSessionKey {
    pub(crate) space_type: SpaceType,
    pub(crate) space_id: String,
    pub(crate) doc_id: String,
}

impl DocSessionKey {
    pub(crate) fn new(space_type: SpaceType, space_id: String, doc_id: String) -> Self {
        Self {
            space_type,
            space_id,
            doc_id,
        }
    }
}

#[derive(Clone)]
pub(crate) struct DocSessionManager {
    sockets: Arc<DashMap<String, HashSet<DocSessionKey>>>,
    doc_counts: Arc<DashMap<DocSessionKey, usize>>,
}

impl Default for DocSessionManager {
    fn default() -> Self {
        Self {
            sockets: Arc::new(DashMap::new()),
            doc_counts: Arc::new(DashMap::new()),
        }
    }
}

impl DocSessionManager {
    pub(crate) fn register(&self, socket_id: &str, key: DocSessionKey) -> bool {
        let mut entry = self
            .sockets
            .entry(socket_id.to_string())
            .or_insert_with(HashSet::new);
        let inserted = entry.insert(key.clone());
        drop(entry);
        if !inserted {
            return false;
        }

        match self.doc_counts.entry(key) {
            Entry::Occupied(mut occ) => {
                *occ.get_mut() += 1;
                false
            }
            Entry::Vacant(vac) => {
                vac.insert(1);
                true
            }
        }
    }

    pub(crate) fn remove_by_space(
        &self,
        socket_id: &str,
        space_type: SpaceType,
        space_id: &str,
    ) -> Vec<DocSessionKey> {
        let Some(mut entry) = self.sockets.get_mut(socket_id) else {
            return Vec::new();
        };

        let mut removed = Vec::new();
        entry.retain(|key| {
            let keep = key.space_type != space_type || key.space_id != space_id;
            if !keep {
                removed.push(key.clone());
            }
            keep
        });

        if entry.is_empty() {
            drop(entry);
            self.sockets.remove(socket_id);
        } else {
            drop(entry);
        }

        self.process_removed(removed)
    }

    pub(crate) fn remove_all(&self, socket_id: &str) -> Vec<DocSessionKey> {
        let removed = self
            .sockets
            .remove(socket_id)
            .map(|(_, set)| set.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        self.process_removed(removed)
    }

    pub(crate) fn remove_doc(&self, space_type: SpaceType, space_id: &str, doc_id: &str) -> bool {
        let target = DocSessionKey::new(space_type, space_id.to_string(), doc_id.to_string());
        let mut empty_sockets = Vec::new();

        for mut entry in self.sockets.iter_mut() {
            if entry.value_mut().remove(&target) && entry.value().is_empty() {
                empty_sockets.push(entry.key().clone());
            }
        }

        for socket_id in empty_sockets {
            self.sockets.remove(&socket_id);
        }
        self.doc_counts.remove(&target).is_some()
    }

    fn process_removed(&self, removed: Vec<DocSessionKey>) -> Vec<DocSessionKey> {
        let mut drained = Vec::new();
        for key in removed {
            if self.decrement_doc_count(&key) {
                drained.push(key);
            }
        }
        drained
    }

    fn decrement_doc_count(&self, key: &DocSessionKey) -> bool {
        let should_remove = if let Some(mut entry) = self.doc_counts.get_mut(key) {
            if *entry > 1 {
                *entry -= 1;
                false
            } else {
                true
            }
        } else {
            false
        };

        if should_remove {
            self.doc_counts.remove(key);
            true
        } else {
            false
        }
    }

    #[cfg(test)]
    pub(crate) fn tracked_doc_count(&self) -> usize {
        self.doc_counts.len()
    }
}

impl AppState {
    pub fn runtime(&self) -> Arc<SocketRuntimeState> {
        self.socket_runtime.clone()
    }
    pub(crate) fn emit_workspace_embedding(
        &self,
        workspace_id: &str,
        enable_doc_embedding: Option<bool>,
    ) {
        self.socket_runtime
            .emit_workspace_embedding(workspace_id, enable_doc_embedding);
    }

    pub(crate) fn subscribe_workspace_embedding(
        &self,
    ) -> broadcast::Receiver<WorkspaceEmbeddingEvent> {
        self.socket_runtime.subscribe_workspace_embedding()
    }
}

fn parse_doc_channel(key: &str) -> Option<(String, String)> {
    let mut parts = key.splitn(2, ':');
    let space_id = parts.next()?.to_string();
    let doc_id = parts.next()?.to_string();
    Some((space_id, doc_id))
}

#[derive(Clone)]
pub(crate) struct SyncEvent {
    pub(crate) kind: SyncEventKind,
    pub(crate) payload: Vec<u8>,
}

impl SyncEvent {
    fn snapshot(payload: Vec<u8>) -> Self {
        Self {
            kind: SyncEventKind::Snapshot,
            payload,
        }
    }

    fn update(payload: Vec<u8>) -> Self {
        Self {
            kind: SyncEventKind::Update,
            payload,
        }
    }
}

#[derive(Clone, Copy)]
pub enum SyncEventKind {
    Snapshot,
    Update,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerMetadata {
    pub compatibility: String,
    pub message: String,
    #[serde(rename = "type")]
    pub deployment_type: String,
    pub flavor: String,
}

impl ServerMetadata {
    pub fn load() -> Self {
        let compatibility = env::var("AFFINE_VERSION")
            .or_else(|_| env::var("BARFFINE_COMPATIBILITY"))
            .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string());

        let deployment_type = env::var("DEPLOYMENT_TYPE")
            .or_else(|_| env::var("BARFFINE_DEPLOYMENT_TYPE"))
            .unwrap_or_else(|_| "selfhosted".to_string());

        let flavor = env::var("SERVER_FLAVOR")
            .or_else(|_| env::var("BARFFINE_FLAVOR"))
            .unwrap_or_else(|_| "allinone".to_string());

        let message = env::var("AFFINE_SERVER_MESSAGE")
            .or_else(|_| env::var("BARFFINE_SERVER_MESSAGE"))
            .unwrap_or_else(|_| format!("AFFiNE {compatibility} Server"));

        Self {
            compatibility,
            message,
            deployment_type,
            flavor,
        }
    }
}

#[derive(Default)]
pub(crate) struct SocketMetrics {
    connections: AtomicUsize,
    broadcast_updates: AtomicUsize,
    awareness_messages: AtomicUsize,
}

impl SocketMetrics {
    pub fn inc_connections(&self) {
        self.connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_connections(&self) {
        self.connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_awareness_messages(&self) {
        self.awareness_messages.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub(crate) struct SocketBroadcastMeta {
    pub space_type: SpaceType,
    pub space_id: String,
    pub doc_id: String,
    pub editor_id: Option<String>,
    pub editor_user: Option<SessionUser>,
    pub timestamp: Option<i64>,
}

impl SocketBroadcastMeta {
    pub fn new(
        space_type: SpaceType,
        space_id: impl Into<String>,
        doc_id: impl Into<String>,
        editor_id: Option<String>,
        editor_user: Option<SessionUser>,
        timestamp: Option<i64>,
    ) -> Self {
        Self {
            space_type,
            space_id: space_id.into(),
            doc_id: doc_id.into(),
            editor_id,
            editor_user,
            timestamp,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FeatureFlagPayload {
    pub(crate) key: String,
    pub(crate) enabled: bool,
    pub(crate) namespace: String,
    #[serde(rename = "expiresAt")]
    pub(crate) expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeatureSnapshotPayload {
    pub(crate) flags: Vec<FeatureFlagPayload>,
    #[serde(rename = "generatedAt")]
    pub(crate) generated_at: chrono::DateTime<chrono::Utc>,
}

impl From<FeatureFlag> for FeatureFlagPayload {
    fn from(flag: FeatureFlag) -> Self {
        let FeatureFlag {
            key,
            enabled,
            namespace,
            expires_at,
            metadata,
        } = flag;

        Self {
            key,
            enabled,
            namespace: namespace_to_string(&namespace),
            expires_at,
            metadata,
        }
    }
}

impl From<FeatureSnapshot> for FeatureSnapshotPayload {
    fn from(snapshot: FeatureSnapshot) -> Self {
        let FeatureSnapshot {
            flags,
            generated_at,
        } = snapshot;

        Self {
            flags: flags.into_iter().map(FeatureFlagPayload::from).collect(),
            generated_at,
        }
    }
}

fn namespace_to_string(namespace: &FeatureNamespace) -> String {
    match namespace {
        FeatureNamespace::Global => "global".to_string(),
        FeatureNamespace::Workspace(id) => format!("workspace:{id}"),
        FeatureNamespace::User(id) => format!("user:{id}"),
    }
}

pub fn build_state(database: &Database, app_config: &AppConfig) -> AppState {
    build_state_with_config(database, app_config, StateBuildConfig::default())
}

pub fn build_state_with_config(
    database: &Database,
    app_config: &AppConfig,
    config: StateBuildConfig,
) -> AppState {
    let deterministic_store = DeterministicFeatureStore::with_global_defaults();
    let workspace_feature_store = WorkspaceFeatureStore::new(database);
    let workspace_embedding_files = Arc::new(DashMap::new());
    let workspace_embedding_ignored_docs = Arc::new(DashMap::new());
    let copilot_sessions = Arc::new(DashMap::new());
    let comment_attachment_store = CommentAttachmentStore::new(database);
    let blob_store = create_blob_store(database, app_config);
    let doc_token_signer = Arc::new(DocTokenSigner::new());
    let server_path = detect_server_path();
    let base_url = compute_base_url(server_path.as_deref());
    let oauth = Arc::new(OAuthService::new(&base_url));
    let socket_io = Arc::new(OnceCell::new());
    let socket_metrics = Arc::new(SocketMetrics::default());
    let (workspace_embedding_events, _) = broadcast::channel(64);
    let document_store = DocumentStore::new(database);
    let user_doc_store = UserDocStore::new(database);
    let doc_cache = match app_config.doc_store_backend {
        DocStoreBackend::Sql => {
            let doc_cache_builder = DocCacheBuilder::new(
                document_store.clone(),
                user_doc_store.clone(),
                database
                    .doc_data_store()
                    .map(|store| store as Arc<dyn DocDataBackend>),
            );
            Arc::new(if let Some(doc_cache_config) = config.doc_cache.clone() {
                doc_cache_builder.with_config(doc_cache_config).build()
            } else {
                doc_cache_builder.build()
            })
        }
        DocStoreBackend::RocksDb => {
            // When the doc store backend is RocksDB, we bypass the
            // in-memory doc cache and go directly to storage for
            // snapshots/updates.
            Arc::new(DocCache::new_bypass(
                document_store.clone(),
                user_doc_store.clone(),
            ))
        }
    };
    let user_store = UserStore::new(database);
    let user_service = Arc::new(UserService::new(user_store.clone()));
    let user_settings = UserSettingsStore::new(database);
    let workspace_store = WorkspaceStore::new(database);
    let doc_role_store = DocumentRoleStore::new(database);
    let access_token_store = AccessTokenStore::new(database);
    let comment_store = create_comment_store(database);
    let notification_center = create_notification_center(database);
    let workspace_service = Arc::new(WorkspaceService::new(
        workspace_store.clone(),
        user_store.clone(),
        document_store.clone(),
        doc_role_store.clone(),
        doc_token_signer.clone(),
    ));
    let doc_access_service = Arc::new(DocAccessService::new(
        document_store.clone(),
        doc_role_store.clone(),
        workspace_service.clone(),
    ));
    let doc_sessions = Arc::new(DocSessionManager::default());
    let sync_hub = SyncHub::new(socket_io.clone(), socket_metrics.clone());
    let request_deduper = Arc::new(SocketRequestDeduper::default());
    let socket_runtime = Arc::new(SocketRuntimeState::new(
        doc_cache.clone(),
        doc_sessions.clone(),
        sync_hub.clone(),
        request_deduper.clone(),
        socket_metrics.clone(),
        workspace_embedding_files.clone(),
        workspace_embedding_ignored_docs.clone(),
        workspace_embedding_events.clone(),
        user_service.clone(),
        workspace_service.clone(),
        doc_access_service.clone(),
        workspace_store.clone(),
        document_store.clone(),
        user_doc_store.clone(),
    ));

    let state = AppState {
        user_store,
        user_settings,
        workspace_store,
        document_store,
        user_doc_store,
        doc_role_store,
        access_token_store,
        comment_store,
        comment_attachment_store,
        notification_center: notification_center.clone(),
        sync_hub,
        doc_sessions,
        doc_cache,
        metadata: ServerMetadata::load(),
        feature_service: FeatureService::new(deterministic_store, workspace_feature_store),
        user_service,
        workspace_service,
        doc_access_service,
        blob_store,
        workspace_embedding_files,
        workspace_embedding_ignored_docs,
        copilot_sessions,
        doc_token_signer,
        server_path,
        base_url,
        socket_io,
        socket_metrics,
        workspace_embedding_events,
        oauth,
        request_deduper,
        socket_runtime,
    };

    spawn_background_tasks(&state);

    state
}

fn create_notification_center(database: &Database) -> Arc<dyn NotificationCenter> {
    if let Ok(value) = env::var("BARFFINE_NOTIFICATION_CENTER_BACKEND") {
        let backend = value.trim().to_ascii_lowercase();
        if !backend.is_empty() && backend != "auto" {
            match backend.as_str() {
                "rocksdb" | "rocks" => {
                    if let Some(doc_data) = database.doc_data_store() {
                        info!(
                            "using RocksNotificationCenter (BARFFINE_NOTIFICATION_CENTER_BACKEND={backend})"
                        );
                        return Arc::new(RocksNotificationCenter::new(doc_data));
                    }
                    panic!(
                        "BARFFINE_NOTIFICATION_CENTER_BACKEND=rocksdb requires \
                         BARFFINE_DOC_DATA_BACKEND=rocksdb; doc data store is not configured"
                    );
                }
                "sqlite" => match database.backend() {
                    DatabaseBackend::Sqlite => {
                        info!(
                            "using SqliteNotificationCenter (BARFFINE_NOTIFICATION_CENTER_BACKEND={backend})"
                        );
                        return Arc::new(SqliteNotificationCenter::new(database));
                    }
                    other => {
                        panic!(
                            "BARFFINE_NOTIFICATION_CENTER_BACKEND=sqlite requires \
                             BARFFINE_DATABASE_BACKEND=sqlite (current backend: {other:?})"
                        );
                    }
                },
                "postgres" | "postgresql" | "pg" => match database.backend() {
                    DatabaseBackend::Postgres => {
                        info!(
                            "using PostgresNotificationCenter (BARFFINE_NOTIFICATION_CENTER_BACKEND={backend})"
                        );
                        return Arc::new(PostgresNotificationCenter::new(database));
                    }
                    other => {
                        panic!(
                            "BARFFINE_NOTIFICATION_CENTER_BACKEND=postgres requires \
                             BARFFINE_DATABASE_BACKEND=postgres (current backend: {other:?})"
                        );
                    }
                },
                other => {
                    warn!(
                        "unsupported BARFFINE_NOTIFICATION_CENTER_BACKEND='{other}'; falling back to default notification backend"
                    );
                }
            }
        }
    }

    if let Some(doc_data) = database.doc_data_store() {
        Arc::new(RocksNotificationCenter::new(doc_data))
    } else {
        match database.backend() {
            DatabaseBackend::Sqlite => Arc::new(SqliteNotificationCenter::new(database)),
            DatabaseBackend::Postgres => Arc::new(PostgresNotificationCenter::new(database)),
        }
    }
}

fn create_blob_store(database: &Database, app_config: &AppConfig) -> Arc<dyn BlobStorage> {
    match app_config.blob_store_backend {
        BlobStoreBackend::Sql => Arc::new(SqlBlobStorage::new(database)),
        BlobStoreBackend::Rocks => Arc::new(
            RocksBlobStorage::open(&app_config.blob_store_path)
                .unwrap_or_else(|err| panic!("failed to open rocksdb blob store: {err}")),
        ),
    }
}

fn create_comment_store(database: &Database) -> Arc<dyn CommentStoreTrait> {
    Arc::new(RepositoryCommentStore::new(database))
}

fn detect_server_path() -> Option<String> {
    let candidates = [
        "BARFFINE_SERVER_PATH",
        "BARFFINE_SERVER_SUB_PATH",
        "AFFINE_SERVER_SUB_PATH",
        "AFFINE_SERVER_PATH",
    ];

    for var in candidates {
        if let Ok(value) = env::var(var) {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                continue;
            }

            let mut path = if trimmed.starts_with('/') {
                trimmed.to_owned()
            } else {
                format!("/{trimmed}")
            };

            while path.ends_with('/') && path.len() > 1 {
                path.pop();
            }

            if path == "/" {
                return None;
            }

            return Some(path);
        }
    }

    None
}

fn compute_base_url(server_path: Option<&str>) -> String {
    if let Some(url) = pick_non_empty_env(&[
        "BARFFINE_PUBLIC_BASE_URL",
        "AFFINE_SERVER_BASE_URL",
        "AFFINE_BASE_URL",
        "BARFFINE_BASE_URL",
    ]) {
        return trim_trailing_slash(url);
    }

    if let Some(url) =
        pick_non_empty_env(&["BARFFINE_SERVER_EXTERNAL_URL", "AFFINE_SERVER_EXTERNAL_URL"])
    {
        return trim_trailing_slash(url);
    }

    let https = parse_bool_env(&["BARFFINE_SERVER_HTTPS", "AFFINE_SERVER_HTTPS"]).unwrap_or(false);
    let scheme = if https { "https" } else { "http" };

    let (default_host, default_port) = default_bind_host_and_port();
    let host =
        pick_non_empty_env(&["BARFFINE_SERVER_HOST", "AFFINE_SERVER_HOST"]).unwrap_or(default_host);
    let port =
        parse_port_env(&["BARFFINE_SERVER_PORT", "AFFINE_SERVER_PORT"]).unwrap_or(default_port);

    let mut base = format!("{scheme}://{host}");
    if (scheme == "http" && port != 80) || (scheme == "https" && port != 443) {
        base.push(':');
        base.push_str(&port.to_string());
    }

    if let Some(path) = server_path {
        base.push_str(path);
    }

    trim_trailing_slash(base)
}

fn default_bind_host_and_port() -> (String, u16) {
    if let Ok(addr) = env::var("BARFFINE_BIND_ADDRESS") {
        if let Ok(socket) = addr.parse::<SocketAddr>() {
            return (format_host(socket.ip()), socket.port());
        }
    }
    ("127.0.0.1".to_string(), 8081)
}

fn format_host(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(addr) => addr.to_string(),
        IpAddr::V6(addr) => format!("[{addr}]"),
    }
}

fn pick_non_empty_env(vars: &[&str]) -> Option<String> {
    for var in vars {
        if let Ok(value) = env::var(var) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }
    None
}

fn spawn_background_tasks(state: &AppState) {
    start_doc_compactor(state.document_store.clone(), state.user_doc_store.clone());
    start_doc_cache_reaper(state.doc_cache.clone());
}

fn start_doc_compactor(document_store: DocumentStore, user_doc_store: UserDocStore) {
    spawn(async move {
        doc_compactor_loop(document_store, user_doc_store).await;
    });
}

fn start_doc_cache_reaper(doc_cache: Arc<DocCache>) {
    spawn(async move {
        doc_cache_reaper_loop(doc_cache).await;
    });
}

const DOC_COMPACTION_INTERVAL: Duration = Duration::from_secs(60);
const DOC_COMPACTION_BATCH_LIMIT: i64 = 64;
const DOC_COMPACTION_FETCH_CONCURRENCY: usize = 8;
const DOC_COMPACTION_APPLY_CONCURRENCY: usize = 8;
const DOC_CACHE_REAPER_INTERVAL: Duration = Duration::from_secs(120);
const DOC_CACHE_IDLE_AFTER: Duration = Duration::from_secs(300);

async fn doc_compactor_loop(document_store: DocumentStore, user_doc_store: UserDocStore) {
    loop {
        if let Err(err) = compact_workspace_docs(&document_store).await {
            warn!(error = %err, "document compaction loop encountered an error");
        }
        if let Err(err) = compact_user_docs(&user_doc_store).await {
            warn!(error = %err, "user document compaction encountered an error");
        }
        sleep(DOC_COMPACTION_INTERVAL).await;
    }
}

async fn compact_workspace_docs(document_store: &DocumentStore) -> AnyResult<()> {
    let targets = document_store
        .docs_requiring_compaction(DOC_UPDATE_LOG_LIMIT, DOC_COMPACTION_BATCH_LIMIT)
        .await?;

    if targets.is_empty() {
        return Ok(());
    }

    let (job_tx, job_rx) = unbounded();

    stream::iter(targets.into_iter())
        .for_each_concurrent(
            Some(DOC_COMPACTION_FETCH_CONCURRENCY),
            |(space_id, doc_id)| {
                let job_tx = job_tx.clone();
                let store = document_store.clone();
                async move {
                    match store.prepare_compaction_job(&space_id, &doc_id).await {
                        Ok(Some(job)) => {
                            let _ = job_tx.send(job);
                        }
                        Ok(None) => {}
                        Err(err) => {
                            warn!(
                                workspace_id = %space_id,
                                doc_id = %doc_id,
                                error = %err,
                                "failed to prepare workspace doc compaction job",
                            );
                        }
                    }
                }
            },
        )
        .await;
    drop(job_tx);

    let plans = spawn_blocking(move || {
        job_rx
            .into_iter()
            .par_bridge()
            .filter_map(|job| {
                let workspace_id = job.workspace_id.clone();
                let doc_id = job.doc_id.clone();
                match DocumentStore::compute_compaction_plan(job) {
                    Ok(plan) => Some(plan),
                    Err(err) => {
                        warn!(
                            workspace_id = %workspace_id,
                            doc_id = %doc_id,
                            error = %err,
                            "failed to merge workspace doc logs",
                        );
                        None
                    }
                }
            })
            .collect::<Vec<_>>()
    })
    .await?;

    stream::iter(plans.into_iter())
        .for_each_concurrent(Some(DOC_COMPACTION_APPLY_CONCURRENCY), |plan| {
            let store = document_store.clone();
            async move {
                let workspace_id = plan.workspace_id.clone();
                let doc_id = plan.doc_id.clone();
                if let Err(err) = store.apply_compaction_plan(plan).await {
                    warn!(
                        workspace_id = %workspace_id,
                        doc_id = %doc_id,
                        error = %err,
                        "failed to compact workspace document",
                    );
                }
            }
        })
        .await;

    Ok(())
}

async fn compact_user_docs(user_doc_store: &UserDocStore) -> AnyResult<()> {
    let targets = user_doc_store
        .docs_requiring_compaction(DOC_UPDATE_LOG_LIMIT, DOC_COMPACTION_BATCH_LIMIT)
        .await?;

    if targets.is_empty() {
        return Ok(());
    }

    let (job_tx, job_rx) = unbounded();

    stream::iter(targets.into_iter())
        .for_each_concurrent(
            Some(DOC_COMPACTION_FETCH_CONCURRENCY),
            |(user_id, doc_id)| {
                let store = user_doc_store.clone();
                let job_tx = job_tx.clone();
                async move {
                    match store.prepare_compaction_job(&user_id, &doc_id).await {
                        Ok(Some(job)) => {
                            let _ = job_tx.send(job);
                        }
                        Ok(None) => {}
                        Err(err) => {
                            warn!(
                                user_id = %user_id,
                                doc_id = %doc_id,
                                error = %err,
                                "failed to prepare userspace compaction job",
                            );
                        }
                    }
                }
            },
        )
        .await;
    drop(job_tx);

    let plans = spawn_blocking(move || {
        job_rx
            .into_iter()
            .par_bridge()
            .filter_map(|job| {
                let user_id = job.user_id.clone();
                let doc_id = job.doc_id.clone();
                match UserDocStore::compute_compaction_plan(job) {
                    Ok(plan) => Some(plan),
                    Err(err) => {
                        warn!(
                            user_id = %user_id,
                            doc_id = %doc_id,
                            error = %err,
                            "failed to merge userspace doc logs",
                        );
                        None
                    }
                }
            })
            .collect::<Vec<_>>()
    })
    .await?;

    stream::iter(plans.into_iter())
        .for_each_concurrent(Some(DOC_COMPACTION_APPLY_CONCURRENCY), |plan| {
            let store = user_doc_store.clone();
            async move {
                let user_id = plan.user_id.clone();
                let doc_id = plan.doc_id.clone();
                if let Err(err) = store.apply_compaction_plan(plan).await {
                    warn!(
                        user_id = %user_id,
                        doc_id = %doc_id,
                        error = %err,
                        "failed to compact userspace document",
                    );
                }
            }
        })
        .await;

    Ok(())
}

async fn doc_cache_reaper_loop(doc_cache: Arc<DocCache>) {
    loop {
        doc_cache.reap_idle_entries(DOC_CACHE_IDLE_AFTER).await;
        sleep(DOC_CACHE_REAPER_INTERVAL).await;
    }
}

fn parse_bool_env(vars: &[&str]) -> Option<bool> {
    for var in vars {
        if let Ok(value) = env::var(var) {
            let trimmed = value.trim();
            if trimmed.eq_ignore_ascii_case("true") || trimmed == "1" {
                return Some(true);
            }
            if trimmed.eq_ignore_ascii_case("false") || trimmed == "0" {
                return Some(false);
            }
        }
    }
    None
}

fn parse_port_env(vars: &[&str]) -> Option<u16> {
    for var in vars {
        if let Ok(value) = env::var(var) {
            if let Ok(port) = value.trim().parse::<u16>() {
                return Some(port);
            }
        }
    }
    None
}

fn trim_trailing_slash(mut input: String) -> String {
    while input.ends_with('/') && input.len() > 1 {
        input.pop();
    }
    input
}

#[cfg(test)]
mod tests {
    use super::*;
    use barffine_core::feature::{DeterministicFeatureStore, FeatureNamespace};

    #[test]
    fn server_metadata_serializes_with_expected_fields() {
        let metadata = ServerMetadata {
            compatibility: "1.2.3".into(),
            message: "AFFiNE 1.2.3 Server".into(),
            deployment_type: "selfhosted".into(),
            flavor: "allinone".into(),
        };

        let json = serde_json::to_value(&metadata).expect("metadata serializes");
        assert_eq!(json["compatibility"], "1.2.3");
        assert_eq!(json["message"], "AFFiNE 1.2.3 Server");
        assert_eq!(json["type"], "selfhosted");
        assert_eq!(json["flavor"], "allinone");
    }

    #[test]
    fn feature_snapshot_payload_formats_namespace() {
        let store = DeterministicFeatureStore::with_global_defaults();
        let snapshot = store.snapshot(&FeatureNamespace::Global);
        let payload = FeatureSnapshotPayload::from(snapshot);

        assert_eq!(payload.flags.len(), 3);
        assert!(payload.flags.iter().all(|flag| flag.namespace == "global"));
    }
}
