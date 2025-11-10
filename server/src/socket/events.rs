use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use socketioxide::{
    SocketIo,
    extract::{AckSender, Data, Extension, SocketRef, State},
    handler::ConnectHandler,
};
use tracing::Instrument;
use tracing::{Span, debug, info, warn};
use y_octo::{Doc as YoctoDoc, StateVector as YoctoStateVector};

use crate::{
    auth::{DocAccessIntent, RpcAccessRequirement, resolve_doc_access, resolve_workspace_access},
    doc::{
        channels::doc_channel_key,
        sync::{UpdateBroadcastContext, apply_doc_updates},
    },
    error::AppError,
    graphql::doc_permissions_for_role,
    socket::{
        ack::{ack_error, ack_ok},
        auth::SocketAuthMiddleware,
        rooms::{RoomKind, SpaceType, space_room_name},
        types::{SocketRequestContext, SocketSpanRegistry, SocketUserContext},
    },
    state::{AppState, DocSessionKey, SocketBroadcastMeta},
    types::RestDocAccess,
    utils::crdt::{decode_state_vector, encode_state_vector},
};

use tokio::sync::Mutex;

const DOC_ACCESS_CACHE_TTL: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct SocketDocAccessCache {
    inner: Arc<Mutex<HashMap<(String, String), CachedDocAccess>>>,
}

impl SocketDocAccessCache {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn get(&self, workspace_id: &str, doc_id: &str) -> Option<RestDocAccess> {
        let mut guard = self.inner.lock().await;
        let key = (workspace_id.to_string(), doc_id.to_string());
        if let Some(entry) = guard.get_mut(&key) {
            if entry.is_valid() {
                entry.refresh();
                return Some(entry.access.clone());
            }
        }
        guard.remove(&key);
        None
    }

    async fn insert(&self, workspace_id: &str, doc_id: &str, access: RestDocAccess) {
        let key = (workspace_id.to_string(), doc_id.to_string());
        let cached = CachedDocAccess::new(access);
        let mut guard = self.inner.lock().await;
        guard.insert(key, cached);
    }

    async fn remove(&self, workspace_id: &str, doc_id: &str) {
        let key = (workspace_id.to_string(), doc_id.to_string());
        let mut guard = self.inner.lock().await;
        guard.remove(&key);
    }
}

impl Default for SocketDocAccessCache {
    fn default() -> Self {
        Self::new()
    }
}

async fn finalize_doc_session(state: &AppState, key: &DocSessionKey) {
    state
        .doc_cache
        .close_session(key.space_type, &key.space_id, &key.doc_id)
        .await;
    let channel_key = doc_channel_key(&key.space_id, &key.doc_id);
    state.sync_hub.remove_channel(&channel_key);
}

struct CachedDocAccess {
    access: RestDocAccess,
    expires_at: Instant,
}

impl CachedDocAccess {
    fn new(access: RestDocAccess) -> Self {
        Self {
            access,
            expires_at: Instant::now() + DOC_ACCESS_CACHE_TTL,
        }
    }

    fn is_valid(&self) -> bool {
        Instant::now() <= self.expires_at
    }

    fn refresh(&mut self) {
        self.expires_at = Instant::now() + DOC_ACCESS_CACHE_TTL;
    }
}

fn start_socket_span<'a>(
    event: &'static str,
    socket: &'a SocketRef,
    user: Option<&SocketUserContext>,
    space: Option<(SpaceType, &str)>,
    doc_id: Option<&str>,
    request_id: Option<&str>,
) -> tracing::Span {
    let registry = socket.extensions.get::<SocketSpanRegistry>();
    let parent_span = space
        .and_then(|(space_type, space_id)| ensure_space_span(socket, user, space_type, space_id))
        .or_else(|| registry.map(|reg| reg.root_span()));
    let socket_id = socket.id.to_string();
    let user_id = user
        .map(|u| u.user_id.clone())
        .unwrap_or_else(|| "anonymous".to_string());
    let (space_type, space_id) = space
        .map(|(ty, id)| (ty.as_str().to_string(), id.to_string()))
        .unwrap_or_else(|| ("unknown".to_string(), String::new()));
    let doc_id = doc_id.unwrap_or("").to_string();
    let request_id = request_id.unwrap_or("").to_string();

    if let Some(parent) = parent_span {
        logfire::span!(
            parent: &parent,
            "socket {event}",
            event = event,
            socket_id = socket_id,
            user_id = user_id,
            space_type = space_type,
            space_id = space_id,
            doc_id = doc_id,
            request_id = request_id
        )
    } else {
        logfire::span!(
            "socket {event}",
            event = event,
            socket_id = socket_id,
            user_id = user_id,
            space_type = space_type,
            space_id = space_id,
            doc_id = doc_id,
            request_id = request_id
        )
    }
}

fn ensure_space_span(
    socket: &SocketRef,
    user: Option<&SocketUserContext>,
    space_type: SpaceType,
    space_id: &str,
) -> Option<Span> {
    let registry = socket.extensions.get::<SocketSpanRegistry>()?;
    if let Some(existing) = registry.space_span(space_type, space_id) {
        return Some(existing);
    }

    let user_id = user.map(|u| u.user_id.as_str()).unwrap_or("anonymous");
    let parent = registry.root_span();
    let span = logfire::span!(
        parent: &parent,
        "socket space {space_id}",
        socket_id = socket.id.to_string(),
        user_id = user_id,
        space_type = space_type.as_str(),
        space_id = space_id
    );
    registry.insert_space_span(space_type, space_id, span.clone());
    Some(span)
}

fn ensure_workspace_embedding_initialized(state: &AppState, workspace_id: &str) {
    if !state.workspace_embedding_files.contains_key(workspace_id) {
        state
            .workspace_embedding_files
            .insert(workspace_id.to_string(), HashMap::new());
        info!(
            workspace_id = workspace_id,
            "initialized workspace embedding cache from socket join"
        );
    }

    state
        .workspace_embedding_ignored_docs
        .entry(workspace_id.to_string())
        .or_default();

    state.emit_workspace_embedding(workspace_id, None);
}

fn socket_in_room(socket: &SocketRef, room: &str) -> bool {
    socket
        .rooms()
        .iter()
        .any(|current| current.as_ref() == room)
}

fn ensure_socket_joined_room(
    socket: &SocketRef,
    space_type: SpaceType,
    space_id: &str,
) -> Result<(), AppError> {
    let sync_room = space_room_name(space_type, space_id, RoomKind::Sync);
    if socket_in_room(socket, &sync_room) {
        Ok(())
    } else {
        Err(AppError::not_in_space(space_id))
    }
}

fn ensure_socket_joined_awareness(
    socket: &SocketRef,
    space_type: SpaceType,
    space_id: &str,
    doc_id: &str,
) -> Result<(), AppError> {
    let room = space_room_name(
        space_type,
        space_id,
        RoomKind::Awareness {
            doc_id: doc_id.to_string(),
        },
    );

    if socket_in_room(socket, &room) {
        Ok(())
    } else {
        Err(AppError::not_in_space(space_id))
    }
}

fn ensure_doc_update_permission(access: &RestDocAccess) -> Result<(), AppError> {
    let role = access
        .doc_role
        .ok_or_else(|| AppError::forbidden("Doc.Update permission required"))?;

    let permissions = doc_permissions_for_role(role);
    if permissions.can_update_doc() {
        Ok(())
    } else {
        Err(AppError::forbidden("Doc.Update permission required"))
    }
}

fn ensure_doc_delete_permission(access: &RestDocAccess) -> Result<(), AppError> {
    let role = access
        .doc_role
        .ok_or_else(|| AppError::forbidden("Doc.Delete permission required"))?;

    let permissions = doc_permissions_for_role(role);
    if permissions.can_delete_doc() {
        Ok(())
    } else {
        Err(AppError::forbidden("Doc.Delete permission required"))
    }
}

async fn resolve_doc_access_cached(
    state: &AppState,
    cache: &SocketDocAccessCache,
    user: &SocketUserContext,
    workspace_id: &str,
    doc_id: &str,
) -> Result<RestDocAccess, AppError> {
    if let Some(access) = cache.get(workspace_id, doc_id).await {
        return Ok(access);
    }

    let headers = user.header_map()?;
    let access = resolve_doc_access(
        state,
        &headers,
        workspace_id,
        doc_id,
        RpcAccessRequirement::Optional,
        DocAccessIntent::Standard,
    )
    .await?;
    cache.insert(workspace_id, doc_id, access.clone()).await;
    Ok(access)
}

fn ensure_userspace_owner(user: &SocketUserContext, space_id: &str) -> Result<(), AppError> {
    if user.user_id == space_id {
        Ok(())
    } else {
        Err(AppError::space_access_denied(space_id))
    }
}

fn attach_editor_metadata(payload: &mut JsonValue, user: &SocketUserContext) {
    if let JsonValue::Object(map) = payload {
        map.entry("editor".to_string())
            .or_insert_with(|| json!(user.user_id.clone()));
        map.entry("editorUser".to_string())
            .or_insert_with(|| json!(user.user.clone()));
    }
}

async fn ensure_socket_space_access(
    state: &AppState,
    user: &SocketUserContext,
    space_type: SpaceType,
    space_id: &str,
) -> Result<(), AppError> {
    match space_type {
        SpaceType::Workspace => {
            let headers = user.header_map()?;
            resolve_workspace_access(state, &headers, space_id).await?;
            Ok(())
        }
        SpaceType::Userspace => ensure_userspace_owner(user, space_id),
    }
}

pub(crate) fn register_namespace(io: &SocketIo, state: AppState) {
    let middleware = SocketAuthMiddleware::new(state);
    let _ = io.ns("/", on_connect.with(middleware));
}

async fn on_connect(socket: SocketRef) {
    socket.extensions.insert(SocketDocAccessCache::default());
    socket.on("space:join", handle_space_join);
    socket.on("space:leave", handle_space_leave);
    socket.on("space:load-doc", handle_space_load_doc);
    socket.on("space:push-doc-update", handle_space_push_doc_update);
    socket.on("space:push-doc-updates", handle_space_push_doc_updates);
    socket.on("space:delete-doc", handle_space_delete_doc);
    socket.on(
        "space:load-doc-timestamps",
        handle_space_load_doc_timestamps,
    );
    socket.on("space:join-awareness", handle_space_join_awareness);
    socket.on("space:leave-awareness", handle_space_leave_awareness);
    socket.on("space:update-awareness", handle_space_update_awareness);
    socket.on("space:load-awarenesses", handle_space_load_awarenesses);

    socket.on_disconnect(handle_disconnect);
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JoinSpaceRequest {
    space_type: SpaceType,
    space_id: String,
    client_version: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct JoinSpaceResponse {
    client_id: String,
    success: bool,
}

async fn handle_space_join(
    socket: SocketRef,
    Data(payload): Data<JoinSpaceRequest>,
    ack: AckSender,
    Extension(user): Extension<SocketUserContext>,
    Extension(request): Extension<SocketRequestContext>,
    Extension(_doc_access_cache): Extension<SocketDocAccessCache>,
    State(state): State<AppState>,
) {
    let span = start_socket_span(
        "space:join",
        &socket,
        Some(&user),
        Some((payload.space_type, payload.space_id.as_str())),
        None,
        Some(request.request_id.as_str()),
    );

    async move {
        let request_id_owned = request.request_id.clone();
        let space_id_owned = payload.space_id.clone();
        let space_type_label = payload.space_type.as_str().to_string();
        let client_version = payload.client_version.clone();

        info!(
            request_id = %request_id_owned,
            space_id = %space_id_owned,
            space_type = %space_type_label,
            "socket space:join received"
        );

        if payload.client_version.starts_with("0.1") {
            ack_ok(
                ack,
                JoinSpaceResponse {
                    client_id: socket.id.to_string(),
                    success: false,
                },
            );
            warn!(
                request_id = %request_id_owned,
                client_version = %client_version,
                "socket space:join blocked due to legacy client"
            );
            return;
        }

        if let SpaceType::Workspace = payload.space_type {
            let header_map = match user.header_map() {
                Ok(map) => map,
                Err(err) => {
                    warn!(
                        request_id = %request_id_owned,
                        error = %err,
                        "socket space:join missing auth headers"
                    );
                    ack_error::<JoinSpaceResponse>(ack, err, Some(&request.request_id));
                    return;
                }
            };

            if let Err(err) = resolve_workspace_access(&state, &header_map, &payload.space_id).await
            {
                warn!(
                    request_id = %request_id_owned,
                    space_id = %space_id_owned,
                    error = %err,
                    "socket space:join denied"
                );
                ack_error::<JoinSpaceResponse>(ack, err, Some(&request.request_id));
                return;
            }
            ensure_workspace_embedding_initialized(&state, &payload.space_id);
        } else if user.user_id != payload.space_id {
            warn!(
                request_id = %request_id_owned,
                expected_user_id = %space_id_owned,
                actual_user_id = %user.user_id,
                "socket space:join denied for userspace mismatch"
            );
            ack_error::<JoinSpaceResponse>(
                ack,
                AppError::space_access_denied(&payload.space_id),
                Some(&request.request_id),
            );
            return;
        }

        socket.join(space_room_name(
            payload.space_type,
            &payload.space_id,
            RoomKind::Sync,
        ));

        socket.join(space_room_name(
            payload.space_type,
            &payload.space_id,
            RoomKind::LegacySync,
        ));

        ack_ok(
            ack,
            JoinSpaceResponse {
                client_id: socket.id.to_string(),
                success: true,
            },
        );

        info!(
            request_id = %request_id_owned,
            space_id = %space_id_owned,
            space_type = %space_type_label,
            "socket space:join success"
        );
    }
    .instrument(span)
    .await;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeaveSpaceRequest {
    space_type: SpaceType,
    space_id: String,
}

async fn handle_space_leave(
    socket: SocketRef,
    Data(payload): Data<LeaveSpaceRequest>,
    ack: AckSender,
    State(state): State<AppState>,
) {
    let span = start_socket_span(
        "space:leave",
        &socket,
        None,
        Some((payload.space_type, payload.space_id.as_str())),
        None,
        None,
    );

    async move {
        let space_id_owned = payload.space_id.clone();
        let space_type_label = payload.space_type.as_str().to_string();
        let socket_id = socket.id.to_string();

        info!(
            space_id = %space_id_owned,
            space_type = %space_type_label,
            "socket space:leave received"
        );

        socket.leave(space_room_name(
            payload.space_type,
            &payload.space_id,
            RoomKind::Sync,
        ));
        socket.leave(space_room_name(
            payload.space_type,
            &payload.space_id,
            RoomKind::LegacySync,
        ));

        ack_ok(
            ack,
            JoinSpaceResponse {
                client_id: socket.id.to_string(),
                success: true,
            },
        );

        info!(
            space_id = %space_id_owned,
            space_type = %space_type_label,
            "socket space:leave success"
        );

        let stale_docs = state
            .doc_sessions
            .remove_by_space(&socket_id, payload.space_type, &payload.space_id)
            .await;

        for key in stale_docs {
            finalize_doc_session(&state, &key).await;
        }

        if let Some(registry) = socket.extensions.get::<SocketSpanRegistry>() {
            registry.remove_space_span(payload.space_type, &payload.space_id);
        }
    }
    .instrument(span)
    .await;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoadDocRequest {
    space_type: SpaceType,
    space_id: String,
    doc_id: String,
    #[serde(default)]
    state_vector: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LoadDocResponse {
    missing: String,
    state: String,
    timestamp: i64,
}

async fn handle_space_load_doc(
    socket: SocketRef,
    Data(payload): Data<LoadDocRequest>,
    ack: AckSender,
    Extension(user): Extension<SocketUserContext>,
    Extension(request): Extension<SocketRequestContext>,
    Extension(doc_access_cache): Extension<SocketDocAccessCache>,
    State(state): State<AppState>,
) {
    let span = start_socket_span(
        "space:load-doc",
        &socket,
        Some(&user),
        Some((payload.space_type, payload.space_id.as_str())),
        Some(payload.doc_id.as_str()),
        Some(request.request_id.as_str()),
    );

    async move {
        let request_id_owned = request.request_id.clone();
        let space_id_owned = payload.space_id.clone();
        let doc_id_owned = payload.doc_id.clone();
        let socket_id = socket.id.to_string();

        info!(
            request_id = %request_id_owned,
            space_id = %space_id_owned,
            doc_id = %doc_id_owned,
            "socket space:load-doc received"
        );

        if let Err(err) = ensure_socket_joined_room(&socket, payload.space_type, &payload.space_id)
        {
            warn!(
                request_id = %request_id_owned,
                space_id = %space_id_owned,
                doc_id = %doc_id_owned,
                error = %err,
                "socket space:load-doc denied (not in room)"
            );
            ack_error::<LoadDocResponse>(ack, err, Some(&request.request_id));
            return;
        }

        let (snapshot_bytes, timestamp) = match payload.space_type {
            SpaceType::Workspace => {
                if let Err(err) = resolve_doc_access_cached(
                    &state,
                    &doc_access_cache,
                    &user,
                    &payload.space_id,
                    &payload.doc_id,
                )
                .await
                {
                    warn!(
                        request_id = %request_id_owned,
                        space_id = %space_id_owned,
                        doc_id = %doc_id_owned,
                        error = %err,
                        "socket space:load-doc failed doc access check"
                    );
                    ack_error::<LoadDocResponse>(ack, err, Some(&request.request_id));
                    return;
                }

                match state
                    .doc_cache
                    .clone()
                    .snapshot(SpaceType::Workspace, &payload.space_id, &payload.doc_id)
                    .await
                {
                    Ok(snapshot) => snapshot,
                    Err(err) => {
                        ack_error::<LoadDocResponse>(
                            ack,
                            AppError::from_anyhow(err),
                            Some(&request.request_id),
                        );
                        return;
                    }
                }
            }
            SpaceType::Userspace => {
                if let Err(err) = ensure_userspace_owner(&user, &payload.space_id) {
                    ack_error::<LoadDocResponse>(ack, err, Some(&request.request_id));
                    return;
                }

                match state
                    .doc_cache
                    .clone()
                    .snapshot(SpaceType::Userspace, &payload.space_id, &payload.doc_id)
                    .await
                {
                    Ok(snapshot) => snapshot,
                    Err(err) => {
                        ack_error::<LoadDocResponse>(
                            ack,
                            AppError::from_anyhow(err),
                            Some(&request.request_id),
                        );
                        return;
                    }
                }
            }
        };

        let doc = match YoctoDoc::try_from_binary_v1(&snapshot_bytes) {
            Ok(doc) => doc,
            Err(err) => {
                warn!(
                    request_id = %request_id_owned,
                    space_id = %space_id_owned,
                    doc_id = %doc_id_owned,
                    error = %err,
                    "socket space:load-doc failed to decode snapshot"
                );
                ack_error::<LoadDocResponse>(
                    ack,
                    AppError::from_anyhow(err.into()),
                    Some(&request.request_id),
                );
                return;
            }
        };

        let state_vector = match payload.state_vector {
            Some(ref value) if !value.is_empty() => match BASE64.decode(value.as_bytes()) {
                Ok(bytes) => match decode_state_vector(&bytes) {
                    Ok(vector) => vector,
                    Err(err) => {
                        warn!(
                            request_id = %request_id_owned,
                            error = %err,
                            "socket space:load-doc invalid state vector"
                        );
                        ack_error::<LoadDocResponse>(ack, err, Some(&request.request_id));
                        return;
                    }
                },
                Err(_) => {
                    warn!(
                        request_id = %request_id_owned,
                        "socket space:load-doc failed to decode state vector base64"
                    );
                    ack_error::<LoadDocResponse>(
                        ack,
                        AppError::bad_request("invalid state vector"),
                        Some(&request.request_id),
                    );
                    return;
                }
            },
            _ => YoctoStateVector::default(),
        };

        let missing_bytes = match doc.encode_state_as_update_v1(&state_vector) {
            Ok(data) => data,
            Err(err) => {
                warn!(
                    request_id = %request_id_owned,
                    error = %err,
                    "socket space:load-doc failed to compute diff"
                );
                ack_error::<LoadDocResponse>(
                    ack,
                    AppError::from_anyhow(err.into()),
                    Some(&request.request_id),
                );
                return;
            }
        };

        let state_bytes = match encode_state_vector(&doc.get_state_vector()) {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(
                    request_id = %request_id_owned,
                    error = %err,
                    "socket space:load-doc failed to encode state vector"
                );
                ack_error::<LoadDocResponse>(ack, err, Some(&request.request_id));
                return;
            }
        };

        let session_key = DocSessionKey::new(
            payload.space_type,
            space_id_owned.clone(),
            doc_id_owned.clone(),
        );
        let first_session = state.doc_sessions.register(&socket_id, session_key).await;
        if first_session {
            if let Err(err) = state
                .doc_cache
                .open_session(payload.space_type, &space_id_owned, &doc_id_owned)
                .await
            {
                warn!(
                    space_type = ?payload.space_type,
                    space_id = %space_id_owned,
                    doc_id = %doc_id_owned,
                    error = %err,
                    "failed to open doc cache session"
                );
            }
        }

        ack_ok(
            ack,
            LoadDocResponse {
                missing: BASE64.encode(missing_bytes),
                state: BASE64.encode(state_bytes),
                timestamp,
            },
        );
        info!(
            request_id = %request_id_owned,
            space_id = %space_id_owned,
            doc_id = %doc_id_owned,
            "socket space:load-doc success"
        );
    }
    .instrument(span)
    .await;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PushDocUpdateRequest {
    space_type: SpaceType,
    space_id: String,
    doc_id: String,
    update: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PushDocUpdatesRequest {
    space_type: SpaceType,
    space_id: String,
    doc_id: String,
    updates: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PushDocUpdateResponse {
    accepted: bool,
    timestamp: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeleteDocRequest {
    space_type: SpaceType,
    space_id: String,
    doc_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DeleteDocResponse {
    success: bool,
}

async fn handle_space_push_doc_update(
    socket: SocketRef,
    Data(payload): Data<PushDocUpdateRequest>,
    ack: AckSender,
    Extension(user): Extension<SocketUserContext>,
    Extension(request): Extension<SocketRequestContext>,
    Extension(doc_access_cache): Extension<SocketDocAccessCache>,
    State(state): State<AppState>,
) {
    let span = start_socket_span(
        "space:push-doc-update",
        &socket,
        Some(&user),
        Some((payload.space_type, payload.space_id.as_str())),
        Some(payload.doc_id.as_str()),
        Some(request.request_id.as_str()),
    );

    async move {
        debug!(
            request_id = request.request_id.as_str(),
            space_id = payload.space_id.as_str(),
            doc_id = payload.doc_id.as_str(),
            "socket push-doc-update received"
        );

        if let Err(err) = ensure_socket_joined_room(&socket, payload.space_type, &payload.space_id)
        {
            warn!(
                request_id = request.request_id.as_str(),
                error = %err,
                "socket push-doc-update denied (not in room)"
            );
            ack_error::<PushDocUpdateResponse>(ack, err, Some(&request.request_id));
            return;
        }

        let update_bytes = match BASE64.decode(payload.update.as_bytes()) {
            Ok(bytes) => bytes,
            Err(_) => {
                warn!(
                    request_id = request.request_id.as_str(),
                    "socket push-doc-update invalid update payload"
                );
                ack_error::<PushDocUpdateResponse>(
                    ack,
                    AppError::bad_request("invalid update payload"),
                    Some(&request.request_id),
                );
                return;
            }
        };

        match payload.space_type {
            SpaceType::Workspace => {
                let mut access = match resolve_doc_access_cached(
                    &state,
                    &doc_access_cache,
                    &user,
                    &payload.space_id,
                    &payload.doc_id,
                )
                .await
                {
                    Ok(access) => access,
                    Err(err) => {
                        ack_error::<PushDocUpdateResponse>(ack, err, Some(&request.request_id));
                        return;
                    }
                };

                if access.metadata.blocked {
                    ack_error::<PushDocUpdateResponse>(
                        ack,
                        AppError::doc_update_blocked(&payload.space_id, &payload.doc_id),
                        Some(&request.request_id),
                    );
                    return;
                }

                if let Err(err) = ensure_doc_update_permission(&access) {
                    ack_error::<PushDocUpdateResponse>(ack, err, Some(&request.request_id));
                    return;
                }

                let cache_result = match apply_doc_updates(
                    &state,
                    SpaceType::Workspace,
                    &payload.space_id,
                    &payload.doc_id,
                    vec![update_bytes.clone()],
                    UpdateBroadcastContext {
                        editor_id: Some(user.user_id.as_str()),
                        editor_user: Some(&user.user),
                    },
                )
                .await
                {
                    Ok(result) => result,
                    Err(err) => {
                        ack_error::<PushDocUpdateResponse>(ack, err, Some(&request.request_id));
                        return;
                    }
                };

                access.metadata.updated_at = cache_result.timestamp;
                doc_access_cache
                    .insert(&payload.space_id, &payload.doc_id, access)
                    .await;

                let timestamp = Some(cache_result.timestamp);
                emit_doc_update_events(
                    &socket,
                    payload.space_type,
                    &payload.space_id,
                    &payload.doc_id,
                    &update_bytes,
                    timestamp,
                    &user,
                )
                .await;
                ack_doc_update(ack, timestamp);
            }
            SpaceType::Userspace => {
                if let Err(err) = ensure_userspace_owner(&user, &payload.space_id) {
                    ack_error::<PushDocUpdateResponse>(ack, err, Some(&request.request_id));
                    return;
                }

                let cache_result = match apply_doc_updates(
                    &state,
                    SpaceType::Userspace,
                    &payload.space_id,
                    &payload.doc_id,
                    vec![update_bytes.clone()],
                    UpdateBroadcastContext {
                        editor_id: Some(user.user_id.as_str()),
                        editor_user: Some(&user.user),
                    },
                )
                .await
                {
                    Ok(result) => result,
                    Err(err) => {
                        ack_error::<PushDocUpdateResponse>(ack, err, Some(&request.request_id));
                        return;
                    }
                };

                let timestamp = Some(cache_result.timestamp);
                emit_doc_update_events(
                    &socket,
                    payload.space_type,
                    &payload.space_id,
                    &payload.doc_id,
                    &update_bytes,
                    timestamp,
                    &user,
                )
                .await;
                ack_doc_update(ack, timestamp);
            }
        }
    }
    .instrument(span)
    .await;
}

async fn handle_space_push_doc_updates(
    socket: SocketRef,
    Data(payload): Data<PushDocUpdatesRequest>,
    ack: AckSender,
    Extension(user): Extension<SocketUserContext>,
    Extension(request): Extension<SocketRequestContext>,
    Extension(doc_access_cache): Extension<SocketDocAccessCache>,
    State(state): State<AppState>,
) {
    let span = start_socket_span(
        "space:push-doc-updates",
        &socket,
        Some(&user),
        Some((payload.space_type, payload.space_id.as_str())),
        Some(payload.doc_id.as_str()),
        Some(request.request_id.as_str()),
    );

    let PushDocUpdatesRequest {
        space_type,
        space_id,
        doc_id,
        updates,
    } = payload;

    async move {
        if updates.is_empty() {
            ack_error::<PushDocUpdateResponse>(
                ack,
                AppError::bad_request("updates payload must not be empty"),
                Some(&request.request_id),
            );
            return;
        }

        if let Err(err) = ensure_socket_joined_room(&socket, space_type, &space_id) {
            ack_error::<PushDocUpdateResponse>(ack, err, Some(&request.request_id));
            return;
        }

        let mut update_bytes = Vec::with_capacity(updates.len());
        for encoded in &updates {
            match BASE64.decode(encoded.as_bytes()) {
                Ok(bytes) => update_bytes.push(bytes),
                Err(_) => {
                    ack_error::<PushDocUpdateResponse>(
                        ack,
                        AppError::bad_request("invalid update payload"),
                        Some(&request.request_id),
                    );
                    return;
                }
            }
        }

        match space_type {
            SpaceType::Workspace => {
                let mut access = match resolve_doc_access_cached(
                    &state,
                    &doc_access_cache,
                    &user,
                    &space_id,
                    &doc_id,
                )
                .await
                {
                    Ok(access) => access,
                    Err(err) => {
                        ack_error::<PushDocUpdateResponse>(ack, err, Some(&request.request_id));
                        return;
                    }
                };

                if access.metadata.blocked {
                    ack_error::<PushDocUpdateResponse>(
                        ack,
                        AppError::doc_update_blocked(&space_id, &doc_id),
                        Some(&request.request_id),
                    );
                    return;
                }

                if let Err(err) = ensure_doc_update_permission(&access) {
                    ack_error::<PushDocUpdateResponse>(ack, err, Some(&request.request_id));
                    return;
                }

                let cache_result = match state
                    .doc_cache
                    .clone()
                    .apply_updates(
                        SpaceType::Workspace,
                        &space_id,
                        &doc_id,
                        update_bytes.clone(),
                        Some(user.user_id.as_str()),
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(err) => {
                        ack_error::<PushDocUpdateResponse>(
                            ack,
                            AppError::from_anyhow(err),
                            Some(&request.request_id),
                        );
                        return;
                    }
                };

                access.metadata.updated_at = cache_result.timestamp;
                doc_access_cache.insert(&space_id, &doc_id, access).await;

                let timestamp = Some(cache_result.timestamp);
                let channel_key = doc_channel_key(&space_id, &doc_id);
                state.sync_hub.publish_updates(
                    &channel_key,
                    &update_bytes,
                    Some(SocketBroadcastMeta::new(
                        space_type,
                        space_id.clone(),
                        doc_id.clone(),
                        Some(user.user_id.clone()),
                        Some(user.user.clone()),
                        timestamp,
                    )),
                );
                state
                    .sync_hub
                    .publish_snapshot(&channel_key, cache_result.snapshot.clone());

                let sync_room = space_room_name(space_type, &space_id, RoomKind::Sync);
                for encoded in &updates {
                    let mut payload_json = json!({
                        "spaceType": space_type,
                        "spaceId": space_id,
                        "docId": doc_id,
                        "update": encoded,
                        "timestamp": timestamp,
                    });
                    attach_editor_metadata(&mut payload_json, &user);

                    if let Err(err) = socket
                        .broadcast()
                        .to(sync_room.clone())
                        .emit("space:broadcast-doc-update", &payload_json)
                        .await
                    {
                        warn!(?err, "failed to emit broadcast update");
                    }
                }

                let mut legacy_payload = json!({
                    "spaceType": space_type,
                    "spaceId": space_id,
                    "docId": doc_id,
                    "updates": updates,
                    "timestamp": timestamp,
                });
                attach_editor_metadata(&mut legacy_payload, &user);

                if let Err(err) = socket
                    .broadcast()
                    .to(space_room_name(space_type, &space_id, RoomKind::LegacySync))
                    .emit("space:broadcast-doc-updates", &legacy_payload)
                    .await
                {
                    info!(?err, "failed to emit legacy doc update");
                }

                ack_ok(
                    ack,
                    PushDocUpdateResponse {
                        accepted: true,
                        timestamp,
                    },
                );
            }
            SpaceType::Userspace => {
                if let Err(err) = ensure_userspace_owner(&user, &space_id) {
                    ack_error::<PushDocUpdateResponse>(ack, err, Some(&request.request_id));
                    return;
                }

                let cache_result = match state
                    .doc_cache
                    .clone()
                    .apply_updates(
                        SpaceType::Userspace,
                        &space_id,
                        &doc_id,
                        update_bytes.clone(),
                        Some(user.user_id.as_str()),
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(err) => {
                        ack_error::<PushDocUpdateResponse>(
                            ack,
                            AppError::from_anyhow(err),
                            Some(&request.request_id),
                        );
                        return;
                    }
                };

                let timestamp = Some(cache_result.timestamp);
                let channel_key = doc_channel_key(&space_id, &doc_id);
                state.sync_hub.publish_updates(
                    &channel_key,
                    &update_bytes,
                    Some(SocketBroadcastMeta::new(
                        space_type,
                        space_id.clone(),
                        doc_id.clone(),
                        Some(user.user_id.clone()),
                        Some(user.user.clone()),
                        timestamp,
                    )),
                );
                state
                    .sync_hub
                    .publish_snapshot(&channel_key, cache_result.snapshot.clone());

                let sync_room = space_room_name(space_type, &space_id, RoomKind::Sync);
                for encoded in &updates {
                    let mut payload_json = json!({
                        "spaceType": space_type,
                        "spaceId": space_id,
                        "docId": doc_id,
                        "update": encoded,
                        "timestamp": timestamp,
                    });
                    attach_editor_metadata(&mut payload_json, &user);

                    if let Err(err) = socket
                        .broadcast()
                        .to(sync_room.clone())
                        .emit("space:broadcast-doc-update", &payload_json)
                        .await
                    {
                        warn!(?err, "failed to emit broadcast update");
                    }
                }

                let mut legacy_payload = json!({
                    "spaceType": space_type,
                    "spaceId": space_id,
                    "docId": doc_id,
                    "updates": updates,
                    "timestamp": timestamp,
                });
                attach_editor_metadata(&mut legacy_payload, &user);

                if let Err(err) = socket
                    .broadcast()
                    .to(space_room_name(space_type, &space_id, RoomKind::LegacySync))
                    .emit("space:broadcast-doc-updates", &legacy_payload)
                    .await
                {
                    info!(?err, "failed to emit legacy doc update");
                }

                ack_ok(
                    ack,
                    PushDocUpdateResponse {
                        accepted: true,
                        timestamp,
                    },
                );
            }
        }
    }
    .instrument(span)
    .await;
}

async fn emit_doc_update_events(
    socket: &SocketRef,
    space_type: SpaceType,
    space_id: &str,
    doc_id: &str,
    update_bytes: &[u8],
    timestamp: Option<i64>,
    user: &SocketUserContext,
) {
    let encoded_update = BASE64.encode(update_bytes);

    let mut broadcast_payload = json!({
        "spaceType": space_type,
        "spaceId": space_id,
        "docId": doc_id,
        "update": encoded_update.clone(),
        "timestamp": timestamp,
    });
    attach_editor_metadata(&mut broadcast_payload, user);

    if let Err(err) = socket
        .broadcast()
        .to(space_room_name(space_type, space_id, RoomKind::Sync))
        .emit("space:broadcast-doc-update", &broadcast_payload)
        .await
    {
        warn!(?err, "failed to emit broadcast update");
    }

    let mut legacy_payload = json!({
        "spaceType": space_type,
        "spaceId": space_id,
        "docId": doc_id,
        "updates": [encoded_update],
        "timestamp": timestamp,
    });
    attach_editor_metadata(&mut legacy_payload, user);

    if let Err(err) = socket
        .broadcast()
        .to(space_room_name(space_type, space_id, RoomKind::LegacySync))
        .emit("space:broadcast-doc-updates", &legacy_payload)
        .await
    {
        info!(?err, "failed to emit legacy doc update");
    }
}

fn ack_doc_update(ack: AckSender, timestamp: Option<i64>) {
    ack_ok(
        ack,
        PushDocUpdateResponse {
            accepted: true,
            timestamp,
        },
    );
}

async fn handle_space_delete_doc(
    socket: SocketRef,
    Data(payload): Data<DeleteDocRequest>,
    ack: AckSender,
    Extension(user): Extension<SocketUserContext>,
    Extension(request): Extension<SocketRequestContext>,
    Extension(doc_access_cache): Extension<SocketDocAccessCache>,
    State(state): State<AppState>,
) {
    let span = start_socket_span(
        "space:delete-doc",
        &socket,
        Some(&user),
        Some((payload.space_type, payload.space_id.as_str())),
        Some(payload.doc_id.as_str()),
        Some(request.request_id.as_str()),
    );

    async move {
        if let Err(err) = ensure_socket_joined_room(&socket, payload.space_type, &payload.space_id)
        {
            ack_error::<DeleteDocResponse>(ack, err, Some(&request.request_id));
            return;
        }

        match payload.space_type {
            SpaceType::Workspace => {
                let access = match resolve_doc_access_cached(
                    &state,
                    &doc_access_cache,
                    &user,
                    &payload.space_id,
                    &payload.doc_id,
                )
                .await
                {
                    Ok(access) => access,
                    Err(err) => {
                        ack_error::<DeleteDocResponse>(ack, err, Some(&request.request_id));
                        return;
                    }
                };

                if access.metadata.blocked {
                    ack_error::<DeleteDocResponse>(
                        ack,
                        AppError::doc_update_blocked(&payload.space_id, &payload.doc_id),
                        Some(&request.request_id),
                    );
                    return;
                }

                if let Err(err) = ensure_doc_delete_permission(&access) {
                    ack_error::<DeleteDocResponse>(ack, err, Some(&request.request_id));
                    return;
                }

                match state
                    .document_store
                    .delete_doc(&payload.space_id, &payload.doc_id)
                    .await
                {
                    Ok(true) => {
                        doc_access_cache
                            .remove(&payload.space_id, &payload.doc_id)
                            .await;
                        state
                            .doc_cache
                            .clone()
                            .invalidate(SpaceType::Workspace, &payload.space_id, &payload.doc_id)
                            .await;
                        state
                            .doc_sessions
                            .remove_doc(SpaceType::Workspace, &payload.space_id, &payload.doc_id)
                            .await;
                        state
                            .sync_hub
                            .remove_channel(&doc_channel_key(&payload.space_id, &payload.doc_id));
                        ack_ok(ack, DeleteDocResponse { success: true });
                    }
                    Ok(false) => {
                        ack_error::<DeleteDocResponse>(
                            ack,
                            AppError::doc_not_found(&payload.space_id, &payload.doc_id),
                            Some(&request.request_id),
                        );
                    }
                    Err(err) => {
                        ack_error::<DeleteDocResponse>(
                            ack,
                            AppError::from_anyhow(err),
                            Some(&request.request_id),
                        );
                    }
                }
            }
            SpaceType::Userspace => {
                if let Err(err) = ensure_userspace_owner(&user, &payload.space_id) {
                    ack_error::<DeleteDocResponse>(ack, err, Some(&request.request_id));
                    return;
                }

                match state
                    .user_doc_store
                    .delete_doc(&payload.space_id, &payload.doc_id)
                    .await
                {
                    Ok(true) => {
                        state
                            .doc_cache
                            .clone()
                            .invalidate(SpaceType::Userspace, &payload.space_id, &payload.doc_id)
                            .await;
                        state
                            .doc_sessions
                            .remove_doc(SpaceType::Userspace, &payload.space_id, &payload.doc_id)
                            .await;
                        state
                            .sync_hub
                            .remove_channel(&doc_channel_key(&payload.space_id, &payload.doc_id));
                        ack_ok(ack, DeleteDocResponse { success: true });
                    }
                    Ok(false) => ack_error::<DeleteDocResponse>(
                        ack,
                        AppError::doc_not_found(&payload.space_id, &payload.doc_id),
                        Some(&request.request_id),
                    ),
                    Err(err) => ack_error::<DeleteDocResponse>(
                        ack,
                        AppError::from_anyhow(err),
                        Some(&request.request_id),
                    ),
                }
            }
        }
    }
    .instrument(span)
    .await;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoadDocTimestampsRequest {
    space_type: SpaceType,
    space_id: String,
    #[serde(default)]
    timestamp: Option<i64>,
}

async fn handle_space_load_doc_timestamps(
    socket: SocketRef,
    Data(payload): Data<LoadDocTimestampsRequest>,
    ack: AckSender,
    Extension(user): Extension<SocketUserContext>,
    Extension(request): Extension<SocketRequestContext>,
    State(state): State<AppState>,
) {
    let span = start_socket_span(
        "space:load-doc-timestamps",
        &socket,
        Some(&user),
        Some((payload.space_type, payload.space_id.as_str())),
        None,
        Some(request.request_id.as_str()),
    );

    async move {
        if let Err(err) = ensure_socket_joined_room(&socket, payload.space_type, &payload.space_id)
        {
            ack_error::<HashMap<String, i64>>(ack, err, Some(&request.request_id));
            return;
        }

        let timestamps_result: Result<HashMap<String, i64>, AppError> = match payload.space_type {
            SpaceType::Workspace => {
                async {
                    let header_map = user.header_map()?;
                    resolve_workspace_access(&state, &header_map, &payload.space_id).await?;
                    state
                        .document_store
                        .list_doc_timestamps(&payload.space_id, payload.timestamp)
                        .await
                        .map_err(AppError::from_anyhow)
                }
                .await
            }
            SpaceType::Userspace => {
                async {
                    ensure_userspace_owner(&user, &payload.space_id)?;
                    state
                        .user_doc_store
                        .timestamps_since(&payload.space_id, payload.timestamp)
                        .await
                        .map_err(AppError::from_anyhow)
                }
                .await
            }
        };

        let timestamps = match timestamps_result {
            Ok(map) => map,
            Err(err) => {
                ack_error::<HashMap<String, i64>>(ack, err, Some(&request.request_id));
                return;
            }
        };

        debug!(
            space_type = ?payload.space_type,
            space_id = %payload.space_id,
            after = payload.timestamp,
            count = timestamps.len(),
            "socket load-doc-timestamps response"
        );

        ack_ok(ack, timestamps);
    }
    .instrument(span)
    .await;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AwarenessRequest {
    space_type: SpaceType,
    space_id: String,
    doc_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AwarenessResponse {
    client_id: String,
    success: bool,
}

async fn handle_space_join_awareness(
    socket: SocketRef,
    Data(payload): Data<AwarenessRequest>,
    ack: AckSender,
    Extension(user): Extension<SocketUserContext>,
    Extension(request): Extension<SocketRequestContext>,
    State(state): State<AppState>,
) {
    let span = start_socket_span(
        "space:join-awareness",
        &socket,
        Some(&user),
        Some((payload.space_type, payload.space_id.as_str())),
        Some(payload.doc_id.as_str()),
        Some(request.request_id.as_str()),
    );

    async move {
        if let Err(err) =
            ensure_socket_space_access(&state, &user, payload.space_type, &payload.space_id).await
        {
            ack_error::<AwarenessResponse>(ack, err, Some(&request.request_id));
            return;
        }

        socket.join(space_room_name(
            payload.space_type,
            &payload.space_id,
            RoomKind::Awareness {
                doc_id: payload.doc_id.clone(),
            },
        ));

        ack_ok(
            ack,
            AwarenessResponse {
                client_id: socket.id.to_string(),
                success: true,
            },
        );
        let doc_key = DocSessionKey::new(
            payload.space_type,
            payload.space_id.clone(),
            payload.doc_id.clone(),
        );
        let first_session = state
            .doc_sessions
            .register(&socket.id.to_string(), doc_key)
            .await;
        if first_session {
            if let Err(err) = state
                .doc_cache
                .open_session(payload.space_type, &payload.space_id, &payload.doc_id)
                .await
            {
                warn!(
                    space_type = ?payload.space_type,
                    space_id = %payload.space_id,
                    doc_id = %payload.doc_id,
                    error = %err,
                    "failed to open doc cache session from awareness join",
                );
            }
        }
    }
    .instrument(span)
    .await;
}

async fn handle_space_leave_awareness(
    socket: SocketRef,
    Data(payload): Data<AwarenessRequest>,
    ack: AckSender,
    Extension(request): Extension<SocketRequestContext>,
    State(state): State<AppState>,
) {
    let span = start_socket_span(
        "space:leave-awareness",
        &socket,
        None,
        Some((payload.space_type, payload.space_id.as_str())),
        Some(payload.doc_id.as_str()),
        Some(request.request_id.as_str()),
    );

    async move {
        if let Err(err) = ensure_socket_joined_awareness(
            &socket,
            payload.space_type,
            &payload.space_id,
            &payload.doc_id,
        ) {
            ack_error::<AwarenessResponse>(ack, err, Some(&request.request_id));
            return;
        }

        socket.leave(space_room_name(
            payload.space_type,
            &payload.space_id,
            RoomKind::Awareness {
                doc_id: payload.doc_id.clone(),
            },
        ));

        ack_ok(
            ack,
            AwarenessResponse {
                client_id: socket.id.to_string(),
                success: true,
            },
        );

        if state
            .doc_sessions
            .remove_doc(payload.space_type, &payload.space_id, &payload.doc_id)
            .await
        {
            let key = DocSessionKey::new(
                payload.space_type,
                payload.space_id.clone(),
                payload.doc_id.clone(),
            );
            finalize_doc_session(&state, &key).await;
        }
    }
    .instrument(span)
    .await;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateAwarenessRequest {
    space_type: SpaceType,
    space_id: String,
    doc_id: String,
    awareness_update: String,
}

async fn handle_space_update_awareness(
    socket: SocketRef,
    Data(payload): Data<UpdateAwarenessRequest>,
    ack: AckSender,
    Extension(request): Extension<SocketRequestContext>,
    State(state): State<AppState>,
) {
    let span = start_socket_span(
        "space:update-awareness",
        &socket,
        None,
        Some((payload.space_type, payload.space_id.as_str())),
        Some(payload.doc_id.as_str()),
        Some(request.request_id.as_str()),
    );

    async move {
        if let Err(err) = ensure_socket_joined_awareness(
            &socket,
            payload.space_type,
            &payload.space_id,
            &payload.doc_id,
        ) {
            ack_error::<serde_json::Value>(ack, err, Some(&request.request_id));
            return;
        }

        state.socket_metrics.inc_awareness_messages();

        let room = space_room_name(
            payload.space_type,
            &payload.space_id,
            RoomKind::Awareness {
                doc_id: payload.doc_id.clone(),
            },
        );

        let payload_json = serde_json::json!({
            "spaceType": payload.space_type,
            "spaceId": payload.space_id,
            "docId": payload.doc_id,
            "awarenessUpdate": payload.awareness_update,
        });

        if let Err(err) = socket
            .broadcast()
            .to(room)
            .emit("space:broadcast-awareness-update", &payload_json)
            .await
        {
            warn!(?err, "failed to broadcast awareness update");
        }

        ack_ok(ack, serde_json::json!({}));
    }
    .instrument(span)
    .await;
}

async fn handle_space_load_awarenesses(
    socket: SocketRef,
    Data(payload): Data<AwarenessRequest>,
    ack: AckSender,
    Extension(request): Extension<SocketRequestContext>,
) {
    let span = start_socket_span(
        "space:load-awarenesses",
        &socket,
        None,
        Some((payload.space_type, payload.space_id.as_str())),
        Some(payload.doc_id.as_str()),
        Some(request.request_id.as_str()),
    );

    async move {
        if let Err(err) = ensure_socket_joined_awareness(
            &socket,
            payload.space_type,
            &payload.space_id,
            &payload.doc_id,
        ) {
            ack_error::<serde_json::Value>(ack, err, Some(&request.request_id));
            return;
        }

        let room = space_room_name(
            payload.space_type,
            &payload.space_id,
            RoomKind::Awareness {
                doc_id: payload.doc_id.clone(),
            },
        );

        let message = serde_json::json!({
            "spaceType": payload.space_type,
            "spaceId": payload.space_id,
            "docId": payload.doc_id,
        });

        let broadcast_room = room.clone();
        if let Err(err) = socket
            .broadcast()
            .to(broadcast_room.clone())
            .emit("space:collect-awareness", &message)
            .await
        {
            warn!(?err, "failed to request awareness collection");
        }

        if matches!(payload.space_type, SpaceType::Workspace) {
            if let Err(err) = socket
                .broadcast()
                .to(broadcast_room)
                .emit("new-client-awareness-init", &serde_json::json!({}))
                .await
            {
                info!(?err, "failed to emit legacy awareness init");
            }
        }

        ack_ok(
            ack,
            serde_json::json!({
                "clientId": socket.id.to_string()
            }),
        );
    }
    .instrument(span)
    .await;
}

async fn handle_disconnect(socket: SocketRef, State(state): State<AppState>) {
    let socket_id = socket.id.to_string();
    socket.extensions.remove::<SocketSpanRegistry>();
    let stale_docs = state.doc_sessions.remove_all(&socket_id).await;
    for key in stale_docs {
        finalize_doc_session(&state, &key).await;
    }
    state.socket_metrics.dec_connections();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::generate_password_hash;
    use barffine_core::{config::AppConfig, db::Database, doc_store::DocumentMetadata};
    use tempfile::TempDir;

    async fn setup_state() -> (TempDir, Database, AppState) {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut config = AppConfig::default();
        let db_path = temp_dir.path().join("test.db");
        config.database_path = db_path.to_string_lossy().into_owned();

        let database = Database::connect(&config).await.expect("connect database");
        crate::utils::db::run_migrations(database.pool())
            .await
            .expect("apply migrations");
        let state = crate::build_state(&database);
        state
            .workspace_store
            .normalize_member_statuses()
            .await
            .expect("normalize member statuses");
        (temp_dir, database, state)
    }

    fn make_metadata() -> DocumentMetadata {
        DocumentMetadata {
            id: "doc-1".into(),
            workspace_id: "ws-1".into(),
            created_at: 0,
            updated_at: 0,
            default_role: "reader".into(),
            public: false,
            blocked: false,
            mode: "page".into(),
            title: None,
            summary: None,
            creator_id: None,
            updater_id: None,
            share_token: None,
            trashed_at: None,
            trashed_by: None,
            snapshot: None,
        }
    }

    #[test]
    fn doc_update_permission_allows_editor() {
        let access = RestDocAccess {
            metadata: make_metadata(),
            user: None,
            set_cookies: Vec::new(),
            workspace_role: Some(crate::graphql::Permission::Collaborator),
            doc_role: Some(crate::graphql::DocRole::Editor),
        };

        assert!(ensure_doc_update_permission(&access).is_ok());
    }

    #[test]
    fn doc_update_permission_blocks_reader() {
        let access = RestDocAccess {
            metadata: make_metadata(),
            user: None,
            set_cookies: Vec::new(),
            workspace_role: Some(crate::graphql::Permission::External),
            doc_role: Some(crate::graphql::DocRole::Reader),
        };

        let err = ensure_doc_update_permission(&access).expect_err("expected rejection");
        assert_eq!(err.to_string(), "Doc.Update permission required");
    }

    #[test]
    fn doc_delete_permission_allows_editor() {
        let access = RestDocAccess {
            metadata: make_metadata(),
            user: None,
            set_cookies: Vec::new(),
            workspace_role: Some(crate::graphql::Permission::Collaborator),
            doc_role: Some(crate::graphql::DocRole::Editor),
        };

        assert!(ensure_doc_delete_permission(&access).is_ok());
    }

    #[test]
    fn doc_delete_permission_blocks_commenter() {
        let access = RestDocAccess {
            metadata: make_metadata(),
            user: None,
            set_cookies: Vec::new(),
            workspace_role: Some(crate::graphql::Permission::Collaborator),
            doc_role: Some(crate::graphql::DocRole::Commenter),
        };

        let err = ensure_doc_delete_permission(&access).expect_err("expected rejection");
        assert_eq!(err.to_string(), "Doc.Delete permission required");
    }

    #[tokio::test]
    async fn emit_workspace_embedding_pushes_event() {
        let (_temp_dir, _database, state) = setup_state().await;
        let mut rx = state.subscribe_workspace_embedding();

        state.emit_workspace_embedding("workspace-embedding", Some(true));
        let event = rx
            .recv()
            .await
            .expect("receive emitted workspace embedding event");

        assert_eq!(event.workspace_id, "workspace-embedding");
        assert_eq!(event.enable_doc_embedding, Some(true));
    }

    #[tokio::test]
    async fn doc_switching_keeps_sessions_and_cache_until_space_leave() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("switcher@example.com", &password_hash, Some("Switcher"))
            .await
            .expect("create user");
        let workspace = state
            .workspace_store
            .create(&user.id, Some("Switching Space"), None, None, None, None)
            .await
            .expect("create workspace");

        let socket_id = "socket-switcher";
        let doc_total = 12;
        for idx in 0..doc_total {
            let doc_id = format!("doc-{idx}");
            state
                .document_store
                .ensure_doc_record(
                    &workspace.id,
                    &doc_id,
                    &user.id,
                    Some(&format!("Doc {idx}")),
                )
                .await
                .expect("ensure doc record");
            state
                .doc_cache
                .snapshot(SpaceType::Workspace, &workspace.id, &doc_id)
                .await
                .expect("cache snapshot");
            let first_session = state
                .doc_sessions
                .register(
                    socket_id,
                    DocSessionKey::new(SpaceType::Workspace, workspace.id.clone(), doc_id.clone()),
                )
                .await;
            if first_session {
                state
                    .doc_cache
                    .open_session(SpaceType::Workspace, &workspace.id, &doc_id)
                    .await
                    .expect("open doc cache session");
            }
        }

        assert_eq!(state.doc_sessions.tracked_doc_count(), doc_total);
        let cache_entries = state.doc_cache.debug_entry_count().await;
        assert_eq!(cache_entries, doc_total as u64);

        for idx in (0..doc_total).rev() {
            let doc_id = format!("doc-{idx}");
            state
                .doc_cache
                .snapshot(SpaceType::Workspace, &workspace.id, &doc_id)
                .await
                .expect("refresh snapshot");
        }

        assert_eq!(state.doc_sessions.tracked_doc_count(), doc_total);
        assert_eq!(state.doc_cache.debug_entry_count().await, doc_total as u64);

        for idx in 0..doc_total {
            let doc_id = format!("doc-{idx}");
            let removed = state
                .doc_sessions
                .remove_doc(SpaceType::Workspace, &workspace.id, &doc_id)
                .await;
            assert!(removed, "doc should be removed on awareness leave");
            let key =
                DocSessionKey::new(SpaceType::Workspace, workspace.id.clone(), doc_id.clone());
            finalize_doc_session(&state, &key).await;
        }

        assert_eq!(state.doc_sessions.tracked_doc_count(), 0);
        assert_eq!(state.doc_cache.debug_entry_count().await, 0);
    }
}
