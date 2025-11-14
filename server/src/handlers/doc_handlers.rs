// Document handlers

use anyhow::Error as AnyError;
use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{CONTENT_TYPE, HeaderName},
    },
    response::{IntoResponse, Response},
};
use barffine_core::blob::BlobDescriptor;
use y_octo::{Doc as YoctoDoc, StateVector as YoctoStateVector};

use crate::{
    auth::{DocAccessIntent, RpcAccessRequirement, parse_history_timestamp, resolve_doc_access},
    doc::{
        channels::comment_attachment_blob_key, history, metadata as doc_metadata,
        mode::DocPublishMode, sync::workspace_snapshot_or_not_found,
    },
    error::AppError,
    handlers::headers::{
        HEADER_DOC_ID, HEADER_DOC_ROLE, HEADER_PUBLISH_MODE, HEADER_USER_ID, HEADER_WORKSPACE_ID,
        HEADER_WORKSPACE_ROLE, doc_role_header_value, permission_header_value,
    },
    handlers::workspace_handlers::ensure_workspace_exists,
    http::{append_set_cookie_headers, http_date_from_datetime},
    socket::rooms::SpaceType,
    state::AppState,
    types::{
        AuthenticatedRestSession, DocContentQuery, DocContentResponse, DocMarkdownResponse,
        DocumentHistoryItem, DocumentMetadataResponse, HistoryQuery, PublishDocRequest,
        RestDocAccess,
    },
    utils::{
        attachments::apply_attachment_headers,
        crdt::{decode_state_vector, encode_state_vector},
    },
};

pub(crate) async fn get_doc_content_handler(
    Path((workspace_id, doc_id)): Path<(String, String)>,
    State(state): State<AppState>,
    Query(query): Query<DocContentQuery>,
) -> Result<impl IntoResponse, AppError> {
    let response = build_doc_content_response(&state, &workspace_id, &doc_id, &query).await?;
    Ok(Json(response))
}

pub(crate) async fn get_comment_attachment_handler(
    Path((workspace_id, doc_id, key)): Path<(String, String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let access = resolve_doc_access(
        &state,
        &headers,
        &workspace_id,
        &doc_id,
        RpcAccessRequirement::Optional,
        DocAccessIntent::RequireAuthenticatedRead,
    )
    .await?;

    let descriptor = BlobDescriptor::new(&workspace_id, comment_attachment_blob_key(&doc_id, &key));

    let download = state
        .blob_store
        .get(&descriptor, true)
        .await
        .map_err(AppError::from_anyhow)?
        .ok_or_else(|| AppError::comment_attachment_not_found(&workspace_id, &doc_id, &key))?;

    if let Some(location) = download.location {
        let response = Response::builder()
            .status(StatusCode::FOUND)
            .header("location", location.uri)
            .body(Body::empty())
            .map_err(|err| AppError::internal(AnyError::new(err)))?;

        let response = finalize_doc_response(response, &access)?;
        return Ok(response);
    }

    let bytes = download
        .bytes
        .ok_or_else(|| AppError::comment_attachment_not_found(&workspace_id, &doc_id, &key))?;

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(bytes))
        .map_err(|err| AppError::internal(AnyError::new(err)))?;

    if let Some(metadata) = download.metadata.as_ref() {
        if let Some(length) = metadata.content_length {
            let value = HeaderValue::from_str(&length.to_string())
                .map_err(|err| AppError::internal(AnyError::new(err)))?;
            response
                .headers_mut()
                .insert(HeaderName::from_static("content-length"), value);
        }

        if let Some(last_modified) = metadata.last_modified {
            if let Some(formatted) = http_date_from_datetime(&last_modified) {
                let value = HeaderValue::from_str(&formatted)
                    .map_err(|err| AppError::internal(AnyError::new(err)))?;
                response
                    .headers_mut()
                    .insert(HeaderName::from_static("last-modified"), value);
            }
        }

        apply_attachment_headers(&mut response, Some(metadata), &key)?;
    } else {
        apply_attachment_headers(&mut response, None, &key)?;
    }

    response.headers_mut().insert(
        HeaderName::from_static("cache-control"),
        HeaderValue::from_static("private, max-age=2592000, immutable"),
    );

    let response = finalize_doc_response(response, &access)?;
    Ok(response)
}

pub(crate) async fn get_doc_markdown_handler(
    Path((workspace_id, doc_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
    let response = build_doc_markdown_response(&state, &workspace_id, &doc_id).await?;
    Ok(Json(response))
}

pub(crate) async fn get_doc_diff_handler(
    Path((workspace_id, doc_id)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    doc_diff_response(
        &state,
        &headers,
        &workspace_id,
        &doc_id,
        RpcAccessRequirement::Optional,
        DocAccessIntent::Standard,
        body,
    )
    .await
}

pub(crate) async fn get_doc_binary_handler(
    Path((workspace_id, doc_id)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    doc_binary_response(
        &state,
        &headers,
        &workspace_id,
        &doc_id,
        RpcAccessRequirement::Optional,
        DocAccessIntent::Standard,
    )
    .await
}

pub(crate) async fn get_doc_history_handler(
    Path((workspace_id, doc_id)): Path<(String, String)>,
    Query(query): Query<HistoryQuery>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    ensure_workspace_exists(&state, &workspace_id).await?;

    let access = resolve_doc_access(
        &state,
        &headers,
        &workspace_id,
        &doc_id,
        RpcAccessRequirement::Optional,
        DocAccessIntent::RequireAuthenticatedRead,
    )
    .await?;

    let history =
        history::fetch_history_records(&state, &workspace_id, &doc_id, query.limit).await?;

    let items = history
        .into_iter()
        .map(DocumentHistoryItem::from)
        .collect::<Vec<_>>();

    let response = Json(items).into_response();
    let response = finalize_doc_response(response, &access)?;
    Ok(response)
}

pub(crate) async fn get_doc_history_snapshot_handler(
    Path((workspace_id, doc_id, timestamp)): Path<(String, String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    ensure_workspace_exists(&state, &workspace_id).await?;

    let access = resolve_doc_access(
        &state,
        &headers,
        &workspace_id,
        &doc_id,
        RpcAccessRequirement::Optional,
        DocAccessIntent::RequireAuthenticatedRead,
    )
    .await?;
    let timestamp = parse_history_timestamp(&timestamp)?;

    let snapshot = state
        .document_store
        .fetch_history_as_of(&workspace_id, &doc_id, timestamp)
        .await
        .map_err(AppError::from_anyhow)?
        .ok_or_else(|| AppError::not_found("history not found"))?;

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/octet-stream")
        .header("content-length", snapshot.len().to_string());

    if access.metadata.public {
        if let Ok(mode) = HeaderValue::from_str(access.metadata.mode.as_str()) {
            builder = builder.header(HeaderName::from_static(HEADER_PUBLISH_MODE), mode);
        }
    }

    let response = builder
        .body(Body::from(snapshot))
        .map_err(|err| AppError::internal(AnyError::new(err)))?;

    let response = finalize_doc_response(response, &access)?;
    Ok(response)
}

async fn build_doc_content_response(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
    query: &DocContentQuery,
) -> Result<DocContentResponse, AppError> {
    let metadata = doc_metadata::fetch_required(state, workspace_id, doc_id).await?;
    let wants_full = query.full.as_deref() == Some("true");

    let content = state
        .doc_cache
        .doc_content_view(SpaceType::Workspace, workspace_id, doc_id, wants_full)
        .await
        .map_err(AppError::from_anyhow)?;

    let title = prefer_or_metadata(content.title, metadata.title.clone());
    let summary = prefer_or_metadata(content.summary, metadata.summary.clone());

    Ok(DocContentResponse { title, summary })
}

async fn build_doc_markdown_response(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
) -> Result<DocMarkdownResponse, AppError> {
    let metadata = doc_metadata::fetch_required(state, workspace_id, doc_id).await?;
    let markdown = state
        .doc_cache
        .doc_markdown_view(SpaceType::Workspace, workspace_id, doc_id)
        .await
        .map_err(AppError::from_anyhow)?;
    let title = prefer_or_metadata(markdown.title.clone(), metadata.title.clone());
    let markdown_text = prefer_or_metadata(markdown.markdown, metadata.summary.clone());

    Ok(DocMarkdownResponse {
        title,
        markdown: markdown_text,
    })
}

async fn doc_binary_response(
    state: &AppState,
    headers: &HeaderMap,
    workspace_id: &str,
    doc_id: &str,
    rpc_requirement: RpcAccessRequirement,
    intent: DocAccessIntent,
) -> Result<Response, AppError> {
    ensure_workspace_exists(state, workspace_id).await?;

    let access = resolve_doc_access(
        state,
        headers,
        workspace_id,
        doc_id,
        rpc_requirement,
        intent,
    )
    .await?;

    let (snapshot, updated_at) =
        workspace_snapshot_or_not_found(state, workspace_id, doc_id).await?;

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/octet-stream")
        .header("content-length", snapshot.len().to_string());

    if access.metadata.public {
        if let Ok(mode) = HeaderValue::from_str(access.metadata.mode.as_str()) {
            builder = builder.header(HeaderName::from_static(HEADER_PUBLISH_MODE), mode);
        }
    }

    let body = Body::from(snapshot);
    let mut response = builder
        .body(body)
        .map_err(|err| AppError::internal(AnyError::new(err)))?;
    response.headers_mut().insert(
        HeaderName::from_static("x-doc-timestamp"),
        HeaderValue::from_str(&updated_at.to_string())
            .map_err(|err| AppError::internal(AnyError::new(err)))?,
    );

    let response = finalize_doc_response(response, &access)?;
    Ok(response)
}

fn prefer_or_metadata(value: String, fallback: Option<String>) -> String {
    if value.trim().is_empty() {
        fallback.unwrap_or_default()
    } else {
        value
    }
}

async fn doc_diff_response(
    state: &AppState,
    headers: &HeaderMap,
    workspace_id: &str,
    doc_id: &str,
    rpc_requirement: RpcAccessRequirement,
    intent: DocAccessIntent,
    body: Bytes,
) -> Result<Response, AppError> {
    let access = resolve_doc_access(
        state,
        headers,
        workspace_id,
        doc_id,
        rpc_requirement,
        intent,
    )
    .await?;

    let (snapshot, updated_at) =
        workspace_snapshot_or_not_found(state, workspace_id, doc_id).await?;

    let doc = YoctoDoc::try_from_binary_v1(&snapshot)
        .map_err(|err| AppError::internal(AnyError::new(err)))?;

    let state_vector = if body.is_empty() {
        YoctoStateVector::default()
    } else {
        decode_state_vector(&body)?
    };

    let missing = doc
        .encode_state_as_update_v1(&state_vector)
        .map_err(|err| AppError::internal(AnyError::new(err)))?;
    let new_state = doc.get_state_vector();
    let state_bytes = encode_state_vector(&new_state)?;

    let mut payload = missing.clone();
    payload.extend_from_slice(&state_bytes);

    let mut response = Response::new(Body::from(payload));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-doc-timestamp"),
        HeaderValue::from_str(&updated_at.to_string())
            .map_err(|err| AppError::internal(AnyError::new(err)))?,
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-doc-missing-offset"),
        HeaderValue::from_str(&format!("0,{}", missing.len()))
            .map_err(|err| AppError::internal(AnyError::new(err)))?,
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-doc-state-offset"),
        HeaderValue::from_str(&format!(
            "{},{}",
            missing.len(),
            missing.len() + state_bytes.len()
        ))
        .map_err(|err| AppError::internal(AnyError::new(err)))?,
    );

    let response = finalize_doc_response(response, &access)?;
    Ok(response)
}

fn finalize_doc_response(
    mut response: Response,
    access: &RestDocAccess,
) -> Result<Response, AppError> {
    append_doc_access_headers(&mut response, access)?;
    append_set_cookie_headers(&mut response, &access.set_cookies)?;
    Ok(response)
}

fn append_doc_access_headers(
    response: &mut Response,
    access: &RestDocAccess,
) -> Result<(), AppError> {
    let headers = response.headers_mut();
    headers.insert(
        HeaderName::from_static(HEADER_WORKSPACE_ID),
        HeaderValue::from_str(&access.metadata.workspace_id)
            .map_err(|err| AppError::internal(AnyError::new(err)))?,
    );
    headers.insert(
        HeaderName::from_static(HEADER_DOC_ID),
        HeaderValue::from_str(&access.metadata.id)
            .map_err(|err| AppError::internal(AnyError::new(err)))?,
    );

    if let Some(user) = &access.user {
        headers.insert(
            HeaderName::from_static(HEADER_USER_ID),
            HeaderValue::from_str(&user.id)
                .map_err(|err| AppError::internal(AnyError::new(err)))?,
        );
    }

    if let Some(role) = access.workspace_role {
        headers.insert(
            HeaderName::from_static(HEADER_WORKSPACE_ROLE),
            HeaderValue::from_static(permission_header_value(role)),
        );
    }

    if let Some(role) = access.doc_role {
        headers.insert(
            HeaderName::from_static(HEADER_DOC_ROLE),
            HeaderValue::from_static(doc_role_header_value(role)),
        );
    }

    Ok(())
}

async fn require_doc_publish_permission(
    state: &AppState,
    headers: &HeaderMap,
    workspace_id: &str,
    doc_id: &str,
) -> Result<AuthenticatedRestSession, AppError> {
    require_doc_permission_rest(
        state,
        headers,
        workspace_id,
        doc_id,
        |perms| perms.can_publish_doc(),
        "Doc.Publish permission required",
    )
    .await
}

async fn require_doc_permission_rest<F>(
    state: &AppState,
    headers: &HeaderMap,
    workspace_id: &str,
    doc_id: &str,
    predicate: F,
    error_message: &'static str,
) -> Result<AuthenticatedRestSession, AppError>
where
    F: FnOnce(&crate::graphql::DocPermissions) -> bool,
{
    let auth = state
        .user_service
        .authenticate_rest_request(headers)
        .await?;

    let workspace = state
        .workspace_service
        .fetch_workspace(workspace_id)
        .await?;
    let metadata = doc_metadata::fetch_required(state, workspace_id, doc_id).await?;

    let authorization = state
        .doc_access_service
        .resolve_doc_authorization(&workspace, &metadata, Some(auth.user.id.as_str()))
        .await?;

    authorization.ensure_permission(predicate, error_message)?;
    Ok(auth)
}

pub(crate) async fn publish_doc_handler(
    Path((workspace_id, doc_id)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<PublishDocRequest>,
) -> Result<Response, AppError> {
    let auth = require_doc_publish_permission(&state, &headers, &workspace_id, &doc_id).await?;

    let doc_mode = payload
        .mode
        .as_deref()
        .map(DocPublishMode::from_str)
        .unwrap_or(Some(DocPublishMode::Page))
        .ok_or_else(|| AppError::bad_request("invalid publish mode"))?;

    let metadata = doc_metadata::publish_doc(&state, &workspace_id, &doc_id, doc_mode).await?;

    let mut response = Json(DocumentMetadataResponse::from(metadata)).into_response();
    append_set_cookie_headers(&mut response, &auth.set_cookies)?;
    Ok(response)
}

pub(crate) async fn unpublish_doc_handler(
    Path((workspace_id, doc_id)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let auth = require_doc_publish_permission(&state, &headers, &workspace_id, &doc_id).await?;

    let metadata = doc_metadata::unpublish_doc(&state, &workspace_id, &doc_id).await?;

    let mut response = Json(DocumentMetadataResponse::from(metadata)).into_response();
    append_set_cookie_headers(&mut response, &auth.set_cookies)?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json,
        body::{Bytes, to_bytes},
        extract::{Path, Query, State},
        http::{
            HeaderMap, HeaderValue, StatusCode,
            header::{CONTENT_TYPE, COOKIE, HeaderName},
        },
    };
    use barffine_core::{
        blob::{BlobDescriptor, BlobMetadata},
        comment_attachment::CommentAttachmentUpsert,
        db::doc_repo::{HistorySnapshotInsert, ReplaceDocSnapshotParams},
    };
    use chrono::Utc;
    use serde_json::Value as JsonValue;
    use uuid::Uuid;
    use y_octo::Doc as YoctoDoc;

    use crate::{
        AppState,
        auth::generate_password_hash,
        cookies::{SESSION_COOKIE_NAME, USER_COOKIE_NAME},
        doc::{
            channels::comment_attachment_blob_key,
            content::{parse_doc_content, parse_doc_markdown},
            paths::public_doc_share_path,
        },
        testing::{
            fixture_snapshot, insert_document, persist_snapshot, seed_workspace, setup_state,
        },
        types::{DocContentQuery, PublishDocRequest},
        utils::crdt::encode_state_vector,
    };

    fn session_cookie_value(session_id: &str, user_id: Option<&str>) -> HeaderValue {
        let value = if let Some(user_id) = user_id {
            format!(
                "{}={}; {}={}",
                SESSION_COOKIE_NAME, session_id, USER_COOKIE_NAME, user_id
            )
        } else {
            format!("{}={}", SESSION_COOKIE_NAME, session_id)
        };
        HeaderValue::from_str(&value).expect("cookie header")
    }

    #[tokio::test]
    async fn get_comment_attachment_handler_returns_blob() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = "doc-attachment".to_string();
        insert_document(&database, &workspace_id, &doc_id, true, "page").await;

        let payload = b"attachment".to_vec();
        let attachment_key = "note.bin".to_string();
        state
            .blob_store
            .put(
                &BlobDescriptor::new(
                    &workspace_id,
                    comment_attachment_blob_key(&doc_id, &attachment_key),
                ),
                &payload,
                BlobMetadata {
                    content_type: Some("application/octet-stream".into()),
                    ..Default::default()
                },
            )
            .await
            .expect("store attachment blob");

        state
            .comment_attachment_store
            .upsert(CommentAttachmentUpsert {
                workspace_id: &workspace_id,
                doc_id: &doc_id,
                key: &attachment_key,
                name: "note.bin",
                mime: "application/octet-stream",
                size: payload.len() as i64,
                created_by: Some(&owner_id),
            })
            .await
            .expect("upsert attachment metadata");

        let session = state
            .user_store
            .create_session(&owner_id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie_value(&session.id, None));

        let response = get_comment_attachment_handler(
            Path((workspace_id.clone(), doc_id.clone(), attachment_key.clone())),
            State(state.clone()),
            headers,
        )
        .await
        .expect("attachment response");

        assert_eq!(response.status(), StatusCode::OK);
        let (parts, body) = response.into_parts();
        assert_eq!(
            parts
                .headers
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/octet-stream")
        );
        assert_eq!(
            parts
                .headers
                .get(HeaderName::from_static("cache-control"))
                .and_then(|value| value.to_str().ok()),
            Some("private, max-age=2592000, immutable")
        );

        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        assert_eq!(bytes.as_ref(), &payload);
    }

    #[tokio::test]
    async fn get_doc_content_handler_returns_summary() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, _owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let updated_default_role = state
            .document_store
            .update_default_role(&workspace_id, &doc_id, "reader")
            .await
            .expect("set default doc role");
        assert!(updated_default_role);

        let snapshot = fixture_snapshot("test-doc.snapshot.bin");
        persist_snapshot(&database, &workspace_id, &doc_id, snapshot.clone()).await;
        let expected = parse_doc_content(&snapshot, false).expect("parse snapshot");

        let response = get_doc_content_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            Query(DocContentQuery { full: None }),
        )
        .await
        .expect("doc content response")
        .into_response();

        let (_parts, body) = response.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["title"], expected.title);
        assert_eq!(json["summary"], expected.summary);
    }

    #[tokio::test]
    async fn get_doc_content_handler_returns_not_found() {
        let (_temp_dir, _database, state) = setup_state().await;
        let err = match get_doc_content_handler(
            Path(("missing-workspace".into(), "missing-doc".into())),
            State(state.clone()),
            Query(DocContentQuery { full: None }),
        )
        .await
        {
            Ok(_) => panic!("expected missing doc to return error"),
            Err(err) => err,
        };
        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(payload.name, "DOC_NOT_FOUND");
    }

    #[tokio::test]
    async fn get_doc_content_handler_honors_full_query() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, _owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let snapshot = fixture_snapshot("test-doc-with-blob.snapshot.bin");
        persist_snapshot(&database, &workspace_id, &doc_id, snapshot.clone()).await;
        let partial_expected = parse_doc_content(&snapshot, false).expect("parse partial snapshot");
        let full_expected = parse_doc_content(&snapshot, true).expect("parse full snapshot");

        let baseline = get_doc_content_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            Query(DocContentQuery { full: None }),
        )
        .await
        .expect("content response")
        .into_response();

        let (_, body) = baseline.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["title"], partial_expected.title);
        assert_eq!(json["summary"], partial_expected.summary);

        let full_response = get_doc_content_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            Query(DocContentQuery {
                full: Some("true".to_string()),
            }),
        )
        .await
        .expect("full content response")
        .into_response();

        let (_, body) = full_response.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["title"], full_expected.title);
        assert_eq!(json["summary"], full_expected.summary);
    }

    #[tokio::test]
    async fn get_doc_markdown_handler_returns_stub() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, _owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let snapshot = fixture_snapshot("test-doc.snapshot.bin");
        persist_snapshot(&database, &workspace_id, &doc_id, snapshot.clone()).await;
        let expected =
            parse_doc_markdown(&workspace_id, &snapshot).expect("parse markdown snapshot");

        let response = get_doc_markdown_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
        )
        .await
        .expect("doc markdown response")
        .into_response();

        let (_parts, body) = response.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["title"], expected.title);
        assert_eq!(json["markdown"], expected.markdown);
    }

    #[tokio::test]
    async fn get_doc_markdown_handler_returns_not_found() {
        let (_temp_dir, _database, state) = setup_state().await;
        let err = match get_doc_markdown_handler(
            Path(("missing-workspace".into(), "missing-doc".into())),
            State(state.clone()),
        )
        .await
        {
            Ok(_) => panic!("expected missing doc to return error"),
            Err(err) => err,
        };
        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(payload.name, "DOC_NOT_FOUND");
    }

    #[tokio::test]
    async fn get_doc_diff_handler_returns_combined_payload() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let session = state
            .user_store
            .create_session(&owner_id)
            .await
            .expect("create session");

        let mut request_headers = HeaderMap::new();
        request_headers.insert(COOKIE, session_cookie_value(&session.id, None));

        let yocto_doc = YoctoDoc::new();
        let snapshot = yocto_doc.encode_update_v1().expect("encode snapshot");
        let state_bytes = encode_state_vector(&yocto_doc.get_state_vector()).expect("encode state");
        let updated_at = Utc::now().timestamp();

        database
            .repositories()
            .doc_repo()
            .replace_doc_snapshot(barffine_core::db::doc_repo::ReplaceDocSnapshotParams {
                workspace_id: workspace_id.clone(),
                doc_id: doc_id.clone(),
                snapshot: snapshot.clone(),
                updated_at,
                title: None,
                summary: None,
                creator_id: None,
                updater_id: None,
                history_entry: None,
            })
            .await
            .expect("update snapshot");

        let response = get_doc_diff_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            request_headers,
            Bytes::new(),
        )
        .await
        .expect("doc diff response");

        assert_eq!(response.status(), StatusCode::OK);
        let (parts, body) = response.into_parts();
        let expected_timestamp = updated_at.to_string();
        let expected_missing = format!("0,{}", snapshot.len());
        let expected_state = format!("{},{}", snapshot.len(), snapshot.len() + state_bytes.len());
        assert_eq!(
            parts
                .headers
                .get("x-doc-timestamp")
                .and_then(|value| value.to_str().ok()),
            Some(expected_timestamp.as_str())
        );
        assert_eq!(
            parts
                .headers
                .get("x-doc-missing-offset")
                .and_then(|value| value.to_str().ok()),
            Some(expected_missing.as_str())
        );
        assert_eq!(
            parts
                .headers
                .get("x-doc-state-offset")
                .and_then(|value| value.to_str().ok()),
            Some(expected_state.as_str())
        );
        assert_eq!(
            parts
                .headers
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/octet-stream")
        );
        assert_eq!(
            parts
                .headers
                .get("x-affine-workspace-role")
                .and_then(|value| value.to_str().ok()),
            Some("OWNER")
        );
        assert_eq!(
            parts
                .headers
                .get("x-affine-doc-role")
                .and_then(|value| value.to_str().ok()),
            Some("OWNER")
        );

        let combined = to_bytes(body, usize::MAX).await.unwrap();
        assert_eq!(&combined[..snapshot.len()], snapshot.as_slice());
        assert_eq!(&combined[snapshot.len()..], state_bytes.as_slice());
    }

    #[tokio::test]
    async fn get_doc_diff_handler_returns_not_found() {
        let (_temp_dir, _database, state) = setup_state().await;
        let err = match get_doc_diff_handler(
            Path(("missing-workspace".into(), "missing-doc".into())),
            State(state.clone()),
            HeaderMap::new(),
            Bytes::new(),
        )
        .await
        {
            Ok(_) => panic!("expected missing doc diff to return error"),
            Err(err) => err,
        };
        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(payload.name, "SPACE_NOT_FOUND");
    }

    #[tokio::test]
    async fn rest_doc_history_snapshot_returns_binary() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let created_at = 1_700_000_000i64;
        database
            .repositories()
            .doc_repo()
            .replace_doc_snapshot(barffine_core::db::doc_repo::ReplaceDocSnapshotParams {
                workspace_id: workspace_id.clone(),
                doc_id: doc_id.clone(),
                snapshot: vec![0],
                updated_at: created_at,
                title: None,
                summary: None,
                creator_id: None,
                updater_id: None,
                history_entry: Some(barffine_core::db::doc_repo::HistorySnapshotInsert {
                    snapshot: vec![1_u8, 2, 3],
                    created_at,
                }),
            })
            .await
            .expect("insert history");

        let session = state
            .user_store
            .create_session(&owner_id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie_value(&session.id, Some(&owner_id)));

        let timestamp_ms = created_at * 1_000 + 500;
        let response = get_doc_history_snapshot_handler(
            Path((
                workspace_id.clone(),
                doc_id.clone(),
                timestamp_ms.to_string(),
            )),
            State(state.clone()),
            headers,
        )
        .await
        .expect("history snapshot available");

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(bytes.as_ref(), &[1, 2, 3]);
    }

    #[tokio::test]
    async fn publish_doc_handler_rejects_invalid_mode() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let session = state
            .user_store
            .create_session(&owner_id)
            .await
            .expect("create owner session");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie_value(&session.id, Some(&owner_id)));

        let err = publish_doc_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            headers,
            Json(PublishDocRequest {
                mode: Some("invalid".to_string()),
            }),
        )
        .await
        .err()
        .expect("expected invalid mode to fail");

        assert_eq!(err.to_string(), "invalid publish mode");
    }

    #[tokio::test]
    async fn unpublish_doc_requires_public_document() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let session = state
            .user_store
            .create_session(&owner_id)
            .await
            .expect("create owner session");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie_value(&session.id, Some(&owner_id)));

        let err = unpublish_doc_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            headers,
        )
        .await
        .err()
        .expect("unpublish should fail for private doc");

        assert_eq!(err.to_string(), "doc is not public");
    }

    #[tokio::test]
    async fn publish_and_unpublish_doc_updates_share_url() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let session = state
            .user_store
            .create_session(&owner_id)
            .await
            .expect("create owner session");

        let cookie_value = session_cookie_value(&session.id, Some(&owner_id));

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, cookie_value.clone());
        let response = publish_doc_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            headers,
            Json(PublishDocRequest {
                mode: Some("edgeless".to_string()),
            }),
        )
        .await
        .expect("publish doc")
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&body_bytes).expect("metadata json");
        assert_eq!(json["public"], true);
        assert_eq!(json["mode"], "edgeless");
        let share_token = json["shareToken"]
            .as_str()
            .expect("share token present")
            .to_string();
        assert_eq!(
            json["shareUrl"],
            JsonValue::String(public_doc_share_path(&workspace_id, &doc_id))
        );

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, cookie_value.clone());
        let response = unpublish_doc_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            headers,
        )
        .await
        .expect("unpublish doc")
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&body_bytes).expect("metadata json");
        assert_eq!(json["public"], false);
        assert!(json.get("shareUrl").is_none() || json["shareUrl"].is_null());
        assert!(json.get("shareToken").is_none() || json["shareToken"].is_null());

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, cookie_value);
        let response = publish_doc_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            headers,
            Json(PublishDocRequest { mode: None }),
        )
        .await
        .expect("re-publish doc")
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&body_bytes).expect("metadata json");
        assert_eq!(json["public"], true);
        assert_ne!(json["shareToken"].as_str(), Some(share_token.as_str()));
        assert_eq!(
            json["shareUrl"],
            JsonValue::String(public_doc_share_path(&workspace_id, &doc_id))
        );
    }

    async fn publish_doc(
        state: &AppState,
        workspace_id: &str,
        doc_id: &str,
        owner_id: &str,
        mode: &str,
    ) {
        let session = state
            .user_store
            .create_session(owner_id)
            .await
            .expect("create owner session");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie_value(&session.id, Some(owner_id)));

        publish_doc_handler(
            Path((workspace_id.to_string(), doc_id.to_string())),
            State(state.clone()),
            headers,
            Json(PublishDocRequest {
                mode: Some(mode.to_string()),
            }),
        )
        .await
        .expect("publish doc");
    }

    #[tokio::test]
    async fn rest_get_doc_requires_auth_for_private() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let err = get_doc_binary_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            HeaderMap::new(),
        )
        .await
        .expect_err("expected authentication error");

        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(payload.message, "authentication required");

        let session = state
            .user_store
            .create_session(&owner_id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie_value(&session.id, Some(&owner_id)));

        let response = get_doc_binary_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            headers,
        )
        .await
        .expect("doc download response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn rest_get_doc_allows_public_without_session() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        publish_doc(&state, &workspace_id, &doc_id, &owner_id, "page").await;

        let response = get_doc_binary_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            HeaderMap::new(),
        )
        .await
        .expect("public doc");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn rest_get_doc_rejects_private_for_non_member_session() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, _owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        let password_hash =
            generate_password_hash("intruder-password").expect("hash intruder password");
        let outsider = state
            .user_store
            .create("intruder@example.com", &password_hash, Some("Intruder"))
            .await
            .expect("create outsider");

        let session = state
            .user_store
            .create_session(&outsider.id)
            .await
            .expect("create outsider session");

        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            session_cookie_value(&session.id, Some(&outsider.id)),
        );

        let err = get_doc_binary_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            headers,
        )
        .await
        .expect_err("non-member session should be rejected");

        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(payload.message, "Doc.Read permission required");
    }

    #[tokio::test]
    async fn rest_get_doc_allows_public_with_non_member_session() {
        let (_temp_dir, database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;
        let doc_id = Uuid::new_v4().to_string();
        insert_document(&database, &workspace_id, &doc_id, false, "page").await;

        publish_doc(&state, &workspace_id, &doc_id, &owner_id, "page").await;

        let password_hash =
            generate_password_hash("viewer-password").expect("hash viewer password");
        let viewer = state
            .user_store
            .create("viewer@example.com", &password_hash, Some("Viewer"))
            .await
            .expect("create viewer");

        let session = state
            .user_store
            .create_session(&viewer.id)
            .await
            .expect("create viewer session");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie_value(&session.id, Some(&viewer.id)));

        let response = get_doc_binary_handler(
            Path((workspace_id.clone(), doc_id.clone())),
            State(state.clone()),
            headers,
        )
        .await
        .expect("public doc access with session");
        assert_eq!(response.status(), StatusCode::OK);
    }
}
