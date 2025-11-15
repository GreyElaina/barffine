// Blob storage handlers

use anyhow::Error as AnyError;
use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::HeaderName},
    response::{IntoResponse, Response},
};
use barffine_core::blob::BlobDescriptor;

use crate::{
    auth::resolve_workspace_access,
    error::AppError,
    handlers::headers::{
        HEADER_USER_ID, HEADER_WORKSPACE_ID, HEADER_WORKSPACE_ROLE, permission_header_value,
    },
    http::{append_set_cookie_headers, http_date_from_datetime},
    state::AppState,
    types::{BlobDownloadQuery, PresignedUrlResponse, WorkspaceAccess},
    utils::attachments::apply_attachment_headers,
};

pub(crate) async fn get_workspace_blob_handler(
    Path((workspace_id, blob_name)): Path<(String, String)>,
    Query(query): Query<BlobDownloadQuery>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let runtime = state.runtime();
    let access = resolve_workspace_access(runtime.as_ref(), &headers, &workspace_id).await?;

    let descriptor = BlobDescriptor::new(&workspace_id, &blob_name);
    let download = state
        .blob_store
        .get(&descriptor, true)
        .await
        .map_err(AppError::from_anyhow)?
        .ok_or_else(|| AppError::blob_not_found(&workspace_id, &blob_name))?;

    if let Some(location) = download.location {
        if query.redirect.as_deref() == Some("manual") {
            let response = Json(PresignedUrlResponse {
                url: location.uri.clone(),
            })
            .into_response();
            let response = finalize_workspace_response(response, &access)?;
            return Ok(response);
        }

        let response = Response::builder()
            .status(StatusCode::FOUND)
            .header("location", location.uri)
            .body(Body::empty())
            .map_err(|err| AppError::internal(AnyError::new(err)))?;

        let response = finalize_workspace_response(response, &access)?;
        return Ok(response);
    }

    let bytes = download
        .bytes
        .ok_or_else(|| AppError::blob_not_found(&workspace_id, &blob_name))?;

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

        apply_attachment_headers(&mut response, Some(metadata), &blob_name)?;
    } else {
        apply_attachment_headers(&mut response, None, &blob_name)?;
    }

    response.headers_mut().insert(
        HeaderName::from_static("cache-control"),
        HeaderValue::from_static("public, max-age=2592000, immutable"),
    );

    let response = finalize_workspace_response(response, &access)?;
    Ok(response)
}

fn finalize_workspace_response(
    mut response: Response,
    access: &WorkspaceAccess,
) -> Result<Response, AppError> {
    append_workspace_headers(&mut response, access)?;
    append_set_cookie_headers(&mut response, &access.set_cookies)?;
    Ok(response)
}

fn append_workspace_headers(
    response: &mut Response,
    access: &WorkspaceAccess,
) -> Result<(), AppError> {
    let headers = response.headers_mut();
    headers.insert(
        HeaderName::from_static(HEADER_WORKSPACE_ID),
        HeaderValue::from_str(&access.workspace.id)
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::to_bytes,
        extract::{Path, Query, State},
        http::{
            HeaderMap, HeaderValue, StatusCode,
            header::{CONTENT_TYPE, COOKIE, HeaderName},
        },
    };
    use barffine_core::blob::{BlobDescriptor, BlobMetadata};

    use crate::{
        cookies::SESSION_COOKIE_NAME,
        testing::{seed_workspace, setup_state},
        types::BlobDownloadQuery,
    };

    #[tokio::test]
    async fn get_workspace_blob_handler_returns_blob() {
        let (_temp_dir, _database, state) = setup_state().await;
        let (workspace_id, owner_id) = seed_workspace(&state).await;

        let data = b"png-bytes".to_vec();
        state
            .blob_store
            .put(
                &BlobDescriptor::new(&workspace_id, "logo.png"),
                &data,
                BlobMetadata {
                    content_type: Some("image/png".into()),
                    ..Default::default()
                },
            )
            .await
            .expect("store blob");

        let session = state
            .user_store
            .create_session(&owner_id)
            .await
            .expect("create session");

        let mut request_headers = HeaderMap::new();
        request_headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!("{}={}", SESSION_COOKIE_NAME, session.id)).unwrap(),
        );

        let response = get_workspace_blob_handler(
            Path((workspace_id.clone(), "logo.png".into())),
            Query(BlobDownloadQuery { redirect: None }),
            State(state.clone()),
            request_headers,
        )
        .await
        .expect("blob response");

        assert_eq!(response.status(), StatusCode::OK);
        let (parts, body) = response.into_parts();
        assert_eq!(
            parts
                .headers
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("image/png")
        );
        assert_eq!(
            parts
                .headers
                .get(HeaderName::from_static("cache-control"))
                .and_then(|value| value.to_str().ok()),
            Some("public, max-age=2592000, immutable")
        );
        assert_eq!(
            parts
                .headers
                .get("x-affine-user-id")
                .and_then(|value| value.to_str().ok()),
            Some(owner_id.as_str())
        );
        assert_eq!(
            parts
                .headers
                .get("x-affine-workspace-id")
                .and_then(|value| value.to_str().ok()),
            Some(workspace_id.as_str())
        );
        assert_eq!(
            parts
                .headers
                .get("x-affine-workspace-role")
                .and_then(|value| value.to_str().ok()),
            Some("OWNER")
        );

        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        assert_eq!(bytes.as_ref(), &data);
    }
}
