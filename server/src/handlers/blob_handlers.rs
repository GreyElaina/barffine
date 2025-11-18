// Blob storage handlers

use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use barffine_core::blob::BlobDescriptor;

use crate::{
    auth::resolve_workspace_access,
    error::AppError,
    state::AppState,
    types::{BlobDownloadQuery, PresignedUrlResponse},
    utils::blob_download::{
        BlobResponseContext, apply_blob_response_context, build_blob_download_response,
    },
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

    if download.location.is_some() && query.redirect.as_deref() == Some("manual") {
        let mut response = Json(PresignedUrlResponse {
            url: download
                .location
                .as_ref()
                .map(|loc| loc.uri.clone())
                .unwrap_or_default(),
        })
        .into_response();
        apply_blob_response_context(&mut response, BlobResponseContext::Workspace(&access))?;
        return Ok(response);
    }

    build_blob_download_response(
        download,
        &blob_name,
        "public, max-age=2592000, immutable",
        BlobResponseContext::Workspace(&access),
        || AppError::blob_not_found(&workspace_id, &blob_name),
    )
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
