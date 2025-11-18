use anyhow::Error as AnyError;
use axum::{
    body::Body,
    http::{HeaderValue, StatusCode, header::HeaderName},
    response::Response,
};
use barffine_core::blob::{BlobDownload, BlobMetadata};

use crate::{
    AppError,
    handlers::headers::{
        HEADER_DOC_ID, HEADER_DOC_ROLE, HEADER_USER_ID, HEADER_WORKSPACE_ID, HEADER_WORKSPACE_ROLE,
        doc_role_header_value, permission_header_value,
    },
    http::{append_set_cookie_headers, http_date_from_datetime},
    types::{RestDocAccess, WorkspaceAccess},
    utils::attachments::apply_attachment_headers,
};

pub enum BlobResponseContext<'a> {
    None,
    Workspace(&'a WorkspaceAccess),
    Document(&'a RestDocAccess),
}

pub fn build_blob_download_response<'a, F>(
    download: BlobDownload,
    filename: &'a str,
    cache_control: &'a str,
    context: BlobResponseContext<'a>,
    not_found: F,
) -> Result<Response, AppError>
where
    F: FnOnce() -> AppError,
{
    if let Some(location) = download.location {
        let mut response = Response::builder()
            .status(StatusCode::FOUND)
            .header("location", location.uri)
            .body(Body::empty())
            .map_err(|err| AppError::internal(AnyError::new(err)))?;
        apply_context(&mut response, context)?;
        return Ok(response);
    }

    let bytes = download.bytes.ok_or_else(not_found)?;
    let byte_len = bytes.len();

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(bytes))
        .map_err(|err| AppError::internal(AnyError::new(err)))?;

    apply_metadata_headers(
        &mut response,
        download.metadata.as_ref(),
        filename,
        Some(byte_len),
    )?;
    set_cache_control(&mut response, cache_control)?;
    apply_context(&mut response, context)?;
    Ok(response)
}

pub fn apply_blob_response_context<'a>(
    response: &mut Response,
    context: BlobResponseContext<'a>,
) -> Result<(), AppError> {
    apply_context(response, context)
}

fn apply_metadata_headers(
    response: &mut Response,
    metadata: Option<&BlobMetadata>,
    filename: &str,
    fallback_len: Option<usize>,
) -> Result<(), AppError> {
    if let Some(meta) = metadata {
        if let Some(length) = meta.content_length {
            set_content_length(response, length)?;
        } else if let Some(len) = fallback_len {
            set_content_length(response, len as u64)?;
        }

        if let Some(last_modified) = meta.last_modified {
            if let Some(formatted) = http_date_from_datetime(&last_modified) {
                let value = HeaderValue::from_str(&formatted)
                    .map_err(|err| AppError::internal(AnyError::new(err)))?;
                response
                    .headers_mut()
                    .insert(HeaderName::from_static("last-modified"), value);
            }
        }

        apply_attachment_headers(response, Some(meta), filename)?;
    } else {
        if let Some(len) = fallback_len {
            set_content_length(response, len as u64)?;
        }
        apply_attachment_headers(response, None, filename)?;
    }

    Ok(())
}

fn set_cache_control(response: &mut Response, value: &str) -> Result<(), AppError> {
    let header_value =
        HeaderValue::from_str(value).map_err(|err| AppError::internal(AnyError::new(err)))?;
    response
        .headers_mut()
        .insert(HeaderName::from_static("cache-control"), header_value);
    Ok(())
}

fn set_content_length(response: &mut Response, length: u64) -> Result<(), AppError> {
    let value = HeaderValue::from_str(&length.to_string())
        .map_err(|err| AppError::internal(AnyError::new(err)))?;
    response
        .headers_mut()
        .insert(HeaderName::from_static("content-length"), value);
    Ok(())
}

fn apply_context<'a>(
    response: &mut Response,
    context: BlobResponseContext<'a>,
) -> Result<(), AppError> {
    match context {
        BlobResponseContext::None => Ok(()),
        BlobResponseContext::Workspace(access) => {
            append_workspace_headers(response, access)?;
            append_set_cookie_headers(response, &access.set_cookies)?;
            Ok(())
        }
        BlobResponseContext::Document(access) => {
            append_doc_headers(response, access)?;
            append_set_cookie_headers(response, &access.set_cookies)?;
            Ok(())
        }
    }
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

fn append_doc_headers(response: &mut Response, access: &RestDocAccess) -> Result<(), AppError> {
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
