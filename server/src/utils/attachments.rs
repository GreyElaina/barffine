use anyhow::Error as AnyError;
use axum::{
    http::{
        HeaderValue,
        header::{CONTENT_TYPE, HeaderName},
    },
    response::Response,
};

use barffine_core::blob::BlobMetadata;

use crate::AppError;

pub const DANGEROUS_INLINE_MIME_PREFIXES: &[&str] = &[
    "text/html",
    "application/xhtml+xml",
    "image/svg+xml",
    "application/xml",
    "text/xml",
    "text/javascript",
];

pub fn sanitize_attachment_filename(name: &str) -> String {
    let filtered: String = name
        .chars()
        .filter(|ch| !matches!(ch, '\r' | '\n'))
        .map(|ch| {
            if ch.is_ascii_alphanumeric()
                || ch == '.'
                || ch == '-'
                || ch == '_'
                || ch.is_ascii_whitespace()
            {
                ch
            } else {
                '_'
            }
        })
        .collect();

    let trimmed = filtered.trim();
    if trimmed.is_empty() {
        "download".to_owned()
    } else {
        trimmed.to_owned()
    }
}

pub fn is_dangerous_inline_mime(mime: &str) -> bool {
    let lower = mime.to_ascii_lowercase();
    DANGEROUS_INLINE_MIME_PREFIXES
        .iter()
        .any(|prefix| lower.starts_with(*prefix))
}

pub fn apply_attachment_headers(
    response: &mut Response,
    metadata: Option<&BlobMetadata>,
    filename: &str,
) -> Result<(), AppError> {
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );

    let mut content_type = metadata
        .and_then(|meta| meta.content_type.clone())
        .filter(|mime| !mime.is_empty());

    if let Some(ref mime) = content_type {
        if mime.starts_with("application/json") {
            content_type = Some("text/json".to_owned());
        }
    }

    if let Some(ref mime) = content_type {
        let value =
            HeaderValue::from_str(mime).map_err(|err| AppError::internal(AnyError::new(err)))?;
        response.headers_mut().insert(CONTENT_TYPE, value);

        if is_dangerous_inline_mime(mime) {
            let safe_name = sanitize_attachment_filename(filename);
            let disposition = format!("attachment; filename=\"{}\"", safe_name);
            let value = HeaderValue::from_str(&disposition)
                .map_err(|err| AppError::internal(AnyError::new(err)))?;
            response
                .headers_mut()
                .insert(HeaderName::from_static("content-disposition"), value);
        }
    } else if !response.headers().contains_key(CONTENT_TYPE) {
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
    }

    Ok(())
}
