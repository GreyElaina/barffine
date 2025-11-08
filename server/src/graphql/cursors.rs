use async_graphql::Result as GraphQLResult;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

use crate::AppError;

use super::helpers::map_app_error;

/// Decode a cursor into its raw string payload, normalizing the GraphQL contract.
pub(crate) fn decode_cursor_payload(cursor: &str) -> Result<String, AppError> {
    let bytes = BASE64
        .decode(cursor)
        .map_err(|_| AppError::bad_request("invalid cursor encoding"))?;

    String::from_utf8(bytes).map_err(|_| AppError::bad_request("invalid cursor payload"))
}

pub(crate) fn decode_cursor_payload_graphql(cursor: &str) -> GraphQLResult<String> {
    decode_cursor_payload(cursor).map_err(map_app_error)
}

fn ensure_non_empty(value: Option<&str>) -> Result<&str, AppError> {
    let value = value.ok_or_else(|| AppError::bad_request("invalid cursor payload"))?;
    if value.is_empty() {
        Err(AppError::bad_request("invalid cursor payload"))
    } else {
        Ok(value)
    }
}

pub(crate) fn parse_timestamp_cursor(decoded: &str) -> Result<(i64, String), AppError> {
    let mut parts = decoded.splitn(2, ':');
    let timestamp = ensure_non_empty(parts.next())?;
    let id = ensure_non_empty(parts.next())?;

    let timestamp = timestamp
        .parse::<i64>()
        .map_err(|_| AppError::bad_request("invalid cursor payload"))?;

    Ok((timestamp, id.to_string()))
}

pub(crate) fn parse_timestamp_cursor_graphql(decoded: &str) -> GraphQLResult<(i64, String)> {
    parse_timestamp_cursor(decoded).map_err(map_app_error)
}
