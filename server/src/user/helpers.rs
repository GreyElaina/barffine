use barffine_core::user;

use crate::AppError;

pub fn is_valid_email(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > 320 {
        return false;
    }

    let mut segments = trimmed.split('@');
    let local = segments.next().unwrap_or_default();
    let domain = segments.next().unwrap_or_default();

    if segments.next().is_some() {
        return false;
    }

    if local.is_empty() || domain.is_empty() {
        return false;
    }

    if local
        .bytes()
        .any(|ch| ch <= b' ' || matches!(ch, b'@' | b';' | b',' | b'"'))
    {
        return false;
    }

    if domain
        .bytes()
        .any(|ch| ch <= b' ' || matches!(ch, b'@' | b';' | b',' | b'"'))
    {
        return false;
    }

    domain.contains('.')
}

pub fn normalize_user_list_params(
    skip: Option<i64>,
    first: Option<i64>,
    query: Option<&str>,
) -> Result<(i64, i64, Option<String>), AppError> {
    let limit = first.unwrap_or(20).clamp(1, 100);
    let offset = skip.unwrap_or(0);

    if offset < 0 {
        return Err(AppError::bad_request("skip must be non-negative"));
    }

    let keyword = query
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());

    Ok((limit, offset, keyword))
}

pub fn display_name_for_user(record: &user::UserRecord) -> String {
    display_name_from_parts(record.name.as_deref(), &record.email)
}

pub fn display_name_from_parts(name: Option<&str>, email: &str) -> String {
    if let Some(explicit) = name.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    }) {
        return explicit;
    }

    fallback_display_name(email)
}

pub fn fallback_display_name(email: &str) -> String {
    email
        .split('@')
        .next()
        .filter(|part| !part.is_empty())
        .unwrap_or(email)
        .to_owned()
}
