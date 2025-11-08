use barffine_core::blob::BlobDescriptor;

/// Pseudo workspace namespace for storing user avatars inside the generic blob store.
pub(crate) const AVATAR_STORAGE_NAMESPACE: &str = "__avatars__";

/// Route prefix used by the HTTP handler that serves avatar binaries.
pub(crate) const AVATAR_ROUTE_PREFIX: &str = "/api/avatars/";

/// Maximum avatar upload size enforced server-side (bytes).
pub(crate) const MAX_AVATAR_UPLOAD_BYTES: usize = 500 * 1024;

/// Upper bound for avatar keys to avoid unbounded allocations and traversal attempts.
const MAX_AVATAR_KEY_LEN: usize = 256;

pub(crate) fn avatar_descriptor(key: &str) -> BlobDescriptor {
    BlobDescriptor::new(AVATAR_STORAGE_NAMESPACE, key)
}

pub(crate) fn avatar_public_prefix(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    format!("{base}{AVATAR_ROUTE_PREFIX}")
}

pub(crate) fn avatar_url(base_url: &str, key: &str) -> String {
    format!("{}{}", avatar_public_prefix(base_url), key)
}

pub(crate) fn parse_avatar_key_from_url(base_url: &str, url: &str) -> Option<String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return None;
    }

    let prefixes = [
        avatar_public_prefix(base_url),
        AVATAR_ROUTE_PREFIX.to_string(),
    ];

    for prefix in prefixes {
        if let Some(rest) = trimmed.strip_prefix(&prefix) {
            let key = rest.split(['?', '#']).next()?.trim();
            if is_valid_avatar_key(key) {
                return Some(key.to_string());
            }
        }
    }

    None
}

pub(crate) fn is_valid_avatar_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_AVATAR_KEY_LEN
        && key
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
}

pub(crate) fn sniff_mime(bytes: &[u8], declared: Option<&str>) -> Option<String> {
    if let Some(kind) = infer::get(bytes) {
        return Some(kind.mime_type().to_string());
    }

    declared
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
}
