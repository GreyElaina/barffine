use std::env;
use std::sync::OnceLock;

use axum::http::{
    HeaderMap,
    header::{AUTHORIZATION, COOKIE},
};
use cookie::{Cookie, SameSite};
use time::{Duration, OffsetDateTime};

use barffine_core::user::SESSION_TTL_SECONDS;

pub const SESSION_COOKIE_NAME: &str = "affine_session";
pub const USER_COOKIE_NAME: &str = "affine_user_id";

const COOKIE_PATH: &str = "/";

fn cookie_expiry(expires_at: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(expires_at).unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn cookie_secure() -> bool {
    static SECURE: OnceLock<bool> = OnceLock::new();
    *SECURE.get_or_init(|| match env::var("BARFFINE_COOKIE_SECURE") {
        Ok(value) => {
            let normalized = value.to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => false,
    })
}

pub fn build_session_cookie(session_id: &str, expires_at: i64) -> String {
    let mut builder = Cookie::build((SESSION_COOKIE_NAME, session_id.to_owned()))
        .path(COOKIE_PATH)
        .http_only(true)
        .same_site(SameSite::Lax)
        .max_age(Duration::seconds(SESSION_TTL_SECONDS))
        .expires(cookie_expiry(expires_at));

    if cookie_secure() {
        builder = builder.secure(true);
    }

    builder.build().to_string()
}

pub fn build_user_cookie(user_id: &str, expires_at: i64) -> String {
    Cookie::build((USER_COOKIE_NAME, user_id.to_owned()))
        .path(COOKIE_PATH)
        .same_site(SameSite::Lax)
        .max_age(Duration::seconds(SESSION_TTL_SECONDS))
        .expires(cookie_expiry(expires_at))
        .build()
        .to_string()
}

pub fn clear_session_cookie() -> String {
    let mut builder = Cookie::build(SESSION_COOKIE_NAME)
        .path(COOKIE_PATH)
        .http_only(true)
        .same_site(SameSite::Lax)
        .removal();

    if cookie_secure() {
        builder = builder.secure(true);
    }

    builder.build().to_string()
}

pub fn clear_user_cookie() -> String {
    Cookie::build(USER_COOKIE_NAME)
        .path(COOKIE_PATH)
        .same_site(SameSite::Lax)
        .removal()
        .build()
        .to_string()
}

pub(crate) fn extract_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(COOKIE)?.to_str().ok()?;
    for parsed in Cookie::split_parse(raw) {
        if let Ok(cookie) = parsed {
            if cookie.name() == name {
                return Some(cookie.value().to_owned());
            }
        }
    }
    None
}

pub(crate) fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?.trim();
    let mut segments = value.split_whitespace();
    let scheme = segments.next()?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = segments.next()?.trim();
    if token.is_empty() {
        return None;
    }
    Some(token.to_owned())
}

pub(crate) fn extract_session_token(headers: &HeaderMap) -> Option<String> {
    extract_cookie(headers, SESSION_COOKIE_NAME).or_else(|| extract_bearer_token(headers))
}

pub fn parse_set_cookie(raw: &str) -> Option<(String, String)> {
    Cookie::parse(raw)
        .ok()
        .map(|cookie| (cookie.name().to_owned(), cookie.value().to_owned()))
}
