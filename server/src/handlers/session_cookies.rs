use axum::response::Response;

use crate::{
    AppError,
    cookies::{build_session_cookie, build_user_cookie, clear_session_cookie, clear_user_cookie},
    http::append_set_cookie_headers,
};

/// Helper for composing session/user cookie mutations in auth handlers.
pub(crate) struct SessionCookies {
    entries: Vec<String>,
}

impl SessionCookies {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn set_session(mut self, session_id: &str, expires_at: i64) -> Self {
        self.entries
            .push(build_session_cookie(session_id, expires_at));
        self
    }

    pub fn set_user(mut self, user_id: &str, expires_at: i64) -> Self {
        self.entries.push(build_user_cookie(user_id, expires_at));
        self
    }

    pub fn clear_session(mut self) -> Self {
        self.entries.push(clear_session_cookie());
        self
    }

    pub fn clear_user(mut self) -> Self {
        self.entries.push(clear_user_cookie());
        self
    }

    pub fn add_cookie(mut self, cookie: String) -> Self {
        self.entries.push(cookie);
        self
    }

    pub fn apply(self, response: &mut Response) -> Result<(), AppError> {
        append_set_cookie_headers(response, &self.entries)
    }
}

impl Default for SessionCookies {
    fn default() -> Self {
        Self::new()
    }
}
