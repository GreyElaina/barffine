use std::sync::Arc;

use axum::{extract::FromRef, http::HeaderMap};
use barffine_core::user::{self, UserStore};

use crate::{
    AppError,
    cookies::{
        build_session_cookie, build_user_cookie, clear_session_cookie, clear_user_cookie,
        extract_session_token,
    },
    state::AppState,
    types::{AuthenticatedRestSession, SessionLookup, SessionUser},
};

pub struct UserService {
    user_store: UserStore,
}

impl UserService {
    pub fn new(user_store: UserStore) -> Self {
        Self { user_store }
    }

    pub async fn ensure_admin_user(
        &self,
        requester_id: &str,
        error_message: &'static str,
    ) -> Result<(), AppError> {
        if self.is_admin(requester_id).await? {
            Ok(())
        } else {
            Err(AppError::forbidden(error_message))
        }
    }

    pub async fn ensure_self_or_admin(
        &self,
        requester_id: &str,
        target_user_id: &str,
        error_message: &'static str,
    ) -> Result<(), AppError> {
        if requester_id == target_user_id {
            Ok(())
        } else {
            self.ensure_admin_user(requester_id, error_message).await
        }
    }

    pub async fn fetch_user_with<F>(
        &self,
        user_id: &str,
        not_found: F,
    ) -> Result<user::UserRecord, AppError>
    where
        F: FnOnce() -> AppError,
    {
        self.user_store
            .find_by_id(user_id)
            .await
            .map_err(AppError::from_anyhow)?
            .ok_or_else(not_found)
    }

    pub async fn fetch_user(&self, user_id: &str) -> Result<user::UserRecord, AppError> {
        self.fetch_user_with(user_id, || AppError::not_found("user not found"))
            .await
    }

    pub async fn authenticate_rest_request(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedRestSession, AppError> {
        let Some(session_id) = extract_session_token(headers) else {
            return Err(AppError::unauthorized("authentication required"));
        };

        let Some(session) = self
            .user_store
            .refresh_session(&session_id)
            .await
            .map_err(AppError::from_anyhow)?
        else {
            return Err(AppError::unauthorized("session expired"));
        };

        let Some(user) = self
            .user_store
            .find_by_id(&session.user_id)
            .await
            .map_err(AppError::from_anyhow)?
        else {
            return Err(AppError::unauthorized("authentication required"));
        };

        if user.disabled {
            return Err(AppError::unauthorized("authentication required"));
        }

        let mut set_cookies = Vec::with_capacity(2);
        set_cookies.push(build_session_cookie(&session.id, session.expires_at));
        set_cookies.push(build_user_cookie(&session.user_id, session.expires_at));

        Ok(AuthenticatedRestSession { user, set_cookies })
    }

    pub async fn pad_session_response(
        &self,
        headers: &HeaderMap,
    ) -> Result<SessionLookup, AppError> {
        let Some(session_id) = extract_session_token(headers) else {
            return Ok(SessionLookup {
                user: None,
                cookies: Vec::new(),
            });
        };

        let mut cookies = Vec::new();

        let Some(session) = self
            .user_store
            .refresh_session(&session_id)
            .await
            .map_err(AppError::from_anyhow)?
        else {
            cookies.push(clear_session_cookie());
            cookies.push(clear_user_cookie());
            return Ok(SessionLookup {
                user: None,
                cookies,
            });
        };

        let Some(user) = self
            .user_store
            .find_by_id(&session.user_id)
            .await
            .map_err(AppError::from_anyhow)?
        else {
            self.delete_session(&session.id).await?;
            cookies.push(clear_session_cookie());
            cookies.push(clear_user_cookie());
            return Ok(SessionLookup {
                user: None,
                cookies,
            });
        };

        if user.disabled {
            self.delete_session(&session.id).await?;
            cookies.push(clear_session_cookie());
            cookies.push(clear_user_cookie());
            return Ok(SessionLookup {
                user: None,
                cookies,
            });
        }

        cookies.push(build_session_cookie(&session.id, session.expires_at));
        cookies.push(build_user_cookie(&session.user_id, session.expires_at));

        Ok(SessionLookup {
            user: Some(SessionUser::from(&user)),
            cookies,
        })
    }

    pub async fn delete_session(&self, session_id: &str) -> Result<(), AppError> {
        self.user_store
            .delete_session(session_id)
            .await
            .map_err(AppError::from_anyhow)
    }

    pub async fn is_admin(&self, user_id: &str) -> Result<bool, AppError> {
        self.user_store
            .is_admin(user_id)
            .await
            .map_err(AppError::from_anyhow)
    }
}

impl FromRef<AppState> for Arc<UserService> {
    fn from_ref(state: &AppState) -> Arc<UserService> {
        Arc::clone(&state.user_service)
    }
}
