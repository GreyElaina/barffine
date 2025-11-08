// User management handlers

use anyhow::Error as AnyError;
use argon2::password_hash::Error as PasswordHashError;
use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::HeaderName},
    response::{IntoResponse, Response},
};

use crate::{
    auth::generate_password_hash,
    error::AppError,
    state::AppState,
    types::{
        CreateUserRequest, CreateUserResponse, ListUsersQuery, ListUsersResponse, UserResponse,
    },
    user::avatar::{avatar_descriptor, is_valid_avatar_key},
    utils::{
        attachments::apply_attachment_headers,
        db::is_unique_violation,
        http::{append_set_cookie_headers, http_date_from_datetime},
        users::normalize_user_list_params,
    },
};

pub(crate) async fn create_user_handler(
    State(state): State<AppState>,
    Json(payload): Json<CreateUserRequest>,
) -> Result<impl IntoResponse, AppError> {
    if payload.email.trim().is_empty() {
        return Err(AppError::bad_request("email must not be empty"));
    }

    if payload.password.is_empty() {
        return Err(AppError::bad_request("password must not be empty"));
    }

    let password_hash = generate_password_hash(&payload.password)
        .map_err(|err: PasswordHashError| AppError::internal(err.into()))?;

    match state
        .user_store
        .create(&payload.email, &password_hash, payload.name.as_deref())
        .await
    {
        Ok(user) => {
            let response = CreateUserResponse {
                id: user.id,
                email: user.email,
                name: user.name,
            };
            Ok((StatusCode::CREATED, Json(response)))
        }
        Err(err) => {
            if is_unique_violation(&err) {
                Err(AppError::conflict("user already exists"))
            } else {
                Err(AppError::from_anyhow(err))
            }
        }
    }
}

pub(crate) async fn get_users_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<ListUsersQuery>,
) -> Result<Response, AppError> {
    let auth = state
        .user_service
        .authenticate_rest_request(&headers)
        .await?;
    state
        .user_service
        .ensure_admin_user(&auth.user.id, "administrator privileges required")
        .await?;

    let (first, skip, keyword) =
        normalize_user_list_params(params.skip, params.first, params.query.as_deref())?;

    let users = state
        .user_store
        .list_paginated(skip, first, keyword.as_deref(), None)
        .await
        .map_err(AppError::from_anyhow)?;

    let total = state
        .user_store
        .count(keyword.as_deref())
        .await
        .map_err(AppError::from_anyhow)?;

    let response = ListUsersResponse {
        users: users.into_iter().map(UserResponse::from).collect(),
        total,
        skip,
        first,
    };

    let mut http_response = Json(response).into_response();
    append_set_cookie_headers(&mut http_response, &auth.set_cookies)?;
    Ok(http_response)
}

pub(crate) async fn get_user_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> Result<Response, AppError> {
    let auth = state
        .user_service
        .authenticate_rest_request(&headers)
        .await?;
    let user = state.user_service.fetch_user(&user_id).await?;

    state
        .user_service
        .ensure_self_or_admin(&auth.user.id, &user_id, "administrator privileges required")
        .await?;

    let mut response = Json(UserResponse::from(user)).into_response();
    append_set_cookie_headers(&mut response, &auth.set_cookies)?;
    Ok(response)
}

pub(crate) async fn get_avatar_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Response, AppError> {
    if !is_valid_avatar_key(&key) {
        return Err(AppError::not_found("user avatar not found").with_name("USER_AVATAR_NOT_FOUND"));
    }

    let descriptor = avatar_descriptor(&key);
    let download = state
        .blob_store
        .get(&descriptor, false)
        .await
        .map_err(AppError::from_anyhow)?
        .ok_or_else(|| {
            AppError::not_found("user avatar not found").with_name("USER_AVATAR_NOT_FOUND")
        })?;

    if let Some(location) = download.location {
        let response = Response::builder()
            .status(StatusCode::FOUND)
            .header("location", location.uri)
            .body(Body::empty())
            .map_err(|err| AppError::internal(AnyError::new(err)))?;
        return Ok(response);
    }

    let bytes = download.bytes.ok_or_else(|| {
        AppError::not_found("user avatar not found").with_name("USER_AVATAR_NOT_FOUND")
    })?;
    let byte_len = bytes.len();

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

        apply_attachment_headers(&mut response, Some(metadata), &key)?;
    } else {
        let value = HeaderValue::from_str(&byte_len.to_string())
            .map_err(|err| AppError::internal(AnyError::new(err)))?;
        response
            .headers_mut()
            .insert(HeaderName::from_static("content-length"), value);
        apply_attachment_headers(&mut response, None, &key)?;
    }

    response.headers_mut().insert(
        HeaderName::from_static("cache-control"),
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::to_bytes,
        extract::{Path, Query, State},
        http::{HeaderMap, HeaderValue, StatusCode, header::COOKIE},
    };

    use crate::{
        auth::generate_password_hash,
        cookies::{SESSION_COOKIE_NAME, USER_COOKIE_NAME},
        test_support::setup_state,
        types::{ListUsersQuery, ListUsersResponse, UserResponse},
    };

    fn session_cookie(session_id: &str, user_id: &str) -> HeaderValue {
        HeaderValue::from_str(&format!(
            "{}={}; {}={}",
            SESSION_COOKIE_NAME, session_id, USER_COOKIE_NAME, user_id
        ))
        .expect("session cookie")
    }

    #[tokio::test]
    async fn get_users_handler_returns_paginated_list() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");

        let admin = state
            .user_store
            .create("admin@example.com", &password_hash, Some("Admin"))
            .await
            .expect("create admin");
        state
            .user_store
            .add_admin(&admin.id)
            .await
            .expect("promote admin");
        let session = state
            .user_store
            .create_session(&admin.id)
            .await
            .expect("create session");

        let alice = state
            .user_store
            .create("alice@example.com", &password_hash, Some("Alice"))
            .await
            .expect("create user");
        let _bob = state
            .user_store
            .create("bob@example.com", &password_hash, Some("Bob"))
            .await
            .expect("create user");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie(&session.id, &admin.id));

        let response = get_users_handler(
            State(state.clone()),
            headers.clone(),
            Query(ListUsersQuery {
                skip: Some(0),
                first: Some(10),
                query: None,
            }),
        )
        .await
        .expect("list users");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: ListUsersResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload.total, 3);
        assert_eq!(payload.users.len(), 3);
        let emails: Vec<_> = payload
            .users
            .iter()
            .map(|user| user.email.clone())
            .collect();
        assert!(emails.contains(&alice.email));
        assert_eq!(payload.users[0].email_verified, false);

        let response = get_users_handler(
            State(state.clone()),
            headers,
            Query(ListUsersQuery {
                skip: Some(0),
                first: Some(10),
                query: Some("alice".into()),
            }),
        )
        .await
        .expect("list filtered");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let filtered_payload: ListUsersResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(filtered_payload.total, 1);
        assert_eq!(filtered_payload.users.len(), 1);
        assert_eq!(filtered_payload.users[0].id, alice.id);
        assert_eq!(filtered_payload.users[0].name, "Alice");
    }

    #[tokio::test]
    async fn get_users_handler_requires_admin() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let requester = state
            .user_store
            .create("requester@example.com", &password_hash, Some("Requester"))
            .await
            .expect("create requester");
        let session = state
            .user_store
            .create_session(&requester.id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie(&session.id, &requester.id));

        let err = get_users_handler(
            State(state.clone()),
            headers,
            Query(ListUsersQuery {
                skip: Some(0),
                first: Some(10),
                query: None,
            }),
        )
        .await
        .expect_err("non-admin should fail");

        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(payload.message, "administrator privileges required");
    }

    #[tokio::test]
    async fn get_users_handler_requires_authentication() {
        let (_temp_dir, _database, state) = setup_state().await;

        let err = get_users_handler(
            State(state.clone()),
            HeaderMap::new(),
            Query(ListUsersQuery {
                skip: Some(0),
                first: Some(10),
                query: None,
            }),
        )
        .await
        .expect_err("missing auth should fail");

        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(payload.message, "authentication required");
    }

    #[tokio::test]
    async fn get_user_handler_returns_not_found_for_unknown_user() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let admin = state
            .user_store
            .create("admin-missing@example.com", &password_hash, Some("Admin"))
            .await
            .expect("create admin");
        state
            .user_store
            .add_admin(&admin.id)
            .await
            .expect("promote admin");
        let session = state
            .user_store
            .create_session(&admin.id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie(&session.id, &admin.id));

        let err = match get_user_handler(State(state.clone()), headers, Path("missing-user".into()))
            .await
        {
            Ok(_) => panic!("expected not found"),
            Err(err) => err,
        };
        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(payload.name, "NOT_FOUND");
    }

    #[tokio::test]
    async fn get_user_handler_allows_self_access() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let user = state
            .user_store
            .create("self@example.com", &password_hash, Some("Self"))
            .await
            .expect("create user");
        let session = state
            .user_store
            .create_session(&user.id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie(&session.id, &user.id));

        let response = get_user_handler(State(state.clone()), headers, Path(user.id.clone()))
            .await
            .expect("self access allowed");

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: UserResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload.id, user.id);
    }

    #[tokio::test]
    async fn get_user_handler_requires_admin_for_other_users() {
        let (_temp_dir, _database, state) = setup_state().await;
        let password_hash = generate_password_hash("secret").expect("hash password");
        let requester = state
            .user_store
            .create("requester@example.com", &password_hash, Some("Requester"))
            .await
            .expect("create requester");
        let target = state
            .user_store
            .create("target@example.com", &password_hash, Some("Target"))
            .await
            .expect("create target");
        let session = state
            .user_store
            .create_session(&requester.id)
            .await
            .expect("create session");

        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, session_cookie(&session.id, &requester.id));

        let err = get_user_handler(State(state.clone()), headers, Path(target.id.clone()))
            .await
            .expect_err("non-admin access should fail");

        let (status, payload) = err.into_payload();
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(payload.message, "administrator privileges required");
    }
}
