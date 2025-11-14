use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use barffine_core::{
    doc_roles::DocumentRoleRecord, doc_store::DocumentMetadata, user::UserRecord,
    workspace::WorkspaceRecord,
};
use dashmap::DashMap;
use tokio::sync::OnceCell;

use crate::{AppError, graphql::DocPermissions};

#[derive(Clone, Default)]
pub struct RequestCaches {
    doc_metadata: Arc<RequestDocMetadataCache>,
    doc_permissions: Arc<RequestDocPermissionCache>,
    workspace_records: Arc<RequestWorkspaceCache>,
    doc_roles: Arc<RequestDocRoleCache>,
    user_records: Arc<RequestUserCache>,
    user_admin: Arc<RequestUserAdminCache>,
}

impl RequestCaches {
    pub fn doc_metadata(&self) -> &RequestDocMetadataCache {
        &self.doc_metadata
    }

    pub fn doc_permissions(&self) -> &RequestDocPermissionCache {
        &self.doc_permissions
    }

    pub fn workspace_records(&self) -> &RequestWorkspaceCache {
        &self.workspace_records
    }

    pub fn doc_roles(&self) -> &RequestDocRoleCache {
        &self.doc_roles
    }

    pub fn user_records(&self) -> &RequestUserCache {
        &self.user_records
    }

    pub fn user_admin(&self) -> &RequestUserAdminCache {
        &self.user_admin
    }
}

#[derive(Default)]
pub struct RequestDocMetadataCache {
    inner: DashMap<CacheKey, Arc<OnceCell<Option<DocumentMetadata>>>>,
}

impl RequestDocMetadataCache {
    pub(crate) fn get(&self, workspace_id: &str, doc_id: &str) -> Option<Option<DocumentMetadata>> {
        let key = CacheKey::new(workspace_id, doc_id);
        self.inner
            .get(&key)
            .and_then(|cell| cell.get().map(|value| value.clone()))
    }

    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        workspace_id: &str,
        doc_id: &str,
        fetcher: F,
    ) -> Result<Option<DocumentMetadata>, AppError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Option<DocumentMetadata>, AppError>>,
    {
        let key = CacheKey::new(workspace_id, doc_id);
        let cell = self
            .inner
            .entry(key)
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        if let Some(value) = cell.get() {
            return Ok(value.clone());
        }

        let fetched = fetcher().await?;
        if cell.set(fetched.clone()).is_err() {
            if let Some(existing) = cell.get() {
                return Ok(existing.clone());
            }
        }

        Ok(fetched)
    }

    pub(crate) fn insert(&self, metadata: DocumentMetadata) {
        let key = CacheKey::from_metadata(&metadata);
        let cell = Arc::new(OnceCell::new());
        let _ = cell.set(Some(metadata));
        self.inner.insert(key, cell);
    }

    pub(crate) fn insert_absent(&self, workspace_id: &str, doc_id: &str) {
        let key = CacheKey::new(workspace_id, doc_id);
        let cell = Arc::new(OnceCell::new());
        let _ = cell.set(None);
        self.inner.insert(key, cell);
    }
}

#[derive(Default)]
pub struct RequestDocRoleCache {
    inner: DashMap<PermissionCacheKey, Arc<OnceCell<Option<DocumentRoleRecord>>>>,
}

impl RequestDocRoleCache {
    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
        fetcher: F,
    ) -> Result<Option<DocumentRoleRecord>, AppError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Option<DocumentRoleRecord>, AppError>>,
    {
        let key = PermissionCacheKey::new(workspace_id, doc_id, user_id);
        let cell = self
            .inner
            .entry(key)
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        if let Some(value) = cell.get() {
            return Ok(value.clone());
        }

        let fetched = fetcher().await?;
        if cell.set(fetched.clone()).is_err() {
            if let Some(existing) = cell.get() {
                return Ok(existing.clone());
            }
        }

        Ok(fetched)
    }

    pub(crate) fn invalidate(&self, workspace_id: &str, doc_id: &str, user_id: &str) {
        self.inner
            .remove(&PermissionCacheKey::new(workspace_id, doc_id, user_id));
    }
}

#[derive(Default)]
pub struct RequestDocPermissionCache {
    inner: DashMap<PermissionCacheKey, Arc<OnceCell<Option<DocPermissions>>>>,
}

impl RequestDocPermissionCache {
    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        workspace_id: &str,
        doc_id: &str,
        user_id: &str,
        fetcher: F,
    ) -> Result<Option<DocPermissions>, AppError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Option<DocPermissions>, AppError>>,
    {
        let key = PermissionCacheKey::new(workspace_id, doc_id, user_id);
        let cell = self
            .inner
            .entry(key)
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        if let Some(value) = cell.get() {
            return Ok(value.clone());
        }

        let fetched = fetcher().await?;
        if cell.set(fetched.clone()).is_err() {
            if let Some(existing) = cell.get() {
                return Ok(existing.clone());
            }
        }

        Ok(fetched)
    }

    pub(crate) fn invalidate(&self, workspace_id: &str, doc_id: &str, user_id: &str) {
        self.inner
            .remove(&PermissionCacheKey::new(workspace_id, doc_id, user_id));
    }
}

#[derive(Default)]
pub struct RequestWorkspaceCache {
    inner: DashMap<String, Arc<OnceCell<WorkspaceRecord>>>,
}

impl RequestWorkspaceCache {
    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        workspace_id: &str,
        fetcher: F,
    ) -> Result<WorkspaceRecord, AppError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<WorkspaceRecord, AppError>>,
    {
        let cell = self
            .inner
            .entry(workspace_id.to_owned())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        if let Some(value) = cell.get() {
            return Ok(value.clone());
        }

        let record = fetcher().await?;
        if cell.set(record.clone()).is_err() {
            if let Some(existing) = cell.get() {
                return Ok(existing.clone());
            }
        }

        Ok(record)
    }

    pub(crate) fn insert(&self, record: WorkspaceRecord) {
        let cell = Arc::new(OnceCell::new());
        let _ = cell.set(record.clone());
        self.inner.insert(record.id.clone(), cell);
    }
}

#[derive(Default)]
pub struct RequestUserCache {
    inner: DashMap<String, Arc<OnceCell<Option<UserRecord>>>>,
}

impl RequestUserCache {
    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        user_id: &str,
        fetcher: F,
    ) -> Result<Option<UserRecord>, AppError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Option<UserRecord>, AppError>>,
    {
        let cell = self
            .inner
            .entry(user_id.to_owned())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        if let Some(value) = cell.get() {
            return Ok(value.clone());
        }

        let fetched = fetcher().await?;
        if cell.set(fetched.clone()).is_err() {
            if let Some(existing) = cell.get() {
                return Ok(existing.clone());
            }
        }

        Ok(fetched)
    }

    pub(crate) fn invalidate(&self, user_id: &str) {
        self.inner.remove(user_id);
    }
}

#[derive(Default)]
pub struct RequestUserAdminCache {
    inner: DashMap<String, Arc<OnceCell<bool>>>,
}

impl RequestUserAdminCache {
    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        user_id: &str,
        fetcher: F,
    ) -> Result<bool, AppError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<bool, AppError>>,
    {
        let cell = self
            .inner
            .entry(user_id.to_owned())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        if let Some(value) = cell.get() {
            return Ok(*value);
        }

        let fetched = fetcher().await?;
        if cell.set(fetched).is_err() {
            if let Some(existing) = cell.get() {
                return Ok(*existing);
            }
        }

        Ok(fetched)
    }

    pub(crate) fn invalidate(&self, user_id: &str) {
        self.inner.remove(user_id);
    }
}

#[derive(Clone, Eq)]
struct CacheKey {
    workspace_id: String,
    doc_id: String,
}

impl CacheKey {
    fn new(workspace_id: &str, doc_id: &str) -> Self {
        Self {
            workspace_id: workspace_id.to_owned(),
            doc_id: doc_id.to_owned(),
        }
    }

    fn from_metadata(metadata: &DocumentMetadata) -> Self {
        Self::new(&metadata.workspace_id, &metadata.id)
    }
}

impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.workspace_id == other.workspace_id && self.doc_id == other.doc_id
    }
}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.workspace_id.hash(state);
        self.doc_id.hash(state);
    }
}

#[derive(Clone, Eq)]
struct PermissionCacheKey {
    workspace_id: String,
    doc_id: String,
    user_id: String,
}

impl PermissionCacheKey {
    fn new(workspace_id: &str, doc_id: &str, user_id: &str) -> Self {
        Self {
            workspace_id: workspace_id.to_owned(),
            doc_id: doc_id.to_owned(),
            user_id: user_id.to_owned(),
        }
    }
}

impl PartialEq for PermissionCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.workspace_id == other.workspace_id
            && self.doc_id == other.doc_id
            && self.user_id == other.user_id
    }
}

impl Hash for PermissionCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.workspace_id.hash(state);
        self.doc_id.hash(state);
        self.user_id.hash(state);
    }
}

tokio::task_local! {
    static ACTIVE_REQUEST_CACHES: RequestCaches;
}

pub async fn scope_with_request_caches<Fut, T>(caches: RequestCaches, fut: Fut) -> T
where
    Fut: std::future::Future<Output = T>,
{
    ACTIVE_REQUEST_CACHES.scope(caches, fut).await
}

pub fn current_request_caches() -> Option<RequestCaches> {
    ACTIVE_REQUEST_CACHES.try_with(|c| c.clone()).ok()
}
