use std::{fs, fs::File, path::PathBuf, sync::Arc};
use self::postgres::{
    access_token_repo::PostgresAccessTokenRepository, blob_repo::PostgresBlobRepository,
    comment_attachment_repo::PostgresCommentAttachmentRepository,
    comment_repo::PostgresCommentRepository, connection as postgres_connection,
    doc_public_link_store::PostgresDocPublicLinkStore, doc_repo::PostgresDocRepository,
    doc_role_repo::PostgresDocRoleRepository, doc_update_log_store::PostgresDocUpdateLogStore,
    user_doc_repo::PostgresUserDocRepository, user_repo::PostgresUserRepository,
    user_settings_repo::PostgresUserSettingsRepository,
    workspace_feature_repo::PostgresWorkspaceFeatureRepository,
    workspace_repo::PostgresWorkspaceRepository,
};
use self::{
    access_token_repo::AccessTokenRepositoryRef,
    blob_repo::BlobRepositoryRef,
    comment_attachment_repo::CommentAttachmentRepositoryRef,
    comment_repo::CommentRepositoryRef,
    doc_public_link_store::DocPublicLinkStoreRef,
    doc_repo::DocRepositoryRef,
    doc_role_repo::DocRoleRepositoryRef,
    sqlite::{
        access_token_repo::SqliteAccessTokenRepository, blob_repo::SqliteBlobRepository,
        comment_attachment_repo::SqliteCommentAttachmentRepository,
        comment_repo::SqliteCommentRepository, connection as sqlite_connection,
        doc_data::SqliteDocDataStore, doc_public_link_store::SqliteDocPublicLinkStore,
        doc_repo::SqliteDocRepository, doc_role_repo::SqliteDocRoleRepository,
        doc_update_log_store::SqliteDocUpdateLogStore, user_doc_repo::SqliteUserDocRepository,
        user_repo::SqliteUserRepository, user_settings_repo::SqliteUserSettingsRepository,
        workspace_feature_repo::SqliteWorkspaceFeatureRepository,
        workspace_repo::SqliteWorkspaceRepository,
    },
    user_doc_repo::UserDocRepositoryRef,
    user_repo::UserRepositoryRef,
    user_settings_repo::UserSettingsRepositoryRef,
    workspace_feature_repo::WorkspaceFeatureRepositoryRef,
    workspace_repo::WorkspaceRepositoryRef,
};
use crate::{
    config::{self, AppConfig, DatabaseBackend, DocDataBackend, DocStoreBackend},
    doc_data::{DocDataBackend as DocDataStoreBackend, RocksDocDataStore},
    doc_snapshot_store::DocSnapshotStore,
    doc_update_log::DocUpdateLogReaderRef,
};
use anyhow::{Context, Result};

pub mod access_token_repo;
pub mod blob_repo;
pub mod comment_attachment_repo;
pub mod comment_repo;
pub mod doc_public_link_store;
pub mod doc_repo;
pub mod doc_role_repo;
pub mod postgres;
pub mod rocks;
pub mod sql;
pub mod sqlite;
pub mod user_doc_repo;
pub mod user_repo;
pub mod user_settings_repo;
pub mod workspace_feature_repo;
pub mod workspace_repo;

#[derive(Clone)]
pub struct RepositoryRegistry {
    doc_repo: DocRepositoryRef,
    workspace_repo: WorkspaceRepositoryRef,
    user_repo: UserRepositoryRef,
    doc_role_repo: DocRoleRepositoryRef,
    comment_repo: CommentRepositoryRef,
    blob_repo: BlobRepositoryRef,
    access_token_repo: AccessTokenRepositoryRef,
    user_settings_repo: UserSettingsRepositoryRef,
    comment_attachment_repo: CommentAttachmentRepositoryRef,
    workspace_feature_repo: WorkspaceFeatureRepositoryRef,
    user_doc_repo: UserDocRepositoryRef,
}

impl RepositoryRegistry {
    pub fn new(
        doc_repo: DocRepositoryRef,
        workspace_repo: WorkspaceRepositoryRef,
        user_repo: UserRepositoryRef,
        doc_role_repo: DocRoleRepositoryRef,
        comment_repo: CommentRepositoryRef,
        blob_repo: BlobRepositoryRef,
        access_token_repo: AccessTokenRepositoryRef,
        user_settings_repo: UserSettingsRepositoryRef,
        comment_attachment_repo: CommentAttachmentRepositoryRef,
        workspace_feature_repo: WorkspaceFeatureRepositoryRef,
        user_doc_repo: UserDocRepositoryRef,
    ) -> Self {
        Self {
            doc_repo,
            workspace_repo,
            user_repo,
            doc_role_repo,
            comment_repo,
            blob_repo,
            access_token_repo,
            user_settings_repo,
            comment_attachment_repo,
            workspace_feature_repo,
            user_doc_repo,
        }
    }

    pub fn doc_repo(&self) -> DocRepositoryRef {
        self.doc_repo.clone()
    }

    pub fn workspace_repo(&self) -> WorkspaceRepositoryRef {
        self.workspace_repo.clone()
    }

    pub fn user_repo(&self) -> UserRepositoryRef {
        self.user_repo.clone()
    }

    pub fn doc_role_repo(&self) -> DocRoleRepositoryRef {
        self.doc_role_repo.clone()
    }

    pub fn comment_repo(&self) -> CommentRepositoryRef {
        self.comment_repo.clone()
    }

    pub fn blob_repo(&self) -> BlobRepositoryRef {
        self.blob_repo.clone()
    }

    pub fn access_token_repo(&self) -> AccessTokenRepositoryRef {
        self.access_token_repo.clone()
    }

    pub fn user_settings_repo(&self) -> UserSettingsRepositoryRef {
        self.user_settings_repo.clone()
    }

    pub fn comment_attachment_repo(&self) -> CommentAttachmentRepositoryRef {
        self.comment_attachment_repo.clone()
    }

    pub fn workspace_feature_repo(&self) -> WorkspaceFeatureRepositoryRef {
        self.workspace_feature_repo.clone()
    }

    pub fn user_doc_repo(&self) -> UserDocRepositoryRef {
        self.user_doc_repo.clone()
    }
}

#[derive(Clone)]
enum DatabasePool {
    Sqlite(sqlite_connection::SqlitePool),
    Postgres(postgres_connection::PostgresPool),
}

#[derive(Clone)]
pub struct Database {
    pool: DatabasePool,
    path: PathBuf,
    repositories: Arc<RepositoryRegistry>,
    doc_update_logs: DocUpdateLogReaderRef,
    doc_data: Option<Arc<RocksDocDataStore>>,
    doc_snapshots: Arc<DocSnapshotStore>,
    backend: DatabaseBackend,
}

impl Database {
    const SQLITE_FILE_NAME: &'static str = "barffine.db";

    pub async fn connect(config: &AppConfig) -> Result<Self> {
        match config.database_backend {
            DatabaseBackend::Sqlite => Self::connect_sqlite(config).await,
            DatabaseBackend::Postgres => Self::connect_postgres(config).await,
        }
    }

    async fn connect_sqlite(config: &AppConfig) -> Result<Self> {
        let (data_dir, db_file) = Self::resolve_database_paths(&config.database_path)?;
        fs::create_dir_all(&data_dir).with_context(|| {
            format!(
                "failed to create database directory: {}",
                data_dir.display()
            )
        })?;

        if !db_file.exists() {
            File::create(&db_file).with_context(|| {
                format!("failed to create database file: {}", db_file.display())
            })?;
        }

        let pool =
            sqlite_connection::create_pool(&db_file, config.database_max_connections).await?;
        sqlite_connection::run_migrations(&pool).await?;

        // For SQLite we support two doc-data backends:
        // - RocksDb, using RocksDocDataStore
        // - Sqlite, using SqliteDocDataStore for log payloads only
        let (doc_data, doc_data_logs_backend, doc_snapshots_backend): (
            Option<Arc<RocksDocDataStore>>,
            Option<Arc<dyn DocDataStoreBackend>>,
            Option<Arc<dyn DocDataStoreBackend>>,
        ) = match config.doc_data_backend {
            DocDataBackend::Sqlite => {
                let sqlite_store =
                    Arc::new(SqliteDocDataStore::new(pool.clone())) as Arc<dyn DocDataStoreBackend>;
                (None, Some(sqlite_store), None)
            }
            DocDataBackend::RocksDb => {
                let kv_path = Self::resolve_db_path(&config.doc_data_path)?;
                let rocks = Arc::new(RocksDocDataStore::open(&kv_path)?);
                let backend = rocks.clone() as Arc<dyn DocDataStoreBackend>;
                (Some(rocks), Some(backend.clone()), Some(backend))
            }
        };

        let sqlite_doc_logs = Arc::new(SqliteDocUpdateLogStore::new(
            pool.clone(),
            doc_data_logs_backend.clone(),
        ));
        let doc_snapshots = Arc::new(DocSnapshotStore::new(doc_snapshots_backend.clone()));

        let (doc_update_logs, doc_repo, user_doc_repo): (
            DocUpdateLogReaderRef,
            DocRepositoryRef,
            UserDocRepositoryRef,
        ) = match config.doc_store_backend {
            DocStoreBackend::Sql => {
                let doc_update_logs: DocUpdateLogReaderRef = sqlite_doc_logs.clone();
                let doc_repo = Arc::new(SqliteDocRepository::new(
                    pool.clone(),
                    sqlite_doc_logs.clone(),
                    doc_snapshots.clone(),
                )) as DocRepositoryRef;
                let user_doc_repo = Arc::new(SqliteUserDocRepository::new(
                    pool.clone(),
                    sqlite_doc_logs.clone(),
                    doc_snapshots.clone(),
                )) as UserDocRepositoryRef;
                (doc_update_logs, doc_repo, user_doc_repo)
            }
            DocStoreBackend::RocksDb => {
                let rocks_store = doc_data.clone().unwrap_or_else(|| {
                    panic!(
                        "DocStoreBackend::RocksDb requires DocDataBackend::RocksDb when using sqlite"
                    )
                });
                let rocks_doc_logs = Arc::new(
                    crate::db::rocks::doc_update_log_store::RocksDocUpdateLogStore::new(
                        rocks_store.clone(),
                    ),
                );
                let links: DocPublicLinkStoreRef =
                    Arc::new(SqliteDocPublicLinkStore::new(pool.clone()));
                let doc_update_logs: DocUpdateLogReaderRef = rocks_doc_logs.clone();
                let doc_repo = Arc::new(crate::db::rocks::doc_repo::RocksDocRepository::new(
                    rocks_store.clone(),
                    rocks_doc_logs.clone(),
                    links,
                )) as DocRepositoryRef;
                let user_doc_repo = Arc::new(
                    crate::db::rocks::user_doc_repo::RocksUserDocRepository::new(
                        rocks_store.clone(),
                        rocks_doc_logs.clone(),
                    ),
                ) as UserDocRepositoryRef;
                (doc_update_logs, doc_repo, user_doc_repo)
            }
        };
        let workspace_repo =
            Arc::new(SqliteWorkspaceRepository::new(pool.clone())) as WorkspaceRepositoryRef;
        let user_repo = Arc::new(SqliteUserRepository::new(pool.clone())) as UserRepositoryRef;
        let doc_role_repo =
            Arc::new(SqliteDocRoleRepository::new(pool.clone())) as DocRoleRepositoryRef;
        let comment_repo =
            Arc::new(SqliteCommentRepository::new(pool.clone())) as CommentRepositoryRef;
        let blob_repo = Arc::new(SqliteBlobRepository::new(pool.clone())) as BlobRepositoryRef;
        let access_token_repo =
            Arc::new(SqliteAccessTokenRepository::new(pool.clone())) as AccessTokenRepositoryRef;
        let comment_attachment_repo = Arc::new(SqliteCommentAttachmentRepository::new(pool.clone()))
            as CommentAttachmentRepositoryRef;
        let workspace_feature_repo = Arc::new(SqliteWorkspaceFeatureRepository::new(pool.clone()))
            as WorkspaceFeatureRepositoryRef;
        let user_settings_repo =
            Arc::new(SqliteUserSettingsRepository::new(pool.clone())) as UserSettingsRepositoryRef;
        let repositories = Arc::new(RepositoryRegistry::new(
            doc_repo,
            workspace_repo,
            user_repo,
            doc_role_repo,
            comment_repo,
            blob_repo,
            access_token_repo,
            user_settings_repo,
            comment_attachment_repo,
            workspace_feature_repo,
            user_doc_repo,
        ));

        Ok(Self {
            pool: DatabasePool::Sqlite(pool),
            path: data_dir,
            repositories,
            doc_update_logs,
            doc_data,
            doc_snapshots,
            backend: DatabaseBackend::Sqlite,
        })
    }

    pub fn pool(&self) -> &sqlite_connection::SqlitePool {
        match &self.pool {
            DatabasePool::Sqlite(pool) => pool,
            DatabasePool::Postgres(_) => panic!("sqlite pool unavailable when backend is postgres"),
        }
    }

    pub fn postgres_pool(&self) -> Option<&postgres_connection::PostgresPool> {
        match &self.pool {
            DatabasePool::Postgres(pool) => Some(pool),
            DatabasePool::Sqlite(_) => None,
        }
    }

    pub fn database_path(&self) -> &PathBuf {
        &self.path
    }

    pub fn repositories(&self) -> Arc<RepositoryRegistry> {
        self.repositories.clone()
    }

    pub fn doc_update_logs(&self) -> DocUpdateLogReaderRef {
        self.doc_update_logs.clone()
    }

    pub fn doc_data_store(&self) -> Option<Arc<RocksDocDataStore>> {
        self.doc_data.clone()
    }

    pub fn doc_snapshot_store(&self) -> Arc<DocSnapshotStore> {
        self.doc_snapshots.clone()
    }

    pub fn backend(&self) -> DatabaseBackend {
        self.backend
    }

    fn resolve_database_paths(path: &str) -> Result<(PathBuf, PathBuf)> {
        if config::database_path_is_file(path) {
            let db_file = Self::resolve_db_path(path)?;
            let dir = if let Some(parent) = db_file.parent() {
                parent.to_path_buf()
            } else {
                std::env::current_dir().context("failed to obtain current directory")?
            };
            Ok((dir, db_file))
        } else {
            let data_dir = Self::resolve_db_path(path)?;
            Ok((data_dir.clone(), data_dir.join(Self::SQLITE_FILE_NAME)))
        }
    }

    fn resolve_db_path(path: &str) -> Result<PathBuf> {
        let path = PathBuf::from(path);
        if path.is_absolute() {
            Ok(path)
        } else {
            let cwd = std::env::current_dir().context("failed to obtain current directory")?;
            Ok(cwd.join(path))
        }
    }

    async fn connect_postgres(config: &AppConfig) -> Result<Self> {
        let url = config
            .database_url
            .as_deref()
            .context("BARFFINE_DATABASE_URL must be set when BARFFINE_DATABASE_BACKEND=postgres")?;
        let pool = postgres_connection::create_pool(url, config.database_max_connections).await?;
        postgres_connection::run_migrations(&pool).await?;

        let (data_dir, _) = Self::resolve_database_paths(&config.database_path)?;
        let doc_data: Option<Arc<RocksDocDataStore>> = match config.doc_data_backend {
            DocDataBackend::Sqlite => None,
            DocDataBackend::RocksDb => {
                let kv_path = Self::resolve_db_path(&config.doc_data_path)?;
                Some(Arc::new(RocksDocDataStore::open(&kv_path)?))
            }
        };

        let doc_data_backend = doc_data
            .clone()
            .map(|store| store as Arc<dyn DocDataStoreBackend>);

        let pg_doc_logs = Arc::new(PostgresDocUpdateLogStore::new(
            pool.clone(),
            doc_data_backend.clone(),
        ));
        let doc_snapshots = Arc::new(DocSnapshotStore::new(doc_data_backend.clone()));

        let (doc_update_logs, doc_repo, user_doc_repo): (
            DocUpdateLogReaderRef,
            DocRepositoryRef,
            UserDocRepositoryRef,
        ) = match config.doc_store_backend {
            DocStoreBackend::Sql => {
                let doc_update_logs: DocUpdateLogReaderRef = pg_doc_logs.clone();
                let doc_repo = Arc::new(PostgresDocRepository::new(
                    pool.clone(),
                    pg_doc_logs.clone(),
                    doc_snapshots.clone(),
                )) as DocRepositoryRef;
                let user_doc_repo = Arc::new(PostgresUserDocRepository::new(
                    pool.clone(),
                    pg_doc_logs.clone(),
                    doc_snapshots.clone(),
                )) as UserDocRepositoryRef;
                (doc_update_logs, doc_repo, user_doc_repo)
            }
            DocStoreBackend::RocksDb => {
                let rocks_store = doc_data.clone().unwrap_or_else(|| {
                    panic!(
                        "DocStoreBackend::RocksDb requires DocDataBackend::RocksDb when using postgres"
                    )
                });
                let rocks_doc_logs = Arc::new(
                    crate::db::rocks::doc_update_log_store::RocksDocUpdateLogStore::new(
                        rocks_store.clone(),
                    ),
                );
                let links: DocPublicLinkStoreRef =
                    Arc::new(PostgresDocPublicLinkStore::new(pool.clone()));
                let doc_update_logs: DocUpdateLogReaderRef = rocks_doc_logs.clone();
                let doc_repo = Arc::new(crate::db::rocks::doc_repo::RocksDocRepository::new(
                    rocks_store.clone(),
                    rocks_doc_logs.clone(),
                    links,
                )) as DocRepositoryRef;
                let user_doc_repo = Arc::new(
                    crate::db::rocks::user_doc_repo::RocksUserDocRepository::new(
                        rocks_store.clone(),
                        rocks_doc_logs.clone(),
                    ),
                ) as UserDocRepositoryRef;
                (doc_update_logs, doc_repo, user_doc_repo)
            }
        };
        let workspace_repo =
            Arc::new(PostgresWorkspaceRepository::new(pool.clone())) as WorkspaceRepositoryRef;
        let user_repo = Arc::new(PostgresUserRepository::new(pool.clone())) as UserRepositoryRef;
        let doc_role_repo =
            Arc::new(PostgresDocRoleRepository::new(pool.clone())) as DocRoleRepositoryRef;
        let comment_repo =
            Arc::new(PostgresCommentRepository::new(pool.clone())) as CommentRepositoryRef;
        let blob_repo = Arc::new(PostgresBlobRepository::new(pool.clone())) as BlobRepositoryRef;
        let access_token_repo =
            Arc::new(PostgresAccessTokenRepository::new(pool.clone())) as AccessTokenRepositoryRef;
        let comment_attachment_repo =
            Arc::new(PostgresCommentAttachmentRepository::new(pool.clone()))
                as CommentAttachmentRepositoryRef;
        let workspace_feature_repo = Arc::new(PostgresWorkspaceFeatureRepository::new(pool.clone()))
            as WorkspaceFeatureRepositoryRef;
        let user_settings_repo = Arc::new(PostgresUserSettingsRepository::new(pool.clone()))
            as UserSettingsRepositoryRef;

        let repositories = Arc::new(RepositoryRegistry::new(
            doc_repo,
            workspace_repo,
            user_repo,
            doc_role_repo,
            comment_repo,
            blob_repo,
            access_token_repo,
            user_settings_repo,
            comment_attachment_repo,
            workspace_feature_repo,
            user_doc_repo,
        ));

        Ok(Self {
            pool: DatabasePool::Postgres(pool),
            path: data_dir,
            repositories,
            doc_update_logs,
            doc_data,
            doc_snapshots,
            backend: DatabaseBackend::Postgres,
        })
    }
}
