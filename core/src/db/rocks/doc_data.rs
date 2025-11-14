use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, DEFAULT_COLUMN_FAMILY_NAME,
    Options,
};

pub struct DocDataStore {
    db: Arc<DB>,
    path: PathBuf,
}

impl crate::doc_data::DocDataBackend for DocDataStore {
    fn put_log_payload(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.db.put_cf(self.logs_cf(), key.as_bytes(), bytes)?;
        Ok(())
    }

    fn delete_log_payload(&self, key: &str) -> Result<()> {
        self.db.delete_cf(self.logs_cf(), key.as_bytes())?;
        Ok(())
    }

    fn load_log_payload(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let value = self.db.get_cf(self.logs_cf(), key.as_bytes())?;
        Ok(value)
    }

    fn put_snapshot(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.db.put_cf(self.snapshots_cf(), key.as_bytes(), bytes)?;
        Ok(())
    }

    fn get_snapshot(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let value = self.db.get_cf(self.snapshots_cf(), key.as_bytes())?;
        Ok(value)
    }

    fn delete_snapshot(&self, key: &str) -> Result<()> {
        self.db.delete_cf(self.snapshots_cf(), key.as_bytes())?;
        Ok(())
    }

    fn put_cache_entry(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.db.put_cf(self.cache_cf(), key.as_bytes(), bytes)?;
        Ok(())
    }

    fn get_cache_entry(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let value = self.db.get_cf(self.cache_cf(), key.as_bytes())?;
        Ok(value)
    }

    fn delete_cache_entry(&self, key: &str) -> Result<()> {
        self.db.delete_cf(self.cache_cf(), key.as_bytes())?;
        Ok(())
    }
}

impl DocDataStore {
    pub const DOC_LOG_CF: &'static str = "doc_logs";
    pub const DOC_SNAPSHOT_CF: &'static str = "doc_snapshots";
    pub const DOC_CACHE_CF: &'static str = "doc_cache";
    pub const NOTIFICATION_CF: &'static str = "notifications";
    /// Column family used for long-lived document history snapshots.
    pub const DOC_HISTORY_CF: &'static str = "doc_history";
    /// Column family used by Rocks-backed doc store implementations for
    /// workspace and userspace document metadata and snapshots.
    pub const DOC_STORE_CF: &'static str = "doc_store";

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create doc data directory at {}",
                    parent.display()
                )
            })?;
        }

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_compression_type(DBCompressionType::Lz4);

        let mut cf_opts = Options::default();
        cf_opts.set_compression_type(DBCompressionType::Lz4);

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(DEFAULT_COLUMN_FAMILY_NAME, cf_opts.clone()),
            ColumnFamilyDescriptor::new(Self::DOC_LOG_CF, cf_opts.clone()),
            ColumnFamilyDescriptor::new(Self::DOC_SNAPSHOT_CF, cf_opts.clone()),
            ColumnFamilyDescriptor::new(Self::DOC_CACHE_CF, cf_opts.clone()),
            ColumnFamilyDescriptor::new(Self::NOTIFICATION_CF, cf_opts.clone()),
            ColumnFamilyDescriptor::new(Self::DOC_STORE_CF, cf_opts.clone()),
            ColumnFamilyDescriptor::new(Self::DOC_HISTORY_CF, cf_opts),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors).with_context(|| {
            format!(
                "failed to open rocksdb doc data store at {}",
                path.display()
            )
        })?;

        Ok(Self {
            db: Arc::new(db),
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn db(&self) -> Arc<DB> {
        self.db.clone()
    }

    pub fn logs_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle(Self::DOC_LOG_CF)
            .expect("doc_logs column family present")
    }

    pub fn snapshots_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle(Self::DOC_SNAPSHOT_CF)
            .expect("doc_snapshots column family present")
    }

    pub fn cache_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle(Self::DOC_CACHE_CF)
            .expect("doc_cache column family present")
    }

    pub fn notifications_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle(Self::NOTIFICATION_CF)
            .expect("notifications column family present")
    }

    pub fn doc_store_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle(Self::DOC_STORE_CF)
            .expect("doc_store column family present")
    }

    pub fn history_cf(&self) -> &ColumnFamily {
        self.db
            .cf_handle(Self::DOC_HISTORY_CF)
            .expect("doc_history column family present")
    }

    pub fn put_snapshot(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.db.put_cf(self.snapshots_cf(), key.as_bytes(), bytes)?;
        Ok(())
    }

    pub fn get_snapshot(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let value = self.db.get_cf(self.snapshots_cf(), key.as_bytes())?;
        Ok(value)
    }

    pub fn delete_snapshot(&self, key: &str) -> Result<()> {
        self.db.delete_cf(self.snapshots_cf(), key.as_bytes())?;
        Ok(())
    }

    pub fn put_cache_entry(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.db.put_cf(self.cache_cf(), key.as_bytes(), bytes)?;
        Ok(())
    }

    pub fn get_cache_entry(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let value = self.db.get_cf(self.cache_cf(), key.as_bytes())?;
        Ok(value)
    }

    pub fn delete_cache_entry(&self, key: &str) -> Result<()> {
        self.db.delete_cf(self.cache_cf(), key.as_bytes())?;
        Ok(())
    }
}
