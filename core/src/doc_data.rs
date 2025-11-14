use anyhow::Result;

/// Abstraction for the doc-data KV responsibilities, implemented by each
/// concrete backend that supports external doc data storage.
///
/// Responsibilities include:
/// - External payloads for doc update logs
/// - Persisted document snapshots
/// - Doc cache persistence
pub trait DocDataBackend: Send + Sync {
    // --- Doc log payloads ---

    /// Store a doc log payload under the given key.
    fn put_log_payload(&self, key: &str, bytes: &[u8]) -> Result<()>;

    /// Delete a previously stored doc log payload by key.
    fn delete_log_payload(&self, key: &str) -> Result<()>;

    /// Load a doc log payload by key, returning `Ok(None)` if not present.
    fn load_log_payload(&self, key: &str) -> Result<Option<Vec<u8>>>;

    // --- Snapshots ---

    fn put_snapshot(&self, key: &str, bytes: &[u8]) -> Result<()>;
    fn get_snapshot(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn delete_snapshot(&self, key: &str) -> Result<()>;

    // --- Doc cache ---

    fn put_cache_entry(&self, key: &str, bytes: &[u8]) -> Result<()>;
    fn get_cache_entry(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn delete_cache_entry(&self, key: &str) -> Result<()>;
}

/// RocksDB-backed implementation for `DocDataBackend`.
pub use crate::db::rocks::doc_data::DocDataStore as RocksDocDataStore;
