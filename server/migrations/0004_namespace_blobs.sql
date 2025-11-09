CREATE TABLE IF NOT EXISTS namespace_blobs (
    namespace TEXT NOT NULL,
    blob_key TEXT NOT NULL,
    content BLOB NOT NULL,
    content_type TEXT,
    content_length INTEGER NOT NULL,
    etag TEXT,
    last_modified INTEGER,
    created_at INTEGER NOT NULL,
    deleted_at INTEGER,
    PRIMARY KEY (namespace, blob_key)
);

CREATE INDEX IF NOT EXISTS idx_namespace_blobs_deleted
    ON namespace_blobs(namespace, deleted_at);
