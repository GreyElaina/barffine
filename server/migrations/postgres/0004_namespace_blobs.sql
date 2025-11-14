CREATE TABLE IF NOT EXISTS namespace_blobs (
    namespace TEXT NOT NULL,
    blob_key TEXT NOT NULL,
    content BYTEA NOT NULL,
    content_type TEXT,
    content_length BIGINT NOT NULL,
    etag TEXT,
    last_modified BIGINT,
    created_at BIGINT NOT NULL,
    deleted_at BIGINT,
    PRIMARY KEY (namespace, blob_key)
);

CREATE INDEX IF NOT EXISTS idx_namespace_blobs_deleted
    ON namespace_blobs(namespace, deleted_at);
