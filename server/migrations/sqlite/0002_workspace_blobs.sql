CREATE TABLE IF NOT EXISTS workspace_blobs (
    workspace_id TEXT NOT NULL,
    blob_key TEXT NOT NULL,
    content BLOB NOT NULL,
    content_type TEXT,
    content_length INTEGER NOT NULL,
    etag TEXT,
    last_modified INTEGER,
    created_at INTEGER NOT NULL,
    deleted_at INTEGER,
    PRIMARY KEY (workspace_id, blob_key),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_workspace_blobs_deleted
    ON workspace_blobs(workspace_id, deleted_at);
