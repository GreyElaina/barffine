CREATE TABLE IF NOT EXISTS user_documents (
    user_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    snapshot BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    editor_id TEXT,
    PRIMARY KEY (user_id, doc_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

ALTER TABLE user_documents
ADD COLUMN snapshot_kv_key TEXT;
