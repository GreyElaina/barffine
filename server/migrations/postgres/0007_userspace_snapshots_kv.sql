CREATE TABLE IF NOT EXISTS user_documents (
    user_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    snapshot BYTEA NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    editor_id TEXT,
    PRIMARY KEY (user_id, doc_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

ALTER TABLE user_documents
ADD COLUMN snapshot_kv_key TEXT;
