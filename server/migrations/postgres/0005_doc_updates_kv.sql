ALTER TABLE doc_updates
ADD COLUMN kv_key TEXT;

CREATE INDEX IF NOT EXISTS idx_doc_updates_kv
    ON doc_updates(kv_key)
    WHERE kv_key IS NOT NULL;
