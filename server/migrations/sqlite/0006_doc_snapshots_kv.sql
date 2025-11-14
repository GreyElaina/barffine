ALTER TABLE documents
ADD COLUMN snapshot_kv_key TEXT;

ALTER TABLE document_history
ADD COLUMN snapshot_kv_key TEXT;
