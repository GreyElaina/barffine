CREATE INDEX IF NOT EXISTS idx_documents_workspace_created_at
    ON documents(workspace_id, created_at, id)
    WHERE trashed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_documents_workspace_updated_at
    ON documents(workspace_id, updated_at, id)
    WHERE trashed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_document_history_lookup
    ON document_history(workspace_id, doc_id, created_at, id);

