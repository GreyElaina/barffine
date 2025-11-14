CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    name TEXT,
    avatar_url TEXT,
    email_verified_at BIGINT,
    disabled BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workspaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    public BIGINT NOT NULL DEFAULT 0,
    enable_ai BIGINT NOT NULL DEFAULT 0,
    enable_doc_embedding BIGINT NOT NULL DEFAULT 0,
    enable_url_preview BIGINT NOT NULL DEFAULT 0,
    avatar_key TEXT,
    indexed BIGINT NOT NULL DEFAULT 0,
    last_check_embeddings BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_workspaces_last_check_embeddings
    ON workspaces(last_check_embeddings);

CREATE TABLE IF NOT EXISTS workspace_members (
    workspace_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    role TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Accepted',
    source TEXT,
    inviter_id TEXT,
    invited_at BIGINT,
    PRIMARY KEY (workspace_id, user_id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    snapshot BYTEA NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    default_role TEXT NOT NULL DEFAULT 'manager',
    public BIGINT NOT NULL DEFAULT 0,
    mode TEXT NOT NULL DEFAULT 'page',
    title TEXT,
    summary TEXT,
    creator_id TEXT,
    updater_id TEXT,
    trashed_at BIGINT,
    trashed_by TEXT,
    blocked BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_documents_workspace ON documents(workspace_id);

CREATE INDEX IF NOT EXISTS idx_documents_workspace_active
    ON documents(workspace_id, id)
    WHERE trashed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_documents_workspace_blocked
    ON documents(workspace_id, blocked);

CREATE TABLE IF NOT EXISTS doc_public_links (
    workspace_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    token TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, doc_id),
    UNIQUE(token),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (doc_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS document_history (
    id BIGSERIAL PRIMARY KEY,
    doc_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    snapshot BYTEA NOT NULL,
    created_at BIGINT NOT NULL,
    FOREIGN KEY (doc_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_document_history_doc ON document_history(doc_id);

CREATE TABLE IF NOT EXISTS doc_user_roles (
    workspace_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, doc_id, user_id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (doc_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_doc_user_roles_doc
    ON doc_user_roles(workspace_id, doc_id, created_at DESC);

CREATE TABLE IF NOT EXISTS access_tokens (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    created_at BIGINT NOT NULL,
    expires_at BIGINT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_access_tokens_user ON access_tokens(user_id);

CREATE TABLE IF NOT EXISTS admin_users (
    user_id TEXT PRIMARY KEY,
    created_at BIGINT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS doc_comments (
    sid BIGSERIAL NOT NULL,
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    author_id TEXT NOT NULL,
    body TEXT NOT NULL,
    metadata TEXT NOT NULL,
    resolved BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_doc_comments_sid ON doc_comments(sid);

CREATE INDEX IF NOT EXISTS idx_doc_comments_doc
    ON doc_comments(workspace_id, doc_id);

CREATE TABLE IF NOT EXISTS doc_comment_replies (
    id TEXT PRIMARY KEY,
    comment_id TEXT NOT NULL,
    author_id TEXT NOT NULL,
    body TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    FOREIGN KEY (comment_id) REFERENCES doc_comments(id) ON DELETE CASCADE,
    FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_doc_comment_replies_comment
    ON doc_comment_replies(comment_id);

CREATE TABLE IF NOT EXISTS comment_attachments (
    workspace_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    key TEXT NOT NULL,
    name TEXT NOT NULL,
    mime TEXT NOT NULL,
    size BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    created_by TEXT,
    PRIMARY KEY (workspace_id, doc_id, key),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_comment_attachments_doc
    ON comment_attachments(workspace_id, doc_id);

CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    read BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_notifications_user
    ON notifications(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS workspace_invite_links (
    workspace_id TEXT PRIMARY KEY,
    token TEXT NOT NULL,
    expires_at BIGINT NOT NULL,
    inviter_id TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_workspace_invite_links_token
    ON workspace_invite_links(token);

CREATE TABLE IF NOT EXISTS workspace_feature_flags (
    workspace_id TEXT NOT NULL,
    key TEXT NOT NULL,
    enabled BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, key),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_workspace_feature_flags_workspace
    ON workspace_feature_flags(workspace_id);

CREATE TABLE IF NOT EXISTS user_settings (
    user_id TEXT PRIMARY KEY,
    receive_comment_email BIGINT NOT NULL DEFAULT 1,
    receive_invitation_email BIGINT NOT NULL DEFAULT 1,
    receive_mention_email BIGINT NOT NULL DEFAULT 1,
    notify_doc_activity BIGINT NOT NULL DEFAULT 1,
    notify_permission_change BIGINT NOT NULL DEFAULT 1,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS doc_comment_changes (
    id BIGSERIAL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    comment_id TEXT,
    reply_id TEXT,
    action TEXT NOT NULL,
    payload TEXT NOT NULL,
    updated_at BIGINT NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_doc_comment_changes_lookup
    ON doc_comment_changes(workspace_id, doc_id, updated_at, id);

CREATE TABLE IF NOT EXISTS doc_updates (
    id BIGSERIAL PRIMARY KEY,
    space_type TEXT NOT NULL,
    space_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    editor_id TEXT,
    update_blob BYTEA NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_doc_updates_space_doc
    ON doc_updates(space_type, space_id, doc_id, id);
