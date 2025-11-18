use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue, json};

use crate::{
    db::{
        Database,
        comment_repo::{CommentChangeRow, CommentRepositoryRef},
    },
    ids::{DocId, WorkspaceId},
    notification::{
        CommentChangeAction, CommentChangeRecord, CommentRecord, CommentRecordWithCursor,
        CommentReplyRecord, CommentStore,
    },
};

#[derive(Clone)]
pub struct RepositoryCommentStore {
    comment_repo: CommentRepositoryRef,
}

impl RepositoryCommentStore {
    pub fn new(database: &Database) -> Self {
        Self {
            comment_repo: database.repositories().comment_repo(),
        }
    }

    fn comment_payload(comment: &CommentRecord) -> JsonValue {
        json!({
            "id": comment.id,
            "workspaceId": comment.workspace_id,
            "docId": comment.doc_id,
            "authorId": comment.author_id,
            "body": comment.body,
            "metadata": comment.metadata,
            "resolved": comment.resolved,
            "createdAt": comment.created_at.to_rfc3339(),
            "updatedAt": comment.updated_at.to_rfc3339(),
        })
    }

    fn reply_payload(reply: &CommentReplyRecord) -> JsonValue {
        json!({
            "id": reply.id,
            "commentId": reply.comment_id,
            "authorId": reply.author_id,
            "body": reply.body,
            "metadata": reply.metadata,
            "createdAt": reply.created_at.to_rfc3339(),
            "updatedAt": reply.updated_at.to_rfc3339(),
        })
    }

    fn deletion_payload(timestamp: DateTime<Utc>) -> JsonValue {
        json!({
            "deletedAt": timestamp.to_rfc3339(),
            "updatedAt": timestamp.to_rfc3339(),
        })
    }
}

#[async_trait]
impl CommentStore for RepositoryCommentStore {
    async fn create(&self, comment: CommentRecord) -> Result<CommentRecord> {
        self.comment_repo.insert_comment(&comment).await?;
        self.comment_repo
            .insert_comment_change(
                comment.workspace_id.as_str(),
                comment.doc_id.as_str(),
                Some(&comment.id),
                None,
                CommentChangeAction::Update,
                &Self::comment_payload(&comment),
                comment.updated_at,
            )
            .await?;

        Ok(comment)
    }

    async fn find(&self, comment_id: &str) -> Result<Option<CommentRecord>> {
        self.comment_repo.fetch_comment(comment_id).await
    }

    async fn list_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<Vec<CommentRecord>> {
        self.comment_repo.list_for_doc(workspace_id, doc_id).await
    }

    async fn count_for_doc(&self, workspace_id: &str, doc_id: &str) -> Result<i64> {
        self.comment_repo.count_for_doc(workspace_id, doc_id).await
    }

    async fn update(&self, comment: CommentRecord) -> Result<CommentRecord> {
        self.comment_repo.update_comment(&comment).await?;
        self.comment_repo
            .insert_comment_change(
                comment.workspace_id.as_str(),
                comment.doc_id.as_str(),
                Some(&comment.id),
                None,
                CommentChangeAction::Update,
                &Self::comment_payload(&comment),
                comment.updated_at,
            )
            .await?;

        Ok(comment)
    }

    async fn delete(&self, comment_id: &str) -> Result<()> {
        let existing = self.comment_repo.fetch_comment(comment_id).await?;
        let timestamp = Utc::now();

        self.comment_repo.delete_comment(comment_id).await?;

        if let Some(comment) = existing {
            self.comment_repo
                .insert_comment_change(
                    &comment.workspace_id,
                    &comment.doc_id,
                    Some(&comment.id),
                    None,
                    CommentChangeAction::Delete,
                    &Self::deletion_payload(timestamp),
                    timestamp,
                )
                .await?;
        }
        Ok(())
    }

    async fn resolve(&self, comment_id: &str, resolved: bool) -> Result<()> {
        let updated_at = Utc::now();
        self.comment_repo
            .set_comment_resolved(comment_id, resolved, updated_at)
            .await?;

        if let Some(comment) = self.comment_repo.fetch_comment(comment_id).await? {
            self.comment_repo
                .insert_comment_change(
                    &comment.workspace_id,
                    &comment.doc_id,
                    Some(&comment.id),
                    None,
                    CommentChangeAction::Update,
                    &Self::comment_payload(&comment),
                    comment.updated_at,
                )
                .await?;
        }
        Ok(())
    }

    async fn create_reply(&self, reply: CommentReplyRecord) -> Result<CommentReplyRecord> {
        self.comment_repo.insert_reply(&reply).await?;

        if let Some(parent) = self.comment_repo.fetch_comment(&reply.comment_id).await? {
            self.comment_repo
                .insert_comment_change(
                    parent.workspace_id.as_str(),
                    parent.doc_id.as_str(),
                    Some(&parent.id),
                    Some(&reply.id),
                    CommentChangeAction::Update,
                    &Self::reply_payload(&reply),
                    reply.updated_at,
                )
                .await?;
        }

        Ok(reply)
    }

    async fn find_reply(&self, reply_id: &str) -> Result<Option<CommentReplyRecord>> {
        self.comment_repo.fetch_reply(reply_id).await
    }

    async fn list_replies(&self, comment_id: &str) -> Result<Vec<CommentReplyRecord>> {
        self.comment_repo.list_replies(comment_id).await
    }

    async fn update_reply(&self, reply: CommentReplyRecord) -> Result<CommentReplyRecord> {
        self.comment_repo.update_reply(&reply).await?;

        if let Some(parent) = self.comment_repo.fetch_comment(&reply.comment_id).await? {
            self.comment_repo
                .insert_comment_change(
                    parent.workspace_id.as_str(),
                    parent.doc_id.as_str(),
                    Some(&parent.id),
                    Some(&reply.id),
                    CommentChangeAction::Update,
                    &Self::reply_payload(&reply),
                    reply.updated_at,
                )
                .await?;
        }

        Ok(reply)
    }

    async fn delete_reply(&self, reply_id: &str) -> Result<()> {
        let existing = self.comment_repo.fetch_reply(reply_id).await?;
        let timestamp = Utc::now();

        self.comment_repo.delete_reply(reply_id).await?;

        if let Some(reply) = existing {
            if let Some(parent) = self.comment_repo.fetch_comment(&reply.comment_id).await? {
                self.comment_repo
                    .insert_comment_change(
                        &parent.workspace_id,
                        &parent.doc_id,
                        Some(&parent.id),
                        Some(&reply.id),
                        CommentChangeAction::Delete,
                        &Self::deletion_payload(timestamp),
                        timestamp,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn list_comments_paginated(
        &self,
        workspace_id: &str,
        doc_id: &str,
        limit: i64,
        after_sid: Option<i64>,
    ) -> Result<Vec<CommentRecordWithCursor>> {
        let limit = limit.max(1);
        self.comment_repo
            .list_comments_paginated(workspace_id, doc_id, limit, after_sid)
            .await
    }

    async fn list_comment_changes(
        &self,
        workspace_id: &str,
        doc_id: &str,
        after_comment_ts: Option<i64>,
        after_reply_ts: Option<i64>,
        after_comment_change_id: Option<i64>,
        after_reply_change_id: Option<i64>,
        limit: i64,
    ) -> Result<Vec<CommentChangeRecord>> {
        let limit = limit.max(1);
        let baseline_ts = after_comment_ts
            .into_iter()
            .chain(after_reply_ts.into_iter())
            .min();
        let baseline = baseline_ts.unwrap_or(0);

        let baseline_row_id = baseline_ts.and_then(|ts| {
            if after_comment_ts == Some(ts) {
                after_comment_change_id
            } else if after_reply_ts == Some(ts) {
                after_reply_change_id
            } else {
                None
            }
        });

        let fetch_limit = limit.saturating_mul(3);

        let rows = self
            .comment_repo
            .list_comment_changes(workspace_id, doc_id, baseline, baseline_row_id, fetch_limit)
            .await?;

        let mut changes = Vec::new();
        for row in rows {
            let CommentChangeRow {
                row_id,
                comment_id,
                reply_id,
                action,
                payload,
                updated_at,
            } = row;

            let item_id = reply_id.clone().or(comment_id.clone());
            let Some(item_id) = item_id else {
                continue;
            };

            let updated_ts = updated_at.timestamp();
            let is_reply = reply_id.is_some();
            if is_reply {
                if let Some(ts) = after_reply_ts {
                    if updated_ts < ts {
                        continue;
                    }
                    if updated_ts == ts {
                        if let Some(last_id) = after_reply_change_id {
                            if row_id <= last_id {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }
                }
            } else if let Some(ts) = after_comment_ts {
                if updated_ts < ts {
                    continue;
                }
                if updated_ts == ts {
                    if let Some(last_id) = after_comment_change_id {
                        if row_id <= last_id {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
            }

            changes.push(CommentChangeRecord {
                action,
                id: item_id,
                comment_id: comment_id.clone(),
                workspace_id: WorkspaceId::from(workspace_id.to_string()),
                doc_id: DocId::from(doc_id.to_string()),
                item: payload.clone(),
                updated_at,
                change_row_id: row_id,
                is_reply,
            });

            if changes.len() as i64 >= limit {
                break;
            }
        }

        Ok(changes)
    }
}
