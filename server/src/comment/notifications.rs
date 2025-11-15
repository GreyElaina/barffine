use std::collections::HashSet;

use barffine_core::notification::{CommentRecord, CommentReplyRecord};
use serde_json::json;

use crate::{AppError, AppState, doc::roles, graphql::doc_permissions_for_user_internal};

pub async fn notify_comment_creation(
    state: &AppState,
    actor_id: &str,
    comment: &CommentRecord,
    doc_title: Option<&str>,
    doc_mode: Option<&str>,
    mentions: &[String],
) -> Result<(), AppError> {
    let mention_ids = resolve_mentions(state, actor_id, comment, mentions).await?;
    let notify_ids = collect_workspace_owner_target(state, actor_id, comment, &mention_ids).await?;

    dispatch_notifications(
        state,
        actor_id,
        comment,
        None,
        doc_title,
        doc_mode,
        &mention_ids,
        &notify_ids,
    )
    .await
}

pub async fn notify_comment_reply(
    state: &AppState,
    actor_id: &str,
    comment: &CommentRecord,
    reply: &CommentReplyRecord,
    doc_title: Option<&str>,
    doc_mode: Option<&str>,
    mentions: &[String],
) -> Result<(), AppError> {
    let mention_ids = resolve_mentions(state, actor_id, comment, mentions).await?;
    let mut notify_ids =
        collect_workspace_owner_target(state, actor_id, comment, &mention_ids).await?;

    if comment.author_id != actor_id && !mention_ids.contains(&comment.author_id) {
        notify_ids.insert(comment.author_id.clone());
    }

    let replies = state
        .comment_store
        .list_replies(&comment.id)
        .await
        .map_err(AppError::from_anyhow)?;

    for existing in replies {
        if existing.author_id != actor_id && !mention_ids.contains(&existing.author_id) {
            notify_ids.insert(existing.author_id);
        }
    }

    dispatch_notifications(
        state,
        actor_id,
        comment,
        Some(reply),
        doc_title,
        doc_mode,
        &mention_ids,
        &notify_ids,
    )
    .await
}

async fn resolve_mentions(
    state: &AppState,
    actor_id: &str,
    comment: &CommentRecord,
    mentions: &[String],
) -> Result<HashSet<String>, AppError> {
    let mut mention_ids = HashSet::new();

    for mention in mentions {
        if mention == actor_id {
            continue;
        }

        if let Some(perms) = doc_permissions_for_user_internal(
            state,
            &comment.workspace_id,
            &comment.doc_id,
            mention,
        )
        .await?
        {
            if perms.can_read_comments() {
                mention_ids.insert(mention.clone());
            }
        }
    }

    Ok(mention_ids)
}

async fn collect_workspace_owner_target(
    state: &AppState,
    actor_id: &str,
    comment: &CommentRecord,
    mention_ids: &HashSet<String>,
) -> Result<HashSet<String>, AppError> {
    let workspace = state
        .workspace_service
        .fetch_workspace(&comment.workspace_id)
        .await?;

    let mut notify_ids = HashSet::new();
    if workspace.owner_id != actor_id && !mention_ids.contains(&workspace.owner_id) {
        notify_ids.insert(workspace.owner_id);
    }

    Ok(notify_ids)
}

async fn dispatch_notifications(
    state: &AppState,
    actor_id: &str,
    comment: &CommentRecord,
    reply: Option<&CommentReplyRecord>,
    doc_title: Option<&str>,
    doc_mode: Option<&str>,
    mention_ids: &HashSet<String>,
    notify_ids: &HashSet<String>,
) -> Result<(), AppError> {
    for user_id in mention_ids {
        enqueue_notification(
            state,
            user_id,
            "comment.mention",
            actor_id,
            comment,
            reply,
            doc_title,
            doc_mode,
            true,
        )
        .await?;
    }

    for user_id in notify_ids {
        enqueue_notification(
            state, user_id, "comment", actor_id, comment, reply, doc_title, doc_mode, false,
        )
        .await?;
    }

    Ok(())
}

async fn enqueue_notification(
    state: &AppState,
    recipient_id: &str,
    kind: &str,
    actor_id: &str,
    comment: &CommentRecord,
    reply: Option<&CommentReplyRecord>,
    doc_title: Option<&str>,
    doc_mode: Option<&str>,
    is_mention: bool,
) -> Result<(), AppError> {
    let payload = json!({
        "workspaceId": comment.workspace_id,
        "docId": comment.doc_id,
        "commentId": comment.id,
        "replyId": reply.map(|r| r.id.clone()),
        "docTitle": doc_title,
        "docMode": doc_mode,
        "createdByUserId": actor_id,
        "isMention": is_mention,
    });

    roles::enqueue_notification_record(state, recipient_id, actor_id, kind, payload).await
}
