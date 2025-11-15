use std::collections::HashMap;

use barffine_core::notification::CommentReplyRecord;

use crate::{AppError, AppState};

pub async fn fetch_replies_for_comment(
    state: &AppState,
    comment_id: &str,
) -> Result<Vec<CommentReplyRecord>, AppError> {
    state
        .comment_store
        .list_replies(comment_id)
        .await
        .map_err(AppError::from_anyhow)
}

pub async fn fetch_replies_for_comments(
    state: &AppState,
    comment_ids: &[String],
) -> Result<HashMap<String, Vec<CommentReplyRecord>>, AppError> {
    let mut replies = HashMap::with_capacity(comment_ids.len());
    for comment_id in comment_ids {
        let records = fetch_replies_for_comment(state, comment_id).await?;
        replies.insert(comment_id.clone(), records);
    }

    Ok(replies)
}
