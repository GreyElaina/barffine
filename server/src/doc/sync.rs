use super::cache::DocCacheApplyResult;

use crate::{
    AppError, AppState, socket::rooms::SpaceType, state::SocketBroadcastMeta, types::SessionUser,
    utils::channels::doc_channel_key,
};

/// Controls whether the cache should be refreshed from storage before broadcasting a snapshot.
#[derive(Clone, Copy, Debug)]
pub enum SnapshotCachePolicy {
    UseCache,
    RefreshCache,
}

impl SnapshotCachePolicy {
    #[inline]
    fn should_refresh(self) -> bool {
        matches!(self, SnapshotCachePolicy::RefreshCache)
    }
}

pub async fn broadcast_workspace_snapshot(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
    cache_policy: SnapshotCachePolicy,
) -> Result<(), AppError> {
    if cache_policy.should_refresh() {
        state
            .doc_cache
            .invalidate(SpaceType::Workspace, workspace_id, doc_id)
            .await;
    }

    let (snapshot, _) = state
        .doc_cache
        .snapshot(SpaceType::Workspace, workspace_id, doc_id)
        .await
        .map_err(AppError::from_anyhow)?;

    state
        .sync_hub
        .publish_snapshot(&doc_channel_key(workspace_id, doc_id), snapshot);

    Ok(())
}

pub async fn workspace_snapshot_or_not_found(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
) -> Result<(Vec<u8>, i64), AppError> {
    match state
        .doc_cache
        .snapshot(SpaceType::Workspace, workspace_id, doc_id)
        .await
    {
        Ok(snapshot) => Ok(snapshot),
        Err(err) => {
            let exists = state
                .document_store
                .find_metadata(workspace_id, doc_id)
                .await
                .map_err(AppError::from_anyhow)?
                .is_some();
            if exists {
                Err(AppError::from_anyhow(err))
            } else {
                Err(AppError::doc_not_found(workspace_id, doc_id))
            }
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct UpdateBroadcastContext<'a> {
    pub editor_id: Option<&'a str>,
    pub editor_user: Option<&'a SessionUser>,
}

pub async fn apply_doc_updates(
    state: &AppState,
    space_type: SpaceType,
    workspace_id: &str,
    doc_id: &str,
    updates: Vec<Vec<u8>>,
    context: UpdateBroadcastContext<'_>,
) -> Result<DocCacheApplyResult, AppError> {
    let cache_result = state
        .doc_cache
        .apply_updates(
            space_type,
            workspace_id,
            doc_id,
            updates.clone(),
            context.editor_id,
        )
        .await
        .map_err(AppError::from_anyhow)?;

    let channel_key = doc_channel_key(workspace_id, doc_id);
    let meta = SocketBroadcastMeta::new(
        space_type,
        workspace_id.to_string(),
        doc_id.to_string(),
        context.editor_id.map(str::to_owned),
        context.editor_user.cloned(),
        Some(cache_result.timestamp),
    );

    state
        .sync_hub
        .publish_updates(&channel_key, &updates, Some(meta));
    state
        .sync_hub
        .publish_snapshot(&channel_key, cache_result.snapshot.clone());

    Ok(cache_result)
}
