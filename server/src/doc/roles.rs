use std::collections::HashSet;

use barffine_core::{
    doc_roles::{DocumentRoleRecord, DocumentRoleStore},
    ids::UserId,
    notification::NotificationRecord,
    user_settings::NotificationPreferenceKind,
    workspace::WorkspaceRecord,
};
use chrono::Utc;
use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

use crate::{AppError, AppState, request_cache};

fn notification_preference_from_kind(kind: &str) -> NotificationPreferenceKind {
    if kind == "comment.mention" {
        NotificationPreferenceKind::CommentMention
    } else if kind == "comment" {
        NotificationPreferenceKind::Comment
    } else if kind.starts_with("doc.permission") {
        NotificationPreferenceKind::PermissionChange
    } else if kind.starts_with("doc.activity") {
        NotificationPreferenceKind::DocActivity
    } else if kind.starts_with("invitation") {
        NotificationPreferenceKind::Invitation
    } else {
        NotificationPreferenceKind::Comment
    }
}

pub async fn enqueue_notification_record(
    state: &AppState,
    recipient_id: &str,
    actor_id: &str,
    kind: &str,
    payload: JsonValue,
) -> Result<(), AppError> {
    if recipient_id == actor_id {
        return Ok(());
    }

    let preference = notification_preference_from_kind(kind);
    if !state
        .user_settings
        .should_notify(recipient_id, preference)
        .await
        .map_err(AppError::from_anyhow)?
    {
        return Ok(());
    }

    let record = NotificationRecord {
        id: Uuid::new_v4().to_string(),
        user_id: UserId::from(recipient_id.to_owned()),
        kind: kind.to_owned(),
        payload,
        read: false,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    state
        .notification_center
        .enqueue(record)
        .await
        .map_err(AppError::from_anyhow)
}

pub async fn notify_doc_activity(
    state: &AppState,
    actor_id: &str,
    workspace_id: &str,
    doc_id: &str,
    target_user_id: &str,
    event: &str,
) -> Result<(), AppError> {
    if target_user_id == actor_id {
        return Ok(());
    }

    let Some(metadata) = state
        .document_store
        .find_metadata(workspace_id, doc_id)
        .await
        .map_err(AppError::from_anyhow)?
    else {
        return Ok(());
    };

    let payload = json!({
        "workspaceId": workspace_id,
        "docId": doc_id,
        "docTitle": metadata.title,
        "docMode": metadata.mode,
        "changedByUserId": actor_id,
        "event": event,
    });

    enqueue_notification_record(state, target_user_id, actor_id, event, payload).await
}

pub async fn notify_owner_and_creator(
    state: &AppState,
    actor_id: &str,
    workspace: &WorkspaceRecord,
    doc_id: &str,
    creator_id: Option<&str>,
    event: &str,
) -> Result<(), AppError> {
    let mut targets: HashSet<&str> = HashSet::new();
    if workspace.owner_id.as_str() != actor_id {
        targets.insert(workspace.owner_id.as_str());
    }
    if let Some(creator_id) = creator_id {
        if creator_id != actor_id {
            targets.insert(creator_id);
        }
    }

    for target in targets {
        notify_doc_activity(state, actor_id, &workspace.id, doc_id, target, event).await?;
    }

    Ok(())
}

pub async fn doc_role_for_user_cached(
    doc_role_store: &DocumentRoleStore,
    workspace_id: &str,
    doc_id: &str,
    user_id: &str,
) -> Result<Option<DocumentRoleRecord>, AppError> {
    let fetch = || async {
        doc_role_store
            .find_for_user(workspace_id, doc_id, user_id)
            .await
            .map_err(AppError::from_anyhow)
    };

    if let Some(caches) = request_cache::current_request_caches() {
        return caches
            .doc_roles()
            .get_or_fetch(workspace_id, doc_id, user_id, || fetch())
            .await;
    }

    fetch().await
}

pub fn invalidate_doc_role_cache(workspace_id: &str, doc_id: &str, user_id: &str) {
    if let Some(caches) = request_cache::current_request_caches() {
        caches.doc_roles().invalidate(workspace_id, doc_id, user_id);
        caches
            .doc_permissions()
            .invalidate(workspace_id, doc_id, user_id);
    }
}

pub async fn assign_doc_roles(
    state: &AppState,
    workspace: &WorkspaceRecord,
    doc_id: &str,
    actor_id: &str,
    user_ids: &[String],
    role_label: &str,
) -> Result<(), AppError> {
    for user_id in user_ids {
        ensure_assignable_user(
            state,
            workspace,
            user_id,
            "cannot grant doc role for workspace owner",
        )
        .await?;

        state
            .doc_role_store
            .upsert(&workspace.id, doc_id, user_id, role_label)
            .await
            .map_err(AppError::from_anyhow)?;
        invalidate_doc_role_cache(&workspace.id, doc_id, user_id);

        notify_doc_activity(
            state,
            actor_id,
            &workspace.id,
            doc_id,
            user_id,
            "doc.activity.role.granted",
        )
        .await?;
    }

    Ok(())
}

pub async fn revoke_doc_role(
    state: &AppState,
    workspace: &WorkspaceRecord,
    doc_id: &str,
    actor_id: &str,
    user_id: &str,
) -> Result<(), AppError> {
    ensure_assignable_user(
        state,
        workspace,
        user_id,
        "cannot revoke doc role for workspace owner",
    )
    .await?;

    state
        .doc_role_store
        .remove(&workspace.id, doc_id, user_id)
        .await
        .map_err(AppError::from_anyhow)?;
    invalidate_doc_role_cache(&workspace.id, doc_id, user_id);

    notify_doc_activity(
        state,
        actor_id,
        &workspace.id,
        doc_id,
        user_id,
        "doc.activity.role.revoked",
    )
    .await
}

pub async fn update_doc_role(
    state: &AppState,
    workspace: &WorkspaceRecord,
    doc_id: &str,
    actor_id: &str,
    user_id: &str,
    role_label: &str,
) -> Result<(), AppError> {
    ensure_assignable_user(
        state,
        workspace,
        user_id,
        "cannot modify doc role for workspace owner",
    )
    .await?;

    state
        .doc_role_store
        .upsert(&workspace.id, doc_id, user_id, role_label)
        .await
        .map_err(AppError::from_anyhow)?;
    invalidate_doc_role_cache(&workspace.id, doc_id, user_id);

    notify_doc_activity(
        state,
        actor_id,
        &workspace.id,
        doc_id,
        user_id,
        "doc.activity.role.updated",
    )
    .await
}

pub async fn transfer_doc_owner(
    state: &AppState,
    workspace: &WorkspaceRecord,
    doc_id: &str,
    actor_id: &str,
    new_owner_id: &str,
) -> Result<(), AppError> {
    state.user_service.fetch_user(new_owner_id).await?;

    // Downgrade existing owners first so that only one owner record remains.
    let owners = state
        .doc_role_store
        .owners_for_doc(&workspace.id, doc_id)
        .await
        .map_err(AppError::from_anyhow)?;

    for owner in owners {
        if owner.user_id.as_str() == new_owner_id {
            continue;
        }

        state
            .doc_role_store
            .upsert(&workspace.id, doc_id, &owner.user_id, "manager")
            .await
            .map_err(AppError::from_anyhow)?;
        invalidate_doc_role_cache(&workspace.id, doc_id, &owner.user_id);

        notify_doc_activity(
            state,
            actor_id,
            &workspace.id,
            doc_id,
            &owner.user_id,
            "doc.activity.role.updated",
        )
        .await?;
    }

    state
        .doc_role_store
        .upsert(&workspace.id, doc_id, new_owner_id, "owner")
        .await
        .map_err(AppError::from_anyhow)?;
    invalidate_doc_role_cache(&workspace.id, doc_id, new_owner_id);

    notify_doc_activity(
        state,
        actor_id,
        &workspace.id,
        doc_id,
        new_owner_id,
        "doc.activity.role.updated",
    )
    .await
}

async fn ensure_assignable_user(
    state: &AppState,
    workspace: &WorkspaceRecord,
    user_id: &str,
    owner_error: &'static str,
) -> Result<(), AppError> {
    state.user_service.fetch_user(user_id).await?;

    if workspace.owner_id.as_str() == user_id {
        Err(AppError::bad_request(owner_error))
    } else {
        Ok(())
    }
}
