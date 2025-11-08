use barffine_core::doc_store::DocumentHistoryRecord;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

use crate::{AppError, AppState, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT};

#[derive(Debug, Clone)]
pub struct EncodedDocHistory {
    pub snapshot: String,
    pub created_at: i64,
}

impl From<DocumentHistoryRecord> for EncodedDocHistory {
    fn from(record: DocumentHistoryRecord) -> Self {
        Self {
            snapshot: BASE64.encode(record.snapshot),
            created_at: record.created_at,
        }
    }
}

pub fn normalize_history_limit(requested: Option<i64>) -> i64 {
    requested
        .unwrap_or(DEFAULT_HISTORY_LIMIT)
        .clamp(1, MAX_HISTORY_LIMIT)
}

pub async fn fetch_history_records(
    state: &AppState,
    workspace_id: &str,
    doc_id: &str,
    limit: Option<i64>,
) -> Result<Vec<DocumentHistoryRecord>, AppError> {
    let normalized = normalize_history_limit(limit);
    state
        .document_store
        .list_history(workspace_id, doc_id, normalized)
        .await
        .map_err(AppError::from_anyhow)
}
