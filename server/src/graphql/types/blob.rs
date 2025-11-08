use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};

#[derive(SimpleObject, Clone)]
#[graphql(name = "ListedBlob")]
pub(crate) struct ListedBlob {
    pub key: String,
    pub mime: Option<String>,
    pub size: i32,
    #[graphql(name = "createdAt")]
    pub created_at: String,
}

impl ListedBlob {
    pub fn from_record(record: barffine_core::blob::ListedBlobRecord) -> Self {
        let created_at = record
            .created_at
            .and_then(|ts| DateTime::<Utc>::from_timestamp(ts, 0))
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| "".to_string());
        Self {
            key: record.key,
            mime: record.mime,
            size: record.size.min(i32::MAX as i64) as i32,
            created_at,
        }
    }
}
