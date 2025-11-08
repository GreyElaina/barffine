use async_graphql::{InputObject, SimpleObject};
use chrono::{DateTime, Utc};

use barffine_core::access_token::AccessTokenRecord;

use super::super::helpers::timestamp_to_datetime;

#[derive(SimpleObject, Clone)]
#[graphql(name = "AccessToken")]
pub struct AccessTokenObject {
    pub id: String,
    pub name: String,
    #[graphql(name = "createdAt")]
    pub created_at: DateTime<Utc>,
    #[graphql(name = "expiresAt")]
    pub expires_at: Option<DateTime<Utc>>,
}

impl From<AccessTokenRecord> for AccessTokenObject {
    fn from(record: AccessTokenRecord) -> Self {
        Self {
            id: record.id,
            name: record.name,
            created_at: timestamp_to_datetime(record.created_at),
            expires_at: record.expires_at.map(timestamp_to_datetime),
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "RevealedAccessToken")]
pub struct RevealedAccessTokenObject {
    pub id: String,
    pub name: String,
    #[graphql(name = "createdAt")]
    pub created_at: DateTime<Utc>,
    #[graphql(name = "expiresAt")]
    pub expires_at: Option<DateTime<Utc>>,
    pub token: String,
}

impl From<AccessTokenRecord> for RevealedAccessTokenObject {
    fn from(record: AccessTokenRecord) -> Self {
        Self {
            id: record.id,
            name: record.name,
            created_at: timestamp_to_datetime(record.created_at),
            expires_at: record.expires_at.map(timestamp_to_datetime),
            token: record.token.expect("revealed token should include value"),
        }
    }
}

#[derive(InputObject)]
#[graphql(name = "GenerateAccessTokenInput")]
pub struct GenerateAccessTokenInput {
    pub name: String,
    #[graphql(name = "expiresAt")]
    pub expires_at: Option<DateTime<Utc>>,
}
