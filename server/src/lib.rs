pub mod auth;
pub mod blob_store;
pub mod cli;
pub mod comment;
pub mod cookies;
pub mod crypto;
pub mod doc;
pub mod error;
pub mod feature_service;
pub mod graphql;
pub mod handlers;
pub mod http;
pub mod observability;
pub mod oauth;
pub mod router;
pub mod socket;
pub mod state;
pub mod testing;
pub mod types;
pub mod user;
pub mod utils;
pub mod workspace;

pub use error::AppError;
pub use state::{build_state, build_state_with_config, AppState, StateBuildConfig};

pub const DEFAULT_HISTORY_LIMIT: i64 = 20;
pub const MAX_HISTORY_LIMIT: i64 = 100;

#[cfg(test)]
pub mod test_support;
