use anyhow::Error as AnyError;
use sqlx::{Error as SqlxError, error::DatabaseError};
use std::error::Error as StdError;

const POSTGRES_UNIQUE_VIOLATION: &str = "23505";
const SQLITE_UNIQUE_VIOLATION: &str = "2067";
const SQLITE_PRIMARY_KEY_VIOLATION: &str = "1555";

/// Returns `true` if the provided error represents a uniqueness constraint
/// violation on any supported backend.
pub fn is_unique_violation(err: &AnyError) -> bool {
    err.chain().any(is_unique_violation_cause)
}

fn is_unique_violation_cause(cause: &(dyn StdError + 'static)) -> bool {
    if let Some(sqlx_error) = cause.downcast_ref::<SqlxError>() {
        if matches_sqlx_unique(sqlx_error) {
            return true;
        }
    }

    message_indicates_unique(cause)
}

fn matches_sqlx_unique(err: &SqlxError) -> bool {
    match err {
        SqlxError::Database(db_err) => {
            if database_code_is_unique(db_err.as_ref()) {
                return true;
            }

            if let Some(constraint) = db_err.constraint() {
                if constraint.to_ascii_lowercase().contains("unique") {
                    return true;
                }
            }

            db_err
                .message()
                .to_ascii_lowercase()
                .contains("unique constraint")
                || db_err
                    .message()
                    .to_ascii_lowercase()
                    .contains("duplicate key")
        }
        _ => false,
    }
}

fn database_code_is_unique(err: &(dyn DatabaseError + 'static)) -> bool {
    err.code()
        .map(|code_ref| {
            let code = code_ref.as_ref();
            matches!(
                code,
                POSTGRES_UNIQUE_VIOLATION | SQLITE_UNIQUE_VIOLATION | SQLITE_PRIMARY_KEY_VIOLATION
            )
        })
        .unwrap_or(false)
}

fn message_indicates_unique(err: &(dyn StdError + 'static)) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("unique constraint failed")
        || message.contains("duplicate key value violates unique constraint")
        || message.contains("violates unique constraint")
}
