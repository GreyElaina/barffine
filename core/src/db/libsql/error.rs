use anyhow::{Result, anyhow};

pub fn not_ready<T>(component: &str) -> Result<T> {
    Err(anyhow!(
        "libsql backend: {component} not implemented yet (see LIBSQL.md)"
    ))
}
