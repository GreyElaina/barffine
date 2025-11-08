use std::{env, net::SocketAddr};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
    #[serde(default = "default_database_path")]
    pub database_path: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
            database_path: default_database_path(),
        }
    }
}

impl AppConfig {
    const BIND_ADDRESS_ENV: &'static str = "BARFFINE_BIND_ADDRESS";
    const DATABASE_PATH_ENV: &'static str = "BARFFINE_DATABASE_PATH";

    pub fn load() -> Result<Self> {
        let mut config = Self::default();

        if let Ok(addr) = env::var(Self::BIND_ADDRESS_ENV) {
            config.bind_address = addr
                .parse()
                .with_context(|| format!("invalid {name}", name = Self::BIND_ADDRESS_ENV))?;
        }

        if let Ok(path) = env::var(Self::DATABASE_PATH_ENV) {
            config.database_path = path;
        }

        Ok(config)
    }
}

fn default_bind_address() -> SocketAddr {
    "127.0.0.1:8081"
        .parse()
        .expect("default bind address must be valid")
}

fn default_database_path() -> String {
    "./data/barffine.db".to_owned()
}
