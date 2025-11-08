use std::{env, fs, net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result, anyhow};
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
    const CONFIG_ENV: &'static str = "BARFFINE_CONFIG_FILE";
    const BIND_ADDRESS_ENV: &'static str = "BARFFINE_BIND_ADDRESS";
    const DATABASE_PATH_ENV: &'static str = "BARFFINE_DATABASE_PATH";

    /// Load configuration from defaults layered with optional config files and
    /// environment variables.
    pub fn load() -> Result<Self> {
        Self::load_with(None)
    }

    pub fn load_with(config_path: Option<PathBuf>) -> Result<Self> {
        let mut config = Self::default();

        if let Some(path) = Self::resolve_config_path(config_path)? {
            let contents = fs::read_to_string(&path)
                .with_context(|| format!("failed to read config file: {}", path.display()))?;
            let file_config: Self = toml::from_str(&contents)
                .with_context(|| format!("invalid config file: {}", path.display()))?;

            config = file_config;
        }

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

    fn resolve_config_path(explicit: Option<PathBuf>) -> Result<Option<PathBuf>> {
        if let Some(path) = explicit {
            return Self::validate_path(path);
        }

        if let Ok(path) = env::var(Self::CONFIG_ENV) {
            return Self::validate_path(PathBuf::from(path));
        }

        let mut candidates = vec![PathBuf::from("barffine.toml")];
        if let Some(dir) = Self::default_config_dir() {
            candidates.push(dir.join("config.toml"));
        }

        for candidate in candidates {
            if candidate.exists() {
                return Ok(Some(candidate));
            }
        }

        Ok(None)
    }

    fn validate_path(path: PathBuf) -> Result<Option<PathBuf>> {
        if path.exists() {
            Ok(Some(path))
        } else {
            Err(anyhow!(
                "configuration file does not exist: {}",
                path.display()
            ))
        }
    }

    fn default_config_dir() -> Option<PathBuf> {
        home_dir().map(|home| home.join(".barffine"))
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

fn home_dir() -> Option<PathBuf> {
    if let Some(path) = env::var_os("HOME") {
        return Some(PathBuf::from(path));
    }

    if let Some(path) = env::var_os("USERPROFILE") {
        return Some(PathBuf::from(path));
    }

    None
}
