use std::env;

use anyhow::{Context, Result, anyhow};

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub database_url: String,
    pub redis_url: String,
    pub log_format: LogFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Pretty,
    Json,
}

impl Config {
    /// Load configuration from environment variables.
    ///
    /// Required: `DATABASE_URL`, `REDIS_URL`
    /// Optional: `LISTEN_ADDR` (default `0.0.0.0:8080`),
    ///           `LOG_FORMAT` (default `pretty`)
    pub fn from_env() -> Result<Self> {
        let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
        let database_url = env::var("DATABASE_URL")
            .context("DATABASE_URL is required (e.g. postgres://user:pass@host/db)")?;
        let redis_url = env::var("REDIS_URL")
            .context("REDIS_URL is required (e.g. redis://host:6379)")?;
        let log_format = match env::var("LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "json" => LogFormat::Json,
            "pretty" => LogFormat::Pretty,
            other => {
                return Err(anyhow!(
                    "LOG_FORMAT must be 'pretty' or 'json', got {other:?}"
                ))
            }
        };

        Ok(Self {
            listen_addr,
            database_url,
            redis_url,
            log_format,
        })
    }
}
