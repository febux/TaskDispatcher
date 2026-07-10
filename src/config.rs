use std::env;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub database_url: String,
    pub redis_url: String,
    pub log_format: LogFormat,
    pub scheduler: SchedulerConfig,
}

/// Scheduler / delivery tuning (DESIGN §2.2, §3). All overridable from env
/// with sane defaults so the engine runs correctly out of the box.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// How often the loop wakes to claim due tasks.
    pub tick_interval: Duration,
    /// Max tasks claimed (and fired concurrently) per tick.
    pub batch_size: u32,
    /// Per-webhook delivery timeout (HttpTransport, Phase 4 generalizes this).
    pub http_timeout: Duration,
    /// Hard cap on in-flight drain during graceful shutdown.
    pub shutdown_timeout: Duration,
    /// Base delay for exponential backoff (DESIGN §3: retry + backoff).
    pub backoff_base: Duration,
    /// Ceiling for a single backoff delay.
    pub backoff_max: Duration,
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
            scheduler: SchedulerConfig::from_env(),
        })
    }
}

impl SchedulerConfig {
    fn from_env() -> Self {
        Self {
            tick_interval: dur("SCHEDULER_TICK_MS", 250),
            batch_size: env_u32("SCHEDULER_BATCH_SIZE", 64),
            http_timeout: dur("HTTP_TIMEOUT_MS", 10_000),
            shutdown_timeout: dur("SHUTDOWN_TIMEOUT_MS", 30_000),
            backoff_base: dur("BACKOFF_BASE_MS", 500),
            backoff_max: dur("BACKOFF_MAX_MS", 30_000),
        }
    }

    /// Exponential backoff (base * 2^(attempt-1)) capped at `backoff_max`,
    /// plus up to +50% jitter (DESIGN §3). `attempt` is 1-based.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let mut delay = self.backoff_base.as_millis() as u64;
        if attempt > 1 {
            // Saturate the shift; for any realistic config this is plenty.
            let shift = (attempt - 1).min(31);
            delay = delay.saturating_mul(2u64.saturating_pow(shift));
        }
        if delay > self.backoff_max.as_millis() as u64 {
            delay = self.backoff_max.as_millis() as u64;
        }
        // Full jitter up to +50% of the computed delay, using rand (already a
        // transitive dep). Keeps thundering-herd of retries from synchronizing.
        use rand::Rng as _;
        let jitter = rand::thread_rng().gen_range(0..=delay / 2);
        Duration::from_millis(delay + jitter)
    }
}

fn dur(var: &str, default_ms: u64) -> Duration {
    Duration::from_millis(env_u64(var, default_ms))
}

fn env_u64(var: &str, default: u64) -> u64 {
    env::var(var)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

fn env_u32(var: &str, default: u32) -> u32 {
    env::var(var)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}
