//! taskmanager — reliable cron-to-notification dispatcher.
//!
//! See `../DESIGN.md` for the full design. Phase 0 shipped the skeleton
//! (config, pools, migrations, health routes, tracing). Phase 1 added
//! spec/target CRUD with sqlx persistence and serde validation. Phase 2 added
//! the scheduler loop (ZSET + atomic Lua claim), the `Transport` trait +
//! `HttpTransport`, hot-reload of the schedule on mutation, retry/backoff,
//! catch-up policies, and graceful shutdown (DESIGN §2.2, §2.3, §2.4, §3).
//! Phase 3 added reliability: `task_executions` history, `dead_letter`, and
//! the stable `X-Fire-Id` idempotency header (DESIGN §3, §5). Phase 4 added
//! ops: Prometheus `/metrics`, scheduler-aware readiness, webhook
//! HMAC-SHA256 signing, and `{{ scheduled_time }}` payload templating. Phase 5
//! added the service-healthcheck extension: a separate `healthchecker` binary
//! polls each target's optional `healthcheck_url` and writes `service_health`;
//! the scheduler skips fires for unhealthy targets and requeues them at the
//! next poll interval.

pub mod config;
pub mod error;
pub mod metrics;
pub mod models;
pub mod payload;
pub mod routes;
pub mod schedule;
pub mod scheduler;
pub mod state;
pub mod storage;
pub mod transport;

pub use config::{Config, LogFormat, SchedulerConfig};
pub use error::{AppError, AppResult};
pub use metrics::Metrics;
pub use models::{HealthStatus, ServiceHealth};
pub use routes::app_router;
pub use scheduler::Scheduler;
pub use state::{AppState, SchedulerHealth};
pub use transport::Registry;

use tracing_subscriber::EnvFilter;

/// Initialize the tracing subscriber.
///
/// Honors `RUST_LOG` for filtering and `LOG_FORMAT` for output shape
/// (pretty for local dev, JSON for production containers).
pub fn init_tracing(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,redis=warn"));

    match format {
        LogFormat::Json => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .with_target(true)
            .init(),
        LogFormat::Pretty => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .init(),
    }
}
