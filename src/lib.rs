//! taskmanager — reliable cron-to-notification dispatcher.
//!
//! See `../DESIGN.md` for the full design. Phase 0 shipped the skeleton
//! (config, pools, migrations, health routes, tracing). Phase 1 adds
//! spec/target CRUD with sqlx persistence and serde validation. The
//! scheduler loop (Phase 2) layers on top of `next_run` seeds.

pub mod config;
pub mod error;
pub mod models;
pub mod routes;
pub mod schedule;
pub mod state;
pub mod storage;

pub use config::{Config, LogFormat};
pub use error::{AppError, AppResult};
pub use routes::app_router;
pub use state::AppState;

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
