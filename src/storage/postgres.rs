//! Postgres connection via sqlx.
//!
//! Phase 0 only establishes the pool and a ping query. Compile-time
//! query checking (../DESIGN.md §6) is enabled via the `sqlx` `macros`
//! feature and will be exercised in Phase 1 once `task_specs` exists.

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Build a `PgPool` with sane defaults for the dispatcher.
pub async fn connect(url: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Some(Duration::from_secs(600)))
        .connect(url)
        .await
        .context("failed to connect to Postgres")?;
    Ok(pool)
}

/// Run migrations from the `migrations/` directory.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .context("failed to run sqlx migrations")?;
    Ok(())
}

/// Verify the database is reachable.
pub async fn ping(pool: &PgPool) -> Result<()> {
    sqlx::query("SELECT 1")
        .fetch_one(pool)
        .await
        .context("postgres ping failed")?;
    Ok(())
}
