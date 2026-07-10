//! Healthchecker binary — extension feature.
//!
//! Runs in a separate Docker container and polls each target's
//! `healthcheck_url`. Writes the latest status to `service_health` (SQL source
//! of truth). The scheduler reads this table before firing and skips tasks
//! whose target is unhealthy, requeueing them for the next poll interval.

use std::time::Duration;

use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use taskmanager::storage::health;
use taskmanager::{Config, HealthStatus, init_tracing, storage};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    let config = Config::from_env()?;
    init_tracing(config.log_format);

    tracing::info!(
        "starting taskmanager-healthchecker; poll services and write service_health"
    );

    let pg = storage::connect_pg(&config.database_url)
        .instrument(tracing::info_span!("pg_connect"))
        .await?;

    // At startup, mark every target that has a healthcheck_url as unknown
    // so the scheduler treats it as "not yet proven healthy" rather than
    // implicitly healthy.
    let targets = health::list_targets_with_healthchecks(&pg).await?;
    for t in &targets {
        if t.healthcheck_url.is_some() && let Err(e) = health::set_unknown(&pg, t.id).await {
            tracing::warn!(target = %t.id, error = %e, "failed to seed unknown health");
        }
    }

    let shutdown = CancellationToken::new();
    {
        let s = shutdown.clone();
        tokio::spawn(async move {
            wait_for_signal().await;
            s.cancel();
        });
    }

    let client = reqwest::Client::new();

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                if let Err(e) = run_pass(&pg, &client).await {
                    tracing::warn!(error = %e, "healthcheck pass failed");
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }

    tracing::info!("healthchecker shutdown complete");
    Ok(())
}

/// One poll of all targets that have a healthcheck_url configured.
async fn run_pass(pg: &sqlx::PgPool, client: &reqwest::Client) -> anyhow::Result<()> {
    let targets = health::list_targets_with_healthchecks(pg).await?;
    if targets.is_empty() {
        return Ok(());
    }

    for target in targets {
        let Some(url) = target.healthcheck_url.as_ref() else {
            continue;
        };
        let timeout = Duration::from_secs(target.healthcheck_timeout_seconds.max(1) as u64);
        let checked_at = Utc::now();

        match client.get(url).timeout(timeout).send().await {
            Ok(resp) => {
                let status_code = resp.status().as_u16() as i32;
                let is_healthy = resp.status().is_success();
                let health_status = if is_healthy {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unhealthy
                };
                let error = if is_healthy {
                    None
                } else {
                    Some(format!("healthcheck returned {status_code}"))
                };
                if let Err(e) = health::upsert(pg, target.id, health_status, Some(status_code), error.as_deref(), checked_at).await {
                    tracing::warn!(target = %target.id, error = %e, "failed to write healthy/unhealthy status");
                } else {
                    tracing::info!(
                        target = %target.id,
                        url = %url,
                        status = status_code,
                        healthy = is_healthy,
                        "healthcheck polled"
                    );
                }
            }
            Err(err) => {
                let msg = format!("healthcheck request failed: {err}");
                if let Err(e) = health::upsert(pg, target.id, HealthStatus::Unhealthy, None, Some(&msg), checked_at).await {
                    tracing::warn!(target = %target.id, error = %e, "failed to write unhealthy status");
                } else {
                    tracing::warn!(
                        target = %target.id,
                        url = %url,
                        error = %msg,
                        "healthcheck failed"
                    );
                }
            }
        }
    }
    Ok(())
}

async fn wait_for_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received SIGINT, shutting down"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}
