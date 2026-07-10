use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use taskmanager::{AppState, Config, Metrics, Scheduler, SchedulerHealth, init_tracing, storage};
use taskmanager::Registry;

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env if present (dev convenience; ignored in containerized deploys).
    let _ = dotenvy::dotenv();

    // Read config first so we can pick the log format before initializing tracing.
    let config = Config::from_env()?;
    init_tracing(config.log_format);

    tracing::info!(
        listen_addr = %config.listen_addr,
        log_format = ?config.log_format,
        "starting taskmanager"
    );

    let pg = storage::connect_pg(&config.database_url)
        .instrument(tracing::info_span!("pg_connect"))
        .await?;
    let redis = storage::connect_redis(&config.redis_url)
        .instrument(tracing::info_span!("redis_connect"))
        .await?;

    storage::postgres::migrate(&pg).await?;

    // Phase 4: shared observability + scheduler health state.
    let metrics = Metrics::new();
    let scheduler_health = SchedulerHealth::new(config.scheduler.tick_interval);

    let state = AppState {
        pg: pg.clone(),
        redis: redis.clone(),
        scheduler_health: scheduler_health.clone(),
        metrics: metrics.clone(),
    };

    // Background connectivity sanity-check; logged once at boot.
    match (
        storage::postgres::ping(&pg).await,
        storage::redis::ping(&state.redis).await,
    ) {
        (Ok(()), Ok(())) => tracing::info!("deps_ready: postgres + redis reachable"),
        (pg_err, redis_err) => tracing::warn!(
            pg_err = ?pg_err.map_err(|e| e.to_string()).err(),
            redis_err = ?redis_err.map_err(|e| e.to_string()).err(),
            "dependency ping failed at boot; /readyz will report unhealthy",
        ),
    }

    // Shared shutdown signal (DESIGN §3): one token fans out to axum's
    // graceful-shutdown future and the scheduler's drain.
    let shutdown = CancellationToken::new();
    {
        let s = shutdown.clone();
        tokio::spawn(async move {
            wait_for_signal().await;
            s.cancel();
        });
    }

    // Scheduler (DESIGN §2.2): one background task owning the ZSET claim loop
    // + delivery. Metrics and health are shared with the HTTP layer (Phase 4).
    let scheduler = Scheduler::new(
        config.scheduler.clone(),
        pg.clone(),
        redis.clone(),
        Registry::v1(config.scheduler.http_timeout),
        scheduler_health,
        metrics,
    );
    let scheduler_handle =
        tokio::spawn(scheduler.run(shutdown.clone()).instrument(tracing::info_span!("scheduler")));

    let app = taskmanager::app_router(state);
    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    tracing::info!(addr = %config.listen_addr, "listening");

    // axum drains in-flight HTTP on shutdown; the scheduler drains in-flight
    // fires. Both react to the same cancellation token.
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;

    tracing::info!("http server drained; awaiting scheduler drain");
    // The scheduler task returns once its JoinSet is drained (bounded by
    // `shutdown_timeout`).
    let _ = scheduler_handle.await;

    tracing::info!("shutdown complete");
    Ok(())
}

/// Wait for SIGINT / SIGTERM. On receipt the caller cancels the shared
/// `CancellationToken`, which triggers axum + scheduler shutdown (DESIGN §3).
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
