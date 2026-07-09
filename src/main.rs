use anyhow::Result;
use tracing::Instrument;

use taskmanager::{AppState, Config, init_tracing, storage};

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

    let state = AppState {
        pg: pg.clone(),
        redis,
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

    let app = taskmanager::app_router(state);
    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    tracing::info!(addr = %config.listen_addr, "listening");

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("shutdown complete");
    Ok(())
}

/// Wait for SIGINT / SIGTERM. The scheduler drain (Phase 4) will hook here
/// via a `CancellationToken` shared with the scheduler task.
async fn shutdown_signal() {
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
