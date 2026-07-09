//! Health & readiness endpoints.
//!
//! - `GET /healthz`: liveness — the process is up and serving. Requires
//!   no state, so it can be tested in isolation without DB or Redis.
//! - `GET /readyz`: readiness — both Postgres and Redis are reachable.
//!   Returns 503 with a JSON breakdown if either dependency is down.

use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::state::AppState;

/// Liveness only — no state. Useful for tests that don't have DB/Redis.
pub fn liveness_router() -> Router<()> {
    Router::new().route("/healthz", get(liveness))
}

/// Full health router — liveness + readiness. Requires `AppState`.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(liveness))
        .route("/readyz", get(readiness))
}

async fn liveness() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({ "status": "ok" })))
}

#[derive(Serialize)]
struct Readiness {
    status: &'static str,
    postgres: ComponentStatus,
    redis: ComponentStatus,
}

#[derive(Serialize)]
struct ComponentStatus {
    ok: bool,
    latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

async fn readiness(State(state): State<AppState>) -> Response {
    let pg = ping_component(|| async {
        crate::storage::postgres::ping(&state.pg).await
    })
    .await;

    let redis = ping_component(|| async {
        crate::storage::redis::ping(&state.redis).await
    })
    .await;

    let ok = pg.ok && redis.ok;
    let status = if ok { "ok" } else { "degraded" };
    let body = Json(Readiness {
        status,
        postgres: pg,
        redis,
    });

    let code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, body).into_response()
}

/// Time a dependency probe and normalize the result into a `ComponentStatus`.
async fn ping_component<F, Fut>(probe: F) -> ComponentStatus
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let start = Instant::now();
    match probe().await {
        Ok(()) => ComponentStatus {
            ok: true,
            latency_ms: start.elapsed().as_millis() as u64,
            error: None,
        },
        Err(e) => ComponentStatus {
            ok: false,
            latency_ms: start.elapsed().as_millis() as u64,
            error: Some(e.to_string()),
        },
    }
}
