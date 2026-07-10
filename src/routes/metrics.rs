//! Prometheus `/metrics` endpoint (Phase 4, DESIGN §3).
//!
//! Returns the registry text format. Used by the scheduler for fire/queue
//! metrics and by the HTTP layer for scraping.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Router, body::Body};

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/metrics", get(metrics))
}

async fn metrics(State(state): State<AppState>) -> Response {
    match state.metrics.render() {
        Ok(text) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            Body::from(text),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to render metrics");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Body::from(format!("failed to render metrics: {e}")),
            )
                .into_response()
        }
    }
}
