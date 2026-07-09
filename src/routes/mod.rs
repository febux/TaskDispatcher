//! HTTP routes.
//!
//! Phase 1 ships spec + target CRUD on top of the Phase 0 health routes.
//! gRPC (tonic) is deferred per DESIGN §7.2.

pub mod health;
pub mod pagination;
pub mod specs;
pub mod targets;

use axum::Router;

use crate::state::AppState;

/// Build the full application router with shared state.
///
/// Each resource router owns its full `/v1/...` path (no `nest`), so routing
/// is explicit and free of trailing-slash surprises.
pub fn app_router(state: AppState) -> Router {
    Router::new()
        .merge(health::router())
        .merge(targets::router())
        .merge(specs::router())
        .with_state(state)
}
