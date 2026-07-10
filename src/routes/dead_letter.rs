//! Dead-letter query route — `/v1/dead_letter` (Phase 3, DESIGN §3).
//!
//! The audit trail of exhausted / terminally-failed fires. Newest-first,
//! optionally filtered to one task. Entries are written best-effort by the
//! scheduler on exhaustion; this endpoint is read-only.

use axum::extract::{Query, State};
use axum::{Json, Router};
use serde::Deserialize;
use uuid::Uuid;

use crate::error::AppResult;
use crate::models::DeadLetterEntry;
use crate::routes::pagination::Pagination;
use crate::state::AppState;
use crate::storage;

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/dead_letter", axum::routing::get(list))
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Filter to dead-letter entries for a single spec.
    #[serde(default)]
    pub task_id: Option<Uuid>,
    #[serde(flatten)]
    pub page: Pagination,
}

async fn list(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> AppResult<Json<Vec<DeadLetterEntry>>> {
    let (limit, offset) = q.page.bounds()?;
    let entries = storage::dead_letter::list(&state.pg, q.task_id, limit, offset).await?;
    Ok(Json(entries))
}
