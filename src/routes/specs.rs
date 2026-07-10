//! Spec CRUD routes — `/specs`.
//!
//! The Phase 1 deliverable (DESIGN §8): create/pause/resume/delete specs
//! via curl, persisted in Postgres. SQL is the source of truth; the Redis
//! ZSET seeding lands in Phase 2 (§2.2, §2.3).
//!
//! Mutation contract (DESIGN §2.3): every write that changes the schedule
//! recomputes `next_run` from `now`. Pause clears `next_run` (NULL); resume
//! re-seeds it. Updates are gated on `version` for optimistic concurrency.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use crate::error::{AppError, AppResult};
use crate::models::{CatchUpPolicy, SpecStatus, SpecType, TaskExecution, TaskSpec};
use crate::routes::pagination::Pagination;
use crate::schedule;
use crate::state::AppState;
use crate::storage::{self, specs::SpecRow};

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/specs",
            axum::routing::post(create).get(list),
        )
        .route(
            "/v1/specs/:id",
            axum::routing::get(get).patch(update).delete(delete),
        )
        .route("/v1/specs/:id/pause", axum::routing::post(pause))
        .route("/v1/specs/:id/resume", axum::routing::post(resume))
        .route("/v1/specs/:id/executions", axum::routing::get(executions))
}

// ---------------------------------------------------------------------------
// Request DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct CreateSpecRequest {
    pub name: String,
    pub spec_type: SpecType,
    pub cron_expr: Option<String>,
    pub interval_seconds: Option<i64>,
    pub run_at: Option<chrono::DateTime<chrono::Utc>>,
    /// IANA timezone (e.g. `"UTC"`, `"America/New_York"`). Defaults to UTC.
    #[serde(default)]
    pub timezone: Option<String>,
    pub target_id: Uuid,
    /// Payload template; `{{ scheduled_time }}` substitution at fire time
    /// (DESIGN §7.5). Defaults to `{}`.
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
    #[serde(default)]
    pub catch_up: Option<CatchUpPolicy>,
    #[serde(default)]
    pub max_attempts: Option<i32>,
}

/// PATCH merges into the existing spec; every field is optional. Schedule
/// fields that change trigger a `next_run` recompute.
#[derive(Debug, Default, Deserialize)]
pub struct UpdateSpecRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub spec_type: Option<SpecType>,
    #[serde(default)]
    pub cron_expr: Option<String>,
    #[serde(default)]
    pub interval_seconds: Option<i64>,
    #[serde(default)]
    pub run_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub target_id: Option<Uuid>,
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
    #[serde(default)]
    pub catch_up: Option<CatchUpPolicy>,
    #[serde(default)]
    pub max_attempts: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(default)]
    pub status: Option<SpecStatus>,
    #[serde(flatten)]
    pub page: Pagination,
}

#[derive(Debug, Deserialize)]
pub struct VersionQuery {
    /// Expected current `version`. Required for optimistic-concurrency writes.
    pub version: i32,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn create(
    State(state): State<AppState>,
    Json(req): Json<CreateSpecRequest>,
) -> AppResult<Response> {
    let row = build_row(
        &state,
        req.name.as_str(),
        Some(req.spec_type),
        req.cron_expr.as_deref(),
        req.interval_seconds,
        req.run_at,
        req.timezone.as_deref(),
        Some(req.target_id),
        req.payload.as_ref(),
        req.catch_up,
        req.max_attempts,
        // A new spec is always created Active (pause/resume toggle later).
        SpecStatus::Active,
    )
    .await?;

    let spec = storage::specs::insert(&state.pg, &row).await?;
    // Hot-reload (DESIGN §2.3): mutating the spec IS the reload — push the
    // derived ZSET change so the scheduler sees it without a restart.
    reflect_schedule(&state, &spec).await;
    Ok((StatusCode::CREATED, Json(spec)).into_response())
}

async fn get(State(state): State<AppState>, Path(id): Path<Uuid>) -> AppResult<Json<TaskSpec>> {
    let spec = storage::specs::get(&state.pg, id).await?;
    Ok(Json(spec))
}

async fn list(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> AppResult<Json<Vec<TaskSpec>>> {
    let (limit, offset) = q.page.bounds()?;
    let specs = storage::specs::list(&state.pg, q.status, limit, offset).await?;
    Ok(Json(specs))
}

/// Per-spec delivery-attempt history (Phase 3, DESIGN §3) — newest-first.
async fn executions(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(page): Query<Pagination>,
) -> AppResult<Json<Vec<TaskExecution>>> {
    // 404 if the spec doesn't exist, rather than an empty-but-200 history.
    let _ = storage::specs::get(&state.pg, id).await?;
    let (limit, offset) = page.bounds()?;
    let rows = storage::executions::list_for_spec(&state.pg, id, limit, offset).await?;
    Ok(Json(rows))
}

async fn update(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(vq): Query<VersionQuery>,
    Json(req): Json<UpdateSpecRequest>,
) -> AppResult<Response> {
    // Read-modify-write: load current, merge patch, recompute next_run if any
    // schedule-affecting field changed, then gated UPDATE on version.
    let current = storage::specs::get(&state.pg, id).await?;

    // Capture which schedule fields moved BEFORE we consume `req` (is_some
    // borrows; the unwraps below move).
    let schedule_changed = req.spec_type.is_some()
        || req.cron_expr.is_some()
        || req.interval_seconds.is_some()
        || req.run_at.is_some()
        || req.timezone.is_some();

    let name = req.name.unwrap_or_else(|| current.name.clone());
    let spec_type = req.spec_type.unwrap_or(current.spec_type);
    let cron_expr = req
        .cron_expr
        .map(|s| s.trim().to_string())
        .or(current.cron_expr.clone());
    let interval_seconds = req.interval_seconds.or(current.interval_seconds);
    let run_at = req.run_at.or(current.run_at);
    let timezone = req
        .timezone
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| current.timezone.clone());
    let target_id = req.target_id.unwrap_or(current.target_id);
    let payload = req.payload.unwrap_or_else(|| current.payload.clone());
    let catch_up = req.catch_up.unwrap_or(current.catch_up);
    let max_attempts = req.max_attempts.unwrap_or(current.max_attempts);
    // PATCH does not change status; pause/resume has dedicated endpoints.
    let status = current.status;

    let mut row = build_row(
        &state,
        &name,
        Some(spec_type),
        cron_expr.as_deref(),
        interval_seconds,
        run_at,
        Some(timezone.as_str()),
        Some(target_id),
        Some(&payload),
        Some(catch_up),
        Some(max_attempts),
        status,
    )
    .await?;

    // Only re-seed next_run when something affecting the schedule moved.
    // On a cosmetic PATCH (e.g. renaming) we keep the existing seed so the
    // firing time doesn't drift. Paused specs never carry a seed.
    if status == SpecStatus::Paused {
        row.next_run = None;
    } else if !schedule_changed {
        row.next_run = current.next_run;
    }

    let updated = storage::specs::update(&state.pg, id, vq.version, &row)
        .await?
        .ok_or_else(|| AppError::Conflict(format!(
            "spec {id} was modified or version {} is stale; re-read and retry",
            vq.version
        )))?;

    reflect_schedule(&state, &updated).await;
    Ok((StatusCode::OK, Json(updated)).into_response())
}

async fn delete(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> AppResult<StatusCode> {
    if !storage::specs::delete(&state.pg, id).await? {
        return Err(AppError::NotFound(format!("spec {id} not found")));
    }
    // Best-effort removal from the derived schedule. SQL is already the source
    // of truth (row gone), so a Redis hiccup here just leaves a stale entry
    // that the next claim treats as `Unmappable` and drops.
    if let Err(e) = storage::redis::schedule_remove(&state.redis, id).await {
        tracing::warn!(spec = %id, error = %e, "failed to remove deleted spec from Redis schedule");
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn pause(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(vq): Query<VersionQuery>,
) -> AppResult<Response> {
    transition_status(&state, id, vq.version, SpecStatus::Paused).await
}

async fn resume(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(vq): Query<VersionQuery>,
) -> AppResult<Response> {
    transition_status(&state, id, vq.version, SpecStatus::Active).await
}

/// Pause/resume core. Pausing clears `next_run`; resuming re-seeds from now.
async fn transition_status(
    state: &AppState,
    id: Uuid,
    expected_version: i32,
    new_status: SpecStatus,
) -> AppResult<Response> {
    let next_run = if new_status == SpecStatus::Active {
        // Re-seed from the current spec's schedule. Load it first to read
        // spec_type / cron / interval / run_at / timezone.
        let spec = storage::specs::get(&state.pg, id).await?;
        let nr = schedule::compute_next_run(
            spec.spec_type,
            spec.cron_expr.as_deref(),
            spec.interval_seconds,
            spec.run_at,
            &spec.timezone,
            Utc::now(),
        )
        .map_err(AppError::internal)?;
        // If there is genuinely no next run (e.g. a spent one-shot), we still
        // mark Active but with no seed; Phase 2 will surface the no-fire case.
        nr
    } else {
        None
    };

    let updated = storage::specs::set_status(&state.pg, id, expected_version, new_status, next_run)
        .await?
        .ok_or_else(|| AppError::Conflict(format!(
            "spec {id} not found or version {expected_version} is stale"
        )))?;

    reflect_schedule(state, &updated).await;
    Ok((StatusCode::OK, Json(updated)).into_response())
}

// ---------------------------------------------------------------------------
// Validation + row assembly
// ---------------------------------------------------------------------------

/// Centralize validation, then build a `SpecRow`. `Option`-wrapping the
/// fields lets this serve both create (all required) and update (merged).
#[allow(clippy::too_many_arguments)]
async fn build_row(
    state: &AppState,
    name: &str,
    spec_type: Option<SpecType>,
    cron_expr: Option<&str>,
    interval_seconds: Option<i64>,
    run_at: Option<chrono::DateTime<chrono::Utc>>,
    timezone: Option<&str>,
    target_id: Option<Uuid>,
    payload: Option<&serde_json::Value>,
    catch_up: Option<CatchUpPolicy>,
    max_attempts: Option<i32>,
    status: SpecStatus,
) -> AppResult<SpecRow> {
    crate::routes::targets::validate_name(name)?;

    let spec_type = spec_type.ok_or_else(|| {
        AppError::Validation("spec_type is required".into())
    })?;
    let target_id = target_id.ok_or_else(|| {
        AppError::Validation("target_id is required".into())
    })?;

    validate_schedule_fields(spec_type, cron_expr, interval_seconds, run_at)?;

    let timezone = timezone.unwrap_or("UTC").trim().to_string();
    // Validate timezone parses (chrono-tz) before writing, so a bad tz is a
    // 400, not a silent seed failure.
    validate_timezone(&timezone)?;

    let payload = payload
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    crate::routes::targets::validate_json_object(&payload, "payload")?;

    let catch_up = catch_up.unwrap_or(CatchUpPolicy::Skip);
    let max_attempts = max_attempts.unwrap_or(5);
    if max_attempts < 1 {
        return Err(AppError::Validation("max_attempts must be >= 1".into()));
    }

    if !storage::targets::exists(&state.pg, target_id).await? {
        return Err(AppError::Validation(format!(
            "target_id {target_id} does not exist"
        )));
    }

    // Compute the UTC next_run seed (tz-aware). DESIGN §3, §2.3.
    let now = Utc::now();
    let next_run = if status == SpecStatus::Paused {
        None
    } else {
        schedule::compute_next_run(
            spec_type,
            cron_expr.map(str::trim),
            interval_seconds,
            run_at,
            &timezone,
            now,
        )
        .map_err(AppError::internal)?
    };

    Ok(SpecRow {
        name: name.trim().to_string(),
        spec_type,
        cron_expr: cron_expr.map(str::trim).map(str::to_string),
        interval_seconds,
        run_at,
        timezone,
        target_id,
        payload,
        status,
        catch_up,
        max_attempts,
        next_run,
    })
}

fn validate_schedule_fields(
    spec_type: SpecType,
    cron_expr: Option<&str>,
    interval_seconds: Option<i64>,
    run_at: Option<chrono::DateTime<chrono::Utc>>,
) -> AppResult<()> {
    match spec_type {
        SpecType::Cron => {
            let expr = cron_expr
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AppError::Validation(format!(
                        "cron_expr is required for spec_type='cron'. {}",
                        schedule::CRON_FORMAT_HINT
                    ))
                })?;
            // Parse-check the expression up front so bad cron is a 400.
            let _ = expr
                .parse::<cron::Schedule>()
                .map_err(|e| AppError::Validation(format!(
                    "invalid cron_expr: {e}. {}",
                    schedule::CRON_FORMAT_HINT
                )))?;
            Ok(())
        }
        SpecType::Interval => {
            let secs = interval_seconds.ok_or_else(|| {
                AppError::Validation("interval_seconds is required for spec_type='interval'".into())
            })?;
            if secs <= 0 {
                return Err(AppError::Validation(
                    "interval_seconds must be positive".into(),
                ));
            }
            Ok(())
        }
        SpecType::Once => {
            let _ = run_at.ok_or_else(|| {
                AppError::Validation("run_at is required for spec_type='once'".into())
            })?;
            Ok(())
        }
    }
}

fn validate_timezone(tz: &str) -> AppResult<()> {
    tz.parse::<chrono_tz::Tz>()
        .map_err(|_| AppError::Validation(format!("invalid timezone {tz:?}")))?;
    Ok(())
}

/// Mirror a spec mutation into the derived Redis schedule (hot-reload,
/// DESIGN §2.3). Active specs with a seed are upserted; everything else
/// (paused, spent one-shot) is removed. Redis is *derived* — a failure here
/// is logged, not surfaced, so SQL (source of truth) writes never roll back.
async fn reflect_schedule(state: &AppState, spec: &TaskSpec) {
    let res = match (spec.status, spec.next_run) {
        (SpecStatus::Active, Some(nr)) => {
            storage::redis::schedule_upsert(&state.redis, spec.id, nr).await
        }
        _ => storage::redis::schedule_remove(&state.redis, spec.id).await,
    };
    if let Err(e) = res {
        tracing::warn!(
            spec = %spec.id,
            error = %e,
            "failed to mirror spec mutation to Redis schedule"
        );
    }
}
