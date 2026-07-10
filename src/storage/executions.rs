//! Execution history repository — `task_executions` table (Phase 3, DESIGN §3).
//!
//! One row per delivery attempt. Written best-effort by the scheduler on
//! every transport outcome — a failure to record history must never fail a
//! fire (SQL remains the source of truth for *scheduling*). The table is the
//! audit trail behind SLAs and debugging ("what fired last Tuesday and how
//! did it end").
//!
//! All queries are compile-time checked (`sqlx::query_as!`), so schema drift
//! fails the build (DESIGN §6).

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::AppResult;
use crate::models::{ExecutionStatus, TaskExecution};

/// Record a single delivery-attempt outcome. `latency_ms` / `response_code`
/// are `None` when no HTTP response was received (connect/timeout failure).
#[allow(clippy::too_many_arguments)]
pub async fn record(
    pool: &PgPool,
    task_id: Uuid,
    scheduled_fire_time: DateTime<Utc>,
    attempt: i32,
    status: ExecutionStatus,
    latency_ms: Option<i32>,
    response_code: Option<i32>,
    error: Option<&str>,
) -> AppResult<TaskExecution> {
    let row = sqlx::query_as!(
        TaskExecution,
        r#"
        INSERT INTO task_executions (
            task_id, scheduled_fire_time, attempt, status, latency_ms, response_code, error
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING
            id, task_id, scheduled_fire_time, attempt,
            status         AS "status: ExecutionStatus",
            latency_ms, response_code, error, created_at
        "#,
        task_id,
        scheduled_fire_time,
        attempt,
        status as ExecutionStatus,
        latency_ms,
        response_code,
        error,
    )
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Newest-first history for a single spec (the hot debugging query).
pub async fn list_for_spec(
    pool: &PgPool,
    task_id: Uuid,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<TaskExecution>> {
    let rows = sqlx::query_as!(
        TaskExecution,
        r#"
        SELECT
            id, task_id, scheduled_fire_time, attempt,
            status         AS "status: ExecutionStatus",
            latency_ms, response_code, error, created_at
        FROM task_executions
        WHERE task_id = $1
        ORDER BY created_at DESC, id DESC
        LIMIT $2 OFFSET $3
        "#,
        task_id,
        limit,
        offset,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
