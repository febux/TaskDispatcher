//! Spec repository — `task_specs` table (DESIGN §2.1).
//!
//! SQL is the source of truth. `next_run` is the UTC seed the scheduler
//! (Phase 2) pops from the Redis ZSET; the API recomputes it on every
//! create/update that changes the schedule (DESIGN §2.3).
//!
//! All queries are compile-time checked (`sqlx::query_as!`); the column
//! list is repeated per-query because the macro needs string literals, not
//! `format!` results. That repetition is the explicit tradeoff for "schema
//! drift fails the build" (DESIGN §6).
//!
//! Updates use read-modify-write with optimistic concurrency on `version`:
//! the UPDATE is gated on `WHERE id = $1 AND version = $2` and bumps the
//! version; a zero-row result means a stale read → 409.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::AppResult;
use crate::models::{CatchUpPolicy, SpecStatus, SpecType, TaskSpec};

pub async fn insert(pool: &PgPool, input: &SpecRow) -> AppResult<TaskSpec> {
    let spec = sqlx::query_as!(
        TaskSpec,
        r#"
        INSERT INTO task_specs (
            name, spec_type, cron_expr, interval_seconds, run_at,
            timezone, target_id, payload, status, catch_up, max_attempts, next_run
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING
            id, name,
            spec_type      AS "spec_type: SpecType",
            cron_expr,
            interval_seconds,
            run_at,
            timezone,
            target_id,
            payload,
            status         AS "status: SpecStatus",
            catch_up       AS "catch_up: CatchUpPolicy",
            max_attempts,
            next_run,
            created_at,
            updated_at,
            version
        "#,
        input.name,
        input.spec_type as SpecType,
        input.cron_expr.as_deref(),
        input.interval_seconds,
        input.run_at,
        input.timezone,
        input.target_id,
        input.payload,
        input.status as SpecStatus,
        input.catch_up as CatchUpPolicy,
        input.max_attempts,
        input.next_run,
    )
    .fetch_one(pool)
    .await?;
    Ok(spec)
}

pub async fn get(pool: &PgPool, id: Uuid) -> AppResult<TaskSpec> {
    let spec = sqlx::query_as!(
        TaskSpec,
        r#"
        SELECT
            id, name,
            spec_type      AS "spec_type: SpecType",
            cron_expr,
            interval_seconds,
            run_at,
            timezone,
            target_id,
            payload,
            status         AS "status: SpecStatus",
            catch_up       AS "catch_up: CatchUpPolicy",
            max_attempts,
            next_run,
            created_at,
            updated_at,
            version
        FROM task_specs
        WHERE id = $1
        "#,
        id,
    )
    .fetch_one(pool)
    .await?;
    Ok(spec)
}

/// List specs, optionally filtered by status. Ordered by next_run so the
/// earliest-due active specs come first (the scheduler's natural order).
pub async fn list(
    pool: &PgPool,
    status: Option<SpecStatus>,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<TaskSpec>> {
    let specs = match status {
        Some(st) => {
            sqlx::query_as!(
                TaskSpec,
                r#"
                SELECT
                    id, name,
                    spec_type      AS "spec_type: SpecType",
                    cron_expr,
                    interval_seconds,
                    run_at,
                    timezone,
                    target_id,
                    payload,
                    status         AS "status: SpecStatus",
                    catch_up       AS "catch_up: CatchUpPolicy",
                    max_attempts,
                    next_run,
                    created_at,
                    updated_at,
                    version
                FROM task_specs
                WHERE status = $1
                ORDER BY next_run ASC NULLS LAST, created_at DESC
                LIMIT $2 OFFSET $3
                "#,
                st as SpecStatus,
                limit,
                offset,
            )
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_as!(
                TaskSpec,
                r#"
                SELECT
                    id, name,
                    spec_type      AS "spec_type: SpecType",
                    cron_expr,
                    interval_seconds,
                    run_at,
                    timezone,
                    target_id,
                    payload,
                    status         AS "status: SpecStatus",
                    catch_up       AS "catch_up: CatchUpPolicy",
                    max_attempts,
                    next_run,
                    created_at,
                    updated_at,
                    version
                FROM task_specs
                ORDER BY next_run ASC NULLS LAST, created_at DESC
                LIMIT $1 OFFSET $2
                "#,
                limit,
                offset,
            )
            .fetch_all(pool)
            .await?
        }
    };
    Ok(specs)
}

/// Transition a spec's status (pause/resume). Recomputes `next_run` so a
/// paused spec clears its seed (NULL) and a resumed spec re-seeds from now.
///
/// Gated on `version` for optimistic concurrency. Returns `Ok(None)` if the
/// row was not found OR the version was stale; the caller distinguishes the
/// two (404 vs 409) by first checking existence.
pub async fn set_status(
    pool: &PgPool,
    id: Uuid,
    expected_version: i32,
    new_status: SpecStatus,
    next_run: Option<DateTime<Utc>>,
) -> AppResult<Option<TaskSpec>> {
    let spec = sqlx::query_as!(
        TaskSpec,
        r#"
        UPDATE task_specs
        SET status = $1, next_run = $2, version = version + 1
        WHERE id = $3 AND version = $4
        RETURNING
            id, name,
            spec_type      AS "spec_type: SpecType",
            cron_expr,
            interval_seconds,
            run_at,
            timezone,
            target_id,
            payload,
            status         AS "status: SpecStatus",
            catch_up       AS "catch_up: CatchUpPolicy",
            max_attempts,
            next_run,
            created_at,
            updated_at,
            version
        "#,
        new_status as SpecStatus,
        next_run,
        id,
        expected_version,
    )
    .fetch_optional(pool)
    .await?;
    Ok(spec)
}

/// Full row replacement (PATCH expands to this). Gated on `version`.
/// Returns `Ok(None)` if not found / stale.
pub async fn update(
    pool: &PgPool,
    id: Uuid,
    expected_version: i32,
    row: &SpecRow,
) -> AppResult<Option<TaskSpec>> {
    let spec = sqlx::query_as!(
        TaskSpec,
        r#"
        UPDATE task_specs SET
            name             = $1,
            spec_type        = $2,
            cron_expr        = $3,
            interval_seconds = $4,
            run_at           = $5,
            timezone         = $6,
            target_id        = $7,
            payload          = $8,
            status           = $9,
            catch_up         = $10,
            max_attempts     = $11,
            next_run         = $12,
            version          = version + 1
        WHERE id = $13 AND version = $14
        RETURNING
            id, name,
            spec_type      AS "spec_type: SpecType",
            cron_expr,
            interval_seconds,
            run_at,
            timezone,
            target_id,
            payload,
            status         AS "status: SpecStatus",
            catch_up       AS "catch_up: CatchUpPolicy",
            max_attempts,
            next_run,
            created_at,
            updated_at,
            version
        "#,
        row.name,
        row.spec_type as SpecType,
        row.cron_expr.as_deref(),
        row.interval_seconds,
        row.run_at,
        row.timezone,
        row.target_id,
        row.payload,
        row.status as SpecStatus,
        row.catch_up as CatchUpPolicy,
        row.max_attempts,
        row.next_run,
        id,
        expected_version,
    )
    .fetch_optional(pool)
    .await?;
    Ok(spec)
}

/// Delete a spec. Returns `false` if no row matched (caller → 404).
pub async fn delete(pool: &PgPool, id: Uuid) -> AppResult<bool> {
    let affected = sqlx::query!("DELETE FROM task_specs WHERE id = $1", id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(affected > 0)
}

/// Every active spec that carries a `next_run` seed — the source for the
/// Redis ZSET rebuild at boot (DESIGN §2.1: "if Redis evaporates, rebuild it
/// from SQL in one pass"). Ordered for deterministic seeding.
pub async fn list_active_next_runs(pool: &PgPool) -> AppResult<Vec<(Uuid, Option<DateTime<Utc>>)>> {
    let rows = sqlx::query!(
        r#"
        SELECT id, next_run
        FROM task_specs
        WHERE status = 'active' AND next_run IS NOT NULL
        ORDER BY next_run ASC
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| (r.id, r.next_run)).collect())
}

/// Scheduler re-seed of `next_run` after a successful fire (DESIGN §2.2).
///
/// Gated on `version`: if a user PATCH changed the spec in flight, the
/// scheduler's stale write is skipped (the PATCH already set the right
/// `next_run`). We intentionally do NOT bump `version` here — scheduler
/// writes must not break concurrent user optimistic-concurrency. The
/// `updated_at` trigger still fires. Returns `true` if the row was updated.
pub async fn set_next_run(
    pool: &PgPool,
    id: Uuid,
    expected_version: i32,
    next_run: Option<DateTime<Utc>>,
) -> AppResult<bool> {
    let affected = sqlx::query!(
        "UPDATE task_specs SET next_run = $1 WHERE id = $2 AND version = $3",
        next_run,
        id,
        expected_version,
    )
    .execute(pool)
    .await?
    .rows_affected();
    Ok(affected > 0)
}

/// The full, validated field set of a spec row, ready to INSERT or to
/// overwrite an existing row on UPDATE. Built by the route layer after
/// validation and `next_run` computation.
#[derive(Debug, Clone)]
pub struct SpecRow {
    pub name: String,
    pub spec_type: SpecType,
    pub cron_expr: Option<String>,
    pub interval_seconds: Option<i64>,
    pub run_at: Option<DateTime<Utc>>,
    pub timezone: String,
    pub target_id: Uuid,
    pub payload: serde_json::Value,
    pub status: SpecStatus,
    pub catch_up: CatchUpPolicy,
    pub max_attempts: i32,
    pub next_run: Option<DateTime<Utc>>,
}
