//! Dead-letter repository — `dead_letter` table (Phase 3, DESIGN §3).
//!
//! The audit trail of "we gave up" on a logical fire. Written once when a
//! fire's retries exhaust (max_attempts reached) or a terminal failure
//! occurs, in addition to the per-attempt rows in `task_executions`. The
//! scheduler writes it best-effort; SQL remains the source of truth for
//! scheduling decisions.
//!
//! All queries are compile-time checked (`sqlx::query_as!`), so schema drift
//! fails the build (DESIGN §6).

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::AppResult;
use crate::models::DeadLetterEntry;

/// Insert a dead-letter entry for an exhausted / terminally-failed fire.
pub async fn insert(
    pool: &PgPool,
    task_id: Uuid,
    scheduled_fire_time: DateTime<Utc>,
    attempts: i32,
    last_error: Option<&str>,
    last_response_code: Option<i32>,
) -> AppResult<DeadLetterEntry> {
    let row = sqlx::query_as!(
        DeadLetterEntry,
        r#"
        INSERT INTO dead_letter (
            task_id, scheduled_fire_time, attempts, last_error, last_response_code
        )
        VALUES ($1, $2, $3, $4, $5)
        RETURNING
            id, task_id, scheduled_fire_time, attempts,
            last_error, last_response_code, created_at
        "#,
        task_id,
        scheduled_fire_time,
        attempts,
        last_error,
        last_response_code,
    )
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Newest-first dead-letter listing, optionally filtered to one task.
pub async fn list(
    pool: &PgPool,
    task_id: Option<Uuid>,
    limit: i64,
    offset: i64,
) -> AppResult<Vec<DeadLetterEntry>> {
    let rows = match task_id {
        Some(tid) => {
            sqlx::query_as!(
                DeadLetterEntry,
                r#"
                SELECT
                    id, task_id, scheduled_fire_time, attempts,
                    last_error, last_response_code, created_at
                FROM dead_letter
                WHERE task_id = $1
                ORDER BY created_at DESC, id DESC
                LIMIT $2 OFFSET $3
                "#,
                tid,
                limit,
                offset,
            )
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_as!(
                DeadLetterEntry,
                r#"
                SELECT
                    id, task_id, scheduled_fire_time, attempts,
                    last_error, last_response_code, created_at
                FROM dead_letter
                ORDER BY created_at DESC, id DESC
                LIMIT $1 OFFSET $2
                "#,
                limit,
                offset,
            )
            .fetch_all(pool)
            .await?
        }
    };
    Ok(rows)
}
