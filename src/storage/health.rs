//! Service-health repository — `service_health` table (extension).
//!
//! A separate `healthchecker` process writes rows here; the scheduler reads
//! them to decide whether a target is healthy enough to fire. SQL remains the
//! source of truth; a Redis mirror is optional for hot cache but not required.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::AppResult;
use crate::models::{HealthStatus, ServiceHealth, Target};

/// Upsert the health status for a target. `changed_at` is bumped only when
/// the status differs from the previous row.
#[allow(clippy::too_many_arguments)]
pub async fn upsert(
    pool: &PgPool,
    target_id: Uuid,
    status: HealthStatus,
    status_code: Option<i32>,
    error: Option<&str>,
    checked_at: DateTime<Utc>,
) -> AppResult<ServiceHealth> {
    let row = sqlx::query_as!(
        ServiceHealth,
        r#"
        INSERT INTO service_health (target_id, status, status_code, error, checked_at, changed_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (target_id) DO UPDATE SET
            status        = EXCLUDED.status,
            status_code   = EXCLUDED.status_code,
            error         = EXCLUDED.error,
            checked_at    = EXCLUDED.checked_at,
            changed_at    = CASE
                                WHEN service_health.status IS DISTINCT FROM EXCLUDED.status
                                THEN NOW()
                                ELSE service_health.changed_at
                            END
        RETURNING
            target_id, status AS "status: HealthStatus",
            status_code, error, checked_at, changed_at
        "#,
        target_id,
        status as HealthStatus,
        status_code,
        error,
        checked_at,
    )
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Get the latest health row for a target.
pub async fn get(pool: &PgPool, target_id: Uuid) -> AppResult<ServiceHealth> {
    let row = sqlx::query_as!(
        ServiceHealth,
        r#"
        SELECT
            target_id, status AS "status: HealthStatus",
            status_code, error, checked_at, changed_at
        FROM service_health
        WHERE target_id = $1
        "#,
        target_id,
    )
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// List all targets that have a healthcheck configured, joined with their
/// latest status. Used by the healthchecker to know what to poll.
pub async fn list_targets_with_healthchecks(pool: &PgPool) -> AppResult<Vec<Target>> {
    let rows = sqlx::query_as!(
        Target,
        r#"
        SELECT
            id, name, transport, url, secret_hmac, headers,
            healthcheck_url, healthcheck_interval_seconds,
            healthcheck_timeout_seconds, created_at, updated_at
        FROM targets
        WHERE healthcheck_url IS NOT NULL
        ORDER BY updated_at DESC, id DESC
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// True if the target is healthy (or has no healthcheck URL — treated as
/// always healthy). Used by the scheduler before firing.
pub async fn is_healthy(pool: &PgPool, target_id: Uuid) -> AppResult<bool> {
    let row = sqlx::query_as!(
        ServiceHealth,
        r#"
        SELECT
            target_id, status AS "status: HealthStatus",
            status_code, error, checked_at, changed_at
        FROM service_health
        WHERE target_id = $1
        "#,
        target_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(matches!(
        row.map(|r| r.status),
        None | Some(HealthStatus::Healthy)
    ))
}

/// Mark a target as unknown (used by the healthchecker when a target has a
/// healthcheck URL but the first probe hasn't run, or at startup).
pub async fn set_unknown(pool: &PgPool, target_id: Uuid) -> AppResult<ServiceHealth> {
    upsert(pool, target_id, HealthStatus::Unknown, None, None, Utc::now()).await
}
