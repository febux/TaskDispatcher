//! Target repository — `targets` table (DESIGN §2.1).
//!
//! A `Target` is a delivery destination: transport key + endpoint + auth.
//! v1 ships the `http` transport only (DESIGN §2.4). Transports are looked
//! up by name in the registry at fire time.

use sqlx::PgPool;

use crate::error::AppResult;
use crate::models::Target;

/// Insert a new target. A duplicate `name` violates the unique constraint
/// and is mapped to `Conflict` (409) by the error layer.
#[allow(clippy::too_many_arguments)]
pub async fn insert(
    pool: &PgPool,
    name: &str,
    transport: &str,
    url: &str,
    secret_hmac: Option<&str>,
    headers: &serde_json::Value,
    healthcheck_url: Option<&str>,
    healthcheck_interval_seconds: i32,
    healthcheck_timeout_seconds: i32,
) -> AppResult<Target> {
    let target = sqlx::query_as!(
        Target,
        r#"
        INSERT INTO targets (
            name, transport, url, secret_hmac, headers,
            healthcheck_url, healthcheck_interval_seconds, healthcheck_timeout_seconds
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING
            id, name, transport, url, secret_hmac, headers,
            healthcheck_url, healthcheck_interval_seconds,
            healthcheck_timeout_seconds, created_at, updated_at
        "#,
        name,
        transport,
        url,
        secret_hmac,
        headers,
        healthcheck_url,
        healthcheck_interval_seconds,
        healthcheck_timeout_seconds,
    )
    .fetch_one(pool)
    .await?;
    Ok(target)
}

pub async fn get(pool: &PgPool, id: uuid::Uuid) -> AppResult<Target> {
    let target = sqlx::query_as!(
        Target,
        r#"
        SELECT
            id, name, transport, url, secret_hmac, headers,
            healthcheck_url, healthcheck_interval_seconds,
            healthcheck_timeout_seconds, created_at, updated_at
        FROM targets
        WHERE id = $1
        "#,
        id,
    )
    .fetch_one(pool)
    .await?;
    Ok(target)
}

pub async fn list(pool: &PgPool, limit: i64, offset: i64) -> AppResult<Vec<Target>> {
    let targets = sqlx::query_as!(
        Target,
        r#"
        SELECT
            id, name, transport, url, secret_hmac, headers,
            healthcheck_url, healthcheck_interval_seconds,
            healthcheck_timeout_seconds, created_at, updated_at
        FROM targets
        ORDER BY created_at DESC, id DESC
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset,
    )
    .fetch_all(pool)
    .await?;
    Ok(targets)
}

/// Delete a target. Returns `false` if no row matched (caller surfaces 404).
/// Deleting a target referenced by a spec hits FK RESTRICT → mapped to 409.
pub async fn delete(pool: &PgPool, id: uuid::Uuid) -> AppResult<bool> {
    let affected = sqlx::query!("DELETE FROM targets WHERE id = $1", id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(affected > 0)
}

/// True if a target with this id exists. Used to validate spec create/update
/// before the FK constraint catches it (cleaner 400 vs raw 409).
pub async fn exists(pool: &PgPool, id: uuid::Uuid) -> AppResult<bool> {
    let row = sqlx::query!("SELECT EXISTS(SELECT 1 FROM targets WHERE id = $1) AS exists_", id)
        .fetch_one(pool)
        .await?;
    Ok(row.exists_.unwrap_or(false))
}
