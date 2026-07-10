//! Domain models — the source-of-truth types persisted in Postgres.
//!
//! These map 1:1 to the `targets` and `task_specs` tables (migration 0002).
//! HTTP DTOs live in `routes::{specs, targets}` and convert into these;
//! storage functions return these directly via `sqlx::query_as!`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Type;
use uuid::Uuid;

/// A destination for deliveries. v1 ships the `http` transport only
/// (DESIGN §2.4); `transport` is the registry key future impls key on.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Target {
    pub id: Uuid,
    pub name: String,
    pub transport: String,
    pub url: String,
    /// HMAC-SHA256 signing key; `None` means unsigned webhooks (DESIGN §3).
    #[serde(skip_serializing)]
    pub secret_hmac: Option<String>,
    pub headers: serde_json::Value,
    /// Optional healthcheck endpoint for the external service (extension).
    pub healthcheck_url: Option<String>,
    /// How often the healthchecker polls `healthcheck_url` (seconds).
    pub healthcheck_interval_seconds: i32,
    /// Per-healthcheck request timeout (seconds).
    pub healthcheck_timeout_seconds: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// What kind of schedule a spec carries. Decides which seed field drives
/// `next_run` (see `schedule::compute_next_run`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "TEXT", rename_all = "lowercase")]
pub enum SpecType {
    Cron,
    Interval,
    Once,
}

/// Lifecycle of a spec. `Paused` removes the spec from the scheduling ZSET
/// (Phase 2); it is NOT a delete. Pause/resume is the hot-toggle primitive
/// (DESIGN §3, §2.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "TEXT", rename_all = "lowercase")]
pub enum SpecStatus {
    Active,
    Paused,
}

/// Behaviour after missed fires (downtime recovery). DESIGN §3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "TEXT", rename_all = "snake_case")]
pub enum CatchUpPolicy {
    RunMissed,
    Skip,
    RunOnce,
}

/// A task spec — the persisted scheduling contract.
///
/// `next_run` is the UTC seed the scheduler (Phase 2) pops from the Redis
/// ZSET. It is recomputed on every create/update that changes the schedule.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct TaskSpec {
    pub id: Uuid,
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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: i32,
}

/// Outcome of a single delivery attempt, recorded in `task_executions`
/// (Phase 3, DESIGN §3). Maps 1:1 to `SendResult` variants — the audit trail
/// behind "what happened on each try".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "TEXT", rename_all = "lowercase")]
pub enum ExecutionStatus {
    /// The handler accepted the fire (2xx).
    Delivered,
    /// Transient failure (5xx / timeout / connect) — retried with backoff.
    Retryable,
    /// Permanent failure (4xx other than 408/429) — not retried.
    Terminal,
}

/// One row per delivery attempt — the "why did it fail on Tuesday" table
/// (DESIGN §3). Written best-effort by the scheduler on every transport
/// outcome. `scheduled_fire_time` is stable across retries of the same
/// logical fire, so it groups an attempt chain together.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct TaskExecution {
    pub id: Uuid,
    pub task_id: Uuid,
    pub scheduled_fire_time: DateTime<Utc>,
    pub attempt: i32,
    pub status: ExecutionStatus,
    pub latency_ms: Option<i32>,
    pub response_code: Option<i32>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// A dead-lettered logical fire — the audit trail of "we gave up" (DESIGN §3).
/// Written once when retries exhaust or a terminal failure occurs, in
/// addition to the per-attempt `TaskExecution` rows for the same fire.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct DeadLetterEntry {
    pub id: Uuid,
    pub task_id: Uuid,
    pub scheduled_fire_time: DateTime<Utc>,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub last_response_code: Option<i32>,
    pub created_at: DateTime<Utc>,
}

/// Latest health status of a target's external service, as polled by the
/// separate `healthchecker` service (extension).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "TEXT", rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Unhealthy,
    /// No healthcheck has completed yet (or target has no healthcheck URL).
    Unknown,
}

/// Row in `service_health`, updated by the healthchecker.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ServiceHealth {
    pub target_id: Uuid,
    pub status: HealthStatus,
    pub status_code: Option<i32>,
    pub error: Option<String>,
    pub checked_at: DateTime<Utc>,
    pub changed_at: DateTime<Utc>,
}
