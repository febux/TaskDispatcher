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
