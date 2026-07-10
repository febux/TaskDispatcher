//! The scheduler loop — DESIGN §2.2, §2.3, §3.
//!
//! One background tokio task, spawned by `main`, that:
//! 1. On boot rebuilds the `schedule` ZSET from SQL (source of truth).
//! 2. Every tick atomically claims due tasks (Lua `ZREM schedule` + `SADD
//!    processing`) — the at-most-once-per-tick guarantee.
//! 3. Spawns each fire on a `JoinSet`; per-fire attempt state lives in Redis.
//! 4. On delivery: recompute `next_run` (cron-aware, no drift), write it back
//!    to SQL + the ZSET; a spent one-shot drops out (`next_run = NULL`).
//! 5. On failure: exponential backoff + jitter (DESIGN §3), bounded by the
//!    spec's `max_attempts`; on exhaustion the task is dead-lettered (Phase 3
//!    `dead_letter` table) and dropped from the schedule. Each attempt is
//!    also recorded in `task_executions` (the history table, DESIGN §3).
//! 6. Honours catch-up policy when a fire is detected overdue.
//! 7. Drains in-flight fires on graceful shutdown.
//!
//! SQL is the source of truth; Redis is derived. The API keeps both in sync
//! on mutation (DESIGN §2.3 hot-reload), so the loop only ever *re-seeds*
//! `next_run` — it never invents scheduling state.

pub mod lua;

use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, warn};
use uuid::Uuid;

use crate::config::SchedulerConfig;
use crate::metrics::Metrics;
use crate::models::{CatchUpPolicy, ExecutionStatus, TaskSpec};
use crate::schedule;
// Alias our storage module so the external `redis` crate stays in scope.
use crate::storage::redis::{self as store, RedisPool};
use crate::storage::{self, specs};
use crate::state::SchedulerHealth;
use crate::transport::{FireMeta, Registry, SendResult};

/// Attempt state for an in-flight fire, kept in Redis as a hash so it
/// survives across backoff ticks. `attempt` is 1-based; `fire_at` is the
/// original scheduled fire time (ZSET score we popped).
mod fire_state {
    use chrono::{DateTime, Utc};
    use redis::aio::ConnectionManager;
    use uuid::Uuid;

    fn key(id: Uuid) -> String {
        format!("fire:{id}")
    }

    /// Initialise (or bump) the attempt counter for a claimed task. Returns
    /// the resulting attempt number and the stable `fire_at` — the original
    /// scheduled fire time, which `HSETNX` keeps constant across retries so
    /// `X-Fire-Id` groups the whole attempt chain (DESIGN §5).
    pub async fn bump_attempt(
        conn: &mut ConnectionManager,
        id: Uuid,
        fire_at: DateTime<Utc>,
    ) -> anyhow::Result<(u32, DateTime<Utc>)> {
        let k = key(id);
        let mut pipe = redis::pipe();
        pipe.atomic()
            .hset_nx(&k, "fire_at", fire_at.timestamp_millis())
            .ignore()
            .hincr(&k, "attempt", 1i64)
            .ignore()
            .expire(&k, 86_400i64) // 24h TTL guards against orphaned state.
            .ignore();
        pipe.query_async::<()>(conn).await?;

        let attempt: i64 = redis::cmd("HGET")
            .arg(&k)
            .arg("attempt")
            .query_async(conn)
            .await
            .unwrap_or(0);
        let fire_at_ms: i64 = redis::cmd("HGET")
            .arg(&k)
            .arg("fire_at")
            .query_async(conn)
            .await
            .unwrap_or(0);
        let fire_at = DateTime::<Utc>::from_timestamp_millis(fire_at_ms).unwrap_or(fire_at);
        Ok((attempt.max(0) as u32, fire_at))
    }

    /// Drop attempt state — called on success, terminal failure, or exhaustion.
    pub async fn clear(conn: &mut ConnectionManager, id: Uuid) {
        let _ = redis::cmd("DEL")
            .arg(key(id))
            .query_async::<()>(conn)
            .await;
    }
}

pub struct Scheduler {
    cfg: SchedulerConfig,
    pg: PgPool,
    redis: RedisPool,
    transports: Arc<Registry>,
    health: SchedulerHealth,
    metrics: Metrics,
}

/// Per-fire outcome the scheduler acts on after a transport returns.
enum FireOutcome {
    Delivered,
    Retry(u32), // next attempt number
    /// Target healthcheck reports unhealthy; fire skipped and task requeued
    /// at the next healthcheck interval.
    Skipped,
    /// Retries exhausted or a terminal failure — the fire is dead. Carries
    /// enough to write one `dead_letter` row (DESIGN §3) in addition to the
    /// per-attempt rows already recorded.
    Exhausted {
        scheduled_fire_time: DateTime<Utc>,
        attempts: u32,
        last_error: String,
        last_status: Option<u16>,
    },
    Unmappable, // spec/target vanished mid-flight; drop
}

impl Scheduler {
    pub fn new(
        cfg: SchedulerConfig,
        pg: PgPool,
        redis: RedisPool,
        transports: Registry,
        health: SchedulerHealth,
        metrics: Metrics,
    ) -> Self {
        Self {
            cfg,
            pg,
            redis,
            transports: Arc::new(transports),
            health,
            metrics,
        }
    }

    /// Rebuild the `schedule` ZSET from SQL, then run the loop until cancelled.
    /// In-flight fires are drained (bounded by `shutdown_timeout`) on cancel.
    pub async fn run(self, shutdown: CancellationToken) {
        if let Err(e) = self.seed_schedule().await {
            // Seeding failure is non-fatal: log and proceed — the loop is
            // still correct, it just starts empty until the API hot-reloads.
            error!(error = %e, "schedule seed failed; starting with empty schedule");
        }

        let Scheduler {
            cfg,
            pg,
            redis,
            transports,
            health,
            metrics,
        } = self;
        let mut interval = tokio::time::interval(cfg.tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut inflight: JoinSet<()> = JoinSet::new();

        health.mark_healthy();
        info!(
            tick_ms = cfg.tick_interval.as_millis() as u64,
            batch = cfg.batch_size,
            "scheduler loop started"
        );

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    tick(
                        &cfg, &pg, &redis, &transports, &health, &metrics, &mut inflight
                    ).await;
                }
                _ = shutdown.cancelled() => break,
            }
        }

        health.mark_unhealthy();
        info!(inflight = inflight.len(), "scheduler cancelling; draining in-flight fires");
        // Stop spawning; let the ones already running finish within a timeout.
        let drain = async {
            while !inflight.is_empty() {
                if inflight.join_next().await.is_none() {
                    break;
                }
            }
        };
        tokio::select! {
            _ = drain => info!("scheduler drain complete"),
            _ = tokio::time::sleep(cfg.shutdown_timeout) => {
                warn!(remaining = inflight.len(), "shutdown timeout; abandoning in-flight fires");
                inflight.abort_all();
                // Best-effort reap of the aborted tasks.
                while inflight.join_next().await.is_some() {}
            }
        }
    }

    /// Rebuild the derived ZSET from the SQL source of truth (DESIGN §2.1).
    /// Fully idempotent: wipe both keys, then re-seed. Safe to run every boot.
    async fn seed_schedule(&self) -> anyhow::Result<()> {
        let rows = specs::list_active_next_runs(&self.pg).await?;
        let mut conn = self.redis.clone();

        let mut pipe = redis::pipe();
        pipe.atomic()
            .del(store::SCHEDULE_KEY)
            .ignore()
            .del(store::PROCESSING_KEY)
            .ignore();
        for (id, next_run) in &rows {
            let Some(nr) = next_run else { continue };
            // Raw ZADD (key score member) — the high-level pipeline method
            // takes (member, score) which is easy to transpose; the explicit
            // cmd avoids that footgun.
            pipe.cmd("ZADD")
                .arg(store::SCHEDULE_KEY)
                .arg(store::score(*nr))
                .arg(id.to_string())
                .ignore();
        }
        pipe.query_async::<()>(&mut conn).await?;

        info!(seeded = rows.len(), "schedule ZSET rebuilt from SQL");
        Ok(())
    }
}

/// One scheduler tick: claim due tasks and spawn their fire tasks.
async fn tick(
    cfg: &SchedulerConfig,
    pg: &PgPool,
    redis: &RedisPool,
    transports: &Arc<Registry>,
    health: &SchedulerHealth,
    metrics: &Metrics,
    inflight: &mut JoinSet<()>,
) {
    health.tick().await;
    let now = Utc::now();

    // Update queue-depth + schedule-lag gauges every tick (Phase 4).
    match store::schedule_len(redis).await {
        Ok(n) => metrics.set_queue_depth(n),
        Err(e) => warn!(error = %e, "failed to read schedule depth"),
    }
    match store::schedule_oldest_score(redis).await {
        Ok(Some(score)) => {
            let oldest_ms = score as i64;
            let lag_ms = now.timestamp_millis().saturating_sub(oldest_ms).max(0);
            metrics.set_schedule_lag(lag_ms as f64 / 1_000.0);
        }
        Ok(None) => metrics.set_schedule_lag(0.0),
        Err(e) => warn!(error = %e, "failed to read oldest schedule score"),
    }

    let claimed = match lua::claim_due(&mut redis.clone(), now.timestamp_millis(), cfg.batch_size)
        .await
    {
        Ok(ids) => ids,
        Err(e) => {
            warn!(error = %e, "claim failed this tick");
            return;
        }
    };
    if claimed.is_empty() {
        return;
    }

    let span = tracing::info_span!("tick", claimed = claimed.len());
    let _enter = span.enter();
    for id_str in claimed {
        let Ok(id) = Uuid::parse_str(&id_str) else {
            warn!(%id_str, "claimed non-uuid schedule member; dropping");
            let mut conn = redis.clone();
            let _ = redis::cmd("SREM")
                .arg(store::PROCESSING_KEY)
                .arg(&id_str)
                .query_async::<()>(&mut conn)
                .await;
            continue;
        };

        let ctx = FireCtx {
            cfg: cfg.clone(),
            pg: pg.clone(),
            redis: redis.clone(),
            transports: transports.clone(),
            id,
            claimed_at: now,
            metrics: metrics.clone(),
        };
        inflight.spawn(
            async move { ctx.fire().await }
                .instrument(tracing::info_span!("fire", %id)),
        );
    }
}

/// Everything a single fire task needs, captured by value for the spawn.
struct FireCtx {
    cfg: SchedulerConfig,
    pg: PgPool,
    redis: RedisPool,
    transports: Arc<Registry>,
    id: Uuid,
    claimed_at: DateTime<Utc>,
    metrics: Metrics,
}

impl FireCtx {
    async fn fire(self) {
        match self.attempt_fire().await {
            FireOutcome::Delivered => {} // logged inside attempt_fire
            FireOutcome::Skipped => {
                tracing::warn!(task = %self.id, "fire skipped; target unhealthy");
                // requeue_at already removed the task from `processing` and
                // re-added it to `schedule`; clear only the attempt state so
                // the next probe starts fresh.
                fire_state::clear(&mut self.redis.clone(), self.id).await;
            }
            FireOutcome::Retry(next) => {
                let backoff = self.cfg.backoff(next);
                tracing::warn!(task = %self.id, attempt = next, ?backoff, "retry scheduled");
                self.requeue_retry(backoff).await;
            }
            FireOutcome::Exhausted {
                scheduled_fire_time,
                attempts,
                ref last_error,
                last_status,
            } => {
                tracing::error!(
                    task = %self.id,
                    attempts,
                    status = last_status,
                    error = %last_error,
                    "max_attempts exhausted; dead-lettering (DESIGN §3)"
                );
                self.dead_letter(scheduled_fire_time, attempts, last_error, last_status)
                    .await;
                self.finalize_drop().await;
            }
            FireOutcome::Unmappable => {
                tracing::warn!(task = %self.id, "spec/target vanished mid-flight; dropping");
                self.finalize_drop().await;
            }
        }
    }

    /// Load spec + target, dispatch via the transport, and classify the result.
    async fn attempt_fire(&self) -> FireOutcome {
        // Hydrate from SQL (source of truth).
        let spec = match specs::get(&self.pg, self.id).await {
            Ok(s) => s,
            Err(crate::error::AppError::NotFound(_)) => return FireOutcome::Unmappable,
            Err(e) => {
                tracing::warn!(task = %self.id, error = %e, "spec load failed; will retry");
                return FireOutcome::Retry(1);
            }
        };
        let target = match storage::targets::get(&self.pg, spec.target_id).await {
            Ok(t) => t,
            Err(crate::error::AppError::NotFound(_)) => return FireOutcome::Unmappable,
            Err(e) => {
                tracing::warn!(task = %self.id, error = %e, "target load failed; will retry");
                return FireOutcome::Retry(1);
            }
        };

        // Bump the attempt counter (1 on first claim). `fire_at` is the
        // *original* scheduled fire time — HSETNX keeps it stable across
        // retries, so X-Fire-Id groups the whole attempt chain (DESIGN §5).
        let (attempt, fire_at) =
            match fire_state::bump_attempt(&mut self.redis.clone(), self.id, self.claimed_at).await {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!(
                        task = %self.id,
                        error = %e,
                        "attempt tracking failed; treating as attempt 1"
                    );
                    (1u32, self.claimed_at)
                }
            };

        // Extension: skip fire when target healthcheck is unhealthy.
        // Targets with no healthcheck configured are implicitly healthy.
        if !self.is_target_healthy(&target).await {
            let interval = target.healthcheck_interval_seconds.max(1) as i64;
            let requeue_at = Utc::now() + chrono::Duration::seconds(interval);
            tracing::warn!(
                task = %self.id,
                target = %target.id,
                "target unhealthy; skipping fire and requeueing at next healthcheck interval"
            );
            self.requeue_at(requeue_at).await;
            return FireOutcome::Skipped;
        }

        let Some(transport) = self.transports.get(&target.transport) else {
            let msg = format!("no transport registered for {:?}", target.transport);
            tracing::error!(task = %self.id, transport = %target.transport, "terminal: {msg}");
            self.metrics.observe_fire(&target.transport, "terminal", None);
            self.record_attempt(fire_at, attempt, ExecutionStatus::Terminal, None, None, Some(&msg))
                .await;
            return FireOutcome::Exhausted {
                scheduled_fire_time: fire_at,
                attempts: attempt,
                last_error: msg,
                last_status: None,
            };
        };

        let meta = FireMeta {
            task_id: self.id,
            scheduled_fire_time: fire_at,
            attempt,
        };
        let result = transport.send(&target, &spec.payload, &meta).await;
        let transport_key = target.transport.clone();

        match result {
            SendResult::Delivered { latency_ms } => {
                tracing::info!(task = %self.id, attempt, latency_ms, "delivered");
                self.metrics
                    .observe_fire(&transport_key, "delivered", Some(latency_ms));
                self.record_attempt(
                    fire_at,
                    attempt,
                    ExecutionStatus::Delivered,
                    Some(latency_ms),
                    None,
                    None,
                )
                .await;
                if let Err(e) = self.on_delivered(&spec).await {
                    tracing::warn!(task = %self.id, error = %e, "post-delivery re-seed failed");
                }
                fire_state::clear(&mut self.redis.clone(), self.id).await;
                FireOutcome::Delivered
            }
            SendResult::Retryable { error, status } => {
                tracing::warn!(task = %self.id, attempt, status, error, "retryable failure");
                self.metrics.observe_fire(&transport_key, "retryable", None);
                self.record_attempt(
                    fire_at,
                    attempt,
                    ExecutionStatus::Retryable,
                    None,
                    status.map(|s| s as i32),
                    Some(&error),
                )
                .await;
                if attempt >= spec.max_attempts as u32 {
                    FireOutcome::Exhausted {
                        scheduled_fire_time: fire_at,
                        attempts: attempt,
                        last_error: error,
                        last_status: status,
                    }
                } else {
                    FireOutcome::Retry(attempt + 1)
                }
            }
            SendResult::Terminal { error, status } => {
                tracing::error!(task = %self.id, attempt, status, error, "terminal failure");
                self.metrics.observe_fire(&transport_key, "terminal", None);
                self.record_attempt(
                    fire_at,
                    attempt,
                    ExecutionStatus::Terminal,
                    None,
                    status.map(|s| s as i32),
                    Some(&error),
                )
                .await;
                FireOutcome::Exhausted {
                    scheduled_fire_time: fire_at,
                    attempts: attempt,
                    last_error: error,
                    last_status: status,
                }
            }
        }
    }

    /// Recompute `next_run` after a successful delivery and write it back to
    /// SQL + the ZSET (DESIGN §2.2). Honours catch-up when the fire was late.
    async fn on_delivered(&self, spec: &TaskSpec) -> anyhow::Result<()> {
        // The cadence reference is the *scheduled* fire time, not `now`, so a
        // cron/interval cadence does not drift when the scheduler runs late.
        // For an overdue fire (catch-up), the policy adjusts the reference.
        let reference = self.cadence_reference(spec);
        let next = schedule::compute_next_run(
            spec.spec_type,
            spec.cron_expr.as_deref(),
            spec.interval_seconds,
            spec.run_at,
            &spec.timezone,
            reference,
        )?;

        // Always clear processing membership (the fire is done either way).
        let mut conn = self.redis.clone();
        let _ = redis::cmd("SREM")
            .arg(store::PROCESSING_KEY)
            .arg(self.id.to_string())
            .query_async::<()>(&mut conn)
            .await;

        if next.is_none() {
            // Spent one-shot: NULL next_run means it naturally leaves the
            // schedule (the boot seed skips NULLs).
            let _ = specs::set_next_run(&self.pg, self.id, spec.version, None).await;
            return Ok(());
        }

        // Keep SQL (source of truth) in sync. Version-gated: if a user PATCH
        // landed in flight, skip — the PATCH already owns next_run.
        let nr = next.unwrap();
        if specs::set_next_run(&self.pg, self.id, spec.version, Some(nr)).await? {
            store::schedule_upsert(&self.redis, self.id, nr).await?;
        }
        Ok(())
    }

    /// Decide the reference instant for recomputing the cadence, given the
    /// catch-up policy and how late this fire was (DESIGN §3).
    ///
    /// - `Skip` / `RunOnce`: a late fire collapses to one delivery; the next
    ///   cadence is computed from `now` so we resume in-step.
    /// - `RunMissed`: keep catching up slot-by-slot — the next occurrence is
    ///   computed from the original scheduled time.
    fn cadence_reference(&self, spec: &TaskSpec) -> DateTime<Utc> {
        let now = Utc::now();
        let late = now - self.claimed_at;
        if late <= chrono::Duration::milliseconds(500) {
            return self.claimed_at; // on time — no catch-up adjustment
        }
        match spec.catch_up {
            CatchUpPolicy::RunMissed => self.claimed_at,
            CatchUpPolicy::Skip | CatchUpPolicy::RunOnce => now,
        }
    }

    /// Re-add the task to the schedule at `now + backoff` for the next attempt.
    async fn requeue_retry(&self, backoff: StdDuration) {
        let delta = chrono::Duration::from_std(backoff).unwrap_or(chrono::Duration::seconds(1));
        let when = Utc::now() + delta;
        self.requeue_at(when).await;
    }

    /// Re-add the task to the schedule at an absolute time, clearing
    /// `processing` membership. Used by retry backoff and by the unhealthy
    /// skip path.
    async fn requeue_at(&self, when: DateTime<Utc>) {
        let mut conn = self.redis.clone();
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("ZADD")
            .arg(store::SCHEDULE_KEY)
            .arg(store::score(when))
            .arg(self.id.to_string())
            .ignore()
            .srem(store::PROCESSING_KEY, self.id.to_string())
            .ignore();
        if let Err(e) = pipe.query_async::<()>(&mut conn).await {
            tracing::error!(
                task = %self.id,
                error = %e,
                "failed to requeue — task left in processing"
            );
        }
    }

    /// Check target health via `service_health` (extension). Targets with no
    /// healthcheck_url are always considered healthy.
    async fn is_target_healthy(&self, target: &crate::models::Target) -> bool {
        if target.healthcheck_url.is_none() {
            return true;
        }
        match storage::health::is_healthy(&self.pg, target.id).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    target = %target.id,
                    error = %e,
                    "failed to read target health; treating as unhealthy to be safe"
                );
                false
            }
        }
    }

    /// Remove the task from scheduling entirely (exhausted / unmappable).
    async fn finalize_drop(&self) {
        fire_state::clear(&mut self.redis.clone(), self.id).await;
        let _ = store::schedule_remove(&self.redis, self.id).await;
    }

    /// Best-effort write of a `task_executions` row (DESIGN §3). A failure
    /// here is logged, not surfaced — the audit table must never cause a
    /// scheduling failure. `latency_ms`/`response_code` are `None` when no
    /// HTTP response was received.
    async fn record_attempt(
        &self,
        scheduled_fire_time: DateTime<Utc>,
        attempt: u32,
        status: ExecutionStatus,
        latency_ms: Option<u64>,
        response_code: Option<i32>,
        error: Option<&str>,
    ) {
        // The transport reports latency as u64; the audit column is INTEGER
        // (i32). Saturate — a realistic HTTP latency always fits.
        let latency = latency_ms.map(|l| (l.min(i32::MAX as u64)) as i32);
        if let Err(e) = storage::executions::record(
            &self.pg,
            self.id,
            scheduled_fire_time,
            attempt as i32,
            status,
            latency,
            response_code,
            error,
        )
        .await
        {
            tracing::warn!(task = %self.id, error = %e, "failed to record execution history");
        }
    }

    /// Best-effort dead-letter insert on exhaustion (DESIGN §3). Logged on
    /// failure; the drop still proceeds so the task leaves the schedule.
    async fn dead_letter(
        &self,
        scheduled_fire_time: DateTime<Utc>,
        attempts: u32,
        last_error: &str,
        last_status: Option<u16>,
    ) {
        let code = last_status.map(|s| s as i32);
        if let Err(e) = storage::dead_letter::insert(
            &self.pg,
            self.id,
            scheduled_fire_time,
            attempts as i32,
            Some(last_error),
            code,
        )
        .await
        {
            tracing::warn!(task = %self.id, error = %e, "failed to write dead_letter entry");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_then_caps() {
        let cfg = SchedulerConfig {
            tick_interval: StdDuration::from_millis(10),
            batch_size: 1,
            http_timeout: StdDuration::from_secs(1),
            shutdown_timeout: StdDuration::from_secs(1),
            backoff_base: StdDuration::from_millis(100),
            backoff_max: StdDuration::from_millis(1_000),
        };
        let b1 = cfg.backoff(1);
        let b2 = cfg.backoff(2);
        let b10 = cfg.backoff(10);
        // base + [0, base/2] jitter => at least base
        assert!(b1 >= StdDuration::from_millis(100));
        // doubling
        assert!(b2 >= StdDuration::from_millis(200));
        // capped (base*2^9 huge > 1000 max), plus jitter <= +50% of cap
        assert!(b10 <= StdDuration::from_millis(1_500));
    }
}
