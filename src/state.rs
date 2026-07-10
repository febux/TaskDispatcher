//! Application state shared across handlers.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::sync::Mutex;

use crate::metrics::Metrics;
use crate::storage::redis::RedisPool;

#[derive(Clone)]
pub struct AppState {
    pub pg: PgPool,
    pub redis: RedisPool,
    pub scheduler_health: SchedulerHealth,
    pub metrics: Metrics,
}

/// Shared scheduler health visible to the readiness probe (Phase 4).
/// `healthy` is set once the scheduler loop is running; `last_tick` is
/// updated every successful tick. Readiness treats the scheduler as degraded
/// if no tick has occurred for several tick intervals.
#[derive(Clone)]
pub struct SchedulerHealth {
    pub healthy: Arc<AtomicBool>,
    pub last_tick: Arc<Mutex<Instant>>,
    pub tick_interval: Duration,
}

impl SchedulerHealth {
    pub fn new(tick_interval: Duration) -> Self {
        Self {
            healthy: Arc::new(AtomicBool::new(false)),
            last_tick: Arc::new(Mutex::new(Instant::now())),
            tick_interval,
        }
    }

    /// Mark the scheduler as running/healthy.
    pub fn mark_healthy(&self) {
        self.healthy.store(true, Ordering::Relaxed);
    }

    /// Mark unhealthy (e.g. during shutdown).
    pub fn mark_unhealthy(&self) {
        self.healthy.store(false, Ordering::Relaxed);
    }

    /// Update the last-tick timestamp.
    pub async fn tick(&self) {
        *self.last_tick.lock().await = Instant::now();
    }

    /// True if the scheduler is healthy AND has ticked within a reasonable
    /// multiple of its configured tick interval.
    pub async fn is_ready(&self) -> bool {
        if !self.healthy.load(Ordering::Relaxed) {
            return false;
        }
        let last = *self.last_tick.lock().await;
        let elapsed = last.elapsed();
        // Allow 5 missed ticks before reporting degraded.
        elapsed <= self.tick_interval.saturating_mul(5)
    }
}
