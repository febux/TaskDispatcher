//! Delivery transports (DESIGN §2.4).
//!
//! A `Transport` turns a due task into a side-effect on an external handler.
//! v1 ships `HttpTransport` only; the trait + registry mean new transports are
//! "one struct + `impl Transport` + one registry entry" (DESIGN §2.4).
//!
//! The scheduler holds `Arc<dyn Transport>` and dispatches by the target's
//! `transport` key. Adding gRPC/AMQP/Kafka/SMTP later does not touch the
//! scheduler.

pub mod http;

pub use http::HttpTransport;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::models::Target;

/// Per-fire metadata handed to a transport. `scheduled_fire_time` is the UTC
/// instant the task was *due* (the ZSET score we popped), not `now` — this is
/// the basis for the `X-Fire-Id` idempotency contract (DESIGN §5, Phase 3).
#[derive(Debug, Clone, Copy)]
pub struct FireMeta {
    pub task_id: Uuid,
    pub scheduled_fire_time: DateTime<Utc>,
    pub attempt: u32,
}

/// Outcome of a delivery attempt (DESIGN §2.4).
///
/// - `Delivered`: the handler accepted the fire. Re-seed `next_run`.
/// - `Retryable`: transient (5xx, timeout, connection). Retry with backoff.
/// - `Terminal`: permanent (4xx other than 408/429). Stop retrying.
#[derive(Debug)]
pub enum SendResult {
    Delivered { latency_ms: u64 },
    Retryable { error: String, status: Option<u16> },
    Terminal { error: String, status: Option<u16> },
}

impl SendResult {
    pub fn delivered_or(&self) -> bool {
        matches!(self, Self::Delivered { .. })
    }
}

/// The capability every transport implements.
///
/// `#[async_trait]` keeps the registry `Arc<dyn Transport>` object-safe and
/// `Send`-safe across spawned fire tasks.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(
        &self,
        target: &Target,
        payload: &serde_json::Value,
        meta: &FireMeta,
    ) -> SendResult;
}

/// Named transport lookup. `{"http": <HttpTransport>, ...}`.
#[derive(Clone, Default)]
pub struct Registry {
    transports: HashMap<String, Arc<dyn Transport>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a transport under a key. The key must match a target's
    /// `transport` column (DESIGN §2.4).
    pub fn register(&mut self, key: &str, transport: Arc<dyn Transport>) {
        self.transports.insert(key.to_string(), transport);
    }

    /// Look up a transport by key. Returns `None` if unknown.
    pub fn get(&self, key: &str) -> Option<&Arc<dyn Transport>> {
        self.transports.get(key)
    }

    /// Build the v1 registry with the single shipped transport (`http`).
    /// Phase 5 adds more transports here.
    pub fn v1(http_timeout: std::time::Duration) -> Self {
        let mut reg = Self::new();
        reg.register("http", Arc::new(HttpTransport::new(http_timeout)));
        reg
    }
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("keys", &self.transports.keys().collect::<Vec<_>>())
            .finish()
    }
}
