//! Prometheus metrics (Phase 4, DESIGN §3).
//!
//! One central `Metrics` struct owns the `prometheus::Registry` and the
//! concrete counters/gauges/histograms. It is shared with the scheduler
//! (for fire observations and queue snapshots) and with the HTTP layer
//! (for `/metrics` scraping).
//!
//! Metrics exposed:
//! - `taskmanager_fires_total{transport, status}`     — counter per fire outcome
//! - `taskmanager_schedule_lag_seconds`                — gauge (now - oldest due score)
//! - `taskmanager_queue_depth`                         — gauge (ZSET cardinality)
//! - `taskmanager_fire_latency_seconds{transport}`    — histogram of 2xx latency

use std::sync::Arc;

use prometheus::{
    CounterVec, Encoder, Gauge, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};

/// Shared metric state. Cheaply cloneable via `Arc`.
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    registry: Registry,
    fires_total: CounterVec,
    schedule_lag: Gauge,
    queue_depth: Gauge,
    fire_latency: HistogramVec,
}

impl Metrics {
    /// Build the default registry and pre-register all Phase 4 metrics.
    pub fn new() -> Self {
        let registry = Registry::new();

        let fires_total = CounterVec::new(
            Opts::new(
                "taskmanager_fires_total",
                "Total fires by transport and outcome status",
            ),
            &["transport", "status"],
        )
        .expect("valid fires_total metric");
        registry
            .register(Box::new(fires_total.clone()))
            .expect("register fires_total");

        let schedule_lag = Gauge::new(
            "taskmanager_schedule_lag_seconds",
            "Seconds between now and the oldest scheduled fire time (0 if empty)",
        )
        .expect("valid schedule_lag metric");
        registry
            .register(Box::new(schedule_lag.clone()))
            .expect("register schedule_lag");

        let queue_depth = Gauge::new(
            "taskmanager_queue_depth",
            "Number of active specs in the scheduling ZSET",
        )
        .expect("valid queue_depth metric");
        registry
            .register(Box::new(queue_depth.clone()))
            .expect("register queue_depth");

        let fire_latency = HistogramVec::new(
            HistogramOpts::new(
                "taskmanager_fire_latency_seconds",
                "Upstream webhook latency for delivered fires",
            )
            .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["transport"],
        )
        .expect("valid fire_latency metric");
        registry
            .register(Box::new(fire_latency.clone()))
            .expect("register fire_latency");

        Self {
            inner: Arc::new(MetricsInner {
                registry,
                fires_total,
                schedule_lag,
                queue_depth,
                fire_latency,
            }),
        }
    }

    /// Record one fire outcome.
    pub fn observe_fire(
        &self,
        transport: &str,
        status: &str, // "delivered", "retryable", "terminal"
        latency_ms: Option<u64>,
    ) {
        self.inner
            .fires_total
            .with_label_values(&[transport, status])
            .inc();
        if status == "delivered" && let Some(ms) = latency_ms {
            self.inner
                .fire_latency
                .with_label_values(&[transport])
                .observe(ms as f64 / 1_000.0);
        }
    }

    /// Update the schedule-lag gauge (seconds). `0.0` when the schedule is empty.
    pub fn set_schedule_lag(&self, seconds: f64) {
        self.inner.schedule_lag.set(seconds);
    }

    /// Update the queue-depth gauge.
    pub fn set_queue_depth(&self, depth: u64) {
        self.inner.queue_depth.set(depth as f64);
    }

    /// Render all metrics in Prometheus text format.
    pub fn render(&self) -> anyhow::Result<String> {
        let encoder = TextEncoder::new();
        let metric_families = self.inner.registry.gather();
        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|e| anyhow::anyhow!("encode metrics: {e}"))?;
        String::from_utf8(buffer).map_err(|e| anyhow::anyhow!("metrics not utf8: {e}"))
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
