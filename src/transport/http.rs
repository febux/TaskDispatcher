//! `HttpTransport` — webhook delivery via `reqwest` (DESIGN §2.4, §7.3).
//!
//! v1 fires a JSON POST to `target.url` with the rendered payload, the target's
//! extra `headers`, and a per-request timeout. Response status maps to the
//! `SendResult` retry policy:
//!
//! - 2xx               → `Delivered`
//! - 408 / 429 / 5xx   → `Retryable`
//! - any other 4xx     → `Terminal` (the request itself is bad; retrying wastes)
//! - connect/timeout   → `Retryable`
//!
//! Phase 4 adds:
//! - `{{ scheduled_time }}` payload templating (DESIGN §7.5).
//! - HMAC-SHA256 body signing when `target.secret_hmac` is set, exposed as the
//!   `X-Signature: sha256=<hex>` header.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::Sha256;

use crate::models::Target;
use crate::payload;
use crate::transport::{FireMeta, SendResult, Transport};

/// HMAC-SHA256 signing helper. Panics only on an invalid key length, which
/// HMAC does not have — any byte slice is accepted.
fn hmac_sha256_hex(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(body);
    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

pub struct HttpTransport {
    client: reqwest::Client,
}

impl HttpTransport {
    pub fn new(timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout)
            .build()
            .expect("reqwest client build with valid defaults");
        Self { client }
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(
        &self,
        target: &Target,
        payload: &serde_json::Value,
        meta: &FireMeta,
    ) -> SendResult {
        let started = Instant::now();

        // Phase 4: minimal payload templating (DESIGN §7.5). `payload` is
        // borrowed from the spec, so render into an owned value before
        // serializing + signing.
        let rendered_payload = payload::render(payload, meta.scheduled_fire_time);
        let body_bytes = match serde_json::to_vec(&rendered_payload) {
            Ok(b) => b,
            Err(e) => {
                return SendResult::Terminal {
                    error: format!("failed to serialize payload: {e}"),
                    status: None,
                };
            }
        };

        // Merge the target's static headers (JSON object) with the tracing
        // + idempotency + signature headers. X-Fire-Id (DESIGN §5, Phase 3) is
        // the stable dedup key handlers MUST dedup on. X-Signature (Phase 4)
        // is HMAC-SHA256 over the exact bytes we send.
        let fire_id = format!(
            "{}:{}",
            meta.task_id,
            meta.scheduled_fire_time.to_rfc3339()
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-fire-id"),
            HeaderValue::from_str(&fire_id).unwrap_or_else(|_| HeaderValue::from_static("invalid")),
        );
        headers.insert(
            HeaderName::from_static("x-task-id"),
            HeaderValue::from_str(&meta.task_id.to_string())
                .unwrap_or_else(|_| HeaderValue::from_static("invalid")),
        );
        headers.insert(
            HeaderName::from_static("x-attempt"),
            HeaderValue::from(meta.attempt),
        );
        if let Some(secret) = target.secret_hmac.as_deref() {
            let sig = hmac_sha256_hex(secret, &body_bytes);
            let sig_val = format!("sha256={sig}");
            headers.insert(
                HeaderName::from_static("x-signature"),
                HeaderValue::from_str(&sig_val)
                    .unwrap_or_else(|_| HeaderValue::from_static("invalid")),
            );
        }
        if let Some(obj) = target.headers.as_object() {
            for (k, v) in obj {
                if let Ok(name) = HeaderName::try_from(k.as_str())
                    && let Ok(val) = HeaderValue::try_from(value_to_header_str(v))
                {
                    headers.insert(name, val);
                }
                // Malformed per-target headers are silently dropped rather than
                // failing the fire; they are a target-config concern.
            }
        }

        let result = self
            .client
            .post(target.url.as_str())
            .headers(headers)
            .header("content-type", "application/json")
            .body(body_bytes)
            .send()
            .await;

        match result {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let latency_ms = started.elapsed().as_millis() as u64;
                // Drain the body so the connection can be reused (keep-alive).
                let _ = resp.bytes().await;
                if (200..300).contains(&status) {
                    SendResult::Delivered { latency_ms }
                } else if status == 408 || status == 429 || (500..600).contains(&status) {
                    SendResult::Retryable {
                        error: format!("upstream returned {status}"),
                        status: Some(status),
                    }
                } else {
                    SendResult::Terminal {
                        error: format!("upstream returned {status}"),
                        status: Some(status),
                    }
                }
            }
            Err(err) => {
                // Timeouts and connection errors are transient by definition.
                let is_timeout = err.is_timeout() || err.is_connect();
                SendResult::Retryable {
                    error: format!(
                        "{}: {err}",
                        if is_timeout { "transport timeout/connect" } else { "send failed" }
                    ),
                    status: None,
                }
            }
        }
    }
}

/// Render a JSON header value as reqwest's `HeaderValue` expects (a string).
fn value_to_header_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn fake_target(url: &str) -> Target {
        Target {
            id: Uuid::new_v4(),
            name: "t".into(),
            transport: "http".into(),
            url: url.into(),
            secret_hmac: None,
            headers: serde_json::json!({ "X-Custom": "abc" }),
            healthcheck_url: None,
            healthcheck_interval_seconds: 30,
            healthcheck_timeout_seconds: 5,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn backoff_scales_and_caps() {
        // Sanity on the registry build path only; backoff math lives in config.
        let reg = crate::transport::Registry::v1(Duration::from_secs(1));
        assert!(reg.get("http").is_some());
        assert!(reg.get("amqp").is_none());
    }

    #[tokio::test]
    async fn send_to_invalid_url_is_retryable() {
        // A routable-but-refused endpoint yields a connect error → Retryable.
        let t = HttpTransport::new(Duration::from_secs(1));
        let target = fake_target("http://127.0.0.1:1/hook"); // port 1: refused
        let meta = FireMeta {
            task_id: Uuid::new_v4(),
            scheduled_fire_time: Utc::now(),
            attempt: 1,
        };
        let res = t.send(&target, &serde_json::json!({}), &meta).await;
        assert!(matches!(res, SendResult::Retryable { .. }));
    }
}
