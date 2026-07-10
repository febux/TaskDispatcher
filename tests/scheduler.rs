//! Integration tests for the Phase 2 scheduler (DESIGN §2.2, §2.3).
//!
//! These exercise the real scheduler loop + transport against a live
//! Postgres + Redis and a tiny in-process HTTP webhook mock. Skipped
//! automatically when `DATABASE_URL` / `REDIS_URL` are unset.
//!
//! Run locally (with docker compose up):
//!     DATABASE_URL=postgres://postgres:postgres@localhost:5432/taskmanager \
//!     REDIS_URL=redis://localhost:6379 \
//!     cargo test --test scheduler -- --nocapture

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;
use taskmanager::scheduler::lua::claim_due;
use taskmanager::storage::redis as rstore;
use taskmanager::storage::{self, specs};
use taskmanager::{AppState, Scheduler};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

/// Serialize all tests so they can flush the shared DB/Redis safely.
static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

static LOG_INIT: std::sync::Once = std::sync::Once::new();

fn init_logs() {
    LOG_INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_target(true)
            .try_init();
    });
}

struct TestEnv {
    state: AppState,
    cfg: taskmanager::SchedulerConfig,
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

async fn setup() -> Option<TestEnv> {
    let pg_url = std::env::var("DATABASE_URL").ok()?;
    let redis_url = std::env::var("REDIS_URL").ok()?;
    let guard = LOCK.lock().await;
    init_logs();
    let pg = storage::connect_pg(&pg_url).await.expect("pg connect");
    let redis = storage::connect_redis(&redis_url).await.expect("redis connect");

    sqlx::query("TRUNCATE dead_letter, task_executions, task_specs, targets RESTART IDENTITY CASCADE")
        .execute(&pg)
        .await
        .expect("truncate");
    // Flush the derived scheduling keys so each test starts clean.
    flush_schedule(&redis).await;

    // Fast, small config for deterministic, quick tests.
    let cfg = taskmanager::SchedulerConfig {
        tick_interval: Duration::from_millis(50),
        batch_size: 32,
        http_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(5),
        backoff_base: Duration::from_millis(80),
        backoff_max: Duration::from_millis(400),
    };
    let metrics = taskmanager::Metrics::new();
    let health = taskmanager::SchedulerHealth::new(cfg.tick_interval);
    Some(TestEnv {
        state: AppState {
            pg,
            redis,
            scheduler_health: health,
            metrics,
        },
        cfg,
        _guard: guard,
    })
}

async fn flush_schedule(redis: &rstore::RedisPool) {
    let mut c = redis.clone();
    let _ = redis_c("DEL", &[rstore::SCHEDULE_KEY], &mut c).await;
    let _ = redis_c("DEL", &[rstore::PROCESSING_KEY], &mut c).await;
}

async fn redis_c(cmd: &str, args: &[&str], conn: &mut rstore::RedisPool) -> anyhow::Result<()> {
    let mut req = redis::cmd(cmd);
    for a in args {
        req.arg(a);
    }
    req.query_async::<()>(conn).await?;
    Ok(())
}

/// Minimal webhook mock: counts every request and replies with `status`,
/// forcing `Connection: close` so each fire is a distinct TCP connection
/// (makes hit-counting reliable). Returns the URL to target.
///
/// Runs on a dedicated OS thread with blocking I/O so it never competes with
/// the tokio runtime for poll budget — reqwest's internal hyper tasks and the
/// mock can therefore always make progress concurrently.
fn mock_webhook(hits: Arc<AtomicUsize>, status: u16) -> String {
    mock_webhook_inner(hits, status, None, None)
}

/// Like `mock_webhook` but also captures every `X-Fire-Id` header value seen
/// (Phase 3 idempotency contract, DESIGN §5). The captured values are pushed
/// into the shared vec in arrival order, one per fire.
fn mock_webhook_capturing(
    hits: Arc<AtomicUsize>,
    status: u16,
    fire_ids: Arc<std::sync::Mutex<Vec<String>>>,
) -> String {
    mock_webhook_inner(hits, status, Some(fire_ids), None)
}

/// Like `mock_webhook` but also captures the full request body into a shared
/// buffer for each request (Phase 4 HMAC / templating tests).
fn mock_webhook_capturing_body(
    hits: Arc<AtomicUsize>,
    status: u16,
    bodies: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
) -> String {
    mock_webhook_inner(hits, status, None, Some(bodies))
}

fn mock_webhook_inner(
    hits: Arc<AtomicUsize>,
    status: u16,
    fire_ids: Option<Arc<std::sync::Mutex<Vec<String>>>>,
    bodies: Option<Arc<std::sync::Mutex<Vec<Vec<u8>>>>>,
) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut sock) = stream else { continue };
            let hits = hits.clone();
            // Read everything the client sends (bounded by a read timeout),
            // so closing the socket after responding never RSTs an unread
            // body. The timeout lets us return from a keep-alive connection
            // that has nothing left to send.
            let _ = sock.set_read_timeout(Some(Duration::from_millis(300)));
            let mut got = Vec::new();
            loop {
                let mut buf = [0u8; 4096];
                match std::io::Read::read(&mut sock, &mut buf) {
                    Ok(0) => break,
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut =>
                    {
                        break;
                    }
                    Err(_) => break,
                }
            }
            if !got.is_empty() {
                hits.fetch_add(1, Ordering::SeqCst);
                if let Some(ids) = &fire_ids
                    && let Some(val) = extract_header(&got, "x-fire-id")
                {
                    ids.lock().unwrap().push(val.to_string());
                }
                if let Some(b) = &bodies {
                    b.lock().unwrap().push(got.clone());
                }
            }
            let reason = if status == 200 { "OK" } else { "Internal Server Error" };
            let resp = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            let _ = std::io::Write::write_all(&mut sock, resp.as_bytes());
            let _ = std::io::Write::flush(&mut sock);
        }
    });
    format!("http://{addr}/hook")
}

/// Case-insensitive extraction of a header value from a raw HTTP request.
fn extract_header<'a>(req: &'a [u8], name: &str) -> Option<&'a str> {
    let txt = std::str::from_utf8(req).ok()?;
    for line in txt.split("\r\n") {
        if let Some((k, v)) = line.split_once(':')
            && k.trim().eq_ignore_ascii_case(name)
        {
            return Some(v.trim());
        }
    }
    None
}

/// Create a target via the API and return its id.
async fn create_target(state: &AppState, name: &str, url: &str) -> uuid::Uuid {
    create_target_with_healthcheck(state, name, url, None, None, None).await
}

/// Create a target via the API, optionally with a healthcheck, and return its id.
async fn create_target_with_healthcheck(
    state: &AppState,
    name: &str,
    url: &str,
    healthcheck_url: Option<&str>,
    healthcheck_interval_seconds: Option<i32>,
    healthcheck_timeout_seconds: Option<i32>,
) -> uuid::Uuid {
    use axum::body::Body;
    use axum::http::{Method, Request};

    let mut body = json!({ "name": name, "url": url });
    if let Some(hc) = healthcheck_url {
        body["healthcheck_url"] = json!(hc);
    }
    if let Some(interval) = healthcheck_interval_seconds {
        body["healthcheck_interval_seconds"] = json!(interval);
    }
    if let Some(timeout) = healthcheck_timeout_seconds {
        body["healthcheck_timeout_seconds"] = json!(timeout);
    }

    let resp = taskmanager::app_router(state.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/targets")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::CREATED);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    uuid::Uuid::parse_str(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap()
}

/// Drive the scheduler for up to `budget`, polling `cond` every ~25ms.
async fn run_until<F>(env: &TestEnv, token: CancellationToken, budget: Duration, cond: F)
where
    F: Fn() -> bool,
{
    let sched = Scheduler::new(
        env.cfg.clone(),
        env.state.pg.clone(),
        env.state.redis.clone(),
        taskmanager::Registry::v1(env.cfg.http_timeout),
        env.state.scheduler_health.clone(),
        env.state.metrics.clone(),
    );
    let handle = tokio::spawn(sched.run(token.clone()));
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if cond() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    token.cancel();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boot_seed_fires_overdue_webhook() {
    // DoD: a spec whose next_run is due fires the webhook. This exercises the
    // boot ZSET rebuild (SQL→Redis), the atomic claim, the transport, and the
    // post-fire re-seed — the full Phase 2 path.
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let url = mock_webhook(hits.clone(), 200);
    let target_id = create_target(&env.state, "hook", &url).await;

    // Insert a spec with a PAST next_run directly into SQL (bypassing the API,
    // so Redis is NOT seeded — the scheduler's boot rebuild must pick it up).
    let row = specs::SpecRow {
        name: "overdue".into(),
        spec_type: taskmanager::models::SpecType::Interval,
        cron_expr: None,
        interval_seconds: Some(3600),
        run_at: None,
        timezone: "UTC".into(),
        target_id,
        payload: json!({ "hello": "world" }),
        status: taskmanager::models::SpecStatus::Active,
        catch_up: taskmanager::models::CatchUpPolicy::Skip,
        max_attempts: 3,
        next_run: Some(Utc::now() - chrono::Duration::seconds(60)),
    };
    let spec = specs::insert(&env.state.pg, &row).await.unwrap();
    let id = spec.id;
    run_until(&env, CancellationToken::new(), Duration::from_secs(5), || {
        hits.load(Ordering::SeqCst) >= 1
    })
    .await;

    assert!(
        hits.load(Ordering::SeqCst) >= 1,
        "webhook should have fired for the overdue spec"
    );

    // Post-fire re-seed: next_run must have moved forward into the future
    // and SQL must reflect it (source of truth).
    let after = specs::get(&env.state.pg, id).await.unwrap();
    let nr = after.next_run.expect("next_run re-seeded");
    assert!(nr > Utc::now(), "re-seeded next_run must be in the future");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hot_reload_fires_newly_created_spec() {
    // Mutation = reload (DESIGN §2.3): a spec created via the API while the
    // scheduler is already running must fire without a restart.
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let url = mock_webhook(hits.clone(), 200);
    let target_id = create_target(&env.state, "hook2", &url).await;

    let token = CancellationToken::new();
    let sched = Scheduler::new(
        env.cfg.clone(),
        env.state.pg.clone(),
        env.state.redis.clone(),
        taskmanager::Registry::v1(env.cfg.http_timeout),
        env.state.scheduler_health.clone(),
        env.state.metrics.clone(),
    );
    let handle = tokio::spawn(sched.run(token.clone()));

    // Create the spec AFTER the loop is running; reflect_schedule pushes it.
    use axum::body::Body;
    use axum::http::{Method, Request};
    let created = taskmanager::app_router(env.state.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/specs")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "name": "fast",
                        "spec_type": "interval",
                        "interval_seconds": 1,
                        "target_id": target_id,
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.status(), axum::http::StatusCode::CREATED);

    // Wait for the first fire (next_run ≈ now+1s).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    loop {
        if hits.load(Ordering::SeqCst) >= 1 || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    token.cancel();
    let _ = handle.await;

    assert!(
        hits.load(Ordering::SeqCst) >= 1,
        "hot-reloaded spec should have fired"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn atomic_claim_is_exclusive() {
    // DESIGN §2.2 / §5: the Lua claim atomically moves due tasks from
    // `schedule` to `processing`. Claiming a second time yields nothing.
    let Some(env) = setup().await else {
        return;
    };
    let id = uuid::Uuid::new_v4();
    let when = Utc::now() - chrono::Duration::seconds(5);
    rstore::schedule_upsert(&env.state.redis, id, when)
        .await
        .unwrap();

    let first = claim_due(&mut env.state.redis.clone(), Utc::now().timestamp_millis(), 32)
        .await
        .unwrap();
    let second = claim_due(&mut env.state.redis.clone(), Utc::now().timestamp_millis(), 32)
        .await
        .unwrap();

    assert_eq!(first.len(), 1, "first claim pops the due task");
    assert!(second.is_empty(), "second claim finds nothing (atomic)");

    // The member must have moved into the processing set.
    let mut conn = env.state.redis.clone();
    let in_processing: bool = redis::cmd("SISMEMBER")
        .arg(rstore::PROCESSING_KEY)
        .arg(id.to_string())
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(in_processing);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_then_exhaust_on_5xx() {
    // A permanently-failing target is retried with backoff up to max_attempts,
    // then dropped (Phase 3 adds the DLQ). With max_attempts=2 we expect
    // exactly 2 deliveries.
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let url = mock_webhook(hits.clone(), 500);
    let target_id = create_target(&env.state, "hook500", &url).await;

    let row = specs::SpecRow {
        name: "flaky".into(),
        spec_type: taskmanager::models::SpecType::Interval,
        cron_expr: None,
        interval_seconds: Some(3600),
        run_at: None,
        timezone: "UTC".into(),
        target_id,
        payload: json!({}),
        status: taskmanager::models::SpecStatus::Active,
        catch_up: taskmanager::models::CatchUpPolicy::Skip,
        max_attempts: 2,
        next_run: Some(Utc::now() - chrono::Duration::seconds(60)),
    };
    let spec = specs::insert(&env.state.pg, &row).await.unwrap();
    let id = spec.id;

    run_until(&env, CancellationToken::new(), Duration::from_secs(8), || {
        hits.load(Ordering::SeqCst) >= 2
    })
    .await;

    // Allow a brief settle window for the exhaustion to drop the task.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "should deliver exactly max_attempts times then stop"
    );

    // After exhaustion the task is removed from scheduling (and from SQL
    // scheduling state it stays Active but should no longer be claimable).
    let mut conn = env.state.redis.clone();
    let still_scheduled: Option<f64> = redis::cmd("ZSCORE")
        .arg(rstore::SCHEDULE_KEY)
        .arg(id.to_string())
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(still_scheduled.is_none(), "exhausted task leaves the ZSET");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhaustion_dead_letters_and_records_history() {
    // Phase 3 DoD: a failed webhook is retried then dead-lettered, and the
    // per-attempt history is queryable (DESIGN §3). With max_attempts=2 we
    // expect exactly 2 retryable attempts, then one dead_letter row.
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let url = mock_webhook(hits.clone(), 500);
    let target_id = create_target(&env.state, "hook-dlq", &url).await;

    let row = specs::SpecRow {
        name: "flaky-dlq".into(),
        spec_type: taskmanager::models::SpecType::Interval,
        cron_expr: None,
        interval_seconds: Some(3600),
        run_at: None,
        timezone: "UTC".into(),
        target_id,
        payload: json!({}),
        status: taskmanager::models::SpecStatus::Active,
        catch_up: taskmanager::models::CatchUpPolicy::Skip,
        max_attempts: 2,
        next_run: Some(Utc::now() - chrono::Duration::seconds(60)),
    };
    let spec = specs::insert(&env.state.pg, &row).await.unwrap();
    let id = spec.id;

    run_until(&env, CancellationToken::new(), Duration::from_secs(8), || {
        hits.load(Ordering::SeqCst) >= 2
    })
    .await;
    // Let the exhaustion + dead-letter write settle.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(hits.load(Ordering::SeqCst), 2, "exactly max_attempts deliveries");

    // Per-attempt history: two retryable rows, both 500.
    let hist =
        taskmanager::storage::executions::list_for_spec(&env.state.pg, id, 50, 0)
            .await
            .unwrap();
    assert_eq!(hist.len(), 2, "one history row per attempt");
    assert!(hist.iter().all(|h| h.status
        == taskmanager::models::ExecutionStatus::Retryable));
    assert!(hist.iter().all(|h| h.response_code == Some(500)));

    // Dead-letter: a single exhausted entry.
    let dlq = taskmanager::storage::dead_letter::list(&env.state.pg, Some(id), 50, 0)
        .await
        .unwrap();
    assert_eq!(dlq.len(), 1, "one dead_letter entry");
    let entry = &dlq[0];
    assert_eq!(entry.task_id, id);
    assert_eq!(entry.attempts, 2);
    assert_eq!(entry.last_response_code, Some(500));
    assert!(entry.last_error.as_ref().is_some_and(|e| e.contains("500")));

    // The DLQ is queryable over HTTP too.
    use axum::body::Body;
    use axum::http::{Method, Request};
    let resp = taskmanager::app_router(env.state.clone())
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/v1/dead_letter?task_id={id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let arr: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(arr.as_array().unwrap().len(), 1);
    assert_eq!(arr[0]["attempts"], 2);
    assert_eq!(arr[0]["last_response_code"], 500);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_fire_records_delivered_execution() {
    // A delivered fire records a `delivered` history row with latency,
    // queryable via GET /v1/specs/:id/executions (DESIGN §3).
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let url = mock_webhook(hits.clone(), 200);
    let target_id = create_target(&env.state, "hook-ok", &url).await;

    let row = specs::SpecRow {
        name: "ok-fire".into(),
        spec_type: taskmanager::models::SpecType::Interval,
        cron_expr: None,
        interval_seconds: Some(3600),
        run_at: None,
        timezone: "UTC".into(),
        target_id,
        payload: json!({ "k": "v" }),
        status: taskmanager::models::SpecStatus::Active,
        catch_up: taskmanager::models::CatchUpPolicy::Skip,
        max_attempts: 3,
        next_run: Some(Utc::now() - chrono::Duration::seconds(60)),
    };
    let spec = specs::insert(&env.state.pg, &row).await.unwrap();
    let id = spec.id;

    run_until(&env, CancellationToken::new(), Duration::from_secs(5), || {
        hits.load(Ordering::SeqCst) >= 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let hist =
        taskmanager::storage::executions::list_for_spec(&env.state.pg, id, 50, 0)
            .await
            .unwrap();
    assert!(!hist.is_empty(), "delivered fire records history");
    let delivered = hist
        .iter()
        .find(|h| h.status == taskmanager::models::ExecutionStatus::Delivered)
        .expect("at least one delivered row");
    assert!(delivered.latency_ms.is_some(), "latency recorded on delivery");
    assert!(delivered.error.is_none());
    assert!(delivered.response_code.is_none(), "no upstream code surfaced on 2xx");

    // History is queryable over HTTP.
    use axum::body::Body;
    use axum::http::{Method, Request};
    let resp = taskmanager::app_router(env.state.clone())
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/v1/specs/{id}/executions"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let arr: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        arr.as_array().is_some_and(|a| !a.is_empty()),
        "executions endpoint returns history"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn x_fire_id_is_stable_across_retries() {
    // DESIGN §5: X-Fire-Id must be identical across retries of the same
    // logical fire so handlers can dedup. A permanently-5xx target with
    // max_attempts=2 yields two requests carrying the same X-Fire-Id.
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let fire_ids: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(vec![]));
    let url = mock_webhook_capturing(hits.clone(), 500, fire_ids.clone());
    let target_id = create_target(&env.state, "hook-fireid", &url).await;

    let row = specs::SpecRow {
        name: "fireid".into(),
        spec_type: taskmanager::models::SpecType::Interval,
        cron_expr: None,
        interval_seconds: Some(3600),
        run_at: None,
        timezone: "UTC".into(),
        target_id,
        payload: json!({}),
        status: taskmanager::models::SpecStatus::Active,
        catch_up: taskmanager::models::CatchUpPolicy::Skip,
        max_attempts: 2,
        next_run: Some(Utc::now() - chrono::Duration::seconds(60)),
    };
    specs::insert(&env.state.pg, &row).await.unwrap();

    run_until(&env, CancellationToken::new(), Duration::from_secs(8), || {
        hits.load(Ordering::SeqCst) >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let captured = fire_ids.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        2,
        "both attempts carried an X-Fire-Id header"
    );
    assert_eq!(
        captured[0], captured[1],
        "X-Fire-Id is stable across retries (idempotency contract)"
    );
    // And it is shaped "<task_id>:<rfc3339 time>".
    assert!(captured[0].contains(':'), "X-Fire-Id is task_id:time");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_hmac_signs_body() {
    // Phase 4: when a target carries secret_hmac, every fire sends an
    // X-Signature header equal to HMAC-SHA256("sha256=" + hex(body)).
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let bodies: Arc<std::sync::Mutex<Vec<Vec<u8>>>> = Arc::new(std::sync::Mutex::new(vec![]));
    let url = mock_webhook_capturing_body(hits.clone(), 200, bodies.clone());
    let secret = "super-secret-key";
    let target_id = create_target_with_secret(&env.state, "hook-hmac", &url, secret).await;

    let row = specs::SpecRow {
        name: "hmac".into(),
        spec_type: taskmanager::models::SpecType::Interval,
        cron_expr: None,
        interval_seconds: Some(3600),
        run_at: None,
        timezone: "UTC".into(),
        target_id,
        payload: json!({ "msg": "hello" }),
        status: taskmanager::models::SpecStatus::Active,
        catch_up: taskmanager::models::CatchUpPolicy::Skip,
        max_attempts: 3,
        next_run: Some(Utc::now() - chrono::Duration::seconds(60)),
    };
    specs::insert(&env.state.pg, &row).await.unwrap();

    run_until(&env, CancellationToken::new(), Duration::from_secs(5), || {
        hits.load(Ordering::SeqCst) >= 1
    })
    .await;

    assert!(hits.load(Ordering::SeqCst) >= 1, "webhook should have fired");
    let captured = bodies.lock().unwrap();
    assert!(!captured.is_empty(), "at least one request body captured");

    // Parse the first request to extract X-Signature and the JSON body.
    let req = std::str::from_utf8(&captured[0]).expect("request is utf8");
    let sig_line = req
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("x-signature:"))
        .expect("X-Signature header present");
    let sig = sig_line.split_once(':').unwrap().1.trim();
    assert!(sig.starts_with("sha256="), "signature uses sha256= prefix");
    let sig_hex = &sig[7..];

    // Extract the JSON body (last chunk after the double CRLF).
    let body_json = req.split("\r\n\r\n").nth(1).expect("body after headers");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(body_json.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());
    assert_eq!(
        sig_hex, expected,
        "X-Signature matches HMAC-SHA256 of the body bytes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_template_renders_scheduled_time() {
    // Phase 4: {{ scheduled_time }} is replaced with the RFC3339 scheduled
    // fire time in the JSON body sent to the webhook.
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let bodies: Arc<std::sync::Mutex<Vec<Vec<u8>>>> = Arc::new(std::sync::Mutex::new(vec![]));
    let url = mock_webhook_capturing_body(hits.clone(), 200, bodies.clone());
    let target_id = create_target(&env.state, "hook-template", &url).await;

    let row = specs::SpecRow {
        name: "template".into(),
        spec_type: taskmanager::models::SpecType::Interval,
        cron_expr: None,
        interval_seconds: Some(3600),
        run_at: None,
        timezone: "UTC".into(),
        target_id,
        payload: json!({ "when": "{{ scheduled_time }}", "keep": "static" }),
        status: taskmanager::models::SpecStatus::Active,
        catch_up: taskmanager::models::CatchUpPolicy::Skip,
        max_attempts: 3,
        next_run: Some(Utc::now() - chrono::Duration::seconds(60)),
    };
    specs::insert(&env.state.pg, &row).await.unwrap();

    run_until(&env, CancellationToken::new(), Duration::from_secs(5), || {
        hits.load(Ordering::SeqCst) >= 1
    })
    .await;

    assert!(hits.load(Ordering::SeqCst) >= 1, "webhook should have fired");
    let captured = bodies.lock().unwrap();
    let req = std::str::from_utf8(&captured[0]).expect("request is utf8");
    let body_json = req.split("\r\n\r\n").nth(1).expect("body after headers");
    let body: serde_json::Value = serde_json::from_str(body_json).expect("valid JSON body");
    assert!(
        body["when"].as_str().unwrap().starts_with("2026-"),
        "{{ scheduled_time }} rendered to an RFC3339 timestamp: {}",
        body["when"]
    );
    assert_eq!(body["keep"], "static", "non-template strings stay intact");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_reflects_scheduler_health() {
    // Phase 4: /readyz includes a scheduler component and reports ok once
    // the scheduler is running and ticking.
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let url = mock_webhook(hits.clone(), 200);
    let _target_id = create_target(&env.state, "hook-ready", &url).await;

    let token = CancellationToken::new();
    let sched = Scheduler::new(
        env.cfg.clone(),
        env.state.pg.clone(),
        env.state.redis.clone(),
        taskmanager::Registry::v1(env.cfg.http_timeout),
        env.state.scheduler_health.clone(),
        env.state.metrics.clone(),
    );
    let handle = tokio::spawn(sched.run(token.clone()));

    // Poll /readyz until the scheduler reports healthy (it ticks every 50ms).
    use axum::body::Body;
    use axum::http::{Method, Request};
    let ready = std::time::Instant::now() + Duration::from_secs(3);
    let mut scheduler_ok = false;
    loop {
        let resp = taskmanager::app_router(env.state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if json.get("scheduler").and_then(|s| s.get("ok")).and_then(|v| v.as_bool()) == Some(true) {
            scheduler_ok = true;
            break;
        }
        if std::time::Instant::now() >= ready {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    token.cancel();
    let _ = handle.await;

    assert!(scheduler_ok, "/readyz reports scheduler healthy after first tick");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_exposes_fire_counters() {
    // Phase 4: /metrics returns Prometheus text with at least the registered
    // metrics families after a fire.
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let url = mock_webhook(hits.clone(), 200);
    let target_id = create_target(&env.state, "hook-metrics", &url).await;

    let row = specs::SpecRow {
        name: "metrics".into(),
        spec_type: taskmanager::models::SpecType::Interval,
        cron_expr: None,
        interval_seconds: Some(3600),
        run_at: None,
        timezone: "UTC".into(),
        target_id,
        payload: json!({}),
        status: taskmanager::models::SpecStatus::Active,
        catch_up: taskmanager::models::CatchUpPolicy::Skip,
        max_attempts: 3,
        next_run: Some(Utc::now() - chrono::Duration::seconds(60)),
    };
    specs::insert(&env.state.pg, &row).await.unwrap();

    run_until(&env, CancellationToken::new(), Duration::from_secs(5), || {
        hits.load(Ordering::SeqCst) >= 1
    })
    .await;

    use axum::body::Body;
    use axum::http::{Method, Request};
    let resp = taskmanager::app_router(env.state.clone())
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let text = std::str::from_utf8(&body).unwrap();
    assert!(
        text.contains("taskmanager_fires_total"),
        "/metrics exposes taskmanager_fires_total"
    );
    assert!(
        text.contains("taskmanager_queue_depth"),
        "/metrics exposes taskmanager_queue_depth"
    );
}

async fn create_target_with_secret(
    state: &AppState,
    name: &str,
    url: &str,
    secret: &str,
) -> uuid::Uuid {
    use axum::body::Body;
    use axum::http::{Method, Request};

    let resp = taskmanager::app_router(state.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/targets")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "name": name,
                        "url": url,
                        "secret_hmac": secret,
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::CREATED);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    uuid::Uuid::parse_str(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
            .as_str()
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduler_skips_when_target_unhealthy_and_requeues() {
    // Extension: if the target's service health is Unhealthy, the scheduler
    // must not fire the webhook and should requeue the task for the next
    // healthcheck interval.
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let url = mock_webhook(hits.clone(), 200);
    let hc_url = "http://127.0.0.1:1/never-reachable";
    let target_id = create_target_with_healthcheck(
        &env.state,
        "hook-unhealthy",
        &url,
        Some(hc_url),
        Some(2),
        Some(1),
    )
    .await;

    // Seed the service as Unhealthy before the scheduler runs.
    taskmanager::storage::health::upsert(
        &env.state.pg,
        target_id,
        taskmanager::HealthStatus::Unhealthy,
        None,
        Some("forced unhealthy for test"),
        Utc::now(),
    )
    .await
    .unwrap();

    let row = specs::SpecRow {
        name: "unhealthy-skip".into(),
        spec_type: taskmanager::models::SpecType::Interval,
        cron_expr: None,
        interval_seconds: Some(3600),
        run_at: None,
        timezone: "UTC".into(),
        target_id,
        payload: json!({ "hello": "world" }),
        status: taskmanager::models::SpecStatus::Active,
        catch_up: taskmanager::models::CatchUpPolicy::Skip,
        max_attempts: 3,
        next_run: Some(Utc::now() - chrono::Duration::seconds(60)),
    };
    let spec = specs::insert(&env.state.pg, &row).await.unwrap();

    let token = CancellationToken::new();
    let sched = Scheduler::new(
        env.cfg.clone(),
        env.state.pg.clone(),
        env.state.redis.clone(),
        taskmanager::Registry::v1(env.cfg.http_timeout),
        env.state.scheduler_health.clone(),
        env.state.metrics.clone(),
    );
    let handle = tokio::spawn(sched.run(token.clone()));

    // Wait for the scheduler to claim, skip, and requeue at least once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut saw_requeue = false;
    loop {
        let mut conn = env.state.redis.clone();
        let score: Option<f64> = redis::cmd("ZSCORE")
            .arg(rstore::SCHEDULE_KEY)
            .arg(spec.id.to_string())
            .query_async(&mut conn)
            .await
            .ok()
            .flatten();
        if let Some(s) = score {
            let ms = s as i64;
            if ms > Utc::now().timestamp_millis() {
                saw_requeue = true;
                break;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    token.cancel();
    let _ = handle.await;

    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "webhook must not fire while target is unhealthy"
    );
    assert!(saw_requeue, "spec should be requeued into the future");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduler_fires_when_target_healthy() {
    // Extension: a target with a healthcheck configured and a Healthy service
    // status should fire exactly like an unhealthchecked target.
    let Some(env) = setup().await else {
        return;
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let url = mock_webhook(hits.clone(), 200);
    let target_id = create_target_with_healthcheck(
        &env.state,
        "hook-healthy",
        &url,
        Some("http://127.0.0.1:1/ignored"),
        Some(2),
        Some(1),
    )
    .await;

    taskmanager::storage::health::upsert(
        &env.state.pg,
        target_id,
        taskmanager::HealthStatus::Healthy,
        Some(200),
        None,
        Utc::now(),
    )
    .await
    .unwrap();

    let row = specs::SpecRow {
        name: "healthy-fire".into(),
        spec_type: taskmanager::models::SpecType::Interval,
        cron_expr: None,
        interval_seconds: Some(3600),
        run_at: None,
        timezone: "UTC".into(),
        target_id,
        payload: json!({ "hello": "world" }),
        status: taskmanager::models::SpecStatus::Active,
        catch_up: taskmanager::models::CatchUpPolicy::Skip,
        max_attempts: 3,
        next_run: Some(Utc::now() - chrono::Duration::seconds(60)),
    };
    specs::insert(&env.state.pg, &row).await.unwrap();

    run_until(&env, CancellationToken::new(), Duration::from_secs(5), || {
        hits.load(Ordering::SeqCst) >= 1
    })
    .await;

    assert!(
        hits.load(Ordering::SeqCst) >= 1,
        "webhook should fire when target is healthy"
    );
}
