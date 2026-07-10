//! Integration tests for the Phase 1 spec/target CRUD API.
//!
//! These exercise the real HTTP router against a live Postgres + Redis.
//! Skipped automatically when `DATABASE_URL` / `REDIS_URL` are unset, so
//! `cargo test` still passes in environments without dependencies (the
//! Phase 0 smoke tests cover the dep-free surface).
//!
//! Run locally (with docker compose up):
//!     DATABASE_URL=postgres://postgres:postgres@localhost:5432/taskmanager \
//!     REDIS_URL=redis://localhost:6379 \
//!     cargo test --test integration -- --nocapture

use std::env;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use taskmanager::storage;
use taskmanager::{app_router, AppState};
use tower::ServiceExt;

/// Serialize all tests so they can truncate the shared DB safely.
static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct TestEnv {
    state: AppState,
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

/// Connect to Postgres + Redis, run a clean truncate, and return app state.
/// Returns `None` (skip) if either URL is missing — the test early-returns.
async fn setup() -> Option<TestEnv> {
    let pg_url = env::var("DATABASE_URL").ok()?;
    let redis_url = env::var("REDIS_URL").ok()?;

    let guard = LOCK.lock().await;
    let pg = storage::connect_pg(&pg_url).await.expect("pg connect");
    let redis = storage::connect_redis(&redis_url).await.expect("redis connect");

    // Wipe both tables; CASCADE handles the FK from task_specs.
    sqlx::query("TRUNCATE dead_letter, task_executions, task_specs, targets RESTART IDENTITY CASCADE")
        .execute(&pg)
        .await
        .expect("truncate");

    let metrics = taskmanager::Metrics::new();
    let health = taskmanager::SchedulerHealth::new(std::time::Duration::from_millis(250));
    Some(TestEnv {
        state: AppState {
            pg,
            redis,
            scheduler_health: health,
            metrics,
        },
        _guard: guard,
    })
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.expect("collect").to_bytes();
    serde_json::from_slice(&bytes).expect("valid json")
}

fn req(method: Method, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).expect("serialize")))
        .expect("request")
}

fn empty(method: Method, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

/// Create a target and return its id (string). Reused across spec tests.
async fn create_target(env: &TestEnv, name: &str) -> String {
    let resp = app_router(env.state.clone())
        .oneshot(req(
            Method::POST,
            "/v1/targets",
            json!({
                "name": name,
                "url": "https://example.test/hook",
                "secret_hmac": "topsecret",
            }),
        ))
        .await
        .expect("target create");
    assert_eq!(resp.status(), StatusCode::CREATED);
    body_json(resp).await["id"].as_str().expect("id").to_string()
}

#[tokio::test]
async fn target_crud_roundtrip() {
    let Some(env) = setup().await else {
        return;
    };

    // create
    let id = create_target(&env, "hook-a").await;

    // get
    let got = app_router(env.state.clone())
        .oneshot(empty(Method::GET, &format!("/v1/targets/{id}")))
        .await
        .expect("get");
    assert_eq!(got.status(), StatusCode::OK);
    let body = body_json(got).await;
    assert_eq!(body["name"], "hook-a");
    assert_eq!(body["transport"], "http");
    // secret_hmac is never serialized.
    assert!(body.get("secret_hmac").is_none() || body["secret_hmac"].is_null());

    // list
    let listed = app_router(env.state.clone())
        .oneshot(empty(Method::GET, "/v1/targets"))
        .await
        .expect("list");
    assert_eq!(listed.status(), StatusCode::OK);
    assert_eq!(body_json(listed).await.as_array().unwrap().len(), 1);

    // delete
    let del = app_router(env.state.clone())
        .oneshot(empty(Method::DELETE, &format!("/v1/targets/{id}")))
        .await
        .expect("delete");
    assert_eq!(del.status(), StatusCode::NO_CONTENT);

    // get again -> 404
    let miss = app_router(env.state.clone())
        .oneshot(empty(Method::GET, &format!("/v1/targets/{id}")))
        .await
        .expect("get missing");
    assert_eq!(miss.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn target_rejects_bad_inputs() {
    let Some(env) = setup().await else {
        return;
    };

    // non-http transport
    let r = app_router(env.state.clone())
        .oneshot(req(Method::POST, "/v1/targets", json!({
            "name": "x", "transport": "amqp", "url": "https://x.test"
        })))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    // bad url scheme
    let r = app_router(env.state.clone())
        .oneshot(req(Method::POST, "/v1/targets", json!({
            "name": "x", "url": "ftp://x.test"
        })))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    // empty name
    let r = app_router(env.state.clone())
        .oneshot(req(Method::POST, "/v1/targets", json!({
            "name": "  ", "url": "https://x.test"
        })))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn spec_cron_seeds_next_run() {
    let Some(env) = setup().await else {
        return;
    };
    let target_id = create_target(&env, "cron-target").await;

    // Every minute: sec=0, min=*. 6-field cron format.
    let created = app_router(env.state.clone())
        .oneshot(req(
            Method::POST,
            "/v1/specs",
            json!({
                "name": "minutely",
                "spec_type": "cron",
                "cron_expr": "0 * * * * *",
                "target_id": target_id,
                "timezone": "UTC",
                "payload": { "hello": "world" },
            }),
        ))
        .await
        .expect("create spec");
    assert_eq!(created.status(), StatusCode::CREATED, "{:?}", created.status());

    let body = body_json(created).await;
    assert_eq!(body["spec_type"], "cron");
    assert_eq!(body["status"], "active");
    assert_eq!(body["payload"]["hello"], "world");
    assert!(body["next_run"].is_string(), "next_run must be seeded");

    // id is a valid uuid
    let id = body["id"].as_str().unwrap().to_string();

    // GET by id
    let got = app_router(env.state.clone())
        .oneshot(empty(Method::GET, &format!("/v1/specs/{id}")))
        .await
        .expect("get spec");
    assert_eq!(got.status(), StatusCode::OK);
}

#[tokio::test]
async fn spec_interval_and_once_seeds_next_run() {
    let Some(env) = setup().await else {
        return;
    };
    let target_id = create_target(&env, "i-target").await;

    let r = app_router(env.state.clone())
        .oneshot(req(Method::POST, "/v1/specs", json!({
            "name": "every-60",
            "spec_type": "interval",
            "interval_seconds": 60,
            "target_id": target_id,
        })))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);
    let b = body_json(r).await;
    assert!(b["next_run"].is_string());

    // once, future
    let future = chrono::Utc::now() + chrono::Duration::days(1);
    let r = app_router(env.state.clone())
        .oneshot(req(Method::POST, "/v1/specs", json!({
            "name": "one-shot",
            "spec_type": "once",
            "run_at": future,
            "target_id": target_id,
        })))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);
    let b = body_json(r).await;
    assert!(b["next_run"]
        .as_str()
        .unwrap()
        .starts_with(&future.format("%Y-%m-%dT%H").to_string()));

    // once, past -> next_run is null (no future occurrence)
    let past = chrono::Utc::now() - chrono::Duration::days(1);
    let r = app_router(env.state.clone())
        .oneshot(req(Method::POST, "/v1/specs", json!({
            "name": "spent",
            "spec_type": "once",
            "run_at": past,
            "target_id": target_id,
        })))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);
    let b = body_json(r).await;
    assert!(b["next_run"].is_null());
}

#[tokio::test]
async fn spec_validation_errors() {
    let Some(env) = setup().await else {
        return;
    };
    let target_id = create_target(&env, "v-target").await;

    // cron missing cron_expr
    let r = app_router(env.state.clone())
        .oneshot(req(Method::POST, "/v1/specs", json!({
            "name": "bad", "spec_type": "cron", "target_id": target_id
        })))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    // invalid cron expression
    let r = app_router(env.state.clone())
        .oneshot(req(Method::POST, "/v1/specs", json!({
            "name": "bad", "spec_type": "cron", "cron_expr": "not a cron",
            "target_id": target_id
        })))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    // interval <= 0
    let r = app_router(env.state.clone())
        .oneshot(req(Method::POST, "/v1/specs", json!({
            "name": "bad", "spec_type": "interval", "interval_seconds": 0,
            "target_id": target_id
        })))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    // nonexistent target_id
    let bogus = uuid::Uuid::new_v4();
    let r = app_router(env.state.clone())
        .oneshot(req(Method::POST, "/v1/specs", json!({
            "name": "bad", "spec_type": "interval", "interval_seconds": 30,
            "target_id": bogus
        })))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    // invalid timezone
    let r = app_router(env.state.clone())
        .oneshot(req(Method::POST, "/v1/specs", json!({
            "name": "bad", "spec_type": "interval", "interval_seconds": 30,
            "target_id": target_id, "timezone": "Mars/Olympus"
        })))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn pause_and_resume_toggle_next_run() {
    let Some(env) = setup().await else {
        return;
    };
    let target_id = create_target(&env, "p-target").await;

    let created = body_json(
        app_router(env.state.clone())
            .oneshot(req(Method::POST, "/v1/specs", json!({
                "name": "p", "spec_type": "interval", "interval_seconds": 30,
                "target_id": target_id,
            })))
            .await
            .unwrap(),
    )
    .await;
    let id = created["id"].as_str().unwrap().to_string();
    let v = created["version"].as_i64().unwrap();
    assert!(created["next_run"].is_string());

    // pause -> status paused, next_run null
    let paused = body_json(
        app_router(env.state.clone())
            .oneshot(empty(
                Method::POST,
                &format!("/v1/specs/{id}/pause?version={v}"),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(paused["status"], "paused");
    assert!(paused["next_run"].is_null());
    let v2 = paused["version"].as_i64().unwrap();

    // resume -> status active, next_run re-seeded
    let resumed = body_json(
        app_router(env.state.clone())
            .oneshot(empty(
                Method::POST,
                &format!("/v1/specs/{id}/resume?version={v2}"),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(resumed["status"], "active");
    assert!(resumed["next_run"].is_string());

    // stale version on pause -> 409 conflict
    let r = app_router(env.state.clone())
        .oneshot(empty(
            Method::POST,
            &format!("/v1/specs/{id}/pause?version={v}"),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn patch_updates_and_bumps_version() {
    let Some(env) = setup().await else {
        return;
    };
    let target_id = create_target(&env, "u-target").await;

    let created = body_json(
        app_router(env.state.clone())
            .oneshot(req(Method::POST, "/v1/specs", json!({
                "name": "u", "spec_type": "interval", "interval_seconds": 30,
                "target_id": target_id,
            })))
            .await
            .unwrap(),
    )
    .await;
    let id = created["id"].as_str().unwrap().to_string();
    let v = created["version"].as_i64().unwrap();

    // rename only — cosmetic, next_run should be preserved, version bumped.
    let orig_next = created["next_run"].as_str().unwrap().to_string();
    let patched = body_json(
        app_router(env.state.clone())
            .oneshot(req(
                Method::PATCH,
                &format!("/v1/specs/{id}?version={v}"),
                json!({ "name": "u-renamed" }),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(patched["name"], "u-renamed");
    assert_eq!(patched["next_run"], orig_next);
    assert_eq!(patched["version"], v + 1);

    // stale version now -> conflict
    let r = app_router(env.state.clone())
        .oneshot(req(
            Method::PATCH,
            &format!("/v1/specs/{id}?version={v}"),
            json!({ "name": "stale" }),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT);

    // change interval (schedule) -> next_run recomputed
    let v3 = patched["version"].as_i64().unwrap();
    let r = body_json(
        app_router(env.state.clone())
            .oneshot(req(
                Method::PATCH,
                &format!("/v1/specs/{id}?version={v3}"),
                json!({ "interval_seconds": 120 }),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(r["version"], v3 + 1);
    assert!(r["next_run"].is_string());
}

#[tokio::test]
async fn delete_spec_returns_404_when_missing() {
    let Some(env) = setup().await else {
        return;
    };
    let bogus = uuid::Uuid::new_v4();
    let r = app_router(env.state.clone())
        .oneshot(empty(Method::DELETE, &format!("/v1/specs/{bogus}")))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_filters_by_status() {
    let Some(env) = setup().await else {
        return;
    };
    let target_id = create_target(&env, "l-target").await;

    // two active + one paused
    for i in 0..2 {
        let r = app_router(env.state.clone())
            .oneshot(req(Method::POST, "/v1/specs", json!({
                "name": format!("a{i}"), "spec_type": "interval",
                "interval_seconds": 30, "target_id": target_id,
            })))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);
    }
    let created = body_json(
        app_router(env.state.clone())
            .oneshot(req(Method::POST, "/v1/specs", json!({
                "name": "paused-one", "spec_type": "interval",
                "interval_seconds": 30, "target_id": target_id,
            })))
            .await
            .unwrap(),
    )
    .await;
    let id = created["id"].as_str().unwrap().to_string();
    let v = created["version"].as_i64().unwrap();
    let _ = app_router(env.state.clone())
        .oneshot(empty(
            Method::POST,
            &format!("/v1/specs/{id}/pause?version={v}"),
        ))
        .await
        .unwrap();

    let all = body_json(
        app_router(env.state.clone())
            .oneshot(empty(Method::GET, "/v1/specs"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(all.as_array().unwrap().len(), 3);

    let active = body_json(
        app_router(env.state.clone())
            .oneshot(empty(Method::GET, "/v1/specs?status=active"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(active.as_array().unwrap().len(), 2);
}
