//! Smoke tests.
//!
//! Verify the HTTP surface boots and responds without requiring Postgres
//! or Redis — useful as a CI gate. Integration tests that exercise real
//! storage live in `tests/integration.rs` (added in Phase 1) and require
//! a live DB + Redis via `DATABASE_URL`/`REDIS_URL`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use taskmanager::routes::health::liveness_router;
use tower::ServiceExt;

#[tokio::test]
async fn healthz_returns_ok_without_deps() {
    let app = liveness_router();

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router responded");

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("body collected")
        .to_bytes();
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("healthz body is valid JSON");
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn unknown_route_returns_404() {
    let app = liveness_router();

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router responded");

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
