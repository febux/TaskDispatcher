//! Target CRUD routes — `/targets`.
//!
//! A target is a delivery destination (transport + endpoint + auth). v1
//! ships the `http` transport only (DESIGN §2.4, §7.3). Creating a spec
//! requires a target to exist (FK), so target CRUD is part of the Phase 1
//! foundation even though the DoD names specs specifically.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde::Deserialize;
use uuid::Uuid;

use crate::error::{AppError, AppResult};
use crate::models::{ServiceHealth, Target};
use crate::routes::pagination::Pagination;
use crate::state::AppState;
use crate::storage;

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/targets",
            axum::routing::post(create).get(list),
        )
        .route(
            "/v1/targets/:id",
            axum::routing::get(get).delete(delete),
        )
        .route("/v1/targets/:id/health", axum::routing::get(health))
}

#[derive(Debug, Deserialize)]
pub struct CreateTargetRequest {
    pub name: String,
    /// Registry key (DESIGN §2.4). Defaults to `"http"`; v1 rejects others.
    #[serde(default)]
    pub transport: Option<String>,
    pub url: String,
    /// HMAC-SHA256 signing key. `None` → unsigned webhooks (DESIGN §3).
    #[serde(default)]
    pub secret_hmac: Option<String>,
    /// Extra transport headers. Defaults to `{}`.
    #[serde(default)]
    pub headers: Option<serde_json::Value>,
    /// Optional external-service healthcheck URL. When set, the separate
    /// `taskmanager-healthchecker` service polls it and the scheduler skips
    /// fires while the service is unhealthy.
    #[serde(default)]
    pub healthcheck_url: Option<String>,
    /// Seconds between healthchecker polls. Defaults to 30.
    #[serde(default)]
    pub healthcheck_interval_seconds: Option<i32>,
    /// Healthcheck request timeout in seconds. Defaults to 5.
    #[serde(default)]
    pub healthcheck_timeout_seconds: Option<i32>,
}

async fn create(
    State(state): State<AppState>,
    Json(req): Json<CreateTargetRequest>,
) -> AppResult<Response> {
    let transport = req.transport.as_deref().unwrap_or("http");
    validate_transport(transport)?;
    validate_name(&req.name)?;
    validate_url(&req.url)?;

    if req.secret_hmac.as_ref().is_some_and(|s| s.is_empty()) {
        return Err(AppError::Validation("secret_hmac must be non-empty if present".into()));
    }
    let headers = req.headers.unwrap_or_else(|| serde_json::json!({}));
    validate_json_object(&headers, "headers")?;

    let hc_url = req
        .healthcheck_url
        .as_deref()
        .filter(|s| !s.trim().is_empty());
    if let Some(u) = hc_url {
        validate_url(u)?;
    }
    let hc_interval = req.healthcheck_interval_seconds.unwrap_or(30).max(1);
    let hc_timeout = req.healthcheck_timeout_seconds.unwrap_or(5).max(1);

    let target = storage::targets::insert(
        &state.pg,
        req.name.trim(),
        transport,
        req.url.trim(),
        req.secret_hmac.as_deref().map(str::trim),
        &headers,
        hc_url.map(str::trim),
        hc_interval,
        hc_timeout,
    )
    .await?;

    Ok((StatusCode::CREATED, Json(target)).into_response())
}

async fn list(
    State(state): State<AppState>,
    Query(page): Query<Pagination>,
) -> AppResult<Json<Vec<Target>>> {
    let (limit, offset) = page.bounds()?;
    let targets = storage::targets::list(&state.pg, limit, offset).await?;
    Ok(Json(targets))
}

async fn get(State(state): State<AppState>, Path(id): Path<Uuid>) -> AppResult<Json<Target>> {
    let target = storage::targets::get(&state.pg, id).await?;
    Ok(Json(target))
}

async fn health(State(state): State<AppState>, Path(id): Path<Uuid>) -> AppResult<Json<ServiceHealth>> {
    let _ = storage::targets::get(&state.pg, id).await?; // 404 if target missing
    let row = storage::health::get(&state.pg, id).await?;
    Ok(Json(row))
}

async fn delete(State(state): State<AppState>, Path(id): Path<Uuid>) -> AppResult<StatusCode> {
    if !storage::targets::delete(&state.pg, id).await? {
        return Err(AppError::NotFound(format!("target {id} not found")));
    }
    Ok(StatusCode::NO_CONTENT)
}

// --- validation helpers (shared across routes) ---

pub(super) fn validate_name(name: &str) -> AppResult<()> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation("name must not be empty".into()));
    }
    if trimmed.len() > 255 {
        return Err(AppError::Validation("name must be at most 255 characters".into()));
    }
    Ok(())
}

pub(super) fn validate_transport(transport: &str) -> AppResult<()> {
    // DESIGN §7.3 / §2.4: v1 ships the 'http' transport only.
    if transport != "http" {
        return Err(AppError::Validation(format!(
            "unsupported transport {transport:?}; only 'http' is available in v1"
        )));
    }
    Ok(())
}

pub(super) fn validate_url(url: &str) -> AppResult<()> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation("url must not be empty".into()));
    }
    let parsed = url::Url::parse(trimmed).map_err(|e| {
        AppError::Validation(format!("invalid url: {e}"))
    })?;
    match parsed.scheme() {
        "http" | "https" => Ok(()),
        other => Err(AppError::Validation(format!(
            "url scheme must be http or https, got {other:?}"
        ))),
    }
}

pub(super) fn validate_json_object(v: &serde_json::Value, field: &str) -> AppResult<()> {
    if !v.is_object() {
        return Err(AppError::Validation(format!(
            "{field} must be a JSON object"
        )));
    }
    Ok(())
}
