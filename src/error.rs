//! Error types.
//!
//! `AppError` is the single error enum returned from all handlers and
//! storage functions. Its `IntoResponse` impl maps each variant to an
//! HTTP status + JSON body, so handlers stay focused on business logic.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    /// The referenced resource does not exist. → 404
    #[error("{0}")]
    NotFound(String),

    /// The request body failed validation or is semantically invalid. → 400
    #[error("{0}")]
    Validation(String),

    /// The request conflicts with current state (e.g. unique violation,
    /// FK referenced elsewhere). → 409
    #[error("{0}")]
    Conflict(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl AppError {
    /// Wrap any `anyhow::Error` into an internal error without losing context.
    pub fn internal<E>(e: E) -> Self
    where
        E: Into<anyhow::Error>,
    {
        Self::Internal(e.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        use AppError as E;

        let (status, message) = match &self {
            E::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            E::Validation(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            E::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            // A missing row surfaces as a sqlx::Error::RowNotFound — promote it
            // to a 404 so GET-by-id reads as "not found", not a 500.
            E::Database(sqlx::Error::RowNotFound) => {
                (StatusCode::NOT_FOUND, "resource not found".to_string())
            }
            // Postgres unique_violation (23505) and foreign_key_violation (23503)
            // are user-correctable, not 500s.
            E::Database(err) if is_postgres_constraint(err) => {
                (StatusCode::CONFLICT, err.to_string())
            }
            E::Database(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
            E::Redis(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
            E::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };

        // Log at warn for client errors (4xx) so they don't look like bugs;
        // error for 5xx which are genuinely our problem.
        if status.is_server_error() {
            tracing::error!(error = %self, "request failed");
        } else {
            tracing::warn!(error = %self, "client error");
        }

        (
            status,
            Json(json!({ "error": { "kind": status.as_u16(), "message": message } })),
        )
            .into_response()
    }
}

/// True if the sqlx error wraps a Postgres unique- or foreign-key constraint
/// violation (SQLSTATE 23505 / 23503). Used to map these to 409 instead of 503.
fn is_postgres_constraint(err: &sqlx::Error) -> bool {
    if let Some(db) = err.as_database_error()
        && let Some(code) = db.code()
    {
        return code == "23505" || code == "23503";
    }
    false
}

pub type AppResult<T> = Result<T, AppError>;
