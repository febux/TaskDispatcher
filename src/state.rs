//! Application state shared across handlers.

use sqlx::PgPool;

use crate::storage::redis::RedisPool;

#[derive(Clone)]
pub struct AppState {
    pub pg: PgPool,
    pub redis: RedisPool,
}
