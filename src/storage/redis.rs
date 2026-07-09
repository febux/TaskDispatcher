//! Redis connection via `redis::aio::ConnectionManager`.
//!
//! `ConnectionManager` is `Clone`, multiplexed, and auto-reconnects —
//! ideal for sharing across scheduler + HTTP tasks. Phase 0 only
//! establishes it and pings; the ZSET/Lua pipeline lands in Phase 2.

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;

pub type RedisPool = ConnectionManager;

/// Build a reconnecting `ConnectionManager` from a `redis://` URL.
pub async fn connect(url: &str) -> Result<RedisPool> {
    let client = redis::Client::open(url).context("invalid REDIS_URL")?;
    let manager = ConnectionManager::new(client)
        .await
        .context("failed to connect to Redis")?;
    Ok(manager)
}

/// Verify Redis is reachable with a `PING`.
pub async fn ping(pool: &RedisPool) -> Result<()> {
    // `ConnectionManager` multiplexes; clone is cheap (Arc-backed).
    let mut conn = pool.clone();
    let pong: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .context("redis ping failed")?;
    debug_assert_eq!(pong, "PONG");
    Ok(())
}

// Lua evalsha helpers (atomic ZREM+SADD claim for the scheduler) land in
    // Phase 2 alongside the scheduling loop. See ../DESIGN.md §2.2.
