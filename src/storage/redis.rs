//! Redis connection via `redis::aio::ConnectionManager` + the derived
//! scheduling state (DESIGN §2.1).
//!
//! `ConnectionManager` is `Clone`, multiplexed, and auto-reconnects — ideal
//! for sharing across scheduler + HTTP tasks. The ZSET (`schedule`) and
//! `processing` set are *derived* from SQL: SQL is the source of truth. If
//! Redis evaporates, the scheduler rebuilds the ZSET from `task_specs` in one
//! pass at boot (DESIGN §2.1, §2.2).

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use uuid::Uuid;

pub type RedisPool = ConnectionManager;

/// ZSET of `task_id -> next_run` (scored by millisecond epoch). Popped by the
/// scheduler's atomic claim (../scheduler/lua.rs).
pub const SCHEDULE_KEY: &str = "schedule";
/// Set of task ids currently claimed/in-flight. Used for visibility and to
/// guard against double-claim across instances (DESIGN §2.2).
pub const PROCESSING_KEY: &str = "processing";

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

/// Convert a UTC instant to a ZSET score (ms since epoch as f64).
pub fn score(t: DateTime<Utc>) -> f64 {
    t.timestamp_millis() as f64
}

/// Upsert a task into the `schedule` ZSET at its `next_run` (hot-reload +
/// post-fire re-seed, DESIGN §2.3, §2.2). No-op-friendly: ZADD defaults to
/// update existing members.
pub async fn schedule_upsert(pool: &RedisPool, id: Uuid, next_run: DateTime<Utc>) -> Result<()> {
    let mut conn = pool.clone();
    redis::cmd("ZADD")
        .arg(SCHEDULE_KEY)
        .arg(score(next_run))
        .arg(id.to_string())
        .query_async::<()>(&mut conn)
        .await
        .context("ZADD schedule failed")?;
    Ok(())
}

/// Number of members in the `schedule` ZSET.
pub async fn schedule_len(pool: &RedisPool) -> Result<u64> {
    let mut conn = pool.clone();
    let n: u64 = redis::cmd("ZCARD")
        .arg(SCHEDULE_KEY)
        .query_async(&mut conn)
        .await
        .context("ZCARD schedule failed")?;
    Ok(n)
}

/// Lowest score in the `schedule` ZSET (the oldest/next-due fire), or `None`
/// if the ZSET is empty. Used to compute `schedule_lag_seconds` (Phase 4).
pub async fn schedule_oldest_score(pool: &RedisPool) -> Result<Option<f64>> {
    let mut conn = pool.clone();
    let result: Vec<(String, f64)> = redis::cmd("ZRANGE")
        .arg(SCHEDULE_KEY)
        .arg(0i64)
        .arg(0i64)
        .arg("WITHSCORES")
        .query_async(&mut conn)
        .await
        .context("ZRANGE schedule WITHSCORES failed")?;
    Ok(result.into_iter().next().map(|(_, score)| score))
}

/// Remove a task from the schedule (pause / delete / paused-on-update /
/// spent one-shot). Also clears any stale `processing` membership so a
/// re-add after pause/resume starts clean.
pub async fn schedule_remove(pool: &RedisPool, id: Uuid) -> Result<()> {
    let mut conn = pool.clone();
    let id = id.to_string();
    redis::pipe()
        .atomic()
        .cmd("ZREM")
        .arg(SCHEDULE_KEY)
        .arg(&id)
        .ignore()
        .cmd("SREM")
        .arg(PROCESSING_KEY)
        .arg(&id)
        .ignore()
        .query_async::<()>(&mut conn)
        .await
        .context("ZREM/SREM schedule failed")?;
    Ok(())
}
