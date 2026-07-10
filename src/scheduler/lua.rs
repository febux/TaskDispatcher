//! Atomic scheduling primitives for the scheduler loop (DESIGN §2.2).
//!
//! The claim is a single Lua script so that `ZRANGEBYSCORE` + `ZREM` +
//! `SADD` run atomically under Redis' single-threaded model: two instances
//! can never pop the same due task in the same tick. This is the
//! idempotency primitive (DESIGN §5). `redis::Script` handles `SCRIPT LOAD`
//! + `EVALSHA` with an automatic `EVAL` fallback.

use redis::Script;

use crate::storage::redis::{PROCESSING_KEY, SCHEDULE_KEY};

/// `KEYS[1] = schedule`, `KEYS[2] = processing`,
/// `ARGV[1] = now_ms`, `ARGV[2] = batch_size`.
///
/// Pops up to `batch_size` members scored `<= now` from the schedule ZSET and
/// parks them in the processing set, all under one atomic Lua execution.
pub const CLAIM_SRC: &str = r#"
    local due = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, ARGV[2])
    for _, id in ipairs(due) do
        redis.call('ZREM', KEYS[1], id)
        redis.call('SADD', KEYS[2], id)
    end
    return due
"#;

/// Run the claim script against a connection, returning the claimed task ids.
/// `now_ms` is the inclusive upper score bound; `batch` caps the count.
pub async fn claim_due(
    conn: &mut redis::aio::ConnectionManager,
    now_ms: i64,
    batch: u32,
) -> anyhow::Result<Vec<String>> {
    let script = Script::new(CLAIM_SRC);
    let claimed: Vec<String> = script
        .key(SCHEDULE_KEY)
        .key(PROCESSING_KEY)
        .arg(now_ms)
        .arg(batch)
        .invoke_async(conn)
        .await
        .map_err(|e| anyhow::anyhow!("claim script failed: {e}"))?;
    Ok(claimed)
}
