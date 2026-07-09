//! Storage adapters.
//!
//! - `postgres` (sqlx): source of truth (`targets`, `task_specs`,
//!   `task_executions`, `dead_letter` in later phases).
//! - `redis`: derived scheduling state (ZSET, processing set,
//!   rate-limit counters). See ../DESIGN.md §2.1.
//! - `specs` / `targets`: typed repositories over compile-time-checked
//!   sqlx queries (DESIGN §6).

pub mod postgres;
pub mod redis;
pub mod specs;
pub mod targets;

pub use postgres::connect as connect_pg;
pub use redis::connect as connect_redis;
