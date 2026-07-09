# TaskManagerEngine — Design Analysis & Reimagining (Rust)

> Status: active development. Phase 0 (skeleton) and Phase 1 (spec CRUD + persistence) are
> complete. See `docs/PLAN.md` for the current roadmap and status.
>
> This doc frames the architecture, cuts scope, and names the decisions to lock
> before Phase 0.
>
> Language choice: Rust. Learning project — bounded scope (I/O-bound
> dispatcher), clean crate mapping (tokio/axum/sqlx/redis/reqwest/serde),
> and the `Transport` trait is exactly where Rust's type system pays off.

---

## 0. What this actually is (one line)

**A reliable cron-to-notification dispatcher.** You own *scheduling* and
*reliable delivery*; an external service owns *execution*. That clean
boundary is the entire value proposition — protect it.

It is **not** Airflow/Temporal/Celery. It does not run task bodies. If you
catch yourself adding "run this function", you have left scope.

---

## 1. The idea, restated & sharpened

Original asks:
1. Receive task specs via API/gRPC.
2. Loop, check cron time, fire when due.
3. Hot-reload tasks without restarting the service.
4. Redis + a SQL relational DB.
5. NOT the executor — a **notification sender** to an external handler.
6. Several transports for notifications.

All six are coherent and buildable. The risk is not feasibility, it is
**scope creep into orchestration** (see §6). Everything below assumes the
scheduler/deliverer boundary holds.

---

## 2. Core architecture (minimal viable)

```
                ┌──────────────────────────────────────────────┐
   spec write → │  axum HTTP API (tonic gRPC later)            │
   (CRUD)       │  - validate spec  - mutate SQL (tx)          │
                │  - recompute next_run  - update Redis ZSET   │
                └───────────────┬──────────────────────────────┘
                                │ (SQL = source of truth, ZSET = derived)
                                ▼
   ┌────────────────────────────────────────────────────────┐
   │  Scheduler task (tokio task, same binary)              │
   │  loop {                                                │
   │    let due = ZRANGEBYSCORE schedule 0 now LIMIT 100    │
   │    for id in due: atomic claim (Lua ZREM+SADD)         │
   │      → transport.send(target, payload).await           │
   │      → on Ok:  next_run = cron.next(now); ZADD back     │
   │      → on Err: retry w/ backoff OR → DLQ               │
   │    sleep(tick)                                         │
   │  }                                                     │
   └────────────────────────────────────────────────────────┘
                                │
                                ▼
   External handler  ←── HTTP / gRPC / AMQP / Kafka / SMTP ────  (transports)
```

### 2.1 Redis + SQL division of labor

| Store | Role | What lives there |
|-------|------|------------------|
| **SQL** | Source of truth, audit | `task_specs`, `targets` (+ encrypted transport secrets), `task_executions` (history), `dead_letter` |
| **Redis** | Hot scheduling state | `schedule` ZSET (id → next_run score), `processing` set (in-flight), per-target rate-limit counters, optional pub/sub for live invalidation, optional distributed lock |

Why both: SQL gives you durability, queries, and audit ("what fired last
Tuesday and why did it fail"). Redis gives you a sorted, atomic, O(log N)
scheduling wheel that scales to millions of tasks without scanning them all
every tick. The ZSET is **derived** from SQL — if Redis evaporates, you
rebuild it from SQL in one pass. Never the other way around.

### 2.2 The scheduling loop — done right, not naively

**Naive (don't):** every tick scan all tasks, compare `now >= next_run`.
O(N) per tick; with 100k tasks you re-scan constantly and burn CPU.

**Proven (do):** one ZSET entry per *active* task, scored by `next_run`.
- Pop due tasks with an **atomic** Lua script (`ZREM` from `schedule` +
  `SADD` to `processing`), so multiple instances never double-fire the same
  tick (this is your idempotency primitive — §5).
- On successful delivery: `next_run = cron.next(now)`, `ZADD schedule`.
- On failure: retry policy → re-add at `now + backoff`; exhausted → DLQ.

Cost per tick is O(due tasks), not O(all tasks). Memory is bounded by the
active task count (one ZSET entry each). This is the Sidekiq/Bull/rqueue
pattern for a reason.

### 2.3 Hot-reload without restart (the clean way)

The service never restarts because the scheduler reads due-tasks from the
ZSET, and the ZSET always reflects current specs:

```
API mutates spec → BEGIN SQL tx
                  → write task_specs
                  → recompute next_run from cron
                  → Lua: update ZSET score (or ZREM if disabled/paused)
                  → COMMIT
                  → (optional) PUBLISH spec:changed for cache invalidation
```

No reload signal, no restart, no "apply changes" button. Mutating the spec
*is* the reload. Disabling a task removes it from the ZSET; re-enabling
re-adds it. This is the design — don't build a separate "reload" path.

### 2.4 Transports — a Rust trait from day one

This is the one capability the user explicitly wants to extend later
(several transports). Put a Rust trait in front of it immediately;
everything above depends on the trait, never the concrete impl.

```rust
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, target: &Target, payload: &Payload, meta: &ExecMeta) -> SendResult;
}

pub enum SendResult {
    Delivered { latency_ms: u64 },
    Retryable { err: String, status: Option<u16> },
    Terminal  { err: String, status: Option<u16> },
}
```

Registry: `HashMap<&str, Arc<dyn Transport>>` = `{"http": ..., "grpc": ..., "amqp": ...}`.
Adding a transport = one struct + one `impl Transport` + one registry entry.
No factory-of-factories, no trait-object gymnastics beyond the `Arc<dyn>`.

**v1 ships `HttpTransport` only.** gRPC/AMQP/Kafka/SMTP come when asked.

> Rust learning note: the `Transport` trait is the textbook payoff. A trait
> with multiple impls is exactly where Rust's type system feels good instead
> of punitive. The one friction point: the scheduler will hold
> `Arc<dyn Transport>` (shared across spawned tasks), and you'll fight the
> borrow checker once on the spawn boundary. Learnable, expected.

---

## 3. Recommended feature set (ranked, with YAGNI flags)

### v1 — reliability non-negotiables
- **Retry with exponential backoff + jitter**, per-task max attempts. Without
  this, one flaky downstream turns into silent dropped fires.
- **Dead-letter on exhaustion** (SQL table). The audit trail of "we gave up".
- **Idempotency** via `(task_id, scheduled_fire_time)` dedup key. Two
  scheduler instances racing on the same tick must not double-fire. The Lua
  atomic-claim in §2.2 is the mechanism; this key is the contract.
- **Timezone-aware scheduling.** Store tz per task, compute in UTC, fire in
  target tz. Classic cron pain; do it right the first time or pay forever
  (DST gaps, tz DB updates).
- **Graceful shutdown:** stop scheduling, drain in-flight, then exit.
  (tokio handles this via a `CancellationToken` + `JoinSet`.)
- **Structured tracing + Prometheus metrics:** `tracing` + `prometheus` or
  `metrics` crate. Fires/s, failures/s, schedule-lag (now − oldest due
  score), queue depth, per-transport latency.

### v1 — high value/cost
- **Health & readiness endpoints** (k8s/ops basic).
- **Per-target rate limit + circuit breaker.** A buggy cron that fires every
  second must not DoS your downstream. Cheap to add, painful to retrofit.
- **Multiple spec types:** cron expression, fixed interval, one-shot
  datetime. `cron` crate handles expression eval; don't pull a whole engine.
- **Execution history:** status, attempts, latency, response code, last
  error. The "why did it fail on Tuesday" table. Drives SLAs and debugging.
- **Catch-up policy on missed fires:** `run_missed | skip | run_once`.
  Configurable per task; decides behavior after downtime.
- **Pause/resume a task** without delete (sets a flag, ZREM from schedule).
- **Webhook HMAC signature** for HTTP transport auth. Security boundary,
  cheap, do it now.

### Defer until asked (explicit YAGNI)
- Task dependencies / DAG. **Huge** complexity. If you need this, you want
  Temporal/Airflow, not this engine. Recommend as a **stated non-goal**.
- Priority queues.
- Multi-tenancy / RBAC.
- Web UI (CLI first; UI only when the API is stable).
- Dynamic plugin loading of transports (static registry is fine).
- **Result callbacks** (handler reports execution result back). This turns
  the engine into a workflow orchestrator — scope creep. Defer hard.
- HA / leader election / multi-replica. Single instance + Redis is honest
  for v1. The atomic-claim loop is already race-safe for *firing*; HA only
  matters for *availability*, and you don't have >1 replica yet.

---

## 4. What to skip (lazy-senior notes)

- **No workflow/DAG engine.** Different product. The handler orchestrates;
  the engine fires.
- **No scripting/eval in specs.** Specs are *data* (cron + target + transport
  + payload template), never code. A payload template with minimal
  `{{ scheduled_time }}` substitution is the most you should entertain.
- **No "apply changes" button.** Mutation = reload (§2.3). A second path is a
  second bug.
- **No factory-of-factories for transports.** Trait + `Arc<dyn Transport>` registry.
- **No separate API binary vs scheduler binary in v1.** One tokio runtime,
  two tasks. Split only when you need to scale them independently (you
  won't, for a long time).
- **No GUI before a CLI.**

---

## 5. Idempotency & exactly-once-ish delivery

True exactly-once across networks is impossible; aim for **at-least-once +
idempotent handlers**, which is the industry-standard contract.

- Atomic Lua claim guarantees **at-most-once per tick** within the cluster.
- Retries mean a downstream may see the same logical fire twice (network
  blip after the handler acted). Mitigate by:
  - Sending a stable `X-Fire-Id: <task_id>:<scheduled_fire_time>` header.
  - Documenting that handlers MUST dedup on it.
- This is the honest, correct contract. Don't pretend to exactly-once.

---

## 6. Tech stack — Rust (opinionated, minimal)

| Concern | Crate | Why |
|---------|-------|-----|
| Async runtime | **tokio** | The async runtime. Multi-thread scheduler, `JoinSet` for in-flight fires, `CancellationToken` for graceful shutdown |
| HTTP API | **axum** | Tokio team, typed routing, `State` extractor, tower middleware. OpenAPI later via `utoipa` |
| gRPC (later) | **tonic** | Tokio-native; shares the runtime with axum |
| SQL | **sqlx** (Postgres) | **Compile-time-checked queries** (`query!` macro) — DB schema drift fails the build. Better than SQLAlchemy for this; no ORM lock-in |
| Migrations | **sqlx-cli** (`sqlx migrate add`) | One tool, versioned `.sql` files, runs at boot |
| Redis | **redis** (async, tokio-rs) | ZSET ops, Lua `evalsha`, multiplexed connection (`MultiplexedConnection`) |
| HTTP client | **reqwest** | Async, timeouts, body, HMAC signing done here |
| Serde | **serde** + **serde_json** | Spec/payload/config (de)serialization. The gold standard |
| Validation | **validator** (or hand-rolled) | Cron sanity, URL checks; don't over-reach |
| Cron eval | **cron** crate | Expression → `next_after(now)`. **Soft spot** vs Python's croniter — thinner DST/edge handling. Acceptable for v1; if tz pain hits, this is where |
| Tracing | **tracing** + `tracing-subscriber` (JSON) | Structured, span-aware, integrates with metrics |
| Metrics | **prometheus** (or `metrics` facade) | `/metrics` endpoint for scrape |
| Errors | **thiserror** (library) + **anyhow** (binary/app) | Idiomatic split |
| Config | **figment** or **config** crate + env | Env-overridable; Postgres/Redis URLs, listen addr |
| Server | `axum::serve` on a `tokio` runtime | One binary: axum routes + one spawned scheduler task |

**Deployment shape (v1):** one static binary = axum server + one tokio task
running the scheduler loop, both talking to the same Postgres + Redis.
Containerize (distroless or `alpine`, multi-stage build) — that's it.

> No entry-point stub to delete later: `cargo init` produces a minimal
> `main.rs` and you grow it. Don't add a CLI subcommand parser (clap) until
> a CLI actually exists.

---

## 7. Architecture-defining forks — decide before Phase 0

These restructure the whole design. Pick deliberately:

1. **HA now, or single-instance for v1?**
   - Single-instance: no leader election, simpler. Recommend.
   - HA: needs Redis lock/lease leader election from day one. ~2x complexity.

2. **gRPC API mandatory in v1, or HTTP-first?**
   - HTTP-first: ship axum now, add tonic service later behind the same
     core. Recommend unless external clients already speak gRPC.
   - Both day one: doubles the API surface (proto + HTTP + shared core).

3. **Which transports in v1?**
   - HTTP webhook only (recommend). Others are one struct + impl when asked.

4. **Result callbacks in scope?**
   - No (recommend): engine fires and forgets; handler owns execution.
   - Yes: engine becomes a workflow orchestrator. Different product, ~3x scope.

5. **Payload templating?**
   - None (static JSON) vs minimal substitution (`{{ scheduled_time }}`).
   - Minimal substitution is cheap and commonly wanted; recommend.

---

## 8. Proposed phased roadmap (sketch)

| Phase | Deliverable | Definition of Done |
|-------|-------------|--------------------|
| 0 | Skeleton: `cargo init`, deps (tokio/axum/sqlx/redis/reqwest/serde/tracing), config (env), sqlx migrations setup, Redis ping, health route, smoke tests | `cargo build` + `cargo test` + `cargo clippy -- -D warnings` green |
| 1 | Spec CRUD API (axum) + sqlx persistence + serde validation | Create/pause/resume/delete specs via curl; persisted |
| 2 | Scheduler task: ZSET + atomic Lua claim + cron eval + HttpTransport + next_run recompute | A cron spec fires a webhook at the right time |
| 3 | Reliability: retry+backoff, DLQ, idempotency header (`X-Fire-Id`), execution history | Failed webhook retried then dead-lettered; history queryable |
| 4 | Ops: graceful shutdown (`CancellationToken`+`JoinSet`), readiness, metrics, HMAC signing | `/metrics` scraped; clean SIGTERM drain |
| 5 | (on demand) tonic gRPC API, more transports, catch-up policies, per-target rate limit | Per the fork decisions above |

---

## 9. TL;DR

- It's a **cron-to-notification dispatcher**, not an executor, not Airflow.
- **SQL = truth, Redis ZSET = scheduling wheel.** Two stores, clear roles.
- **ZSET + atomic Lua claim** is the scheduling pattern; idempotency comes
  free from it. Don't poll all tasks every tick.
- **Mutation = reload.** No restart, no apply button.
- **One `Transport` trait, `HttpTransport` now, others later.**
- **Rust stack:** tokio + axum + sqlx + redis + reqwest + serde + tracing.
  `sqlx` compile-time query checks are a real win over SQLAlchemy here.
  `cron` crate is the one watch-item vs croniter.
- Ship retry/backoff, DLQ, idempotency, tz-awareness, history in v1.
- **Explicitly skip:** DAG/workflow engine, result callbacks, scripting in
  specs, HA (until >1 replica), GUI before CLI.
- Lock the 5 forks in §7 before writing Phase 0.

**Next step:** answer the forks in §7, then I scaffold Phase 0 in
`/home/san/PetProjects/TaskManagerEngine/` (green `cargo test` + `clippy -D warnings`).
