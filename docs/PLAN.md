# TaskManagerEngine — Project Plan

> Single source of truth for what has shipped, what is in progress, and what
> remains. See `DESIGN.md` for the full architecture, trade-offs, and
> design rationale (§-style cross-references point there).

---

## Status

| Phase | Status |
|-------|--------|
| Phase 0 | ✅ Skeleton |
| Phase 1 | ✅ Spec CRUD API + persistence |
| Phase 2 | ✅ Scheduler (ZSET + Lua claim + HttpTransport) |
| Phase 3 | ✅ Reliability (retry/backoff, DLQ, idempotency, history) |
| Phase 4 | ✅ Ops (metrics, HMAC, payload templating) |
| Phase 5 | ✅ Healthcheck extension (separate `taskmanager-healthchecker` container, per-target `healthcheck_url`, scheduler skip-when-unhealthy) |

---

## Extension: Per-Target Service Healthcheck ✅ DONE

A separate container polls each target's optional `healthcheck_url` and the
scheduler skips fires while the service is unhealthy, requeueing at the next
poll interval.

- [x] Migration `0004_service_health.sql`: `healthcheck_url`,
      `healthcheck_interval_seconds`, `healthcheck_timeout_seconds` on
      `targets`; new `service_health` table.
- [x] Models: `HealthStatus` enum + `ServiceHealth` row; healthcheck fields on
      `Target`.
- [x] Storage: `storage::health` repository (`upsert`, `get`,
      `is_healthy`, `set_unknown`, `list_targets_with_healthchecks`).
- [x] Healthchecker binary `src/bin/healthchecker.rs` polls all targets with
      `healthcheck_url` every 5 s and writes `service_health`.
- [x] Cargo binary `taskmanager-healthchecker` + `Dockerfile.healthchecker` +
      `docker-compose.yml` service.
- [x] Scheduler checks target health before firing; unhealthy → skip +
      requeue at `now + healthcheck_interval_seconds`; no `dead_letter`/
      retry exhaustion. Targets with no healthcheck are implicitly healthy.
- [x] API: healthcheck fields accepted on `POST /v1/targets`; `GET
      /v1/targets/:id/health` returns current `ServiceHealth`.
- [x] Scheduler integration tests: fires when healthy, skips + requeues when
      unhealthy.

**Definition of Done:** Unhealthy service → no fires; healthy service → fires. ✅

---

## Locked Architectural Forks (DESIGN §7)

These were decided before any implementation and govern every phase below.

1. **HA**: Single-instance for v1. No leader election. (Recommend: keep until >1 replica.)
2. **API surface**: HTTP-first (axum). gRPC (tonic) deferred to Phase 5.
3. **Transports v1**: HTTP webhook only. Trait + registry is in place; new transports = one struct + `impl Transport`.
4. **Result callbacks**: No. Engine fires and forgets; handler owns execution. Keeps scope from becoming an orchestrator.
5. **Payload templating**: Minimal (`{{ scheduled_time }}` substitution). Stored as JSONB; Phase 4 HMAC signing wires into `HttpTransport`.

---

## Phase Roadmap

### Phase 0 — Skeleton ✅ DONE

- [x] `cargo init`, dependencies (tokio, axum, sqlx, redis, reqwest, serde, tracing)
- [x] Env config (`figment`-style from env; `LOG_FORMAT`, `RUST_LOG`, `LISTEN_ADDR`)
- [x] Postgres pool + Redis `ConnectionManager`
- [x] `sqlx migrate` wired at boot (`0001_init.sql`)
- [x] Health routes: `/healthz` (liveness), `/readyz` (readiness with latency)
- [x] Structured tracing (`tracing` + `tracing-subscriber`)
- [x] Smoke tests (no DB required)
- [x] `.sqlx/` offline query cache for CI builds
- [x] Docker Compose stack (Postgres 17, Redis 7, multi-stage Dockerfile)

**Definition of Done:** `cargo build` + `cargo test` + `cargo clippy -- -D warnings` green.

---

### Phase 1 — Spec CRUD API + Persistence ✅ DONE

- [x] Migration `0002_task_specs.sql`: `targets` + `task_specs` tables (DESIGN §2.1)
- [x] Domain models: `Target`, `TaskSpec`, `SpecType`, `SpecStatus`, `CatchUpPolicy`
- [x] `cron` crate + `chrono-tz` for tz-aware `next_run` seeding (DESIGN §3)
- [x] DTOs with serde deserialization + hand-rolled validation (no extra dep)
- [x] Compile-time-checked sqlx queries (`query_as!`) for all CRUD paths
- [x] Error mapping: `NotFound` → 404, `Validation` → 400, `Conflict` → 409, constraint violations → 409
- [x] Target routes: `POST/GET/DELETE /v1/targets`, `GET /v1/targets/:id`
- [x] Spec routes: `POST/GET/PATCH/DELETE /v1/specs/:id`, `POST /v1/specs/:id/pause`, `POST /v1/specs/:id/resume`
- [x] Optimistic concurrency via `version` query param on `PATCH/pause/resume`
- [x] Pause clears `next_run` (NULL); resume re-seeds from now
- [x] PATCH preserves existing `next_run` on cosmetic updates; recomputes on schedule changes
- [x] Integration tests (9 tests): create/get/list/patch/delete, pause/resume, validation errors, stale-version conflict, status filter
- [x] DoD verified: create/pause/resume/delete specs via curl; persisted across restarts
- [x] README updated with API surface and curl examples

**Definition of Done:** Create/pause/resume/delete specs via curl; persisted. ✅

---

### Phase 2 — Scheduler (ZSET + Lua Claim + HttpTransport) ✅ DONE

- [x] Background tokio task running the scheduler loop (`src/scheduler/mod.rs`)
- [x] On boot: seed Redis ZSET from `task_specs.next_run WHERE status='active'`
      (idempotent `DEL` + rebuild; SQL is source of truth, DESIGN §2.1)
- [x] Atomic Lua claim: `ZREM` from `schedule` + `SADD` to `processing`
      (`src/scheduler/lua.rs`, DESIGN §2.2)
- [x] On successful delivery: recompute `next_run` (cadence from the *scheduled*
      fire time, no drift), `ZADD` back + SQL sync (version-gated)
- [x] On failure: retry with exponential backoff + jitter, honoring
      `max_attempts`; on exhaustion, drop from schedule (DLQ table is Phase 3)
- [x] `Transport` trait + `HttpTransport` v1 (webhook POST, timeout, headers)
      (`src/transport/`, DESIGN §2.4)
- [x] Catch-up policy wired: `run_missed | skip | run_once` (DESIGN §3)
- [x] Hot-reload: every API mutation mirrors into the Redis ZSET so scheduling
      changes take effect without a restart (DESIGN §2.3)
- [x] Graceful shutdown: shared `CancellationToken` + `JoinSet` drain, bounded
      by `SHUTDOWN_TIMEOUT_MS` (DESIGN §3)
- [x] Integration tests (4): boot-seed fire, hot-reload fire, atomic-claim
      exclusivity, retry-then-exhaust on 5xx

**Definition of Done:** A cron spec fires a webhook at the right time. ✅

> **Scope notes** (what Phase 2 deliberately leaves to later phases): the
> `dead_letter` table + `task_executions` history + `X-Fire-Id` idempotency
> header land in **Phase 3**; on exhaustion the task is logged + dropped for
> now (clean seam at `finalize_drop`). Webhook HMAC-SHA256 signing, payload
> `{{ scheduled_time }}` templating, the Prometheus `/metrics` endpoint, and
> readiness reflecting scheduler health land in **Phase 4** (the
> `HttpTransport` already receives the full `Target`, so signing is a localized
> add). Full catch-up semantics after extended downtime is a **Phase 5** item.

---

### Phase 3 — Reliability ✅ DONE

- [x] Retry with exponential backoff + jitter, per-task max attempts (backfilled from Phase 2)
- [x] Dead-letter table (`dead_letter`) on exhaustion + terminal failure
- [x] Idempotency header `X-Fire-Id: <task_id>:<scheduled_fire_time>` (DESIGN §5)
- [x] Execution history table (`task_executions`) with status, attempt, latency, response code, error
- [x] DLQ query endpoint (`GET /v1/dead_letter`)
- [x] Spec execution-history endpoint (`GET /v1/specs/:id/executions`)

**Definition of Done:** Failed webhook retried then dead-lettered; history queryable. ✅

**Key seams:**
- Migration `0003_reliability.sql` adds `task_executions` + `dead_letter` tables.
- `fire_state::bump_attempt` now returns the *stable* `fire_at` (original ZSET score)
  via `HSETNX`, so `X-Fire-Id` is identical across retries of the same logical fire.
- Every transport outcome writes a `task_executions` row best-effort (auditable but
  never blocking).
- Exhaustion writes one `dead_letter` row, then drops the task from the schedule.
- New queries are captured in `.sqlx/` for CI builds.
---

### Phase 4 — Ops ✅ DONE

- [x] Graceful shutdown: stop scheduling, drain in-flight, then exit
      (framework existed; readiness now reflects scheduler health)
- [x] Readiness reflects scheduler health (`/readyz` includes scheduler component)
- [x] Prometheus `/metrics` endpoint:
      `taskmanager_fires_total{transport,status}`,
      `taskmanager_schedule_lag_seconds`,
      `taskmanager_queue_depth`,
      `taskmanager_fire_latency_seconds{transport}`
- [x] Webhook HMAC-SHA256 signature using target `secret_hmac`
      (header `X-Signature: sha256=<hex>`)
- [x] Payload template substitution: `{{ scheduled_time }}` → RFC3339 UTC time

**Definition of Done:** `/metrics` scraped; clean SIGTERM drain. ✅

**Key seams:**
- `src/metrics.rs` owns the Prometheus registry + metric helpers.
- `src/state.rs` gains `SchedulerHealth` (healthy flag + last tick) and `Metrics`.
- Scheduler updates gauges every tick and observes fire outcomes.
- `HttpTransport` renders payload templates before serializing, then signs the
  exact bytes with HMAC-SHA256 if `secret_hmac` is set.
- `GET /metrics` and scheduler-aware `/readyz` are live.

---

### Phase 5 — Extensions (on demand) ⬜ NOT STARTED

- [ ] tonic gRPC API (`/v2/` or sidecar)
- [ ] Additional transports: AMQP, Kafka, SMTP, gRPC outbound
- [ ] Per-target rate limit + circuit breaker
- [ ] Task catch-up policies fully exercised after downtime
- [ ] Multi-tenancy / RBAC (if requested)
- [ ] CLI commands (clap) for operator tasks

**Definition of Done:** Per the fork decisions above, deployed when asked.

---

## Quick Reference

| File | Purpose |
|------|---------|
| `docs/DESIGN.md` | Full architecture, trade-offs, design rationale |
| `docs/README.md` | Getting started, API surface, verification commands |
| `docs/PLAN.md`   | This file — roadmap and status |

---

> Next actionable work: pick up Phase 5 — tonic gRPC API, additional
> transports, per-target rate limiting, catch-up policies fully exercised,
> multi-tenancy/RBAC, or CLI commands as needed.
