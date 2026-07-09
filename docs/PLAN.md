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
| Phase 2 | ⬜ Scheduler (ZSET + Lua claim + HttpTransport) |
| Phase 3 | ⬜ Reliability (retry/backoff, DLQ, idempotency, history) |
| Phase 4 | ⬜ Ops (graceful shutdown, metrics, HMAC) |
| Phase 5 | ⬜ Extensions (tonic gRPC, more transports, catch-up, rate limit) |

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

### Phase 2 — Scheduler (ZSET + Lua Claim + HttpTransport) ⬜ NOT STARTED

- [ ] Background tokio task running the scheduler loop
- [ ] On boot: seed Redis ZSET from `task_specs.next_run WHERE status='active'`
- [ ] Atomic Lua claim: `ZREM` from `schedule` + `SADD` to `processing` (DESIGN §2.2)
- [ ] On successful delivery: recompute `next_run`, `ZADD` back to schedule
- [ ] On failure: retry with backoff, or DLQ on exhaustion
- [ ] `Transport` trait + `HttpTransport` v1 (webhook with timeout, headers)
- [ ] Catch-up policy wired: `run_missed | skip | run_once` (DESIGN §3)
- [ ] Graceful shutdown: `CancellationToken` + `JoinSet` drain (DESIGN §3)

**Definition of Done:** A cron spec fires a webhook at the right time.

---

### Phase 3 — Reliability ⬜ NOT STARTED

- [ ] Retry with exponential backoff + jitter, per-task max attempts
- [ ] Dead-letter table (`dead_letter`) on exhaustion
- [ ] Idempotency header `X-Fire-Id: <task_id>:<scheduled_fire_time>` (DESIGN §5)
- [ ] Execution history table (`task_executions`) with status, attempts, latency, response code, last error
- [ ] DLQ query endpoint (`GET /v1/dead_letter`)

**Definition of Done:** Failed webhook retried then dead-lettered; history queryable.

---

### Phase 4 — Ops ⬜ NOT STARTED

- [ ] Graceful shutdown: stop scheduling, drain in-flight, then exit
- [ ] Readiness reflects scheduler health (not just DB/Redis)
- [ ] Prometheus `/metrics` endpoint (fires/s, failures/s, schedule-lag, queue depth, per-transport latency)
- [ ] Webhook HMAC-SHA256 signature using target `secret_hmac` (DESIGN §3)
- [ ] Payload template substitution: `{{ scheduled_time }}` minimal templating

**Definition of Done:** `/metrics` scraped; clean SIGTERM drain.

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

> Next actionable work: pick up Phase 2 (scheduler loop + ZSET + HttpTransport).
