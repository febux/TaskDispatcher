# taskmanager

Reliable cron-to-notification dispatcher. You own scheduling and reliable
delivery; an external service owns execution. See [DESIGN.md](./DESIGN.md)
for the full design and the architectural forks that were locked.

## Status

- **Phase 0** (skeleton): health/readiness + Postgres/Redis pools +
  migrations + tracing. ✓
- **Phase 1**: spec + target CRUD over `/v1/*`, sqlx persistence with
  compile-time-checked queries, serde validation, and tz-aware `next_run`
  seeding. ✓
- **Phase 2**: the scheduler loop — Redis ZSET + atomic Lua claim + cron eval +
  `HttpTransport`, hot-reload on mutation, retry/backoff, catch-up, graceful
  shutdown. ✓
- **Phase 3**: reliability — dead-letter table, execution history,
  and `X-Fire-Id` idempotency header. ✓
- **Phase 4**: ops — Prometheus `/metrics`, webhook HMAC-SHA256 signing,
  `{{ scheduled_time }}` payload templating, scheduler-aware readiness. ✓
- **Phase 5** (extension, shipped): per-target service healthchecks — separate
  `taskmanager-healthchecker` container, target `healthcheck_url`, scheduler
  skip-when-unhealthy with requeue. ✓
- **Next**: further extensions (tonic gRPC, more transports, rate limiting,
  catch-up policies, RBAC/CLI as needed).

## Stack

`tokio` + `axum` + `sqlx` (Postgres) + `redis` + `reqwest` + `serde` +
`tracing` + `cron` + `chrono-tz` + `async-trait` + `tokio-util` +
`rand` + `prometheus` + `hmac` + `sha2`. Queries are compile-time-checked
via `sqlx::query_as!`; an offline cache lives in `.sqlx/` so CI builds
without a live database.

## Scheduler (Phase 2)

A single background tokio task owns the schedule (DESIGN §2.2):

- **Source of truth is SQL**; the Redis `schedule` ZSET is derived. On boot the
  ZSET is rebuilt from `task_specs` in one pass (`DEL` + re-seed).
- Each tick atomically claims due tasks via a Lua script
  (`ZRANGEBYSCORE` + `ZREM schedule` + `SADD processing`) — the
  at-most-once-per-tick guarantee.
- On delivery, `next_run` is recomputed from the *scheduled fire time* (no
  cadence drift) and mirrored back to SQL + the ZSET. A spent one-shot drops
  out via `next_run = NULL`.
- Failures retry with exponential backoff + jitter, bounded by `max_attempts`;
  on exhaustion or a terminal failure the task is **dead-lettered** (Phase 3
  `dead_letter` table) and each attempt is recorded in `task_executions`.
- All webhook fires carry a stable `X-Fire-Id: <task_id>:<scheduled_fire_time>`
  header (Phase 3, DESIGN §5) so handlers can dedup retries.
- API mutations (create/update/pause/resume/delete) mirror into the ZSET, so
  scheduling changes take effect with no restart (hot-reload, DESIGN §2.3).
- Graceful shutdown drains in-flight fires (shared `CancellationToken`).

Tuning (env, all optional with defaults):

| Var | Default | Purpose |
|-----|---------|---------|
| `SCHEDULER_TICK_MS` | `250` | loop wake interval |
| `SCHEDULER_BATCH_SIZE` | `64` | max tasks claimed/fired per tick |
| `HTTP_TIMEOUT_MS` | `10000` | per-webhook delivery timeout |
| `SHUTDOWN_TIMEOUT_MS` | `30000` | hard cap on in-flight drain |
| `BACKOFF_BASE_MS` | `500` | base delay for exponential backoff |
| `BACKOFF_MAX_MS` | `30000` | backoff ceiling |

## Run (Docker Compose — recommended)

Full stack in one command: Postgres 17, Redis 7, and the app, all with
healthchecks and persistent volumes.

```sh
docker compose up -d --build
```

The app listens on host port `8090`.

Operations:

```sh
docker compose logs -f app   # structured JSON logs
docker compose restart app   # rebuild + pick up changes
docker compose down          # stop, keep data
docker compose down -v       # stop AND wipe data
```

## Run (local dev, no containers)

```sh
cp .env.example .env
# start postgres + redis yourself (docker compose up -d postgres redis)
cargo run
```

## API (Phase 1)

All mutating spec writes (create/update/pause/resume) recompute the
`next_run` seed. Pause clears it; resume re-seeds. Updates are gated on a
`version` query param for optimistic concurrency.

### Targets — `/v1/targets`

A target is a delivery destination. v1 ships the `http` transport only.

| Method | Path             | Notes                                            |
|--------|------------------|--------------------------------------------------|
| POST   | `/v1/targets`    | create; body `{name, url, secret_hmac?, headers?, healthcheck_url?, healthcheck_interval_seconds?, healthcheck_timeout_seconds?}` |
| GET    | `/v1/targets`    | list; `?limit=&offset=` (default 50)             |
| GET    | `/v1/targets/:id`| get one                                          |
| GET    | `/v1/targets/:id/health` | current health status from `service_health` |
| DELETE | `/v1/targets/:id`| delete (409 if a spec still references it)       |

`secret_hmac` is write-only — never returned in responses.

### Specs — `/v1/specs`

A spec is a persisted scheduling contract. `spec_type` drives the seed:

- `cron`     — needs `cron_expr` (`cron` crate format:
  `sec min hour day-of-month month day-of-week [year]`, e.g.
  `0 0 9 * * Mon-Fri`).
- `interval` — needs `interval_seconds` (positive).
- `once`     — needs `run_at` (future RFC3339 instant).

| Method | Path                              | Notes                                                       |
|--------|-----------------------------------|-------------------------------------------------------------|
| POST   | `/v1/specs`                       | create; always starts `active`                              |
| GET    | `/v1/specs`                       | list; `?status=active|paused&limit=&offset=`                |
| GET    | `/v1/specs/:id`                   | get one                                                     |
| PATCH  | `/v1/specs/:id?version=N`         | partial update; `version` must match (409 on stale)         |
| DELETE | `/v1/specs/:id`                   | delete                                                      |
| POST   | `/v1/specs/:id/pause?version=N`   | pause: clears `next_run`                                    |
| POST   | `/v1/specs/:id/resume?version=N`  | resume: re-seeds `next_run` from now                        |
| GET    | `/v1/specs/:id/executions`        | execution history (Phase 3); newest-first                   |

Optional fields default to `timezone=UTC`, `payload={}`,
`catch_up=skip`, `max_attempts=5`.

### Dead letter — `/v1/dead_letter` (Phase 3)

Exhausted / terminally-failed fires.

| Method | Path             | Notes                                            |
|--------|------------------|--------------------------------------------------|
| GET    | `/v1/dead_letter`| list; `?task_id=<uuid>&limit=&offset=`           |

### Example

```sh
B=http://localhost:8090
# target
T=$(curl -s -XPOST $B/v1/targets -Hcontent-type:application/json \
  -d '{"name":"demo","url":"https://example.test/hook","secret_hmac":"k"}' \
  | jq -r .id)
# cron spec firing every minute
curl -s -XPOST $B/v1/specs -Hcontent-type:application/json \
  -d "{\"name\":\"minutely\",\"spec_type\":\"cron\",\"cron_expr\":\"0 * * * * *\",\"target_id\":\"$TID\"}"
```

### Errors

JSON body `{"error":{"kind":<status>,"message":...}}`. Validation → 400,
not found → 404, unique/FK/version conflict → 409, dependency down → 503.

## Verify

```sh
cargo build
cargo clippy --all-targets -- -D warnings
cargo test
```

`cargo test` runs the dep-free smoke tests always; the integration suite in
`tests/integration.rs` is skipped when `DATABASE_URL`/`REDIS_URL` are unset.
With the stack up:

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432/taskmanager \
REDIS_URL=redis://localhost:6379 \
cargo test --test integration
cargo test --test scheduler   # exercises the scheduler loop + a webhook mock
```

When schema or queries change, regenerate the offline cache:

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432/taskmanager \
cargo sqlx prepare
```

## Health endpoints

- `GET /healthz` — liveness (always 200).
- `GET /readyz` — readiness: Postgres + Redis + scheduler health (503 if degraded).

## Metrics (Phase 4)

- `GET /metrics` — Prometheus text format.

Exposed metrics:

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `taskmanager_fires_total` | counter | `transport`, `status` | fires by outcome (`delivered`/`retryable`/`terminal`) |
| `taskmanager_schedule_lag_seconds` | gauge | — | `now - oldest due score` (0 if empty) |
| `taskmanager_queue_depth` | gauge | — | active specs in the scheduling ZSET |
| `taskmanager_fire_latency_seconds` | histogram | `transport` | upstream 2xx latency |

## Service healthchecks (Phase 5)

Targets can declare a `healthcheck_url` that a separate
`taskmanager-healthchecker` container probes. The scheduler reads the latest
status from the `service_health` table before firing:

- Healthy / no `healthcheck_url` → fire normally.
- Unhealthy → skip the fire and requeue the task at
  `now + healthcheck_interval_seconds`.

Configure on create:

```sh
curl -s -XPOST $B/v1/targets -Hcontent-type:application/json \
  -d '{"name":"demo","url":"https://example.test/hook","healthcheck_url":"https://example.test/health","healthcheck_interval_seconds":30,"healthcheck_timeout_seconds":5}'
```

Query status:

```sh
curl -s $B/v1/targets/$T/health
```

`taskmanager-healthchecker` is started automatically by `docker compose up -d
--build`.

## Webhook auth (Phase 4)

Targets created with `secret_hmac` cause every fire to carry:

```
X-Signature: sha256=<hex>
```

where `<hex>` is `HMAC-SHA256(secret, body_bytes)`. The body is first rendered
for `{{ scheduled_time }}` templating, then serialized once, then signed. No
signature header is sent when `secret_hmac` is null.
