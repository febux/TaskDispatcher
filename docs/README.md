# taskmanager

Reliable cron-to-notification dispatcher. You own scheduling and reliable
delivery; an external service owns execution. See [DESIGN.md](./DESIGN.md)
for the full design and the architectural forks that were locked.

## Status

- **Phase 0** (skeleton): health/readiness + Postgres/Redis pools +
  migrations + tracing.
- **Phase 1** (this phase): spec + target CRUD over `/v1/*`, sqlx
  persistence with compile-time-checked queries, serde validation, and
  tz-aware `next_run` seeding. Definition of done: create/pause/resume/
  delete specs via curl; persisted. ✓
- **Next**: Phase 2 wires the scheduler — Redis ZSET, atomic Lua claim,
  cron eval, `HttpTransport`, re-seed after fire (DESIGN §2.2).

## Stack

`tokio` + `axum` + `sqlx` (Postgres) + `redis` + `reqwest` + `serde` +
`tracing` + `cron` + `chrono-tz`. Queries are compile-time-checked via
`sqlx::query_as!`; an offline cache lives in `.sqlx/` so CI builds without a
live database.

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
| POST   | `/v1/targets`    | create; body `{name, url, secret_hmac?, headers?}` |
| GET    | `/v1/targets`    | list; `?limit=&offset=` (default 50)             |
| GET    | `/v1/targets/:id`| get one                                          |
| DELETE | `/v1/targets/:id`| delete (409 if a spec still references it)       |

`secret_hmac` is write-only — never returned in responses.

### Specs — `/v1/specs`

A spec is a persisted scheduling contract. `spec_type` drives the seed:

- `cron`     — needs `cron_expr` (`cron` crate format:
  `sec min hour day-of-month month day-of-week [year]`, e.g.
  `0 0 9 * * Mon-Fri`).
- `interval` — needs `interval_seconds` (positive).
- `once`     — needs `run_at` (future RFC3339 instant).

| Method | Path                          | Notes                                                       |
|--------|-------------------------------|-------------------------------------------------------------|
| POST   | `/v1/specs`                   | create; always starts `active`                              |
| GET    | `/v1/specs`                   | list; `?status=active|paused&limit=&offset=`                |
| GET    | `/v1/specs/:id`               | get one                                                     |
| PATCH  | `/v1/specs/:id?version=N`     | partial update; `version` must match (409 on stale)         |
| DELETE | `/v1/specs/:id`               | delete                                                      |
| POST   | `/v1/specs/:id/pause?version=N`  | pause: clears `next_run`                                  |
| POST   | `/v1/specs/:id/resume?version=N` | resume: re-seeds `next_run` from now                       |

Optional fields default to `timezone=UTC`, `payload={}`,
`catch_up=skip`, `max_attempts=5`.

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
```

When schema or queries change, regenerate the offline cache:

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432/taskmanager \
cargo sqlx prepare
```

## Health endpoints

- `GET /healthz` — liveness (always 200).
- `GET /readyz`  — readiness (200 if Postgres + Redis reachable, else 503).
