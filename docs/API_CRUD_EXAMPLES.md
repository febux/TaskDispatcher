# API CRUD examples + real task trigger

This document shows concrete `curl` examples for the `taskmanager` HTTP API.

- Targets = external service registrations (where webhooks are delivered).
- Specs   = scheduling contracts tied to a target.
- Triggers: the scheduler fires specs automatically based on `cron`, `interval`, or `once`. You can also force a near-instant fire by creating an interval spec with a short `interval_seconds`.

Base URL used below:

```sh
B=http://localhost:8090
```

All responses are JSON unless otherwise noted.

---

## Targets CRUD — `/v1/targets`

A target is the external service that receives webhook fires. v1 only supports the `http` transport.

### Create a target

```sh
curl -s -XPOST "$B/v1/targets" \
  -H "content-type: application/json" \
  -d '{
    "name": "my-demo-service",
    "url": "https://httpbin.org/post",
    "secret_hmac": "super-secret-key",
    "headers": { "X-Custom": "demo" },
    "healthcheck_url": "https://httpbin.org/get",
    "healthcheck_interval_seconds": 30,
    "healthcheck_timeout_seconds": 5
  }'
```

Returns `201 Created`:

```json
{
  "id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
  "name": "my-demo-service",
  "transport": "http",
  "url": "https://httpbin.org/post",
  "headers": { "X-Custom": "demo" },
  "healthcheck_url": "https://httpbin.org/get",
  "healthcheck_interval_seconds": 30,
  "healthcheck_timeout_seconds": 5,
  "created_at": "2026-07-10T12:00:00Z",
  "updated_at": "2026-07-10T12:00:00Z"
}
```

`secret_hmac` is write-only and never returned.

Capture the id for later steps:

```sh
T=$(curl -s -XPOST "$B/v1/targets" \
  -H "content-type: application/json" \
  -d '{"name":"demo","url":"https://httpbin.org/post"}' | jq -r '.id')
echo "target_id=$T"
```

### List targets

```sh
curl -s "$B/v1/targets"
# paginated
curl -s "$B/v1/targets?limit=10&offset=0"
```

Returns `200 OK` with an array.

### Get one target

```sh
curl -s "$B/v1/targets/$T"
```

Returns `200 OK` or `404 Not Found`.

### Get target health status

```sh
curl -s "$B/v1/targets/$T/health"
```

Returns `200 OK`:

```json
{
  "target_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
  "status": "healthy",
  "status_code": 200,
  "error": null,
  "checked_at": "2026-07-10T12:01:00Z",
  "changed_at": "2026-07-10T12:01:00Z"
}
```

`status` can be `healthy`, `unhealthy`, or `unknown`.

### Delete a target

```sh
curl -s -XDELETE "$B/v1/targets/$T"
```

Returns `204 No Content` on success, `404` if missing, or `409 Conflict` if a spec still references it.

---

## Specs CRUD — `/v1/specs`

A spec is the persisted schedule. It must reference a valid target id.

### Create an interval spec

```sh
curl -s -XPOST "$B/v1/specs" \
  -H "content-type: application/json" \
  -d "{
    \"name\": \"demo-every-30s\",
    \"spec_type\": \"interval\",
    \"interval_seconds\": 30,
    \"target_id\": \"$T\",
    \"timezone\": \"UTC\",
    \"payload\": { \"hello\": \"world\" },
    \"catch_up\": \"skip\",
    \"max_attempts\": 5
  }"
```

Returns `201 Created`:

```json
{
  "id": "b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22",
  "name": "demo-every-30s",
  "spec_type": "interval",
  "cron_expr": null,
  "interval_seconds": 30,
  "run_at": null,
  "timezone": "UTC",
  "target_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
  "payload": { "hello": "world" },
  "status": "active",
  "catch_up": "skip",
  "max_attempts": 5,
  "next_run": "2026-07-10T12:00:30Z",
  "created_at": "2026-07-10T12:00:00Z",
  "updated_at": "2026-07-10T12:00:00Z",
  "version": 1
}
```

### Create a cron spec

```sh
curl -s -XPOST "$B/v1/specs" \
  -H "content-type: application/json" \
  -d "{
    \"name\": \"demo-cron\",
    \"spec_type\": \"cron\",
    \"cron_expr\": \"0 */5 * * * *\",
    \"target_id\": \"$T\",
    \"timezone\": \"America/New_York\",
    \"payload\": { \"event\": \"five-minutely\" }
  }"
```

Cron format: `sec min hour day-of-month month day-of-week [year]`.

### Create a one-shot spec

```sh
RUN_AT=$(date -u -d '+2 minutes' +%Y-%m-%dT%H:%M:%SZ)
curl -s -XPOST "$B/v1/specs" \
  -H "content-type: application/json" \
  -d "{
    \"name\": \"demo-once\",
    \"spec_type\": \"once\",
    \"run_at\": \"$RUN_AT\",
    \"target_id\": \"$T\",
    \"payload\": { \"event\": \"one-time\" }
  }"
```

### List specs

```sh
# all
curl -s "$B/v1/specs"

# only active, paginated
curl -s "$B/v1/specs?status=active&limit=50&offset=0"

# only paused
curl -s "$B/v1/specs?status=paused"
```

Returns `200 OK` with an array.

### Get one spec

```sh
S=b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22
curl -s "$B/v1/specs/$S"
```

### Update (PATCH) a spec

Optimistic concurrency requires `?version=N`.

```sh
# rename only (cosmetic, next_run preserved)
curl -s -XPATCH "$B/v1/specs/$S?version=1" \
  -H "content-type: application/json" \
  -d '{"name":"demo-every-30s-renamed"}'

# change schedule (next_run recomputed)
curl -s -XPATCH "$B/v1/specs/$S?version=2" \
  -H "content-type: application/json" \
  -d '{"interval_seconds":120}'
```

Returns `200 OK` with the updated spec, or `409 Conflict` if `version` is stale.

### Pause a spec

```sh
curl -s -XPOST "$B/v1/specs/$S/pause?version=3"
```

`next_run` becomes `null` and the spec leaves the scheduling ZSET.

### Resume a spec

```sh
curl -s -XPOST "$B/v1/specs/$S/resume?version=4"
```

`next_run` is re-seeded from the current time.

### Delete a spec

```sh
curl -s -XDELETE "$B/v1/specs/$S"
```

Returns `204 No Content` or `404 Not Found`.

### Query execution history

```sh
curl -s "$B/v1/specs/$S/executions?limit=20&offset=0"
```

Returns `200 OK` with newest-first attempt rows:

```json
[
  {
    "id": "c2eebc99-9c0b-4ef8-bb6d-6bb9bd380a33",
    "task_id": "b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22",
    "scheduled_fire_time": "2026-07-10T12:01:00Z",
    "attempt": 1,
    "status": "delivered",
    "latency_ms": 45,
    "response_code": 200,
    "error": null,
    "created_at": "2026-07-10T12:01:00.100Z"
  }
]
```

`status` is one of `delivered`, `retryable`, or `terminal`.

---

## Running a "real" trigger

There is no dedicated "trigger now" admin endpoint. The scheduler fires specs automatically as soon as their `next_run` is reached.

To see a real fire quickly, create an interval spec with a short interval and watch the target receive POST requests.

### 1. Start a local webhook receiver

Using `netcat` or `nc` for a quick smoke test:

```sh
# terminal 1: listen on port 9000
nc -kl -p 9000
```

Or use a tiny Python handler to see the body + headers:

```python
# webhook.py
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('content-length', 0))
        body = self.rfile.read(n)
        print("---")
        print("Headers:", dict(self.headers))
        print("Body:", body.decode())
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

HTTPServer(("0.0.0.0", 9000), H).serve_forever()
```

```sh
python3 webhook.py
```

### 2. Register the local receiver as a target

```sh
T=$(curl -s -XPOST "$B/v1/targets" \
  -H "content-type: application/json" \
  -d '{"name":"local-webhook","url":"http://host.docker.internal:9000/webhook","secret_hmac":"demo-key"}' \
  | jq -r '.id')
echo "target_id=$T"
```

If the app runs outside Docker, use `http://localhost:9000/webhook` instead of `host.docker.internal`.

### 3. Create a short interval spec

```sh
curl -s -XPOST "$B/v1/specs" \
  -H "content-type: application/json" \
  -d "{
    \"name\": \"real-trigger-demo\",
    \"spec_type\": \"interval\",
    \"interval_seconds\": 10,
    \"target_id\": \"$T\",
    \"payload\": { \"triggered_by\": \"scheduler\", \"value\": 42 },
    \"max_attempts\": 3
  }"
```

The spec is `active` immediately. Within one scheduler tick (`SCHEDULER_TICK_MS` default 250 ms) the scheduler will seed it in Redis and claim it when `next_run` is due. The external service will receive a POST every 10 seconds.

### 4. Inspect the fire in the receiver

You should see repeated POSTs with headers like:

```
POST /webhook HTTP/1.1
content-type: application/json
x-fire-id: <task_id>:<scheduled_fire_time>
x-task-id: <task_id>
x-attempt: 1
x-signature: sha256=<hex>
content-length: 87

{"triggered_by":"scheduler","value":42,"scheduled_time":"2026-07-10T12:01:10Z"}
```

`scheduled_time` is injected when the payload contains `{{ scheduled_time }}` or when the renderer processes the template. The example payload above is sent as-is plus the templating step; for explicit templating use:

```json
{"message":"fire at {{ scheduled_time }}"}
```

### 5. Verify executions via the API

```sh
S=<spec-id-from-create>
curl -s "$B/v1/specs/$S/executions?limit=10"
```

You will see one row per attempt. After enough failures the row will appear in the dead letter endpoint:

```sh
curl -s "$B/v1/dead_letter?task_id=$S&limit=10"
```

---

## Tips

- `interval_seconds` must be > 0; `catch_up` values are `run_missed`, `skip`, or `run_once`.
- Paused specs do not fire and have `next_run: null`.
- Updating a spec with a stale `version` returns `409 Conflict`; re-read the spec and retry.
- A target with a failing `healthcheck_url` is skipped by the scheduler and requeued at the next healthcheck interval.
