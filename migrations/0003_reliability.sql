-- Phase 3: reliability — execution history + dead-letter (DESIGN §3, §5).
--
-- SQL is the source of truth for audit. Two new tables:
--
-- task_executions: one row per delivery ATTEMPT (the "why did it fail on
--   Tuesday" table, DESIGN §3). Drives SLAs and debugging. The scheduler
--   writes a row best-effort on every transport outcome.
-- dead_letter: the audit trail of "we gave up" on a logical fire after
--   exhausting retries or hitting a terminal failure (DESIGN §3). Written
--   once per exhausted fire, in addition to the per-attempt history rows.
--
-- Idempotency is a header-level contract (X-Fire-Id, DESIGN §5), not a SQL
-- constraint — handlers MUST dedup on it. See transport/http.rs.

CREATE TABLE IF NOT EXISTS task_executions (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id             UUID        NOT NULL REFERENCES task_specs(id) ON DELETE CASCADE,
    scheduled_fire_time TIMESTAMPTZ NOT NULL,                 -- the ZSET score we popped (stable across retries)
    attempt             INTEGER     NOT NULL,                  -- 1-based attempt number for this logical fire
    status              TEXT        NOT NULL CHECK (status IN ('delivered', 'retryable', 'terminal')),
    latency_ms          INTEGER,                               -- upstream response latency (NULL if no HTTP response)
    response_code       INTEGER,                               -- upstream HTTP status code (NULL on connect/timeout)
    error               TEXT,                                  -- last error string (NULL on delivered)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Newest-first per task is the hot query (history of a spec).
CREATE INDEX idx_task_executions_task_time
    ON task_executions (task_id, created_at DESC);
-- Global newest-first (operational dashboards).
CREATE INDEX idx_task_executions_created
    ON task_executions (created_at DESC);

CREATE TABLE IF NOT EXISTS dead_letter (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id             UUID        NOT NULL REFERENCES task_specs(id) ON DELETE CASCADE,
    scheduled_fire_time TIMESTAMPTZ NOT NULL,
    attempts            INTEGER     NOT NULL,                  -- attempts made before giving up
    last_error          TEXT,
    last_response_code  INTEGER,                               -- upstream HTTP status of the final attempt (if any)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dead_letter_created
    ON dead_letter (created_at DESC);
CREATE INDEX idx_dead_letter_task
    ON dead_letter (task_id);
