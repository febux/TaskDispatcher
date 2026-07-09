-- Phase 1: task specs + targets (DESIGN.md §2.1, §3).
--
-- SQL is the source of truth; the Redis `schedule` ZSET (Phase 2) is
-- *derived* from task_specs.next_run. If Redis evaporates, the scheduler
-- rebuilds the ZSET from this table in one pass (DESIGN §2.1).
--
-- This migration does NOT touch Redis. Mutation = reload (DESIGN §2.3) is
-- wired into the API handlers; the ZSET seeding lands in Phase 2.

CREATE TABLE IF NOT EXISTS targets (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT        NOT NULL UNIQUE,
    transport    TEXT        NOT NULL DEFAULT 'http',   -- registry key (DESIGN §2.4); v1 ships 'http' only
    url          TEXT        NOT NULL,                   -- webhook endpoint for the 'http' transport
    secret_hmac  TEXT,                                    -- HMAC-SHA256 signing key (DESIGN §3); nullable = unsigned
    headers      JSONB       NOT NULL DEFAULT '{}'::jsonb, -- extra transport headers
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS task_specs (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name             TEXT        NOT NULL,
    spec_type        TEXT        NOT NULL CHECK (spec_type IN ('cron', 'interval', 'once')),
    cron_expr        TEXT,                                  -- spec_type = 'cron' (cron 0.13 format: sec min hour dom mon dow)
    interval_seconds BIGINT,                                -- spec_type = 'interval' (positive)
    run_at           TIMESTAMPTZ,                            -- spec_type = 'once'
    timezone         TEXT        NOT NULL DEFAULT 'UTC',     -- tz-aware scheduling (DESIGN §3)
    target_id        UUID        NOT NULL REFERENCES targets(id) ON DELETE RESTRICT,
    payload          JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- payload template; {{ scheduled_time }} substitution (DESIGN §7.5)
    status           TEXT        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused')),
    catch_up         TEXT        NOT NULL DEFAULT 'skip'  CHECK (catch_up IN ('run_missed', 'skip', 'run_once')),
    max_attempts     INTEGER     NOT NULL DEFAULT 5      CHECK (max_attempts >= 1),
    next_run         TIMESTAMPTZ,                            -- UTC seed for the ZSET; Phase 2 reads & re-seeds this
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version          INTEGER     NOT NULL DEFAULT 1         -- optimistic concurrency, bumped on update
);

-- A spec must carry the fields for its spec_type.
ALTER TABLE task_specs
    ADD CONSTRAINT chk_spec_payload CHECK (
        (spec_type = 'cron'     AND cron_expr IS NOT NULL)
     OR (spec_type = 'interval' AND interval_seconds IS NOT NULL AND interval_seconds > 0)
     OR (spec_type = 'once'     AND run_at IS NOT NULL)
    );

CREATE INDEX idx_task_specs_next_run_active
    ON task_specs (next_run)
    WHERE status = 'active' AND next_run IS NOT NULL;
CREATE INDEX idx_task_specs_status ON task_specs (status);
CREATE INDEX idx_task_specs_target ON task_specs (target_id);

-- Bump updated_at on every UPDATE.
CREATE OR REPLACE FUNCTION taskmanager_set_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_targets_updated
    BEFORE UPDATE ON targets
    FOR EACH ROW EXECUTE FUNCTION taskmanager_set_updated_at();

CREATE TRIGGER trg_task_specs_updated
    BEFORE UPDATE ON task_specs
    FOR EACH ROW EXECUTE FUNCTION taskmanager_set_updated_at();
