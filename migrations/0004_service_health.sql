-- Extension: external-service healthchecks.
--
-- Each target may optionally declare a `healthcheck_url`. A separate
-- `healthchecker` service polls it and writes status to `service_health`.
-- The scheduler skips fires for targets whose latest status is 'unhealthy',
-- requeueing the task at the next healthcheck interval.

ALTER TABLE targets
    ADD COLUMN healthcheck_url  TEXT,                    -- endpoint polled by healthchecker
    ADD COLUMN healthcheck_interval_seconds INTEGER NOT NULL DEFAULT 30 CHECK (healthcheck_interval_seconds > 0),
    ADD COLUMN healthcheck_timeout_seconds  INTEGER NOT NULL DEFAULT 5  CHECK (healthcheck_timeout_seconds > 0);

CREATE TABLE IF NOT EXISTS service_health (
    target_id     UUID        PRIMARY KEY REFERENCES targets(id) ON DELETE CASCADE,
    status        TEXT        NOT NULL CHECK (status IN ('healthy', 'unhealthy', 'unknown')),
    status_code   INTEGER,                               -- last HTTP status (NULL on connection failure)
    error         TEXT,                                    -- last error string
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()       -- last time status changed (not just refreshed)
);

CREATE INDEX idx_service_health_status ON service_health (status);
CREATE INDEX idx_service_health_checked_at ON service_health (checked_at);
