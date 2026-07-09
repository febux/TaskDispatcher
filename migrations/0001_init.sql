-- Phase 0: minimal schema placeholder.
-- task_specs, targets, task_executions, dead_letter land in later phases
-- per DESIGN.md §2.1. This migration exists so the sqlx::migrate! macro
-- has a valid target and boot-time migration runs end-to-end.

CREATE TABLE IF NOT EXISTS engine_meta (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO engine_meta (key, value) VALUES ('schema_version', '0')
ON CONFLICT (key) DO NOTHING;
