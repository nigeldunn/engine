-- v1 schema. JSON payloads (TEXT) for early debugging - swap to BLOB+msgpack later.

-- Immutable event log. Append-only.
CREATE TABLE IF NOT EXISTS events (
    workflow_id            TEXT    NOT NULL,
    sequence               INTEGER NOT NULL,
    event_id               TEXT    NOT NULL UNIQUE,
    recorded_at            TEXT    NOT NULL,
    payload_type           TEXT    NOT NULL,
    payload_schema_version INTEGER NOT NULL,
    causation_kind         TEXT    NOT NULL,
    causation_ref          TEXT,
    payload                TEXT    NOT NULL,
    ingress_dedup_key      TEXT,
    PRIMARY KEY (workflow_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_events_payload_type
    ON events(payload_type);

CREATE INDEX IF NOT EXISTS idx_events_causation_ref
    ON events(causation_ref) WHERE causation_ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_recorded_at
    ON events(recorded_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_ingress_dedup
    ON events(ingress_dedup_key) WHERE ingress_dedup_key IS NOT NULL;

-- State snapshots. Cache only - derivable from events.
CREATE TABLE IF NOT EXISTS snapshots (
    workflow_id   TEXT    PRIMARY KEY,
    sequence      INTEGER NOT NULL,
    state_blob    TEXT    NOT NULL,
    state_version INTEGER NOT NULL,
    updated_at    TEXT    NOT NULL
);

-- Outbox of side-effect intentions. Drained by dispatchers.
CREATE TABLE IF NOT EXISTS actions_outbox (
    action_id          TEXT    PRIMARY KEY,
    workflow_id        TEXT    NOT NULL,
    source_sequence    INTEGER NOT NULL,
    action_kind        TEXT    NOT NULL,
    payload            TEXT    NOT NULL,
    state              TEXT    NOT NULL,
    attempt            INTEGER NOT NULL DEFAULT 0,
    max_attempts       INTEGER NOT NULL DEFAULT 5,
    probe_attempt      INTEGER NOT NULL DEFAULT 0,
    max_probe_attempts INTEGER NOT NULL DEFAULT 20,
    next_attempt_at    TEXT    NOT NULL,
    claimed_by         TEXT,
    lease_expires_at   TEXT,
    created_at         TEXT    NOT NULL,
    updated_at         TEXT    NOT NULL,
    external_ref       TEXT,
    outcome_event_id   TEXT,
    last_error         TEXT,
    FOREIGN KEY (workflow_id, source_sequence)
        REFERENCES events(workflow_id, sequence)
);

-- Ready to claim: pending and due.
CREATE INDEX IF NOT EXISTS idx_outbox_ready
    ON actions_outbox(next_attempt_at) WHERE state = 'pending';

-- Reclaimable: in_progress with expired lease.
CREATE INDEX IF NOT EXISTS idx_outbox_expired_lease
    ON actions_outbox(lease_expires_at) WHERE state = 'in_progress';

CREATE INDEX IF NOT EXISTS idx_outbox_workflow
    ON actions_outbox(workflow_id);

-- Audit trail of every dispatch attempt.
CREATE TABLE IF NOT EXISTS action_attempts (
    action_id      TEXT    NOT NULL,
    attempt        INTEGER NOT NULL,
    started_at     TEXT    NOT NULL,
    finished_at    TEXT,
    outcome        TEXT,
    error_kind     TEXT,
    error_message  TEXT,
    external_ref   TEXT,
    PRIMARY KEY (action_id, attempt),
    FOREIGN KEY (action_id) REFERENCES actions_outbox(action_id)
);

-- Workflow configs, content-addressed.
CREATE TABLE IF NOT EXISTS workflow_configs (
    config_hash TEXT    PRIMARY KEY,
    config_blob TEXT    NOT NULL,
    created_at  TEXT    NOT NULL
);

-- Persisted sink health. Survives process restarts so a crash during an auth
-- outage doesn't burn another action attempt to rediscover the problem.
CREATE TABLE IF NOT EXISTS sink_health (
    sink_key      TEXT    PRIMARY KEY,
    state         TEXT    NOT NULL,        -- 'healthy' | 'unhealthy'
    reason        TEXT,                     -- structured reason code (snake_case enum variant)
    detail        TEXT,                     -- human-readable detail
    updated_at    TEXT    NOT NULL,
    last_check_at TEXT    NOT NULL,
    next_check_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_sink_health_unhealthy
    ON sink_health(next_check_at) WHERE state = 'unhealthy';
