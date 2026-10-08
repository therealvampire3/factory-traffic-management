-- SQLite schema (applied automatically at startup by traffic/store.py)
CREATE TABLE IF NOT EXISTS junctions (
    junction_id TEXT PRIMARY KEY,
    config_json TEXT NOT NULL,          -- JunctionConfig: phases, timings, thresholds
    created_at  TEXT NOT NULL
);

-- Latest full engine state per junction (queues, processed event ids, mode, desired/actual
-- signals, emergency + manual state, pending commands, timestamps). Rewritten in the SAME
-- transaction as the audit rows of every state change.
CREATE TABLE IF NOT EXISTS junction_state (
    junction_id TEXT PRIMARY KEY REFERENCES junctions(junction_id),
    state_json  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- Append-only audit / history log.
CREATE TABLE IF NOT EXISTS audit_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    junction_id    TEXT NOT NULL,
    timestamp      TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    direction      TEXT,
    previous_state TEXT,
    new_state      TEXT,
    command_id     TEXT,
    detail_json    TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_junction ON audit_log(junction_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_audit_type ON audit_log(junction_id, event_type);

-- Every command sent to a physical controller and its lifecycle
-- (PENDING, ACKED, FAILED, TIMED_OUT, SUPERSEDED, ABANDONED).
CREATE TABLE IF NOT EXISTS controller_commands (
    command_id      TEXT PRIMARY KEY,
    junction_id     TEXT NOT NULL,
    direction       TEXT NOT NULL,
    requested_state TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL,
    reason          TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cmd_junction ON controller_commands(junction_id, created_at DESC);
