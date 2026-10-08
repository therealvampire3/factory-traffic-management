import json
import os
import sqlite3
import threading
from datetime import datetime, timezone

SCHEMA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "schema.sql")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


class Store:
    """SQLite persistence. One connection guarded by a lock (small, simple, safe)."""

    def __init__(self, path):
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        with open(SCHEMA) as f:
            self._db.executescript(f.read())

    def save_junction(self, jid, config: dict):
        with self._lock, self._db:
            self._db.execute("INSERT INTO junctions VALUES (?,?,?)", (jid, json.dumps(config), _now_iso()))

    def list_junctions(self):
        with self._lock:
            return [(r["junction_id"], json.loads(r["config_json"]))
                    for r in self._db.execute("SELECT * FROM junctions ORDER BY created_at, junction_id")]

    def load_state(self, jid):
        with self._lock:
            r = self._db.execute("SELECT state_json FROM junction_state WHERE junction_id=?", (jid,)).fetchone()
            return json.loads(r["state_json"]) if r else None

    def commit_changes(self, jid, snapshot, audit, commands):
        """Atomically persist state + audit rows + command rows."""
        with self._lock, self._db:
            self._db.execute("INSERT INTO junction_state VALUES (?,?,?) ON CONFLICT(junction_id) "
                             "DO UPDATE SET state_json=excluded.state_json, updated_at=excluded.updated_at",
                             (jid, json.dumps(snapshot), _now_iso()))
            self._db.executemany(
                "INSERT INTO audit_log (junction_id,timestamp,event_type,direction,previous_state,new_state,"
                "command_id,detail_json) VALUES (?,?,?,?,?,?,?,?)",
                [(a["junction_id"], a["timestamp"], a["event_type"], a["direction"], a["previous_state"],
                  a["new_state"], a["command_id"], json.dumps(a["detail"])) for a in audit])
            self._db.executemany(
                "INSERT INTO controller_commands VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(command_id) DO UPDATE SET "
                "status=excluded.status, attempts=excluded.attempts, updated_at=excluded.updated_at",
                [(c["command_id"], c["junction_id"], c["direction"], c["requested_state"], c["status"],
                  c["attempts"], c.get("reason"), c["created_at"], c["updated_at"]) for c in commands])

    def history(self, jid, limit=100, event_type=None):
        q, args = "SELECT * FROM audit_log WHERE junction_id=?", [jid]
        if event_type:
            q += " AND event_type=?"
            args.append(event_type)
        q += " ORDER BY id DESC LIMIT ?"
        args.append(max(1, min(int(limit), 1000)))
        with self._lock:
            return [{"id": r["id"], "junction_id": r["junction_id"], "timestamp": r["timestamp"],
                     "event_type": r["event_type"], "direction": r["direction"],
                     "previous_state": r["previous_state"], "new_state": r["new_state"],
                     "command_id": r["command_id"], "detail": json.loads(r["detail_json"] or "{}")}
                    for r in self._db.execute(q, args)]
