"""Application layer: wires engine <-> persistence <-> controller port.

Consistency strategy: SERIALISED PER-JUNCTION PROCESSING. Every operation on a junction
(sensor event, admin command, controller event, timer tick) runs under that junction's
lock, so decisions never interleave. The engine is single-threaded logic; state +
audit are persisted atomically before commands are handed to the controller.
"""
import logging
import re
import threading
import time

from .domain.engine import JunctionConfig, JunctionEngine
from .domain.errors import ConflictError, ValidationError
from .ports import ControllerPort
from .store import Store
from dataclasses import asdict

log = logging.getLogger("traffic.service")


class NotFound(Exception):
    pass


class JunctionService:
    def __init__(self, store: Store, controller: ControllerPort, clock=time.time):
        self.store, self.controller, self.clock = store, controller, clock
        self._engines, self._locks = {}, {}
        self._reg = threading.Lock()
        controller.set_event_callback(self._from_controller)

    # ------------------------------------------------------------ lifecycle
    def bootstrap(self, seed_default=True):
        for jid, cfgd in self.store.list_junctions():
            cfg = JunctionConfig.from_dict(cfgd)
            now = self.clock()
            snap = self.store.load_state(jid)
            with self._reg:
                self._locks[jid] = threading.RLock()
            with self._locks[jid]:
                if snap:
                    eng = JunctionEngine.restore(cfg, snap, now)
                    log.warning("junction %s restored from persisted state; physical state UNKNOWN", jid)
                else:
                    eng = JunctionEngine(cfg, now)
                    eng.start(now)
                self._engines[jid] = eng
                self._commit(jid, eng)
        if seed_default and not self._engines:
            self.create_junction({"junction_id": "A", "name": "Junction A"})

    def create_junction(self, data):
        cfg = JunctionConfig.from_dict(data)
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", cfg.junction_id):
            raise ValidationError("junction_id must match [A-Za-z0-9_-]{1,32}")
        with self._reg:
            if cfg.junction_id in self._engines:
                raise ConflictError(f"junction {cfg.junction_id} already exists")
            self._locks[cfg.junction_id] = threading.RLock()
            self._engines[cfg.junction_id] = None  # reserve
        jid, now = cfg.junction_id, self.clock()
        with self._locks[jid]:
            self.store.save_junction(jid, asdict(cfg))
            eng = JunctionEngine(cfg, now)
            eng.start(now)
            self._engines[jid] = eng
            self._commit(jid, eng)
            return eng.status(now)

    # --------------------------------------------------------------- helpers
    def _get(self, jid):
        eng = self._engines.get(jid)
        if eng is None:
            raise NotFound(f"junction {jid!r} not found")
        return eng, self._locks[jid]

    def _commit(self, jid, eng):
        """Persist (state + audit + commands) atomically, THEN dispatch outgoing commands."""
        audit, dirty, out = eng.drain_audit(), eng.drain_dirty_commands(), eng.drain_outbox()
        self.store.commit_changes(jid, eng.snapshot(), audit, dirty)
        for a in audit:
            if a["event_type"] in ("SIGNAL_TRANSITION_STARTED", "MODE_CHANGE", "COMMAND_TIMEOUT",
                                   "DEGRADED_ENTERED", "EMERGENCY_DETECTED", "RECOVERY"):
                log.info("%s %s %s", jid, a["event_type"], a["detail"] or "")
        for cmd in out:
            try:
                self.controller.send(cmd)
            except Exception:
                log.exception("controller send failed for %s (will be retried by timeout logic)", cmd["command_id"])

    def _run(self, jid, fn, reject_payload=None):
        eng, lock = self._get(jid)
        with lock:
            now = self.clock()
            try:
                res = fn(eng, now)
            except ValidationError as e:
                if reject_payload is not None:      # keep rejected input in the audit trail
                    eng.record_rejection(reject_payload, str(e), now)
                self._commit(jid, eng)
                raise
            self._commit(jid, eng)
            return res

    # ------------------------------------------------------------ public API
    def list_status(self):
        return [self.status(j) for j in sorted(k for k, v in self._engines.items() if v)]

    def status(self, jid):
        eng, lock = self._get(jid)
        with lock:
            return eng.status(self.clock())

    def config(self, jid):
        eng, _ = self._get(jid)
        return asdict(eng.cfg)

    def sensor_event(self, payload):
        jid = payload.get("junction_id") if isinstance(payload, dict) else None
        if not isinstance(jid, str) or not jid:
            raise ValidationError("missing field 'junction_id'")
        return self._run(jid, lambda e, n: e.handle_sensor_event(payload, n), reject_payload=payload)

    def command(self, jid, payload):
        return self._run(jid, lambda e, n: e.handle_command(payload, n))

    def controller_event(self, payload):
        jid = payload.get("junction_id") if isinstance(payload, dict) else None
        if not isinstance(jid, str) or not jid:
            raise ValidationError("missing field 'junction_id'")
        return self._run(jid, lambda e, n: e.handle_controller_event(payload, n))

    def history(self, jid, limit=100, event_type=None):
        self._get(jid)
        return self.store.history(jid, limit, event_type)

    def tick_all(self):
        for jid, eng in list(self._engines.items()):
            if eng is None:
                continue
            try:
                self._run(jid, lambda e, n: e.tick(n))
            except Exception:
                log.exception("tick failed for junction %s", jid)

    def _from_controller(self, event):  # callback used by simulator / future MQTT adapter
        try:
            self.controller_event(event)
        except Exception as e:
            log.warning("controller event rejected: %s", e)
