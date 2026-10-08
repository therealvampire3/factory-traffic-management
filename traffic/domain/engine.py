"""Pure traffic-control domain engine.

No HTTP, MQTT, DB or threading imports. Time is always injected (`now`, epoch
seconds) so the engine is deterministic and unit-testable. The engine is NOT
thread-safe by itself: the application layer serialises access per junction.

Inputs  : handle_sensor_event / handle_command / handle_controller_event / tick
Outputs : drain_outbox() (commands for the physical controller),
          drain_audit() (audit entries), drain_dirty_commands() (to persist),
          snapshot() (full state for persistence)
"""
from __future__ import annotations

import uuid
from collections import OrderedDict
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone

from .errors import ConflictError, ValidationError

RED, YELLOW, GREEN, UNKNOWN = "RED", "YELLOW", "GREEN", "UNKNOWN"
ALL_RED = "ALL_RED"  # internal stage name
VEHICLE_WEIGHT = {"EMERGENCY": 100, "TRUCK": 4, "FORKLIFT": 3, "EMPLOYEE_VEHICLE": 1}
SENSOR_EVENT_TYPES = {"VEHICLE_ARRIVED", "VEHICLE_CLEARED"}
DEVICE_STATUSES = {"ONLINE", "OFFLINE", "DEGRADED", "WARNING", "UNKNOWN"}
DEVICE_TYPES = {"SIGNAL_CONTROLLER", "SENSOR"}
MAX_SEEN_EVENTS = 5000


def parse_ts(value) -> float:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("missing or invalid field 'timestamp'")
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        raise ValidationError(f"timestamp '{value}' is not ISO-8601")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class JunctionConfig:
    junction_id: str
    name: str = ""
    phases: dict = field(default_factory=lambda: {
        "NORTH_SOUTH": ["NORTH", "SOUTH"], "EAST_WEST": ["EAST", "WEST"]})
    green_seconds: float = 30.0          # max green under automatic control
    min_green_seconds: float = 10.0      # avoid unnecessary switching
    yellow_seconds: float = 5.0
    all_red_seconds: float = 2.0
    ack_timeout_seconds: float = 5.0
    max_retries: int = 2                 # resends after the first attempt
    manual_timeout_seconds: float = 120.0
    emergency_stale_seconds: float = 60.0
    max_wait_seconds: float = 90.0       # starvation threshold
    event_max_age_seconds: float = 600.0 # older sensor events are "delayed" -> rejected
    switch_hysteresis: float = 1.25
    wait_weight: float = 0.1
    degraded_retry_seconds: float = 10.0

    @classmethod
    def from_dict(cls, data: dict) -> "JunctionConfig":
        if not isinstance(data, dict):
            raise ValidationError("body must be a JSON object")
        known = {f.name for f in fields(cls)}
        kw = {k: v for k, v in data.items() if k in known}
        jid = kw.get("junction_id", data.get("id"))
        if not isinstance(jid, str) or not jid.strip():
            raise ValidationError("junction_id is required")
        kw["junction_id"] = jid.strip()
        cfg = cls(**kw)
        ph = cfg.phases
        if not isinstance(ph, dict) or len(ph) < 2:
            raise ValidationError("phases must define at least two conflicting phases")
        seen = set()
        for name, ds in ph.items():
            if not isinstance(ds, list) or not ds or not all(isinstance(d, str) and d for d in ds):
                raise ValidationError(f"phase {name} must list directions")
            for d in ds:
                if d in seen:
                    raise ValidationError(f"direction {d} is in more than one phase")
                seen.add(d)
        for f in fields(cls):
            if f.name not in ("junction_id", "name", "phases"):
                v = getattr(cfg, f.name)
                if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
                    raise ValidationError(f"{f.name} must be a non-negative number")
        return cfg


class JunctionEngine:
    def __init__(self, config: JunctionConfig, now: float):
        self.cfg = config
        self.dirs = [d for ds in config.phases.values() for d in ds]
        self.phase_of = {d: p for p, ds in config.phases.items() for d in ds}
        self.created = now
        self.vehicles: dict = {}      # vehicle_id -> {direction, vehicle_type, arrived_at}
        self.cleared: dict = {}       # vehicle_id -> sensor ts of clearance (tombstone)
        self.seen: OrderedDict = OrderedDict()   # processed event_ids (idempotency)
        self.last_seq: dict = {}
        self.desired = {d: RED for d in self.dirs}
        self.actual = {d: UNKNOWN for d in self.dirs}
        self.stage = ALL_RED
        self.phase = None
        self.stage_started = now
        self.last_green = None
        self.next_phase = None        # phase we committed to serve after the current transition
        self.last_served: dict = {}
        self.degraded = False
        self.degraded_reason = None
        self.next_degraded_retry = 0.0
        self.manual = None
        self.emergencies: dict = {}   # vehicle_id -> {direction, arrived_at, last_seen}
        self.controller_status = UNKNOWN
        self.sensor_status = {d: "ONLINE" for d in self.dirs}
        self.signal_status = {d: "ONLINE" for d in self.dirs}
        self.commands: dict = {}
        self.mode = "AUTOMATIC"
        self._outbox: list = []
        self._audit: list = []
        self._dirty: set = set()

    # ------------------------------------------------------------------ utils
    def _log(self, now, event_type, direction=None, previous=None, new=None, command_id=None, **detail):
        self._audit.append({"junction_id": self.cfg.junction_id, "event_type": event_type,
                            "direction": direction, "previous_state": previous, "new_state": new,
                            "command_id": command_id, "detail": detail, "timestamp": iso(now)})

    def drain_audit(self):
        a, self._audit = self._audit, []
        return a

    def drain_outbox(self):
        o, self._outbox = self._outbox, []
        return o

    def drain_dirty_commands(self):
        out = [dict(self.commands[c]) for c in self._dirty if c in self.commands]
        self._dirty.clear()
        return out

    def queues(self):
        q = {d: 0 for d in self.dirs}
        for v in self.vehicles.values():
            q[v["direction"]] += 1
        return q

    # --------------------------------------------------------------- commands
    def _pending(self, d=None):
        return [c for c in self.commands.values()
                if c["status"] == "PENDING" and (d is None or c["direction"] == d)]

    def _set_desired(self, d, state, now, reason, force=False):
        if self.desired[d] == state and not force:
            return
        prev = self.desired[d]
        for c in self._pending(d):
            c["status"], c["updated_at"] = "SUPERSEDED", now
            self._dirty.add(c["command_id"])
        self.desired[d] = state
        cid = "cmd-" + uuid.uuid4().hex[:8]
        cmd = {"command_id": cid, "junction_id": self.cfg.junction_id, "direction": d,
               "requested_state": state, "status": "PENDING", "attempts": 1, "reason": reason,
               "created_at": now, "updated_at": now,
               "deadline": now + self.cfg.ack_timeout_seconds}
        self.commands[cid] = cmd
        self._dirty.add(cid)
        self._outbox.append(dict(cmd))
        self._log(now, "SIGNAL_REQUESTED", d, prev, state, cid, reason=reason)

    def _resync_red(self, now, reason):
        if self.controller_status == "OFFLINE":
            return
        for d in self.dirs:
            if self.actual[d] != RED and not any(c["requested_state"] == RED for c in self._pending(d)):
                self._set_desired(d, RED, now, reason, force=True)

    def _safe_resync(self, now, reason):
        """Physical state is unknown: never re-assert GREEN. Walk to ALL RED via YELLOW
        (phase that may be green/yellow) and re-confirm every direction."""
        if self.stage in (GREEN, YELLOW) and self.phase:
            self.stage = YELLOW
        else:
            self.stage, self.phase = ALL_RED, None
        self.stage_started = now
        mine = lambda d: bool(self.phase and self.phase_of[d] == self.phase)
        for d in sorted(self.dirs, key=mine):   # RED directions first
            self._set_desired(d, YELLOW if mine(d) else RED, now, reason, force=True)

    def start(self, now):
        """Fail-safe start: demand ALL RED and wait for confirmation."""
        self.stage, self.phase, self.stage_started = ALL_RED, None, now
        for d in self.dirs:
            self._set_desired(d, RED, now, "startup", force=True)
        self._finish(now)

    # ------------------------------------------------------------------ safety
    def _assert_safe(self):
        greens = {self.phase_of[d] for d in self.dirs if self.desired[d] == GREEN}
        if len(greens) > 1:
            raise AssertionError("SAFETY VIOLATION: conflicting GREEN")
        if greens:
            p = next(iter(greens))
            for d in self.dirs:
                if self.phase_of[d] != p and self.desired[d] != RED:
                    raise AssertionError("SAFETY VIOLATION: conflicting direction not RED during GREEN")

    def _all_actual_red(self):
        return all(self.actual[d] == RED for d in self.dirs)

    def _settled(self, dirs):
        return all(self.actual[d] == self.desired[d] for d in dirs)

    # ------------------------------------------------------------ mode / degrade
    def _recompute_mode(self, now):
        new = ("DEGRADED" if self.degraded else "EMERGENCY" if self.emergencies
               else "MANUAL" if self.manual else "AUTOMATIC")
        if new != self.mode:
            self._log(now, "MODE_CHANGE", previous=self.mode, new=new)
            self.mode = new

    def _enter_degraded(self, now, reason):
        if not self.degraded:
            self.degraded, self.degraded_reason = True, reason
            self.next_degraded_retry = now
            self._log(now, "DEGRADED_ENTERED", reason=reason)
        else:
            self.degraded_reason = reason

    def _can_recover(self):
        return (self.controller_status == "ONLINE" and self.stage == ALL_RED
                and all(s != "OFFLINE" for s in self.signal_status.values())
                and self._all_actual_red())

    # -------------------------------------------------------------- scheduling
    def _phase_metrics(self, p, now):
        dirs = self.cfg.phases[p]
        score, maxwait, demand = 0.0, 0.0, False
        for v in self.vehicles.values():
            if v["direction"] in dirs:
                w = max(0.0, now - v["arrived_at"])
                demand = True
                score += VEHICLE_WEIGHT[v["vehicle_type"]] + self.cfg.wait_weight * w
                maxwait = max(maxwait, w)
        for d in dirs:  # sensor offline: queue unknown -> assume demand, fixed-time fallback
            if self.sensor_status[d] == "OFFLINE":
                w = now - self.last_served.get(p, self.created)
                demand = True
                score += 2 + self.cfg.wait_weight * w
                maxwait = max(maxwait, w)
        if maxwait >= self.cfg.max_wait_seconds:
            score += 1000  # starvation protection
        return score, maxwait, demand

    def _auto_target(self, now):
        m = {p: self._phase_metrics(p, now) for p in self.cfg.phases}
        demanding = [p for p in m if m[p][2]]
        best = lambda c: max(c, key=lambda p: (m[p][1] >= self.cfg.max_wait_seconds, m[p][0]))
        if self.stage == GREEN:
            cur, el = self.phase, now - self.stage_started
            others = [p for p in demanding if p != cur]
            if not others or el < self.cfg.min_green_seconds:
                return cur
            starving = [p for p in others if m[p][1] >= self.cfg.max_wait_seconds]
            if starving:
                return best(starving)
            if not m[cur][2] or el >= self.cfg.green_seconds:
                return best(others)
            cand = best(others)
            return cand if m[cand][0] > m[cur][0] * self.cfg.switch_hysteresis else cur
        if self.next_phase in m and m[self.next_phase][2]:
            return self.next_phase     # honour the decision that started this transition
        if demanding:
            return best(demanding)
        return self.last_green or next(iter(self.cfg.phases))

    def _choose_target(self, now):
        if self.degraded:
            return None
        if self.emergencies:
            first = min(self.emergencies.values(), key=lambda e: e["arrived_at"])
            return self.phase_of[first["direction"]]
        if self.manual:
            return self.manual["phase"]
        return self._auto_target(now)

    # ----------------------------------------------------------- state machine
    def _step(self, now):
        if self.degraded and self._can_recover():
            self.degraded, self.degraded_reason = False, None
            self._log(now, "DEGRADED_CLEARED")
        self._recompute_mode(now)
        want = self._choose_target(now)
        el = now - self.stage_started
        if self.stage == GREEN:
            if self.degraded or (want is not None and want != self.phase):
                self._begin_yellow(now, want)
        elif self.stage == YELLOW:
            if el >= self.cfg.yellow_seconds and (self.degraded or self._settled(self.cfg.phases[self.phase])):
                self._begin_all_red(now)
        else:  # ALL_RED
            if self.degraded:
                if now >= self.next_degraded_retry:
                    self.next_degraded_retry = now + self.cfg.degraded_retry_seconds
                    self._resync_red(now, "degraded-resync")
            elif el >= self.cfg.all_red_seconds and self._all_actual_red() and want is not None:
                self._grant_green(want, now)

    def _begin_yellow(self, now, want):
        self.stage, self.stage_started = YELLOW, now
        self.next_phase = want
        self._log(now, "SIGNAL_TRANSITION_STARTED", from_phase=self.phase, to_phase=want, mode=self.mode,
                  reason="fault" if self.degraded else self.mode.lower())
        for d in self.cfg.phases[self.phase]:
            self._set_desired(d, YELLOW, now, "transition")

    def _begin_all_red(self, now):
        for d in self.cfg.phases[self.phase]:
            self._set_desired(d, RED, now, "transition")
        self.last_served[self.phase] = now
        self.last_green, self.phase = self.phase, None
        self.stage, self.stage_started = ALL_RED, now

    def _grant_green(self, p, now):
        # hard gate: every conflicting direction must be CONFIRMED red
        if not all(self.actual[d] == RED and self.desired[d] == RED
                   for d in self.dirs if self.phase_of[d] != p):
            return
        for d in self.cfg.phases[p]:
            self._set_desired(d, GREEN, now, f"serve:{self.mode.lower()}")
        self.stage, self.phase, self.stage_started = GREEN, p, now
        self.next_phase = None
        self._log(now, "PHASE_GREEN_GRANTED", new=p, mode=self.mode)

    def _finish(self, now):
        self._step(now)
        self._assert_safe()

    # ------------------------------------------------------------ sensor events
    def record_rejection(self, payload, reason, now):
        self._log(now, "SENSOR_EVENT_REJECTED", payload.get("direction") if isinstance(payload, dict) else None,
                  reason=reason, payload=payload)

    def _validate_sensor(self, ev):
        if not isinstance(ev, dict):
            raise ValidationError("body must be a JSON object")
        for k in ("event_id", "vehicle_id"):
            if not isinstance(ev.get(k), str) or not ev[k].strip():
                raise ValidationError(f"missing or invalid field '{k}'")
        if ev.get("direction") not in self.dirs:
            raise ValidationError(f"unknown direction {ev.get('direction')!r}; expected one of {self.dirs}")
        if ev.get("event_type") not in SENSOR_EVENT_TYPES:
            raise ValidationError(f"unknown event_type {ev.get('event_type')!r}")
        if ev["event_type"] == "VEHICLE_ARRIVED" and ev.get("vehicle_type") not in VEHICLE_WEIGHT:
            raise ValidationError(f"unknown vehicle_type {ev.get('vehicle_type')!r}")
        seq = ev.get("sequence_no")
        if seq is not None and (isinstance(seq, bool) or not isinstance(seq, int)):
            raise ValidationError("sequence_no must be an integer")
        return parse_ts(ev.get("timestamp"))

    def _remember(self, key):
        self.seen[key] = True
        while len(self.seen) > MAX_SEEN_EVENTS:
            self.seen.popitem(last=False)

    def handle_sensor_event(self, ev, now):
        ts = self._validate_sensor(ev)
        eid, vid, d = ev["event_id"], ev["vehicle_id"], ev["direction"]
        if eid in self.seen:
            self._log(now, "DUPLICATE_EVENT_IGNORED", d, event_id=eid)
            return {"result": "DUPLICATE", "reason": "event_id already processed"}
        self._remember(eid)
        if now - ts > self.cfg.event_max_age_seconds:
            self._log(now, "SENSOR_EVENT_REJECTED", d, event_id=eid, reason="delayed beyond max age")
            return {"result": "IGNORED", "reason": "event is older than the allowed maximum age"}
        ts = min(ts, now)  # a sensor clock in the future is clamped to server time
        seq = ev.get("sequence_no")
        if seq is not None:
            last = self.last_seq.get(d)
            if last is not None and seq < last:
                self._log(now, "OUT_OF_ORDER_EVENT", d, event_id=eid, sequence_no=seq, last_sequence_no=last)
            self.last_seq[d] = max(seq, last if last is not None else seq)
        if ev["event_type"] == "VEHICLE_ARRIVED":
            res = self._arrival(ev, vid, d, ts, now)
        else:
            res = self._clearance(ev, vid, d, ts, now)
        self._finish(now)
        return res

    def _arrival(self, ev, vid, d, ts, now):
        vtype = ev["vehicle_type"]
        if vid in self.vehicles:
            self.vehicles[vid]["last_seen"] = now
            if vid in self.emergencies:
                self.emergencies[vid]["last_seen"] = now
                self._log(now, "EMERGENCY_REFRESHED", d, vehicle_id=vid)
            self._log(now, "SENSOR_EVENT_REJECTED", d, event_id=ev["event_id"], reason="vehicle already queued")
            return {"result": "IGNORED", "reason": "vehicle already queued (repeated arrival)"}
        if vid in self.cleared and self.cleared[vid] >= ts:
            self._log(now, "SENSOR_EVENT_REJECTED", d, event_id=ev["event_id"],
                      reason="arrival older than recorded clearance (out-of-order)")
            return {"result": "IGNORED", "reason": "arrival is older than a recorded clearance"}
        self.cleared.pop(vid, None)
        self.vehicles[vid] = {"direction": d, "vehicle_type": vtype, "arrived_at": ts, "last_seen": now}
        self._log(now, "VEHICLE_DETECTED", d, vehicle_id=vid, vehicle_type=vtype, event_id=ev["event_id"])
        if vtype == "EMERGENCY":
            self.emergencies[vid] = {"direction": d, "arrived_at": ts, "last_seen": now}
            self._log(now, "EMERGENCY_DETECTED", d, vehicle_id=vid)
        return {"result": "APPLIED", "reason": "vehicle queued"}

    def _clearance(self, ev, vid, d, ts, now):
        if vid not in self.vehicles:
            self.cleared[vid] = max(ts, self.cleared.get(vid, 0))  # tombstone for a late arrival
            self._log(now, "SENSOR_EVENT_REJECTED", d, event_id=ev["event_id"],
                      reason="clearance without matching arrival")
            return {"result": "IGNORED", "reason": "no matching queued vehicle (orphan clearance)"}
        v = self.vehicles.pop(vid)
        self.cleared[vid] = ts
        self._log(now, "VEHICLE_CLEARED", v["direction"], vehicle_id=vid, event_id=ev["event_id"])
        if vid in self.emergencies:
            self._clear_emergency(vid, now, "vehicle cleared")
        return {"result": "APPLIED", "reason": "vehicle removed from queue"}

    def _clear_emergency(self, vid, now, reason):
        e = self.emergencies.pop(vid)
        self._log(now, "EMERGENCY_CLEARED", e["direction"], vehicle_id=vid, reason=reason)

    # ---------------------------------------------------------- manual commands
    def handle_command(self, ev, now):
        if not isinstance(ev, dict):
            raise ValidationError("body must be a JSON object")
        cmd, actor = ev.get("command"), str(ev.get("actor") or "admin")
        if cmd == "MANUAL_GREEN_REQUEST":
            d = ev.get("direction")
            if d not in self.dirs:
                raise ValidationError(f"unknown direction {d!r}; expected one of {self.dirs}")
            if self.degraded:
                raise ConflictError("junction is DEGRADED; manual green is refused until safe state is confirmed")
            if self.emergencies:
                raise ConflictError("emergency in progress; manual control refused")
            prev = self.manual
            self.manual = {"direction": d, "phase": self.phase_of[d], "actor": actor, "started": now,
                           "expires": now + self.cfg.manual_timeout_seconds}
            self._log(now, "MANUAL_OVERRIDE", d, actor=actor, phase=self.manual["phase"],
                      replaced_actor=prev["actor"] if prev else None)
        elif cmd == "RETURN_TO_AUTOMATIC":
            if self.manual:
                self._log(now, "RETURN_TO_AUTOMATIC", actor=actor)
                self.manual = None
            if self.degraded:
                self.next_degraded_retry = now  # retry safe-state confirmation now
        else:
            raise ValidationError(f"unknown command {cmd!r}")
        self._finish(now)
        return {"result": "ACCEPTED", "mode": self.mode}

    # -------------------------------------------------------- controller events
    def handle_controller_event(self, ev, now):
        if not isinstance(ev, dict):
            raise ValidationError("body must be a JSON object")
        if "command_id" in ev:
            res = self._ack(ev, now)
        elif "device_type" in ev:
            res = self._status(ev, now)
        else:
            raise ValidationError("expected 'command_id' (acknowledgement) or 'device_type' (status event)")
        self._finish(now)
        return res

    def _ack(self, ev, now):
        cid, status = ev["command_id"], ev.get("status")
        if status not in ("ACK", "NACK", "FAILED"):
            raise ValidationError("status must be ACK, NACK or FAILED")
        actual = ev.get("actual_state")
        if actual is not None and actual not in (RED, YELLOW, GREEN):
            raise ValidationError("actual_state must be RED, YELLOW or GREEN")
        c = self.commands.get(cid)
        if c is None:
            self._log(now, "CONTROLLER_EVENT_REJECTED", command_id=cid, reason="unknown command_id")
            return {"result": "IGNORED", "reason": "unknown command_id"}
        d = c["direction"]
        late = c["status"] == "TIMED_OUT" and c["requested_state"] == self.desired[d] and status == "ACK"
        if c["status"] in ("ACKED", "FAILED"):
            self._log(now, "DUPLICATE_ACK_IGNORED", d, command_id=cid)
            return {"result": "DUPLICATE", "reason": "command already resolved"}
        if c["status"] != "PENDING" and not late:
            # superseded / abandoned / stale timed-out: must NOT change actual state
            self._log(now, "STALE_ACK_IGNORED", d, command_id=cid, command_status=c["status"])
            return {"result": "IGNORED", "reason": f"command is {c['status']}"}
        self.controller_status = "ONLINE"
        c["updated_at"] = now
        self._dirty.add(cid)
        if status == "ACK" and (actual is None or actual == c["requested_state"]):
            c["status"] = "ACKED"
            prev = self.actual[d]
            self.actual[d] = c["requested_state"]
            self._log(now, "SIGNAL_CONFIRMED", d, prev, self.actual[d], cid, late=late)
        else:
            c["status"] = "FAILED"
            self.actual[d] = actual if actual else UNKNOWN
            self._log(now, "COMMAND_FAILED", d, c["requested_state"], actual, cid, status=status)
            self._enter_degraded(now, f"command {cid} failed ({status}, actual={actual})")
        return {"result": "APPLIED", "reason": f"command {c['status']}"}

    def _status(self, ev, now):
        dt, st, d = ev["device_type"], ev.get("status"), ev.get("direction")
        if dt not in DEVICE_TYPES:
            raise ValidationError(f"device_type must be one of {sorted(DEVICE_TYPES)}")
        if st not in DEVICE_STATUSES:
            raise ValidationError(f"status must be one of {sorted(DEVICE_STATUSES)}")
        if d is not None and d not in self.dirs:
            raise ValidationError(f"unknown direction {d!r}")
        if dt == "SENSOR" and d is None:
            raise ValidationError("SENSOR status requires a direction")
        eid = ev.get("event_id")
        if eid:
            if eid in self.seen:
                self._log(now, "DUPLICATE_EVENT_IGNORED", d, event_id=eid)
                return {"result": "DUPLICATE", "reason": "event_id already processed"}
            self._remember(eid)
        if dt == "SENSOR":
            self._log(now, "SENSOR_STATUS", d, self.sensor_status[d], st)
            self.sensor_status[d] = st
            return {"result": "APPLIED", "reason": "sensor status updated"}
        targets = [d] if d else self.dirs
        if st == "ONLINE":
            if d:
                self.signal_status[d] = st
            else:
                self.controller_status = st
                for x in self.dirs:
                    self.signal_status[x] = "ONLINE" if self.signal_status[x] != "OFFLINE" else self.signal_status[x]
            for x in targets:
                self.actual[x] = UNKNOWN   # reconnect: never trust old physical state
            self._log(now, "DEVICE_ONLINE", d, device_type=dt)
            for c in self._pending():
                if d is None or c["direction"] == d:
                    c["status"], c["updated_at"] = "SUPERSEDED", now
                    self._dirty.add(c["command_id"])
            self.next_degraded_retry = now
            self._safe_resync(now, "reconnect-resync")
        else:
            if d:
                self.signal_status[d] = st
            else:
                self.controller_status = st
            if st in ("OFFLINE", UNKNOWN):
                for x in targets:
                    self.actual[x] = UNKNOWN
            self._log(now, "DEVICE_FAILURE", d, device_type=dt, status=st)
            self._enter_degraded(now, f"{dt}{'/' + d if d else ''} reported {st}")
        return {"result": "APPLIED", "reason": "device status updated"}

    # --------------------------------------------------------------------- tick
    def tick(self, now):
        if self.manual and now >= self.manual["expires"]:
            self._log(now, "MANUAL_EXPIRED", actor=self.manual["actor"])
            self.manual = None
        for vid in [v for v, e in self.emergencies.items()
                    if now - e["last_seen"] > self.cfg.emergency_stale_seconds]:
            self._clear_emergency(vid, now, "stale emergency (timeout)")
            self.vehicles.pop(vid, None)
        for vid in [v for v, t in self.cleared.items() if now - t > self.cfg.event_max_age_seconds * 2]:
            del self.cleared[vid]
        for c in self._pending():
            if now < c["deadline"]:
                continue
            d = c["direction"]
            if c["attempts"] <= self.cfg.max_retries:
                c["attempts"] += 1
                c["deadline"], c["updated_at"] = now + self.cfg.ack_timeout_seconds, now
                self._outbox.append(dict(c))   # same command_id: ACKs stay correlated
                self._log(now, "COMMAND_RETRY", d, command_id=c["command_id"], attempt=c["attempts"])
            else:
                c["status"], c["updated_at"] = "TIMED_OUT", now
                self.actual[d] = UNKNOWN
                self.controller_status = UNKNOWN
                self._log(now, "COMMAND_TIMEOUT", d, c["requested_state"], None, c["command_id"])
                self._enter_degraded(now, f"no ACK for {c['command_id']} ({d} -> {c['requested_state']})")
            self._dirty.add(c["command_id"])
        if len(self.commands) > 400:
            old = sorted((c for c in self.commands.values() if c["status"] != "PENDING"),
                         key=lambda c: c["created_at"])[:-200]
            for c in old:
                del self.commands[c["command_id"]]
        self._finish(now)

    # ------------------------------------------------------------ persistence
    def snapshot(self):
        keep = sorted(self.commands.values(), key=lambda c: c["created_at"])[-150:]
        return {"vehicles": self.vehicles, "cleared": self.cleared, "seen": list(self.seen),
                "last_seq": self.last_seq, "desired": self.desired, "actual": self.actual,
                "stage": self.stage, "phase": self.phase, "stage_started": self.stage_started,
                "last_green": self.last_green, "last_served": self.last_served,
                "degraded": self.degraded, "degraded_reason": self.degraded_reason,
                "manual": self.manual, "emergencies": self.emergencies,
                "controller_status": self.controller_status, "sensor_status": self.sensor_status,
                "signal_status": self.signal_status, "commands": keep, "mode": self.mode,
                "created": self.created}

    @classmethod
    def restore(cls, cfg, snap, now):
        """Recovery policy: queues/manual/emergency/history survive; physical state does NOT.
        Actual state becomes UNKNOWN, pending commands are abandoned, and the junction
        is walked to ALL RED through YELLOW (if it may have been green) before serving again."""
        e = cls(cfg, now)
        e.created = snap.get("created", now)
        e.vehicles, e.cleared = snap.get("vehicles", {}), snap.get("cleared", {})
        e.seen = OrderedDict((k, True) for k in snap.get("seen", []))
        e.last_seq = snap.get("last_seq", {})
        e.desired = {d: snap.get("desired", {}).get(d, RED) for d in e.dirs}
        e.last_green, e.last_served = snap.get("last_green"), snap.get("last_served", {})
        e.degraded, e.degraded_reason = snap.get("degraded", False), snap.get("degraded_reason")
        e.manual, e.emergencies = snap.get("manual"), snap.get("emergencies", {})
        e.sensor_status.update(snap.get("sensor_status", {}))
        e.signal_status.update(snap.get("signal_status", {}))
        e.commands = {c["command_id"]: c for c in snap.get("commands", [])}
        for c in e._pending():
            c["status"], c["updated_at"] = "ABANDONED", now
            e._dirty.add(c["command_id"])
        old_stage, old_phase = snap.get("stage"), snap.get("phase")
        e._log(now, "RECOVERY", previous=old_stage, last_phase=old_phase,
               note="physical state treated as UNKNOWN; pending commands abandoned")
        e.stage, e.phase = (old_stage, old_phase) if old_phase in cfg.phases else (ALL_RED, None)
        e._safe_resync(now, "recovery")
        e._finish(now)
        return e

    # ------------------------------------------------------------------ status
    def status(self, now):
        el = now - self.stage_started
        total = {GREEN: self.cfg.green_seconds, YELLOW: self.cfg.yellow_seconds,
                 ALL_RED: self.cfg.all_red_seconds}[self.stage]
        detail = {d: {} for d in self.dirs}
        for v in self.vehicles.values():
            detail[v["direction"]][v["vehicle_type"]] = detail[v["direction"]].get(v["vehicle_type"], 0) + 1
        pend = self._pending()
        alerts = []
        add = lambda lvl, code, msg, d=None: alerts.append({"level": lvl, "code": code, "message": msg, "direction": d})
        if self.controller_status in ("OFFLINE", UNKNOWN, "DEGRADED", "WARNING"):
            add("critical" if self.controller_status == "OFFLINE" else "warning", "CONTROLLER_" + self.controller_status,
                f"Controller status is {self.controller_status}")
        for d in self.dirs:
            if self.signal_status[d] != "ONLINE":
                add("critical", "SIGNAL_FAILURE", f"Signal {d} is {self.signal_status[d]}", d)
            if self.sensor_status[d] != "ONLINE":
                add("warning", "SENSOR_FAILURE", f"Sensor {d} is {self.sensor_status[d]} (queue unreliable)", d)
            if self.actual[d] == UNKNOWN:
                add("warning", "UNKNOWN_STATE", f"Physical state of {d} is unconfirmed", d)
            elif self.actual[d] != self.desired[d]:
                age = max([now - c["created_at"] for c in pend if c["direction"] == d] or [99])
                if age > 2:
                    add("warning", "STATE_MISMATCH", f"{d}: desired {self.desired[d]} but actual {self.actual[d]}", d)
        for c in self.commands.values():
            if c["status"] == "TIMED_OUT" and now - c["updated_at"] < 120:
                add("critical", "COMMAND_TIMEOUT", f"{c['command_id']} ({c['direction']}->{c['requested_state']}) not acknowledged", c["direction"])
        for vid, em in self.emergencies.items():
            add("critical", "EMERGENCY", f"Emergency vehicle {vid} approaching from {em['direction']}", em["direction"])
        if self.manual:
            add("info", "MANUAL_OVERRIDE", f"Manual override by {self.manual['actor']} ({self.manual['direction']}), "
                f"expires in {max(0, int(self.manual['expires'] - now))}s", self.manual["direction"])
        if self.degraded:
            add("critical", "DEGRADED", self.degraded_reason or "degraded")
        health = ("DEGRADED" if self.degraded else "ONLINE" if self.controller_status == "ONLINE" else self.controller_status)
        return {"junction_id": self.cfg.junction_id, "name": self.cfg.name, "mode": self.mode,
                "phase": self.phase, "stage": self.stage, "stage_remaining_seconds": round(max(0.0, total - el), 1),
                "controller_status": self.controller_status, "health": health,
                "desired_signals": dict(self.desired), "actual_signals": dict(self.actual),
                "queues": self.queues(), "queue_details": detail,
                "emergencies": [{"vehicle_id": v, **e} for v, e in self.emergencies.items()],
                "manual": self.manual, "degraded_reason": self.degraded_reason,
                "pending_commands": [dict(c) for c in pend],
                "device_status": {"sensors": dict(self.sensor_status), "signals": dict(self.signal_status)},
                "directions": self.dirs, "phases": self.cfg.phases, "alerts": alerts,
                "server_time": iso(now)}
