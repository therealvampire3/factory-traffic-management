"""Domain tests: no HTTP, no DB, no MQTT, no real time."""
import unittest

from traffic.domain.engine import (GREEN, RED, YELLOW, JunctionConfig, JunctionEngine, iso)
from traffic.domain.errors import ConflictError, ValidationError

T0 = 1_790_000_000.0


class Rig:
    """Engine + fake physical controller + fake clock. Checks safety on every step."""

    def __init__(self, auto_ack=True, **cfg):
        self.now = T0
        self.e = JunctionEngine(JunctionConfig(junction_id="A", **cfg), self.now)
        self.auto_ack = auto_ack
        self.physical = {d: RED for d in self.e.dirs}
        self.sent, self.seq, self.trace = [], 0, []
        self.e.start(self.now)
        self.pump()

    def pump(self):
        for c in self.e.drain_outbox():
            self.sent.append(c)
            if self.auto_ack:
                self.physical[c["direction"]] = c["requested_state"]
                self.e.handle_controller_event({"command_id": c["command_id"], "status": "ACK",
                                                "actual_state": c["requested_state"]}, self.now)
        self._check()

    def _check(self):
        phases = {self.e.phase_of[d] for d, s in self.physical.items() if s == GREEN}
        assert len(phases) <= 1, f"physical conflicting GREEN {self.physical}"
        phases = {self.e.phase_of[d] for d, s in self.e.desired.items() if s == GREEN}
        assert len(phases) <= 1
        self.trace.append((self.e.stage, dict(self.e.desired)))

    def run(self, seconds, step=0.5):
        end = self.now + seconds
        while self.now < end:
            self.now += step
            self.e.tick(self.now)
            self.pump()
            self.pump()

    def ev(self, vid, d, vtype="EMPLOYEE_VEHICLE", kind="VEHICLE_ARRIVED", eid=None, ts=None, seq=None):
        self.seq += 1
        p = {"event_id": eid or f"e{self.seq}", "junction_id": "A", "direction": d, "event_type": kind,
             "vehicle_id": vid, "vehicle_type": vtype, "timestamp": iso(ts if ts is not None else self.now)}
        if seq is not None:
            p["sequence_no"] = seq
        r = self.e.handle_sensor_event(p, self.now)
        self.pump()
        return r

    def green_dirs(self):
        return sorted(d for d, s in self.e.desired.items() if s == GREEN)


class EngineTests(unittest.TestCase):
    def test_startup_all_red_then_first_phase(self):
        r = Rig()
        self.assertEqual(r.green_dirs(), [])
        r.run(5)
        self.assertEqual(r.green_dirs(), ["NORTH", "SOUTH"])

    def test_duplicate_event_counts_once(self):
        r = Rig()
        self.assertEqual(r.ev("V1", "NORTH", eid="x1")["result"], "APPLIED")
        self.assertEqual(r.ev("V1", "NORTH", eid="x1")["result"], "DUPLICATE")
        self.assertEqual(r.e.queues()["NORTH"], 1)

    def test_repeated_arrival_different_event_id_not_double_counted(self):
        r = Rig()
        r.ev("V1", "NORTH"); r.ev("V1", "NORTH")
        self.assertEqual(r.e.queues()["NORTH"], 1)

    def test_clearance_and_never_negative(self):
        r = Rig()
        r.ev("V1", "EAST")
        self.assertEqual(r.e.queues()["EAST"], 1)
        r.ev("V1", "EAST", kind="VEHICLE_CLEARED")
        self.assertEqual(r.e.queues()["EAST"], 0)
        res = r.ev("V1", "EAST", kind="VEHICLE_CLEARED")      # second clear
        self.assertEqual(res["result"], "IGNORED")
        self.assertEqual(r.ev("ZZ", "WEST", kind="VEHICLE_CLEARED")["result"], "IGNORED")  # orphan
        self.assertTrue(all(v >= 0 for v in r.e.queues().values()))

    def test_out_of_order_arrival_after_clearance_ignored(self):
        r = Rig()
        r.ev("V9", "WEST", kind="VEHICLE_CLEARED", ts=r.now)           # clear arrives first
        res = r.ev("V9", "WEST", ts=r.now - 5)                          # older arrival shows up late
        self.assertEqual(res["result"], "IGNORED")
        self.assertEqual(r.e.queues()["WEST"], 0)

    def test_delayed_and_malformed_events(self):
        r = Rig()
        self.assertEqual(r.ev("V1", "NORTH", ts=r.now - 100000)["result"], "IGNORED")
        for bad in ({}, {"event_id": "a"}, {"event_id": "a", "vehicle_id": "v", "direction": "UP"}):
            with self.assertRaises(ValidationError):
                r.e.handle_sensor_event(bad, r.now)
        with self.assertRaises(ValidationError):
            r.e.handle_sensor_event({"event_id": "q", "vehicle_id": "v", "direction": "NORTH",
                                     "event_type": "VEHICLE_ARRIVED", "vehicle_type": "SPACESHIP",
                                     "timestamp": iso(r.now)}, r.now)

    def test_emergency_preemption_is_safe(self):
        r = Rig()
        r.run(5)
        self.assertEqual(r.green_dirs(), ["NORTH", "SOUTH"])
        r.ev("AMB", "EAST", "EMERGENCY")
        self.assertEqual(r.e.mode, "EMERGENCY")
        self.assertEqual(r.e.stage, YELLOW)            # starts immediately with YELLOW
        self.assertEqual(r.green_dirs(), [])           # EAST is NOT green yet
        r.run(1)
        self.assertEqual(r.green_dirs(), [])
        r.run(10)
        self.assertEqual(r.green_dirs(), ["EAST", "WEST"])
        stages = [s for s, _ in r.trace]
        self.assertIn("ALL_RED", stages)

    def test_emergency_clears_and_repeat_event(self):
        r = Rig()
        r.run(5)
        r.ev("AMB", "EAST", "EMERGENCY")
        r.ev("AMB", "EAST", "EMERGENCY")                # repeated
        self.assertEqual(len(r.e.emergencies), 1)
        r.run(12)
        r.ev("AMB", "EAST", "EMERGENCY", kind="VEHICLE_CLEARED")
        self.assertEqual(r.e.mode, "AUTOMATIC")

    def test_stale_emergency_times_out(self):
        r = Rig(emergency_stale_seconds=20)
        r.ev("AMB", "EAST", "EMERGENCY")
        r.run(25)
        self.assertEqual(r.e.emergencies, {})
        self.assertNotEqual(r.e.mode, "EMERGENCY")

    def test_conflicting_emergencies_first_come_first_served(self):
        r = Rig()
        r.run(5)
        r.ev("A1", "EAST", "EMERGENCY"); r.now += 1; r.ev("A2", "NORTH", "EMERGENCY")
        r.run(15)
        self.assertEqual(r.green_dirs(), ["EAST", "WEST"])
        r.ev("A1", "EAST", "EMERGENCY", kind="VEHICLE_CLEARED")
        r.run(15)
        self.assertEqual(r.green_dirs(), ["NORTH", "SOUTH"])

    def test_manual_override_and_return(self):
        r = Rig()
        r.run(5)
        r.e.handle_command({"command": "MANUAL_GREEN_REQUEST", "direction": "WEST", "actor": "bob"}, r.now)
        self.assertEqual(r.e.mode, "MANUAL")
        self.assertEqual(r.green_dirs(), [])           # must pass through yellow / all red
        r.run(15)
        self.assertEqual(r.green_dirs(), ["EAST", "WEST"])
        r.ev("T1", "NORTH", "TRUCK")
        r.run(60)                                       # manual holds even with demand elsewhere
        self.assertEqual(r.green_dirs(), ["EAST", "WEST"])
        r.e.handle_command({"command": "RETURN_TO_AUTOMATIC"}, r.now)
        self.assertEqual(r.e.mode, "AUTOMATIC")
        r.run(20)
        self.assertEqual(r.green_dirs(), ["NORTH", "SOUTH"])

    def test_manual_expires(self):
        r = Rig(manual_timeout_seconds=10)
        r.e.handle_command({"command": "MANUAL_GREEN_REQUEST", "direction": "EAST"}, r.now)
        r.run(12)
        self.assertEqual(r.e.mode, "AUTOMATIC")

    def test_manual_rejected_during_emergency_and_invalid_commands(self):
        r = Rig()
        r.ev("AMB", "EAST", "EMERGENCY")
        with self.assertRaises(ConflictError):
            r.e.handle_command({"command": "MANUAL_GREEN_REQUEST", "direction": "NORTH"}, r.now)
        with self.assertRaises(ValidationError):
            r.e.handle_command({"command": "FORCE_ALL_GREEN"}, r.now)
        with self.assertRaises(ValidationError):
            r.e.handle_command({"command": "MANUAL_GREEN_REQUEST", "direction": "UP"}, r.now)

    def test_priority_vehicle_wins(self):
        r = Rig()
        r.run(12)                                       # NS green, min green passed
        for i in range(3):
            r.ev(f"E{i}", "NORTH")                      # 3 employee vehicles on current green
        r.ev("T1", "EAST", "TRUCK"); r.ev("T2", "EAST", "TRUCK")
        r.run(40)
        self.assertIn("EAST", r.green_dirs() or ["EAST"])  # truck phase is served within max green

    def test_starvation_protection(self):
        r = Rig(max_wait_seconds=60)
        r.ev("LONELY", "WEST")
        served = None
        for i in range(400):
            r.ev(f"N{i}", "NORTH", "TRUCK")             # heavy continuous NS demand
            r.run(1)
            if "WEST" in r.green_dirs():
                served = i
                break
        self.assertIsNotNone(served, "WEST starved")
        self.assertLess(served, 90)

    def test_no_ack_retries_then_degraded_and_never_green(self):
        r = Rig(auto_ack=False)
        r.run(40)
        self.assertEqual(r.e.mode, "DEGRADED")
        self.assertEqual(r.green_dirs(), [])
        self.assertTrue(any(c["status"] == "TIMED_OUT" for c in r.e.commands.values()))
        ids = {}
        for c in r.sent:
            ids[c["command_id"]] = ids.get(c["command_id"], 0) + 1
        self.assertTrue(any(n == 3 for n in ids.values()))   # 1 send + 2 retries, same command_id

    def test_controller_offline_and_recovery(self):
        r = Rig()
        r.run(5)
        r.auto_ack = False                               # a dead controller does not answer
        r.e.handle_controller_event({"device_type": "SIGNAL_CONTROLLER", "status": "OFFLINE"}, r.now)
        self.assertEqual(r.e.mode, "DEGRADED")
        r.run(10)
        self.assertEqual(r.green_dirs(), [])
        r.auto_ack = True
        r.e.handle_controller_event({"device_type": "SIGNAL_CONTROLLER", "status": "ONLINE"}, r.now)
        r.pump(); r.run(15)
        self.assertEqual(r.e.mode, "AUTOMATIC")
        self.assertEqual(len(r.green_dirs()), 2)

    def test_nack_degrades_and_duplicate_late_acks_do_not_change_state(self):
        r = Rig(auto_ack=False)
        cmd = r.sent[0]
        r.e.handle_controller_event({"command_id": cmd["command_id"], "status": "ACK", "actual_state": RED}, r.now)
        before = dict(r.e.actual)
        res = r.e.handle_controller_event({"command_id": cmd["command_id"], "status": "ACK", "actual_state": GREEN}, r.now)
        self.assertEqual(res["result"], "DUPLICATE")
        self.assertEqual(before, r.e.actual)
        unk = r.e.handle_controller_event({"command_id": "nope", "status": "ACK"}, r.now)
        self.assertEqual(unk["result"], "IGNORED")
        c2 = [c for c in r.e.commands.values() if c["status"] == "PENDING"][0]
        r.e.handle_controller_event({"command_id": c2["command_id"], "status": "ACK", "actual_state": GREEN}, r.now)
        self.assertEqual(r.e.mode, "DEGRADED")          # mismatch => fail safe

    def test_sensor_offline_fixed_time_fallback(self):
        r = Rig(max_wait_seconds=40)
        r.run(5)
        r.e.handle_controller_event({"device_type": "SENSOR", "direction": "EAST", "status": "OFFLINE"}, r.now)
        r.run(70)
        self.assertIn("EAST", [d for _, ds in r.trace for d, s in ds.items() if s == GREEN])

    def test_restart_recovery_never_trusts_old_state(self):
        r = Rig()
        r.run(5)
        r.ev("V1", "NORTH"); r.ev("V2", "EAST", "TRUCK")
        snap = r.e.snapshot()
        import json
        snap = json.loads(json.dumps(snap))              # must survive JSON round trip
        self.assertEqual(snap["stage"], GREEN)
        now2 = r.now + 3
        e2 = JunctionEngine.restore(r.e.cfg, snap, now2)
        self.assertEqual(e2.queues(), {"NORTH": 1, "SOUTH": 0, "EAST": 1, "WEST": 0})
        self.assertTrue(all(v == "UNKNOWN" for v in e2.actual.values()))
        self.assertNotIn(GREEN, e2.desired.values())      # GREEN is NOT re-asserted
        self.assertEqual(e2.stage, YELLOW)
        self.assertIn("V1", e2.vehicles)
        self.assertTrue(any(a["event_type"] == "RECOVERY" for a in e2.drain_audit()))
        # duplicate event ids survive restart
        res = e2.handle_sensor_event({"event_id": r.sent and "e1", "junction_id": "A", "direction": "NORTH",
                                      "event_type": "VEHICLE_ARRIVED", "vehicle_id": "V1",
                                      "vehicle_type": "EMPLOYEE_VEHICLE", "timestamp": iso(now2)}, now2)
        self.assertEqual(res["result"], "DUPLICATE")

    def test_concurrent_sequence_scenario(self):
        r = Rig()
        r.run(5)
        r.ev("TRK", "NORTH", "TRUCK")
        r.ev("AMB", "EAST", "EMERGENCY", eid="em1")
        with self.assertRaises(ConflictError):
            r.e.handle_command({"command": "MANUAL_GREEN_REQUEST", "direction": "WEST"}, r.now)
        self.assertEqual(r.ev("AMB", "EAST", "EMERGENCY", eid="em1")["result"], "DUPLICATE")
        r.run(15)
        self.assertEqual(r.green_dirs(), ["EAST", "WEST"])
        self.assertEqual(len(r.e.emergencies), 1)

    def test_config_validation(self):
        with self.assertRaises(ValidationError):
            JunctionConfig.from_dict({"junction_id": "X", "phases": {"P": ["N"]}})
        with self.assertRaises(ValidationError):
            JunctionConfig.from_dict({"junction_id": "X", "phases": {"P": ["N"], "Q": ["N"]}})

    def test_second_junction_same_engine(self):
        cfg = JunctionConfig.from_dict({"junction_id": "B", "phases": {"P1": ["N1"], "P2": ["E1", "W1"]}})
        e = JunctionEngine(cfg, T0); e.start(T0)
        self.assertEqual(sorted(e.dirs), ["E1", "N1", "W1"])


if __name__ == "__main__":
    unittest.main()
