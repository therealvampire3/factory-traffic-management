"""Integration tests: service + SQLite + simulator + HTTP (no browser, no MQTT)."""
import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from traffic.adapters.simulator import SimulatedController
from traffic.domain.engine import iso
from traffic.domain.errors import ConflictError
from traffic.server import make_server
from traffic.service import JunctionService, NotFound
from traffic.store import Store
import time


def build(path, auto_ack=True):
    ctrl = SimulatedController(auto_ack=auto_ack, ack_delay=0.01)
    svc = JunctionService(Store(path), ctrl)
    svc.bootstrap()
    return svc, ctrl


def ev(i, d, vtype="TRUCK", kind="VEHICLE_ARRIVED", vid=None):
    return {"event_id": f"evt-{i}", "junction_id": "A", "direction": d, "event_type": kind,
            "vehicle_id": vid or f"VH-{i}", "vehicle_type": vtype, "sequence_no": i, "timestamp": iso(time.time())}


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, "t.db")

    def test_concurrent_events_stay_consistent(self):
        svc, _ = build(self.db)
        errors = []

        def worker(k):
            try:
                svc.sensor_event(ev(k, "NORTH", "TRUCK"))
                svc.sensor_event(ev(1000 + k, "EAST", "EMERGENCY", vid="AMB"))      # same logical vehicle
                svc.sensor_event(ev(1000 + k, "EAST", "EMERGENCY", vid="AMB"))      # duplicate event id
                try:
                    svc.command("A", {"command": "MANUAL_GREEN_REQUEST", "direction": "WEST"})
                except ConflictError:
                    pass
                svc.tick_all()
            except Exception as e:  # noqa
                errors.append(e)
        ts = [threading.Thread(target=worker, args=(k,)) for k in range(20)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual(errors, [])
        st = svc.status("A")
        self.assertEqual(st["queues"]["NORTH"], 20)
        self.assertEqual(st["queues"]["EAST"], 1)
        self.assertEqual(len(st["emergencies"]), 1)
        greens = {d for d, s in st["desired_signals"].items() if s == "GREEN"}
        self.assertTrue(greens <= {"NORTH", "SOUTH"} or greens <= {"EAST", "WEST"})

    def test_restart_preserves_state_and_history_but_not_physical(self):
        svc, _ = build(self.db)
        svc.sensor_event(ev(1, "NORTH", "TRUCK"))
        svc.sensor_event(ev(2, "WEST", "FORKLIFT"))
        before = len(svc.history("A", 1000))
        svc2, _ = build(self.db)                         # "restart"
        st = svc2.status("A")
        self.assertEqual(st["queues"], {"NORTH": 1, "SOUTH": 0, "EAST": 0, "WEST": 1})
        self.assertGreaterEqual(len(svc2.history("A", 1000)), before)
        self.assertEqual(svc2.history("A", 5)[-1]["event_type"] != "", True)
        self.assertNotIn("GREEN", st["desired_signals"].values())
        self.assertTrue(any(h["event_type"] == "RECOVERY" for h in svc2.history("A", 1000)))
        self.assertEqual(svc2.sensor_event(ev(1, "NORTH", "TRUCK"))["result"], "DUPLICATE")

    def test_unknown_junction(self):
        svc, _ = build(self.db)
        with self.assertRaises(NotFound):
            svc.status("Z")


class HttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.svc, cls.ctrl = build(os.path.join(cls.tmp, "h.db"))
        cls.srv = make_server(cls.svc, cls.ctrl, "127.0.0.1", 0)
        cls.base = f"http://127.0.0.1:{cls.srv.server_address[1]}"
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def call(self, method, path, body=None):
        req = urllib.request.Request(self.base + path, method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as r:
                return r.status, json.loads(r.read() or b"null")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_status_codes(self):
        self.assertEqual(self.call("GET", "/api/junctions")[0], 200)
        self.assertEqual(self.call("GET", "/api/junctions/A/status")[0], 200)
        self.assertEqual(self.call("GET", "/api/junctions/NOPE/status")[0], 404)
        c, b = self.call("POST", "/api/sensor-events", ev(501, "NORTH"))
        self.assertEqual(c, 201)
        self.assertEqual(self.call("POST", "/api/sensor-events", ev(501, "NORTH"))[0], 200)       # duplicate
        self.assertEqual(self.call("POST", "/api/sensor-events", {"junction_id": "A"})[0], 400)     # malformed
        bad = ev(502, "NORTH"); bad["junction_id"] = "Q"
        self.assertEqual(self.call("POST", "/api/sensor-events", bad)[0], 404)
        bad = ev(503, "NORTH", "SPACESHIP")
        self.assertEqual(self.call("POST", "/api/sensor-events", bad)[0], 400)
        self.assertEqual(self.call("POST", "/api/junctions/A/commands", {"command": "SET_ALL_GREEN"})[0], 400)
        self.assertEqual(self.call("POST", "/api/junctions/A/commands",
                                   {"command": "MANUAL_GREEN_REQUEST", "direction": "WEST"})[0], 202)
        self.assertEqual(self.call("POST", "/api/junctions/A/commands", {"command": "RETURN_TO_AUTOMATIC"})[0], 202)
        c, h = self.call("GET", "/api/junctions/A/history?limit=5")
        self.assertEqual(c, 200); self.assertLessEqual(len(h), 5)
        self.assertEqual(self.call("POST", "/api/controller-events", {"command_id": "zzz", "junction_id": "A", "status": "ACK"})[0], 202)
        self.assertEqual(self.call("POST", "/api/junctions", {"junction_id": "B"})[0], 201)
        self.assertEqual(self.call("POST", "/api/junctions", {"junction_id": "B"})[0], 409)


if __name__ == "__main__":
    unittest.main()
