"""HTTP layer (stdlib only). Thin: parse -> call service -> map errors to status codes."""
import json
import logging
import mimetypes
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .domain.errors import ConflictError, ValidationError
from .service import NotFound

log = logging.getLogger("traffic.http")
STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
MAX_BODY = 64 * 1024


def make_server(service, simulator, host="0.0.0.0", port=8000):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            log.debug("%s %s", self.address_string(), fmt % args)

        # ---- helpers
        def _send(self, code, body, ctype="application/json"):
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            if n > MAX_BODY:
                raise ValidationError("request body too large")
            raw = self.rfile.read(n) if n else b""
            try:
                return json.loads(raw or b"{}")
            except ValueError:
                raise ValidationError("body is not valid JSON")

        def _dispatch(self, method):
            url = urlparse(self.path)
            path, q = url.path.rstrip("/") or "/", parse_qs(url.query)
            try:
                if method == "OPTIONS":
                    return self._send(204, b"")
                if not path.startswith("/api"):
                    return self._static(path)
                code, body = self._route(method, path, q)
                self._send(code, body)
            except ValidationError as e:
                self._send(400, {"error": "VALIDATION_ERROR", "message": str(e)})
            except NotFound as e:
                self._send(404, {"error": "NOT_FOUND", "message": str(e)})
            except ConflictError as e:
                self._send(409, {"error": "CONFLICT", "message": str(e)})
            except Exception:
                log.exception("unhandled error")
                self._send(500, {"error": "INTERNAL_ERROR", "message": "unexpected server error"})

        def _static(self, path):
            name = "index.html" if path in ("/", "") else os.path.basename(path)
            fp = os.path.join(STATIC, name)
            if not os.path.isfile(fp):
                return self._send(404, {"error": "NOT_FOUND", "message": "no such file"})
            with open(fp, "rb") as f:
                self._send(200, f.read(), mimetypes.guess_type(fp)[0] or "text/plain")

        def _route(self, m, path, q):
            if m == "GET" and path == "/api/health":
                return 200, {"status": "ok"}
            if path == "/api/junctions":
                if m == "GET":
                    return 200, service.list_status()
                if m == "POST":
                    return 201, service.create_junction(self._body())
            r = re.fullmatch(r"/api/junctions/([^/]+)(?:/(status|history|commands))?", path)
            if r:
                jid, sub = r.groups()
                if sub is None and m == "GET":
                    return 200, {"config": service.config(jid), "status": service.status(jid)}
                if sub == "status" and m == "GET":
                    return 200, service.status(jid)
                if sub == "history" and m == "GET":
                    lim = int((q.get("limit") or ["100"])[0])
                    return 200, service.history(jid, lim, (q.get("event_type") or [None])[0])
                if sub == "commands" and m == "POST":
                    return 202, service.command(jid, self._body())
            if m == "POST" and path == "/api/sensor-events":
                res = service.sensor_event(self._body())
                return {"APPLIED": 201, "DUPLICATE": 200}.get(res["result"], 202), res
            if m == "POST" and path == "/api/controller-events":
                res = service.controller_event(self._body())
                return {"APPLIED": 200, "DUPLICATE": 200}.get(res["result"], 202), res
            if path == "/api/simulator":
                if m == "POST":
                    b = self._body()
                    if "auto_ack" in b:
                        simulator.auto_ack = bool(b["auto_ack"])
                    if "ack_delay" in b:
                        simulator.ack_delay = max(0.0, float(b["ack_delay"]))
                return 200, {"auto_ack": simulator.auto_ack, "ack_delay": simulator.ack_delay,
                             "recent_commands_sent": list(simulator.sent)[-20:]}
            return 404, {"error": "NOT_FOUND", "message": f"no route for {m} {path}"}

        def do_GET(self): self._dispatch("GET")
        def do_POST(self): self._dispatch("POST")
        def do_OPTIONS(self): self._dispatch("OPTIONS")

    return ThreadingHTTPServer((host, port), Handler)


def start_ticker(service, interval=0.5):
    stop = threading.Event()

    def loop():
        while not stop.wait(interval):
            service.tick_all()
    t = threading.Thread(target=loop, daemon=True, name="ticker")
    t.start()
    return stop
