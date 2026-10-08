import threading
from collections import deque

from ..ports import ControllerPort


class SimulatedController(ControllerPort):
    """REST-driven controller simulator. With auto_ack=True it acknowledges every command
    after `ack_delay` seconds; with auto_ack=False the evaluator acknowledges (or doesn't)
    through POST /api/controller-events, to demonstrate delays / timeouts / failures."""

    def __init__(self, auto_ack=True, ack_delay=0.3):
        self.auto_ack, self.ack_delay = auto_ack, ack_delay
        self.sent = deque(maxlen=200)
        self._callback = None

    def send(self, command):
        self.sent.append(dict(command))
        if self.auto_ack and self._callback:
            t = threading.Timer(self.ack_delay, self._ack, args=(dict(command),))
            t.daemon = True
            t.start()

    def _ack(self, c):
        if not self.auto_ack:
            return
        try:
            self._callback({"command_id": c["command_id"], "junction_id": c["junction_id"],
                            "status": "ACK", "actual_state": c["requested_state"]})
        except Exception:  # simulator must never crash the app
            pass
