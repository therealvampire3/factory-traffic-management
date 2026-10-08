"""Entry point:  python run.py   (env: PORT=8000, TRAFFIC_DB=traffic.db, AUTO_ACK=1)"""
import logging
import os

from traffic.adapters.simulator import SimulatedController
from traffic.server import make_server, start_ticker
from traffic.service import JunctionService
from traffic.store import Store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    controller = SimulatedController(auto_ack=os.environ.get("AUTO_ACK", "1") != "0")
    service = JunctionService(Store(os.environ.get("TRAFFIC_DB", "traffic.db")), controller)
    service.bootstrap()
    start_ticker(service)
    server = make_server(service, controller, port=port)
    logging.info("Dashboard + API on http://localhost:%d", port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
