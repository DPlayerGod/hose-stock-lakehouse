"""
Health server — lightweight HTTP server expose /health endpoint.

Docker HEALTHCHECK dùng endpoint này để kiểm tra container còn sống.
Ngoài ra endpoint /metrics trả structured logging để monitor ngoài.
"""

import http.server
import json
import logging
import threading
import time
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("health_server")

ICT = timezone(timedelta(hours=7))


class HealthServer:
    def __init__(self, detector_ref, port: int = 8080):
        self.port = port
        self._detector_ref = detector_ref  # weak ref-like: just access .running flag
        self._server = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        logger.info(f"Health server listening on :{self.port}")

    def _serve(self) -> None:
        class Handler(http.server.BaseHTTPRequestHandler):
            detector = self._detector_ref
            port = self.port

            def do_GET(self):
                if self.path == "/health":
                    self._health()
                elif self.path == "/metrics":
                    self._metrics()
                elif self.path == "/ready":
                    self._ready()
                else:
                    self.send_response(404)
                    self.end_headers()

            def _health(self):
                # Liveness: process is alive
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok", "ts": datetime.now(ICT).isoformat()}).encode())

            def _ready(self):
                # Readiness: warmup done + running
                is_ready = (
                    hasattr(self.detector, "_warmup_done")
                    and getattr(self.detector, "_warmup_done", False)
                    and getattr(self.detector, "running", False)
                )
                status = 200 if is_ready else 503
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ready": is_ready, "ts": datetime.now(ICT).isoformat()}).encode())

            def _metrics(self):
                # Structured metrics for external monitoring
                d = getattr(self.detector, "_metrics", {})
                metrics = {
                    "ts": datetime.now(ICT).isoformat(),
                    "uptime_sec": getattr(self.detector, "_uptime_start", None),
                    "candles_processed": d.get("candles_processed", 0),
                    "alerts_fired": d.get("alerts_fired", 0),
                    "errors": d.get("errors", 0),
                }
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(metrics).encode())

            def log_message(self, format, *args):
                # Suppress default HTTP logging — use structured logging instead
                pass

        self._server = http.server.HTTPServer(("0.0.0.0", self.port), Handler)
        self._server.serve_forever()

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            logger.info("Health server stopped")


def run_health_server(detector_ref, port: int = 8080) -> None:
    """Blocking entry point for the health server subprocess."""
    server = HealthServer(detector_ref, port)
    try:
        server.start()
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.stop()
