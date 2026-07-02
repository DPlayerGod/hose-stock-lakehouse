"""
Alert Detector Supervisor — quản lý detector process + health server.

Dùng threading thay vì subprocess để đơn giản và tránh overhead.
Detector chạy trong main thread, health server chạy trong daemon thread.
"""

import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone, timedelta

from .config import Config
from .detector import AlertDetector
from .health_server import run_health_server

ICT = timezone(timedelta(hours=7))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("supervisor")


class MonitoredDetector:
    """Wrapper quanh AlertDetector để track metrics."""

    def __init__(self, config: Config):
        self._inner = AlertDetector(config)
        self.running = False
        self._metrics = {"candles_processed": 0, "alerts_fired": 0, "errors": 0}
        self._uptime_start = time.time()

        # Hook alert callback to track metrics
        self._inner._on_alert_fired = lambda _: self._increment_alerts_fired()

    def _increment_alerts_fired(self) -> None:
        self._metrics["alerts_fired"] += 1

    @property
    def _detector(self):
        return self._inner

    @property
    def _warmup_done(self) -> bool:
        return getattr(self._inner, "_warmup_done", False)

    def run_loop(self) -> None:
        """Blocking run loop với metrics tracking."""
        self.running = True
        logger.info("MonitoredDetector loop started")

        while self.running:
            try:
                total = 0
                for symbol in self._inner.config.SYMBOLS:
                    symbol = symbol.strip()
                    candles = self._inner._fetch_new_ohlc(symbol)
                    if not candles:
                        continue
                    for row in candles:
                        ts, sym, open_, high, low, close, volume, _ = row
                        self._inner._process_candle(
                            symbol=sym, ts=ts,
                            open_=float(open_), high=float(high),
                            low=float(low), close=float(close),
                            volume=int(volume),
                        )
                        total += 1
                    last_recv = candles[-1][7]
                    self._inner._last_received_at[symbol] = last_recv

                if total:
                    self._metrics["candles_processed"] += total
                    logger.debug(f"Processed {total} candles")

                self._inner.calc.cleanup_old_anchors(cutoff_days=1)

            except KeyboardInterrupt:
                logger.info("Detector stopped by user.")
                self.running = False
                break
            except Exception as exc:
                self._metrics["errors"] += 1
                logger.error(f"Detector error: {exc}", exc_info=True)

            time.sleep(self._inner.config.POLL_INTERVAL_SEC)

    def stop(self) -> None:
        self.running = False


def run() -> None:
    """Entry point: khởi động health server + detector."""
    config = Config()
    monitored = MonitoredDetector(config)

    # Health server trong daemon thread
    health_thread = threading.Thread(
        target=run_health_server,
        args=(monitored, 8080),
        daemon=True,
    )
    health_thread.start()

    def shutdown_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        monitored.stop()

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    try:
        monitored.run_loop()
    finally:
        uptime = time.time() - monitored._uptime_start
        logger.info(
            f"Detector stopped | uptime={uptime:.0f}s | "
            f"candles={monitored._metrics['candles_processed']} | "
            f"alerts={monitored._metrics['alerts_fired']} | "
            f"errors={monitored._metrics['errors']}"
        )


if __name__ == "__main__":
    run()
