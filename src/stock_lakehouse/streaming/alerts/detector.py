"""
Multi-Signal Alert Detector — Python-based alert engine for the lakehouse.

Polls ClickHouse `rt_hose_ohlcv_1m` for new 1-minute candles, updates VWAP
+ candle buffer, runs all alert rules (VWAP, RSI, Volume Spike), writes alerts
to `rt_hose_alerts` and sends CRITICAL alerts to Slack.

Run with:
    python -m stock_lakehouse.streaming.alerts.detector
or:
    uv run python -m stock_lakehouse.streaming.alerts.detector
"""

import logging
import time
import typing
from datetime import datetime, timezone, timedelta

import clickhouse_connect

from .config import Config
from .vwap import VWAPCalculator
from .candle_buffer import CandleBuffer, Candle
from .models import Alert
from .slack_notifier import SlackNotifier
from .rules.combined_rule import CombinedSignalRule

ICT = timezone(timedelta(hours=7))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('detector')


class AlertDetector:

    def __init__(self, config: Config):
        self.config = config
        self.ch = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_HTTP_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DB,
        )
        self._ensure_rt_hose_alerts()

        self.calc = VWAPCalculator()
        self.buffer = CandleBuffer(maxlen=config.CANDLE_BUFFER_SIZE)

        self.rules = [
            CombinedSignalRule(config, self.calc),
        ]
        rule_names = [r.RULE_NAME for r in self.rules]
        logger.info(f"Registered rules: {rule_names}")

        self.slack = SlackNotifier(webhook_url=config.SLACK_DNSE_WEBHOOK)

        self._on_alert_fired: typing.Callable[[Alert], None] | None = None

        self._warm_up()
        self._warmup_done = True

    def _ensure_rt_hose_alerts(self) -> None:
        """Create rt_hose_alerts table if it does not exist."""
        try:
            stmt = """
            CREATE TABLE IF NOT EXISTS rt_hose_alerts (
                alert_time      DateTime64(3, 'Asia/Ho_Chi_Minh'),
                symbol          LowCardinality(String),
                rule_name       LowCardinality(String),
                alert_type      String,
                severity        LowCardinality(String),
                price           Float64,
                indicator_value Float64,
                threshold       Float64,
                deviation_pct   Float64,
                message         String
            ) ENGINE = MergeTree()
            ORDER BY (alert_time, symbol, rule_name)
            TTL toDate(alert_time) + INTERVAL 90 DAY
            """
            if hasattr(self.ch, 'command'):
                self.ch.command(stmt)
            else:
                self.ch.query(stmt)
            logger.info("rt_hose_alerts table ensured.")
        except Exception as exc:
            logger.debug(f"rt_hose_alerts creation (ignored): {exc}")

    def _warm_up(self) -> None:
        """Load today's OHLCV candles so VWAP + buffer start with correct state."""
        logger.info("Warming up VWAP + candle buffer with today's OHLCV candles...")
        today = datetime.now(ICT).strftime('%Y-%m-%d')
        symbols_sql = ','.join([f"'{s.strip()}'" for s in self.config.SYMBOLS])

        rows = self.ch.query(
            f"SELECT "
            f"  candle_time, "
            f"  symbol, "
            f"  argMax(open, received_at) AS open, "
            f"  argMax(high, received_at) AS high, "
            f"  argMax(low, received_at) AS low, "
            f"  argMax(close, received_at) AS close, "
            f"  argMax(volume, received_at) AS volume, "
            f"  max(received_at) AS last_received "
            f"FROM rt_hose_ohlcv_1m "
            f"WHERE toDate(candle_time) = '{today}' AND symbol IN ({symbols_sql}) "
            f"GROUP BY candle_time, symbol "
            f"ORDER BY candle_time ASC"
        ).result_rows

        self._last_received_at: dict[str, datetime] = {}

        for row in rows:
            ts, symbol, open_, high, low, close, volume, last_recv = row
            self.calc.update(
                symbol=symbol,
                high=float(high),
                low=float(low),
                close=float(close),
                volume=int(volume),
                ts=ts,
            )
            self.buffer.push(symbol, Candle(
                ts=ts, open=float(open_), high=float(high),
                low=float(low), close=float(close), volume=int(volume),
            ))
            self._last_received_at[symbol] = last_recv

        for sym in self.config.SYMBOLS:
            s = sym.strip()
            logger.info(
                f"  {s}: buffer={self.buffer.size(s)} candles, "
                f"vwap={self.calc.get_session_vwap(s)}"
            )

        logger.info(f"Warm-up done: {len(rows):,} candles loaded")

    def _fetch_new_ohlc(self, symbol: str):
        """Fetch new candles from ClickHouse since last seen watermark."""
        last_recv = self._last_received_at.get(symbol)
        today = datetime.now(ICT).strftime('%Y-%m-%d')

        if last_recv:
            recv_str = last_recv.strftime('%Y-%m-%d %H:%M:%S.%f')
            where = (
                f"symbol = '{symbol}' "
                f"AND toDate(candle_time) = '{today}' "
                f"AND received_at > '{recv_str}'"
            )
        else:
            where = f"symbol = '{symbol}' AND toDate(candle_time) = '{today}'"

        return self.ch.query(
            f"SELECT "
            f"  candle_time, symbol, "
            f"  argMax(open, received_at) AS open, "
            f"  argMax(high, received_at) AS high, "
            f"  argMax(low, received_at) AS low, "
            f"  argMax(close, received_at) AS close, "
            f"  argMax(volume, received_at) AS volume, "
            f"  max(received_at) AS last_received "
            f"FROM rt_hose_ohlcv_1m WHERE {where} "
            f"GROUP BY candle_time, symbol "
            f"ORDER BY candle_time ASC "
            f"LIMIT 5000"
        ).result_rows

    def _fire_alert(self, alert: Alert) -> None:
        """Write alert to ClickHouse alerts_v2 + send to Slack."""
        logger.warning(
            f"[{alert.rule_name}] {alert.alert_type} | {alert.symbol} "
            f"price={alert.price:.2f} indicator={alert.indicator_value:.2f} "
            f"severity={alert.severity} | {alert.message}"
        )
        self.ch.insert(
            'rt_hose_alerts',
            [[
                alert.alert_time, alert.symbol, alert.rule_name,
                alert.alert_type, alert.severity, alert.price,
                alert.indicator_value, alert.threshold,
                alert.deviation_pct, alert.message,
            ]],
            column_names=[
                'alert_time', 'symbol', 'rule_name', 'alert_type',
                'severity', 'price', 'indicator_value', 'threshold',
                'deviation_pct', 'message',
            ],
        )

        self.slack.send_alert(alert)

        if self._on_alert_fired:
            self._on_alert_fired(alert)

    def _process_candle(
        self, symbol: str, ts: datetime,
        open_: float, high: float, low: float,
        close: float, volume: int,
    ) -> None:
        """Process one candle: update VWAP + buffer + evaluate all rules."""
        self.calc.update(
            symbol=symbol, high=high, low=low,
            close=close, volume=volume, ts=ts,
        )
        self.buffer.push(symbol, Candle(
            ts=ts, open=open_, high=high,
            low=low, close=close, volume=volume,
        ))

        for rule in self.rules:
            try:
                alert = rule.evaluate(symbol, close, ts, self.buffer)
                if alert:
                    self._fire_alert(alert)
            except Exception as exc:
                logger.error(
                    f"Rule {rule.RULE_NAME} error for {symbol}: {exc}",
                    exc_info=True,
                )

    def run(self) -> None:
        logger.info(
            f"Detector running | rules={[r.RULE_NAME for r in self.rules]} "
            f"| vwap_mode={self.config.ALERT_BAND_MODE} "
            f"| sigma_k={self.config.BAND_SIGMA_MULTIPLIER} "
            f"| rsi_period={self.config.RSI_PERIOD} "
            f"| vol_lookback={self.config.VOLUME_LOOKBACK} "
            f"| cooldown={self.config.ALERT_COOLDOWN_SEC}s "
            f"| poll every {self.config.POLL_INTERVAL_SEC}s"
        )
        while True:
            try:
                total_processed = 0
                for symbol in self.config.SYMBOLS:
                    symbol = symbol.strip()
                    candles = self._fetch_new_ohlc(symbol)
                    if not candles:
                        continue

                    for row in candles:
                        ts, sym, open_, high, low, close, volume, _ = row
                        self._process_candle(
                            symbol=sym,
                            ts=ts,
                            open_=float(open_),
                            high=float(high),
                            low=float(low),
                            close=float(close),
                            volume=int(volume),
                        )

                    last_recv_in_batch = candles[-1][7]
                    self._last_received_at[symbol] = last_recv_in_batch
                    total_processed += len(candles)

                if total_processed:
                    logger.debug(f"Processed {total_processed} candles")

                self.calc.cleanup_old_anchors(cutoff_days=1)

            except KeyboardInterrupt:
                logger.info("Detector stopped by user.")
                break
            except Exception as exc:
                logger.error(f"Detector error: {exc}", exc_info=True)

            time.sleep(self.config.POLL_INTERVAL_SEC)


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    AlertDetector(Config()).run()
