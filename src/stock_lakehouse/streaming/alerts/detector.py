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
from typing import Optional

import clickhouse_connect

from .config import Config
from .vwap import VWAPCalculator
from .candle_buffer import CandleBuffer, Candle
from .models import Alert
from .slack_notifier import SlackNotifier
from .rules.combined_rule import CombinedSignalRule
from .indicators.rsi import compute_wilder_rsi
from .indicators.volume import compute_volume_ratio

ICT = timezone(timedelta(hours=7))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('detector')


class AlertDetector:

    def __init__(self, config: Config, target_date: str | None = None):
        self.config = config
        self.target_date = target_date or datetime.now(ICT).strftime('%Y-%m-%d')
        self.ch = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_HTTP_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DB,
        )
        self._verify_streaming_schema()

        self.calc = VWAPCalculator()
        self.buffer = CandleBuffer(maxlen=config.CANDLE_BUFFER_SIZE)

        self.rules = [
            CombinedSignalRule(config),
        ]
        rule_names = [r.RULE_NAME for r in self.rules]
        logger.info(f"Registered rules: {rule_names}")

        self.slack = SlackNotifier(webhook_url=config.SLACK_DNSE_WEBHOOK)

        self._on_alert_fired: typing.Callable[[Alert], None] | None = None

        self._warm_up()
        self._warmup_done = True

    def _verify_streaming_schema(self) -> None:
        """Fail-fast check: streaming tables must already exist.

        DDL is owned by `streaming/clickhouse/init_streaming.sql` and must
        have been applied via `python -m stock_lakehouse.streaming.clickhouse.init`
        before this detector starts. We deliberately do NOT create tables here —
        keeping schema ownership in one place avoids drift between code and SQL.
        """
        required = ('rt_hose_ohlcv_1m', 'rt_hose_indicators', 'rt_hose_alerts')
        rows = self.ch.query(
            "SELECT name FROM system.tables "
            "WHERE database = %(db)s AND name IN %(names)s",
            parameters={'db': self.config.CLICKHOUSE_DB, 'names': required},
        ).result_rows
        present = {r[0] for r in rows}
        missing = [t for t in required if t not in present]
        if missing:
            raise RuntimeError(
                f"Streaming schema not initialised — missing tables: {missing}. "
                f"Run `python -m stock_lakehouse.streaming.clickhouse.init` first "
                f"(or start the `streaming-init` service in docker-compose)."
            )
        logger.info("Streaming schema verified: %s", sorted(present))

    def _warm_up(self) -> None:
        """Load target date's OHLCV candles so VWAP + buffer start with correct state."""
        logger.info(f"Warming up VWAP + candle buffer with {self.target_date}'s OHLCV candles...")
        today = self.target_date
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
            o, h, l, c, v = float(open_), float(high), float(low), float(close), int(volume)
            self.calc.update(symbol=symbol, high=h, low=l, close=c, volume=v, ts=ts)
            self.buffer.push(symbol, Candle(ts=ts, open=o, high=h, low=l, close=c, volume=v))
            self._last_received_at[symbol] = last_recv

            closes = self.buffer.get_closes(symbol, n=None)
            rsi = compute_wilder_rsi(closes, period=self.config.RSI_PERIOD)
            volumes = self.buffer.get_volumes(symbol, n=self.config.VOLUME_LOOKBACK + 1)
            vol_ratio = compute_volume_ratio(volumes, lookback=self.config.VOLUME_LOOKBACK)
            vwap, sigma = self.calc.get_session_vwap_and_sigma(symbol, ts)
            self._insert_indicator(symbol, ts, o, h, l, c, v, vwap, sigma, rsi, vol_ratio)

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
        today = self.target_date

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
        """Process one candle: update VWAP + buffer + compute indicators + evaluate rules."""
        self.calc.update(
            symbol=symbol, high=high, low=low,
            close=close, volume=volume, ts=ts,
        )
        self.buffer.push(symbol, Candle(
            ts=ts, open=open_, high=high,
            low=low, close=close, volume=volume,
        ))

        closes = self.buffer.get_closes(symbol, n=None)
        rsi = compute_wilder_rsi(closes, period=self.config.RSI_PERIOD)

        volumes = self.buffer.get_volumes(symbol, n=self.config.VOLUME_LOOKBACK + 1)
        vol_ratio = compute_volume_ratio(volumes, lookback=self.config.VOLUME_LOOKBACK)

        vwap, sigma = self.calc.get_session_vwap_and_sigma(symbol, ts)

        self._insert_indicator(symbol, ts, open_, high, low, close, volume, vwap, sigma, rsi, vol_ratio)

        for rule in self.rules:
            try:
                alert = rule.evaluate(symbol, close, ts, rsi=rsi, volume_ratio=vol_ratio, vwap=vwap, sigma=sigma)
                if alert:
                    self._fire_alert(alert)
            except Exception as exc:
                logger.error(
                    f"Rule {rule.RULE_NAME} error for {symbol}: {exc}",
                    exc_info=True,
                )

    def _insert_indicator(
        self, symbol: str, ts: datetime,
        open_: float, high: float, low: float,
        close: float, volume: int,
        vwap: Optional[float], sigma: Optional[float],
        rsi: Optional[float], vol_ratio: Optional[float],
    ) -> None:
        """Insert candle + indicators into rt_hose_indicators table."""
        try:
            self.ch.insert(
                'rt_hose_indicators',
                [[
                    ts, symbol, open_, high, low, close, volume,
                    vwap, sigma, rsi, vol_ratio,
                    datetime.now(ICT),
                ]],
                column_names=[
                    'candle_time', 'symbol', 'open', 'high', 'low', 'close', 'volume',
                    'vwap', 'sigma', 'rsi14', 'volume_ratio', 'created_at',
                ],
            )
        except Exception as exc:
            logger.debug(f"Failed to insert indicator (ignored): {exc}")

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
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Alert Detector")
    parser.add_argument(
        '--reset-today', action='store_true',
        help='Delete target date\'s indicators + alerts from ClickHouse before starting (for clean replay)'
    )
    parser.add_argument(
        '--date', type=str, default=None,
        help='Target date in YYYY-MM-DD format (defaults to today)'
    )
    parser.add_argument(
        '--backfill', action='store_true',
        help='Run warmup calculation for target date and exit immediately'
    )
    args = parser.parse_args()

    config = Config()
    target_date = args.date or datetime.now(ICT).strftime('%Y-%m-%d')

    if args.reset_today or args.backfill:
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger('detector')
        client = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_HTTP_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DB,
        )
        logger.info(f"Resetting data for {target_date}...")

        def _wipe(client, db, table, col, date_str):
            try:
                client.command(
                    f"ALTER TABLE {db}.{table} DELETE WHERE toDate({col}) = '{date_str}'"
                )
                logger.info(f"  Wiped {table} for {date_str}")
            except Exception as e:
                # Table may not exist yet — this is fine, skip silently
                if 'UNKNOWN_TABLE' not in str(e):
                    logger.error(f"  Failed to wipe {table}: {e}")
                else:
                    logger.info(f"  {table} not found, skipping wipe")

        _wipe(client, config.CLICKHOUSE_DB, 'rt_hose_indicators', 'candle_time', target_date)
        _wipe(client, config.CLICKHOUSE_DB, 'rt_hose_alerts', 'alert_time', target_date)
        logger.info(f"Waiting for mutations to complete...")
        pending = 1
        while pending > 0:
            time.sleep(2)
            result = client.query(
                f"SELECT count() FROM system.mutations "
                f"WHERE database = '{config.CLICKHOUSE_DB}' AND is_done = 0"
            )
            pending = result.result_rows[0][0] if result.result_rows else 0
        logger.info(f"Reset complete. Starting detector...")

    detector = AlertDetector(config, target_date=target_date)
    if args.backfill:
        logger.info(f"Backfill completed successfully for {target_date}.")
    else:
        detector.run()
