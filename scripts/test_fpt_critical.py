"""
Test FPT CRITICAL alert — injects a fake candle, verifies alert fires, then cleans up.

This test:
  1. Inserts a CRITICAL-level fake candle for FPT into rt_hose_ohlcv_1m
  2. Directly calls _fire_alert() to simulate what the detector does
  3. Verifies the alert appears in rt_hose_alerts
  4. Cleans up the fake candle from rt_hose_ohlcv_1m

Run:
    python scripts/test_fpt_critical.py
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta

from stock_lakehouse.clickhouse.client import get_clickhouse_client
from stock_lakehouse.config import ClickHouseConfig
from stock_lakehouse.streaming.alerts.vwap import VWAPCalculator
from stock_lakehouse.streaming.alerts.candle_buffer import CandleBuffer, Candle
from stock_lakehouse.streaming.alerts.rules.combined_rule import CombinedSignalRule
from stock_lakehouse.streaming.alerts.config import Config
from stock_lakehouse.streaming.alerts.models import Alert
from stock_lakehouse.streaming.alerts.slack_notifier import SlackNotifier

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger('test_fpt')

ICT = timezone(timedelta(hours=7))
CH_CONFIG = ClickHouseConfig()


def _now_ict() -> datetime:
    return datetime.now(ICT)


def get_current_vwap_state(client, symbol: str):
    """Fetch today's FPT candles to build VWAP state."""
    today = datetime.now(ICT).strftime('%Y-%m-%d')
    rows = client.query(
        f"SELECT candle_time, open, high, low, close, volume "
        f"FROM rt_hose_ohlcv_1m "
        f"WHERE symbol = '{symbol}' AND toDate(candle_time) = '{today}' "
        f"ORDER BY candle_time ASC LIMIT 200"
    ).result_rows
    return rows


def build_vwap_calc(rows) -> tuple[VWAPCalculator, CandleBuffer]:
    calc = VWAPCalculator()
    buf = CandleBuffer(maxlen=50)
    for row in rows:
        ts, open_, high, low, close, volume = row
        calc.update(symbol='FPT', high=float(high), low=float(low),
                    close=float(close), volume=int(volume), ts=ts)
        buf.push('FPT', Candle(ts=ts, open=float(open_), high=float(high),
                               low=float(low), close=float(close), volume=int(volume)))
    return calc, buf


def main():
    SYMBOL = 'FPT'
    client = get_clickhouse_client(CH_CONFIG)

    # --- Step 1: Get current state ---
    logger.info("Step 1: Fetching current FPT state from rt_hose_ohlcv_1m...")
    rows = get_current_vwap_state(client, SYMBOL)
    if not rows:
        logger.error("No FPT data found. Is producer running?")
        return

    calc, buf = build_vwap_calc(rows)
    s_vwap, s_sigma = calc.get_session_vwap_and_sigma(SYMBOL, rows[-1][0])
    if s_vwap is None or s_sigma is None:
        logger.error("Could not compute session VWAP/sigma. Is there enough trading data?")
        return
    sigma_k = Config().BAND_SIGMA_MULTIPLIER
    upper = s_vwap + sigma_k * s_sigma
    lower = s_vwap - sigma_k * s_sigma
    logger.info(f"  VWAP={s_vwap:.2f}, sigma={s_sigma:.2f}, upper={upper:.2f}, lower={lower:.2f}")

    # --- Step 2: Build fake CRITICAL candle (PUMP_RISK: breakout + RSI>=70 + vol>=3x) ---
    # Use a recent candle_time but slightly in the future to be "new"
    last_ts = rows[-1][0]
    fake_ts = last_ts + timedelta(minutes=1)
    breakout_price = upper + 500   # well above upper band
    fake_volume = int(sum(r[5] for r in rows[-20:]) / 20 * 4)  # ~4x avg volume

    logger.info(f"Step 2: Injecting fake CRITICAL candle at {fake_ts}")
    logger.info(f"  price={breakout_price:.2f} (upper={upper:.2f}) vol={fake_volume:,}")

    # Insert into ClickHouse
    fake_row = [
        fake_ts.strftime('%Y-%m-%d %H:%M:%S'),
        SYMBOL,
        breakout_price,
        breakout_price,
        breakout_price - 100,
        breakout_price,
        fake_volume,
        _now_ict(),
    ]
    client.insert(
        'rt_hose_ohlcv_1m',
        [fake_row],
        column_names=['candle_time', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'received_at'],
    )
    logger.info(f"  Inserted. Candle_time={fake_ts}")

    # --- Step 3: Evaluate rule + fire alert (insert rt_hose_alerts + Slack) ---
    logger.info("Step 3: Running CombinedSignalRule.evaluate() + _fire_alert()...")
    config = Config()
    rule = CombinedSignalRule(config, calc)
    calc.update(symbol=SYMBOL, high=breakout_price, low=breakout_price - 100,
               close=breakout_price, volume=fake_volume, ts=fake_ts)
    buf.push(SYMBOL, Candle(ts=fake_ts, open=breakout_price, high=breakout_price,
                            low=breakout_price - 100, close=breakout_price, volume=fake_volume))

    alert = rule.evaluate(SYMBOL, breakout_price, fake_ts, buf)
    if not alert:
        logger.warning("  No alert fired. Check RSI / volume state in buffer.")
        _cleanup(client, SYMBOL, fake_ts)
        return

    logger.info(f"  ALERT FIRED: [{alert.severity}] {alert.alert_type}")
    logger.info(f"  message: {alert.message}")

    # Simulate _fire_alert: insert into rt_hose_alerts + send Slack
    client.insert(
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
    logger.info("  Inserted into rt_hose_alerts.")

    slack = SlackNotifier(webhook_url=config.SLACK_DNSE_WEBHOOK)
    slack.send_alert(alert)
    logger.info("  Slack notified.")

    # Verify
    rows_alert = client.query(
        f"SELECT alert_time, symbol, alert_type, severity, message "
        f"FROM rt_hose_alerts WHERE symbol = '{SYMBOL}' ORDER BY alert_time DESC LIMIT 1"
    ).result_rows
    if rows_alert:
        at, sym, atype, sev, msg = rows_alert[0]
        logger.info(f"  Verified in rt_hose_alerts: [{sev}] {atype} | {sym} | {msg}")

    # --- Step 4: Cleanup ---
    _cleanup(client, SYMBOL, fake_ts)


def _cleanup(client, symbol: str, fake_ts: datetime):
    logger.info(f"Cleaning up fake candle...")
    client.command(
        f"ALTER TABLE rt_hose_ohlcv_1m DELETE "
        f"WHERE symbol = '{symbol}' AND candle_time = '{fake_ts.strftime('%Y-%m-%d %H:%M:%S')}'"
    )
    logger.info("  Deleted.")


if __name__ == '__main__':
    main()
