"""Latency Tester — End-to-end streaming pipeline latency measurement.

Measures latency between DNSE candle creation (candle_time) and
ClickHouse receive time (received_at) via rt_hose_ohlcv_1m.

Outputs p50/p95/p99/avg/min/max per iteration.

Run:
    PYTHONPATH=src python -m stock_lakehouse.streaming.tools.latency_tester
    PYTHONPATH=src python -m stock_lakehouse.streaming.tools.latency_tester --interval 10 --window 30
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta, timezone

from stock_lakehouse.clickhouse.client import get_clickhouse_client
from stock_lakehouse.config import ClickHouseConfig


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("latency_tester")

ICT = timezone(timedelta(hours=7))

CH_CONFIG = ClickHouseConfig()


SUMMARY_SQL = """
SELECT
    count()                                                                          AS total_messages,
    round(avg(date_diff('millisecond', candle_time, received_at)), 1)                   AS avg_latency_ms,
    round(quantile(0.50)(date_diff('millisecond', candle_time, received_at)), 1)       AS p50_ms,
    round(quantile(0.95)(date_diff('millisecond', candle_time, received_at)), 1)       AS p95_ms,
    round(quantile(0.99)(date_diff('millisecond', candle_time, received_at)), 1)       AS p99_ms,
    round(min(date_diff('millisecond', candle_time, received_at)), 1)                    AS min_ms,
    round(max(date_diff('millisecond', candle_time, received_at)), 1)                  AS max_ms
FROM rt_hose_ohlcv_1m
WHERE toDate(candle_time) = today()
  AND candle_time >= now() - INTERVAL {window} MINUTE
"""

LATEST_SQL = """
SELECT
    symbol,
    candle_time,
    received_at,
    date_diff('millisecond', candle_time, received_at) AS latency_ms
FROM rt_hose_ohlcv_1m
WHERE toDate(candle_time) = today()
ORDER BY received_at DESC
LIMIT 1
"""

TIMESERIES_SQL = """
SELECT
    toStartOfMinute(received_at)                                                    AS minute,
    count()                                                                         AS msg_count,
    round(avg(date_diff('millisecond', candle_time, received_at)), 1)                   AS avg_ms,
    round(quantile(0.95)(date_diff('millisecond', candle_time, received_at)), 1)       AS p95_ms
FROM rt_hose_ohlcv_1m
WHERE toDate(candle_time) = today()
  AND candle_time >= now() - INTERVAL {window} MINUTE
GROUP BY minute
ORDER BY minute ASC
"""

DISTRIBUTION_SQL = """
SELECT
    multiIf(
        latency_ms < 500,  '<500ms',
        latency_ms < 1000, '500-1000ms',
        latency_ms < 1500, '1000-1500ms',
        latency_ms < 2000, '1500-2000ms',
        latency_ms < 3000, '2000-3000ms',
        '>3000ms'
    ) AS bucket,
    count() AS cnt
FROM (
    SELECT date_diff('millisecond', candle_time, received_at) AS latency_ms
    FROM rt_hose_ohlcv_1m
    WHERE toDate(candle_time) = today()
      AND candle_time >= now() - INTERVAL {window} MINUTE
)
GROUP BY bucket
ORDER BY bucket ASC
"""


def _color(val: float, warn: float = 1500, crit: float = 2000) -> str:
    if val < warn:
        return f"\033[92m{val:.1f}ms\033[0m"
    if val < crit:
        return f"\033[93m{val:.1f}ms\033[0m"
    return f"\033[91m{val:.1f}ms\033[0m"


def _print_header() -> None:
    print("\033[1m")
    print("=" * 72)
    print("  HOSE Streaming — Latency Tester")
    print("  DNSE WS -> Kafka -> ClickHouse (rt_hose_ohlcv_1m)")
    print("=" * 72)
    print("\033[0m")


def _print_summary(client, window: int) -> None:
    rows = client.query(SUMMARY_SQL.format(window=window)).result_rows
    if not rows or rows[0][0] == 0:
        print("  No data in window. Check producer + Kafka + ClickHouse.")
        return

    total, avg, p50, p95, p99, min_l, max_l = rows[0]
    print(f"\n  Summary ({window} min window, {total:,} messages)")
    print(f"  |-- Avg  : {_color(avg)}")
    print(f"  |-- p50  : {_color(p50)}")
    print(f"  |-- p95  : {_color(p95)}")
    print(f"  |-- p99  : {_color(p99)}")
    print(f"  |-- Min  : {_color(min_l)}")
    print(f"  +-- Max  : {_color(max_l)}")


def _print_latest(client) -> None:
    rows = client.query(LATEST_SQL).result_rows
    if not rows:
        return
    symbol, candle_time, received_at, latency_ms = rows[0]
    print(f"\n  Latest: [{symbol}] latency={_color(latency_ms)}")
    print(f"     candle_time = {candle_time}")
    print(f"     received_at = {received_at}")


def _print_distribution(client, window: int) -> None:
    rows = client.query(DISTRIBUTION_SQL.format(window=window)).result_rows
    if not rows:
        return
    print(f"\n  Distribution:")
    max_cnt = max(r[1] for r in rows) if rows else 1
    for bucket, cnt in rows:
        bar_len = int(cnt / max_cnt * 30) if max_cnt > 0 else 0
        bar = "\u2588" * bar_len
        print(f"     {bucket:>12s} | {bar} ({cnt:,})")


def _print_timeseries(client, window: int) -> None:
    rows = client.query(TIMESERIES_SQL.format(window=window)).result_rows
    if not rows:
        return
    print(f"\n  Latency per minute (last 10):")
    print(f"     {'Minute':>8s}  {'Msgs':>6s}  {'Avg(ms)':>10s}  {'p95(ms)':>10s}")
    print(f"     {'--'*4}  {'--'*3}  {'--'*5}  {'--'*5}")
    for minute, msg_count, avg_ms, p95_ms in rows[-10:]:
        min_str = minute.strftime("%H:%M") if hasattr(minute, "strftime") else str(minute)
        print(f"     {min_str:>8s}  {msg_count:>6,}  {avg_ms:>10.1f}  {p95_ms:>10.1f}")


def run(interval: int, window: int) -> None:
    client = get_clickhouse_client(CH_CONFIG)
    logger.info(
        "Connected to ClickHouse %s:%s/%s",
        CH_CONFIG.host, CH_CONFIG.port, CH_CONFIG.database,
    )
    _print_header()
    print(f"  Poll interval: {interval}s | Window: {window} min")
    print(f"  Ctrl+C to stop\n")

    iteration = 0
    try:
        while True:
            iteration += 1
            now_str = datetime.now(ICT).strftime("%H:%M:%S")
            print(f"\n{'-'*72}")
            print(f"  Iteration #{iteration} — {now_str} ICT")

            try:
                _print_latest(client)
                _print_summary(client, window)
                _print_distribution(client, window)
                _print_timeseries(client, window)
            except Exception as exc:
                logger.error("Query error: %s", exc, exc_info=True)

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n\n  Stopped after {iteration} iterations.")


def main() -> None:
    parser = argparse.ArgumentParser(description="HOSE Streaming Latency Tester")
    parser.add_argument(
        "--interval", type=int, default=10,
        help="Poll interval in seconds (default: 10)",
    )
    parser.add_argument(
        "--window", type=int, default=10,
        help="Analysis window in minutes (default: 10)",
    )
    args = parser.parse_args()
    run(interval=args.interval, window=args.window)


if __name__ == "__main__":
    main()
