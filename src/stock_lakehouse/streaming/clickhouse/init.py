"""Run streaming DDL in ClickHouse.

Idempotent: all statements use CREATE TABLE IF NOT EXISTS.
Call after ClickHouse is up (e.g. as a one-time init step or startup task).

Usage:
    python -m stock_lakehouse.streaming.clickhouse.init

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker address (default: kafka:9092)
    KAFKA_TOPIC_OHLC:       Kafka topic for OHLC data (default: dnse.ohlc)
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from stock_lakehouse.clickhouse.client import get_clickhouse_client
from stock_lakehouse.config import ClickHouseConfig


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("streaming.init")


def init_streaming_schema(config: ClickHouseConfig | None = None) -> None:
    """Run init_streaming.sql against ClickHouse."""
    config = config or ClickHouseConfig()
    client = get_clickhouse_client(config)

    sql_path = Path(__file__).parent / "init_streaming.sql"
    sql_content = sql_path.read_text(encoding="utf-8")

    # Replace Kafka broker placeholder — in Docker env the env var is used directly
    # by ClickHouse; for local dev replace with the value from config
    sql_content = sql_content.replace(
        "${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}",
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    sql_content = sql_content.replace(
        "${KAFKA_TOPIC_OHLC:-dnse.ohlc}",
        os.environ.get("KAFKA_TOPIC_OHLC", "dnse.ohlc"),
    )

    logger.info("Running streaming DDL in %s/%s ...", config.database, config.host)
    for statement in sql_content.split(";"):
        stmt = statement.strip()
        if not stmt or stmt.startswith("--") or stmt.startswith("SET"):
            continue
        try:
            client.command(stmt)
            logger.debug("OK: %s", stmt[:80])
        except Exception as exc:
            # Log but continue — IF NOT EXISTS makes most errors benign
            logger.warning("Statement error (may already exist): %s", exc)

    logger.info("Streaming schema initialised.")


if __name__ == "__main__":
    init_streaming_schema()
