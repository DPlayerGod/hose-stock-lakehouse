"""OHLCV Producer — DNSE WebSocket to Kafka topic.

Subscribes to OHLC closed candles (1-minute resolution) from DNSE and
publishes them to the `dnse.ohlc` Kafka topic.

Run:
    PYTHONPATH=src python -m stock_lakehouse.streaming.producer.ohlc_producer
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from dnse_sdk.trading_websocket import TradingClient
from dnse_sdk.trading_websocket.models import Ohlc

from stock_lakehouse.config import SYMBOLS
from stock_lakehouse.streaming.producer.config import StreamingConfig


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ohlc_producer")

ICT = timezone(timedelta(hours=7))


def _build_message(ohlc: Ohlc) -> dict:
    """Build Kafka message payload from an Ohlc tick."""
    return {
        "received_at": datetime.now(ICT).strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "symbol": ohlc.symbol,
        "resolution": ohlc.resolution,
        "open": float(ohlc.open),
        "high": float(ohlc.high),
        "low": float(ohlc.low),
        "close": float(ohlc.close),
        "volume": int(ohlc.volume),
        "type": ohlc.type,
        "time": int(ohlc.time),
        "lastUpdated": int(ohlc.lastUpdated),
    }


def _validate(ohlc: Ohlc) -> bool:
    """Light validation before publishing to Kafka."""
    if not ohlc.symbol:
        return False
    for price in (ohlc.open, ohlc.high, ohlc.low, ohlc.close):
        if price is not None and price <= 0:
            return False
    if ohlc.volume is not None and ohlc.volume < 0:
        return False
    return True


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=500,
        linger_ms=5,
        batch_size=16384,
    )


async def run(config: StreamingConfig) -> None:
    producer = create_kafka_producer(config.kafka_bootstrap)
    logger.info("Kafka producer ready → %s", config.kafka_bootstrap)

    client = TradingClient(
        api_key=config.dnse_api_key,
        api_secret=config.dnse_api_secret,
        base_url=config.dnse_ws_url,
        encoding=config.dnse_encoding,
        auto_reconnect=True,
        max_retries=10,
    )

    stats = {"sent": 0, "dropped": 0, "errors": 0}

    def handle_ohlc(ohlc: Ohlc) -> None:
        if not _validate(ohlc):
            stats["dropped"] += 1
            logger.warning("Dropped invalid tick: symbol=%s price=%s vol=%s",
                           ohlc.symbol, ohlc.close, ohlc.volume)
            return

        message = _build_message(ohlc)
        try:
            future = producer.send(config.kafka_ohlc_topic, value=message)

            def _on_error(exc: BaseException) -> None:
                stats["errors"] += 1
                logger.error(
                    "[%s] Kafka delivery failed: %s — symbol=%s price=%.2f",
                    ohlc.symbol, exc, ohlc.symbol, ohlc.close,
                )

            future.add_errback(_on_error)
            stats["sent"] += 1
            if stats["sent"] % 200 == 0:
                logger.info(
                    "[%s] %sm close=%.2f vol=%s total_sent=%s",
                    ohlc.symbol, ohlc.resolution,
                    ohlc.close, f"{ohlc.volume:,}", f"{stats['sent']:,}",
                )
        except Exception as exc:
            stats["errors"] += 1
            logger.error("Kafka send error: %s", exc)

    await client.connect()
    logger.info("Connected to DNSE. Subscribing: %s", SYMBOLS)

    await client.subscribe_ohlc_closed(
        symbols=list(SYMBOLS),
        resolution=config.ohlc_resolution,
        on_ohlc=handle_ohlc,
        encoding=config.dnse_encoding,
    )

    logger.info("OHLC producer running — Ctrl+C to stop")

    try:
        while True:
            await asyncio.sleep(60)
            logger.info(
                "Heartbeat | sent=%s dropped=%s errors=%s",
                f"{stats['sent']:,}", f"{stats['dropped']:,}", stats["errors"],
            )
    except asyncio.CancelledError:
        logger.info("Shutting down gracefully ...")
    finally:
        await client.disconnect()
        producer.flush(timeout=15)
        producer.close()
        logger.info("Done. Total sent: %s", f"{stats['sent']:,}")


def main() -> None:
    config = StreamingConfig()
    if not config.dnse_api_key or not config.dnse_api_secret:
        logger.error("DNSE_API_KEY and DNSE_API_SECRET must be set in .env")
        return

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(run(config))

    def _shutdown(sig, _frame):
        sig_name = signal.Signals(sig).name
        logger.info("Signal %s received", sig_name)
        task.cancel()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
