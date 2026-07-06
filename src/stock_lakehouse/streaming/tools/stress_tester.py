"""Stress Tester — Kafka & ClickHouse Streaming Pipeline Stress Tester.

Generates high-throughput mock OHLCV candle events and pushes them to Kafka
to measure the pipeline's maximum processing capacity (throughput) and latency under load.

Run:
    PYTHONPATH=src python -m stock_lakehouse.streaming.tools.stress_tester --mps 500 --duration 30
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from stock_lakehouse.streaming.producer.config import StreamingConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("stress_tester")

ICT = timezone(timedelta(hours=7))


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",  # matches production configurations
        retries=5,
        retry_backoff_ms=500,
        linger_ms=5,
        batch_size=16384,
    )


def generate_mock_candle(symbol: str) -> dict:
    """Generate a realistic looking mock OHLCV closed candle."""
    now = datetime.now(ICT)
    base_price = random.uniform(10.0, 150.0)
    change = random.uniform(-0.02, 0.02)
    close_price = base_price * (1 + change)
    open_price = base_price
    high_price = max(open_price, close_price) * random.uniform(1.0, 1.01)
    low_price = min(open_price, close_price) * random.uniform(0.99, 1.0)
    volume = random.randint(100, 50000)

    # Mimics DNSE websocket payload format
    return {
        "received_at": now.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "symbol": symbol,
        "resolution": "1",
        "open": round(open_price, 2),
        "high": round(high_price, 2),
        "low": round(low_price, 2),
        "close": round(close_price, 2),
        "volume": volume,
        "type": "ohlc",
        "time": int(now.timestamp()),
        "lastUpdated": int(now.timestamp() * 1000),
    }


async def run_stress_test(
    mps: int, duration: int, num_symbols: int, config: StreamingConfig
) -> None:
    producer = create_kafka_producer(config.kafka_bootstrap)
    logger.info("Connected to Kafka: %s", config.kafka_bootstrap)
    logger.info(
        "Starting stress test: target %d msg/sec, duration %d sec, %d mock symbols",
        mps,
        duration,
        num_symbols,
    )

    symbols = [f"SYM{i:03d}" for i in range(num_symbols)]
    total_sent = 0
    start_time = time.time()
    end_time = start_time + duration

    interval = 1.0 / mps if mps > 0 else 0
    stats = {"sent": 0, "errors": 0}

    def _on_error(exc):
        stats["errors"] += 1

    next_send = time.time()
    while time.time() < end_time:
        now = time.time()
        # Rate limiting logic
        if now < next_send:
            await asyncio.sleep(max(0, next_send - now))
            continue

        symbol = random.choice(symbols)
        candle = generate_mock_candle(symbol)

        try:
            future = producer.send(config.kafka_ohlc_topic, value=candle)
            future.add_errback(_on_error)
            stats["sent"] += 1
            total_sent += 1
        except Exception as exc:
            stats["errors"] += 1
            logger.error("Failed to send message: %s", exc)

        # Log progress every 5000 messages
        if stats["sent"] % 5000 == 0:
            elapsed = time.time() - start_time
            curr_throughput = stats["sent"] / elapsed if elapsed > 0 else 0
            logger.info(
                "Progress: Sent %d messages | Elapsed: %.2fs | Current Throughput: %.1f msg/sec | Errors: %d",
                stats["sent"],
                elapsed,
                curr_throughput,
                stats["errors"],
            )

        next_send += interval

    # Flush any remaining messages
    logger.info("Flushing producer...")
    producer.flush()

    total_duration = time.time() - start_time
    avg_throughput = total_sent / total_duration if total_duration > 0 else 0

    print("\n" + "=" * 50)
    print("  STRESS TEST COMPLETED  ")
    print("=" * 50)
    print(f"  Target MPS      : {mps} msg/sec")
    print(f"  Total Sent      : {total_sent:,} messages")
    print(f"  Actual Duration : {total_duration:.2f} seconds")
    print(f"  Avg Throughput  : {avg_throughput:.1f} msg/sec")
    print(f"  Failed Sends    : {stats['errors']} messages")
    print("=" * 50)
    print("  To view ingestion performance and latency distribution, run:")
    print("  PYTHONPATH=src python -m stock_lakehouse.streaming.tools.latency_tester --window 5")
    print("=" * 50 + "\n")

    producer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka Streaming Stress Tester")
    parser.add_argument(
        "--mps",
        type=int,
        default=200,
        help="Messages per second to produce (default: 200)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Duration of stress test in seconds (default: 30)",
    )
    parser.add_argument(
        "--symbols",
        type=int,
        default=50,
        help="Number of distinct mock symbols (default: 50)",
    )

    args = parser.parse_args()
    config = StreamingConfig()

    asyncio.run(
        run_stress_test(
            mps=args.mps,
            duration=args.duration,
            num_symbols=args.symbols,
            config=config,
        )
    )


if __name__ == "__main__":
    main()
