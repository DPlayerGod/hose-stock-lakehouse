"""Streaming producer config — reads from .env."""
from __future__ import annotations

import os

from dotenv import load_dotenv


load_dotenv()


class StreamingConfig:
    # Kafka
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_ohlc_topic: str = os.getenv("KAFKA_TOPIC_OHLC", "dnse.ohlc")

    # DNSE WebSocket
    dnse_api_key: str = os.getenv("DNSE_API_KEY", "")
    dnse_api_secret: str = os.getenv("DNSE_API_SECRET", "")
    dnse_ws_url: str = os.getenv("DNSE_WS_URL", "wss://ws-openapi.dnse.com.vn")
    dnse_encoding: str = os.getenv("DNSE_ENCODING", "msgpack")
    ohlc_resolution: str = os.getenv("OHLC_RESOLUTION", "1")
