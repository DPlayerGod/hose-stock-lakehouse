from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv


load_dotenv()


# Phạm vi đồ án: chỉ theo dõi 5 mã (giữ số lần gọi API trong giới hạn free-tier của vnstock).
SYMBOLS: tuple[str, ...] = ("FPT", "VCB", "HPG", "VNM", "MWG")

# Chỉ số thị trường — VNStock trả cùng dạng OHLCV như cổ phiếu (Quote(symbol="VNINDEX")).
INDEX_SYMBOLS: tuple[str, ...] = ("VNINDEX", "VN30")


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
    region: str = os.getenv("AWS_REGION", "us-east-1")
    bucket: str = "lakehouse"


@dataclass(frozen=True)
class IcebergConfig:
    catalog_name: str = "lakehouse"
    uri: str = os.getenv("CATALOG_URI_CLIENT", "http://localhost:8181")
    warehouse: str = os.getenv("CATALOG_WAREHOUSE", "s3://lakehouse/warehouse/")
    s3_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
    region: str = os.getenv("AWS_REGION", "us-east-1")
    namespace: str = "lakehouse"


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    username: str = os.getenv("CLICKHOUSE_USER", "admin")
    password: str = os.getenv("CLICKHOUSE_PASSWORD", "admin123")
    database: str = os.getenv("CLICKHOUSE_DB", "lakehouse")
    secure: bool = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"


@dataclass(frozen=True)
class PipelineConfig:
    minio: MinioConfig = MinioConfig()
    iceberg: IcebergConfig = IcebergConfig()
    clickhouse: ClickHouseConfig = ClickHouseConfig()
