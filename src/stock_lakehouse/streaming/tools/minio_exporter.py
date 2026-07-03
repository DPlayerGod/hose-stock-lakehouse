"""
MinIO Exporter — Daily Parquet backup for Stock Lakehouse Streaming
Gửi OHLCV (rt_hose_ohlcv_1m), Indicators (rt_hose_indicators),
và Alerts (rt_hose_alerts) từ ClickHouse sang MinIO trước khi TTL 90 ngày xóa.

Layout: s3://lakehouse/rt_backup/<candles|indicators|alerts>/date=YYYY-MM-DD/data.parquet
(ghi đè nếu chạy lại — idempotent; row count phải khớp với DB)
Schedule: Airflow DAG, mỗi ngày 1 lần (15:30 ICT Mon-Fri)
Run thủ công: uv run python -m stock_lakehouse.streaming.tools.minio_exporter [YYYY-MM-DD]
"""

import io
import logging
import os
import sys
from datetime import datetime, timezone, timedelta

import clickhouse_connect
import pandas as pd
from minio import Minio

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger('minio_exporter')

ICT = timezone(timedelta(hours=7))


def get_env(key: str, default: str = '') -> str:
    from dotenv import load_dotenv
    # .env ở thư mục gốc dự án: src/stock_lakehouse/streaming/tools/minio_exporter.py
    _dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '.env')
    load_dotenv(dotenv_path=os.path.abspath(_dotenv_path))
    return os.getenv(key, default)


# ──────────────────────────────────────────────────────────────────────────────
# Khai báo schema cho từng bảng — phải khớp 100% với detector.py & init_streaming.sql
# ──────────────────────────────────────────────────────────────────────────────
SCHEMAS = {
    'rt_candles': {
        'table': 'rt_hose_ohlcv_1m',
        'date_col': 'candle_time',
        'path_prefix': 'rt_backup/candles',
        'columns': ['candle_time', 'symbol', 'open', 'high', 'low', 'close', 'volume'],
        'select': "candle_time, symbol, open, high, low, close, volume",
    },
    'rt_indicators': {
        'table': 'rt_hose_indicators',
        'date_col': 'candle_time',
        'path_prefix': 'rt_backup/indicators',
        'columns': [
            'candle_time', 'symbol', 'open', 'high', 'low', 'close', 'volume',
            'vwap', 'sigma', 'rsi14', 'volume_ratio', 'created_at',
        ],
        'select': ("candle_time, symbol, open, high, low, close, volume, "
                   "vwap, sigma, rsi14, volume_ratio, created_at"),
    },
    'rt_alerts': {
        'table': 'rt_hose_alerts',
        'date_col': 'alert_time',
        'path_prefix': 'rt_backup/alerts',
        'columns': [
            'alert_time', 'symbol', 'rule_name', 'alert_type', 'severity',
            'price', 'indicator_value', 'threshold', 'deviation_pct', 'message',
        ],
        'select': ("alert_time, symbol, rule_name, alert_type, severity, "
                   "price, indicator_value, threshold, deviation_pct, message"),
    },
}


def _export_one(ch, mc, bucket: str, date_str: str, spec: dict) -> int:
    """Export 1 bảng từ ClickHouse sang MinIO Parquet. Trả về số row."""
    table = spec['table']
    sql = (
        f"SELECT {spec['select']} FROM {table} "
        f"WHERE toDate({spec['date_col']}) = '{date_str}' "
        f"ORDER BY symbol, {spec['date_col']}"
    )
    rows = ch.query(sql).result_rows
    if not rows:
        logger.info(f"  {table}: 0 rows for {date_str} (skip)")
        return 0
    df = pd.DataFrame(rows, columns=spec['columns'])
    obj = f"{spec['path_prefix']}/date={date_str}/data.parquet"
    _upload_parquet(mc, bucket, obj, df)
    logger.info(f"  {table}: {len(df):,} rows -> s3://{bucket}/{obj}")
    return len(df)


def export(export_date: str | None = None) -> None:
    date_str = export_date or datetime.now(ICT).strftime('%Y-%m-%d')
    logger.info(f"=== Streaming export for {date_str} ===")

    ch = clickhouse_connect.get_client(
        host=get_env('CLICKHOUSE_HOST', 'localhost'),
        port=int(get_env('CLICKHOUSE_HTTP_PORT', '8123')),
        username=get_env('CLICKHOUSE_USER', 'admin'),
        password=get_env('CLICKHOUSE_PASSWORD', 'admin123'),
        database=get_env('CLICKHOUSE_DB', 'lakehouse'),
    )

    raw_endpoint = get_env('MINIO_ENDPOINT', 'localhost:9000')
    endpoint = raw_endpoint.replace('http://', '').replace('https://', '')

    mc = Minio(
        endpoint=endpoint,
        access_key=get_env('MINIO_ACCESS_KEY', 'admin'),
        secret_key=get_env('MINIO_SECRET_KEY', 'admin123'),
        secure=False,
    )
    bucket = get_env('MINIO_BUCKET', 'lakehouse')
    if not mc.bucket_exists(bucket):
        mc.make_bucket(bucket)
        logger.info(f"Created bucket: s3://{bucket}")

    total = 0
    for spec in SCHEMAS.values():
        total += _export_one(ch, mc, bucket, date_str, spec)
    logger.info(f"=== Done. Total: {total:,} rows for {date_str} ===")


def _upload_parquet(mc: Minio, bucket: str, object_name: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, engine='pyarrow', index=False, compression='snappy')
    size = buf.tell()
    buf.seek(0)
    mc.put_object(bucket, object_name, buf, size,
                  content_type='application/octet-stream')


if __name__ == '__main__':
    export(sys.argv[1] if len(sys.argv) > 1 else None)
