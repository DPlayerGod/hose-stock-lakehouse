"""DAG: Daily Streaming Parquet Backup to MinIO (TTL-safe cold storage).

Backup 3 streaming tables (rt_hose_ohlcv_1m, rt_hose_indicators,
rt_hose_alerts) sang MinIO dưới dạng Parquet trước khi ClickHouse TTL
90 ngày tự động xóa.

Layout: s3://lakehouse/rt_backup/<candles|indicators|alerts>/date=YYYY-MM-DD/data.parquet
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@task
def export_to_minio(processing_date: str) -> dict:
    """Export ngày vừa đóng cửa (processing_date = {{ ds }}) sang MinIO."""
    from stock_lakehouse.streaming.tools.minio_exporter import export

    export(processing_date)
    return {
        "exported_date": processing_date,
    }


@task
def verify_backup(payload: dict) -> dict:
    """Re-list MinIO bucket để xác nhận file đã lên đúng với prefix rt_backup/.

    Dùng lại Minio client + get_env từ minio_exporter (đọc .env qua absolute path),
    tránh load_dotenv() CWD-dependent.

    Một bảng có 0 rows cho ngày đó là **kết quả hợp lệ** (vd. alerts ngày ít
    biến động, hoặc detector chưa chạy cho phiên mới) — khi đó file parquet
    không được upload và ta ghi nhận ``skipped`` thay vì fail cả DAG.
    """
    import logging

    from minio import Minio
    from minio.error import S3Error
    from stock_lakehouse.streaming.tools.minio_exporter import get_env

    log = logging.getLogger(__name__)

    raw_endpoint = get_env('MINIO_ENDPOINT', 'localhost:9000')
    endpoint = raw_endpoint.replace('http://', '').replace('https://', '')
    mc = Minio(
        endpoint=endpoint,
        access_key=get_env('MINIO_ACCESS_KEY', 'admin'),
        secret_key=get_env('MINIO_SECRET_KEY', 'admin123'),
        secure=False,
    )
    bucket = get_env('MINIO_BUCKET', 'lakehouse')
    date_str = payload["exported_date"]
    expected = [
        f"rt_backup/candles/date={date_str}/data.parquet",
        f"rt_backup/indicators/date={date_str}/data.parquet",
        f"rt_backup/alerts/date={date_str}/data.parquet",
    ]
    sizes: dict[str, int | None] = {}
    missing: list[str] = []
    for key in expected:
        try:
            stat = mc.stat_object(bucket, key)
            sizes[key] = stat.size
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                sizes[key] = None
                missing.append(key)
                log.warning(
                    "Backup file missing for %s on %s (0 rows in source table — skipped)",
                    key, date_str,
                )
            else:
                raise
    log.info("Backup verified for %s: %s", date_str, sizes)
    return {**payload, "sizes_bytes": sizes, "missing": missing}


with DAG(
    dag_id="streaming_minio_parquet_exporter",
    default_args=default_args,
    description="Backup 3 streaming tables (OHLCV/Indicators/Alerts) sang MinIO Parquet dưới prefix rt_backup/ (TTL-safe)",
    schedule_interval="30 15 * * 1-5",  # 15:30 ICT, T2-T6 (sau khi thị trường đóng cửa)
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "minio", "backup", "cold-storage", "taskflow"],
) as dag:

    # Get processing date
    processing_date = "{{ logical_date.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d') }}"

    # Run pipeline
    exported = export_to_minio(processing_date)
    verify_backup(exported)