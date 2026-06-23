"""DAG: Sự kiện doanh nghiệp (cổ tức, phát hành, GD nội bộ, ĐHĐCĐ, niêm yết thêm…).

Flow (mirror dag_daily_index, nhưng nguồn là feed full-lịch-sử nên overwrite cả bảng,
KHÔNG slice theo ngày D):
    extract_events
    → write_staging
    → validate_staging
    → write_bronze
    → transform_silver
    → validate_silver
    → build_gold_fact   (factless fact: FK fail-loud dim_symbol + dim_date)
    → validate_gold
    → sync_clickhouse

Mỗi task uỷ thác cho function trong ``src/stock_lakehouse/``; file DAG chỉ điều phối.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

BRONZE_TABLE = "bronze_hose_corporate_events"
SILVER_TABLE = "silver_hose_corporate_events"
FACT_TABLE = "fact_corporate_events"


def _get_config():
    from stock_lakehouse.config import PipelineConfig

    return PipelineConfig()


def _get_symbols():
    from stock_lakehouse.config import SYMBOLS

    return list(SYMBOLS)


def task_extract_events(**context):
    """Extract toàn bộ lịch sử sự kiện cho mọi mã, ghi staging Parquet."""
    from stock_lakehouse.ingestion.corporate_events import extract_corporate_events
    from stock_lakehouse.staging.writer import StagingPathBuilder, write_staging_parquet
    from uuid import uuid4

    ds = context["ds"]
    config = _get_config()
    batch_id = uuid4().hex

    df = extract_corporate_events(_get_symbols(), source="VCI", batch_id=batch_id, processing_date=ds)
    staging_uri = StagingPathBuilder(bucket=config.minio.bucket).events(ds, batch_id)
    write_staging_parquet(df, staging_uri, config.minio)

    context["ti"].xcom_push(key="batch_id", value=batch_id)
    context["ti"].xcom_push(key="staging_uri", value=staging_uri)
    context["ti"].xcom_push(key="row_count", value=df.height)


def task_validate_staging(**context):
    """Validate staging trước khi vào Bronze."""
    from stock_lakehouse.quality import validate_bronze_corporate_events
    from stock_lakehouse.staging.writer import read_staging_parquet

    config = _get_config()
    ds = context["ds"]
    batch_id = context["ti"].xcom_pull(task_ids="extract_events", key="batch_id")
    staging_uri = context["ti"].xcom_pull(task_ids="extract_events", key="staging_uri")
    df = read_staging_parquet(staging_uri, config.minio)

    if df.is_empty():
        raise ValueError(f"Staging file is empty: {staging_uri}")
    validate_bronze_corporate_events(df).quarantine_and_raise(
        df, domain="staging_events", processing_date=ds, batch_id=batch_id, config=config.minio
    )


def task_write_bronze(**context):
    """Build Bronze + overwrite Iceberg (full snapshot)."""
    from stock_lakehouse.bronze.corporate_events import build_bronze_corporate_events
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.tables import BRONZE_CORPORATE_EVENTS_SCHEMA
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    from stock_lakehouse.staging.writer import read_staging_parquet

    config = _get_config()
    staging_uri = context["ti"].xcom_pull(task_ids="extract_events", key="staging_uri")
    staging_df = read_staging_parquet(staging_uri, config.minio)

    bronze = build_bronze_corporate_events(staging_df)
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    write_dataframe(
        ensure_table(catalog, f"{ns}.{BRONZE_TABLE}", BRONZE_CORPORATE_EVENTS_SCHEMA),
        bronze,
        mode="overwrite",
    )
    context["ti"].xcom_push(key="bronze_rows", value=bronze.height)


def task_transform_silver(**context):
    """Bronze → Silver (dedup event_id + suy event_label) + overwrite Iceberg."""
    from stock_lakehouse.bronze.corporate_events import build_bronze_corporate_events
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.tables import SILVER_CORPORATE_EVENTS_SCHEMA
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    from stock_lakehouse.silver.corporate_events import build_silver_corporate_events
    from stock_lakehouse.staging.writer import read_staging_parquet

    config = _get_config()
    staging_uri = context["ti"].xcom_pull(task_ids="extract_events", key="staging_uri")
    staging_df = read_staging_parquet(staging_uri, config.minio)

    bronze = build_bronze_corporate_events(staging_df)
    silver = build_silver_corporate_events(bronze)

    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    write_dataframe(
        ensure_table(catalog, f"{ns}.{SILVER_TABLE}", SILVER_CORPORATE_EVENTS_SCHEMA),
        silver,
        mode="overwrite",
    )
    context["ti"].xcom_push(key="silver_rows", value=silver.height)


def task_validate_silver(**context):
    """Validate Silver."""
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.quality import validate_silver_corporate_events

    config = _get_config()
    ds = context["ds"]
    batch_id = context["ti"].xcom_pull(task_ids="extract_events", key="batch_id")
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    silver = read_table(catalog.load_table(f"{ns}.{SILVER_TABLE}"))
    validate_silver_corporate_events(silver).quarantine_and_raise(
        silver, domain="silver_events", processing_date=ds, batch_id=batch_id, config=config.minio
    )


def task_build_gold_fact(**context):
    """Build factless fact + overwrite Iceberg (FK fail-loud dim_symbol + dim_date)."""
    from stock_lakehouse.gold.fact_corporate_events import build_fact_corporate_events
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.iceberg.tables import FACT_CORPORATE_EVENTS_SCHEMA
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe

    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    silver = read_table(catalog.load_table(f"{ns}.{SILVER_TABLE}"))
    dim_symbol = read_table(catalog.load_table(f"{ns}.dim_symbol"))
    dim_date = read_table(catalog.load_table(f"{ns}.dim_date"))

    fact = build_fact_corporate_events(silver, dim_symbol, dim_date)
    write_dataframe(
        ensure_table(catalog, f"{ns}.{FACT_TABLE}", FACT_CORPORATE_EVENTS_SCHEMA),
        fact,
        mode="overwrite",
    )
    context["ti"].xcom_push(key="fact_rows", value=fact.height)


def task_validate_gold(**context):
    """Validate Gold factless fact."""
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.quality import validate_fact_corporate_events

    config = _get_config()
    ds = context["ds"]
    batch_id = context["ti"].xcom_pull(task_ids="extract_events", key="batch_id")
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    fact = read_table(catalog.load_table(f"{ns}.{FACT_TABLE}"))
    dim_symbol = read_table(catalog.load_table(f"{ns}.dim_symbol"))
    dim_date = read_table(catalog.load_table(f"{ns}.dim_date"))
    validate_fact_corporate_events(fact, dim_symbol, dim_date).quarantine_and_raise(
        fact, domain="gold_events_fact", processing_date=ds, batch_id=batch_id, config=config.minio
    )


def task_sync_clickhouse(**context):
    """Sync factless fact sang ClickHouse (truncate + insert full snapshot)."""
    from stock_lakehouse.clickhouse.loader import sync_corporate_events_to_clickhouse
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table

    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    fact = read_table(catalog.load_table(f"{ns}.{FACT_TABLE}"))
    sync_corporate_events_to_clickhouse(fact, config.clickhouse)


with DAG(
    dag_id="dag_corporate_events",
    default_args=default_args,
    description="HOSE corporate events: Staging → Bronze → Silver → Gold (factless fact) → ClickHouse",
    schedule_interval="30 18 * * 1-5",  # 18:30 ICT, sau OHLCV/index
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "corporate_events", "daily"],
) as dag:

    t_extract = PythonOperator(task_id="extract_events", python_callable=task_extract_events)
    t_validate_staging = PythonOperator(task_id="validate_staging", python_callable=task_validate_staging)
    t_bronze = PythonOperator(task_id="write_bronze", python_callable=task_write_bronze)
    t_silver = PythonOperator(task_id="transform_silver", python_callable=task_transform_silver)
    t_validate_silver = PythonOperator(task_id="validate_silver", python_callable=task_validate_silver)
    t_gold = PythonOperator(task_id="build_gold_fact", python_callable=task_build_gold_fact)
    t_validate_gold = PythonOperator(task_id="validate_gold", python_callable=task_validate_gold)
    t_clickhouse = PythonOperator(task_id="sync_clickhouse", python_callable=task_sync_clickhouse)

    (
        t_extract
        >> t_validate_staging
        >> t_bronze
        >> t_silver
        >> t_validate_silver
        >> t_gold
        >> t_validate_gold
        >> t_clickhouse
    )
