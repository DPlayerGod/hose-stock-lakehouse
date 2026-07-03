"""DAG: Corporate Events Pipeline using TaskFlow API.

Flow:
    extract_events
    → validate_staging
    → write_bronze
    → transform_silver
    → validate_silver
    → build_gold_fact
    → validate_gold
    → sync_clickhouse

NOTE: DAG này chạy độc lập theo schedule riêng (không phụ thuộc OHLCV).
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

BRONZE_TABLE = "bronze_hose_corporate_events"
SILVER_TABLE = "silver_hose_corporate_events"
FACT_TABLE = "fact_corporate_events"


def _get_config():
    from stock_lakehouse.config import PipelineConfig
    return PipelineConfig()


def _get_symbols():
    from stock_lakehouse.config import SYMBOLS
    return list(SYMBOLS)


def _get_processing_date(data_interval_end=None, logical_date=None):
    """Extract processing date from context."""
    if data_interval_end:
        return data_interval_end.date().isoformat()
    return logical_date


# ---------------------------------------------------------------------------
# TaskFlow tasks
# ---------------------------------------------------------------------------

@task
def extract_events(processing_date: str) -> dict:
    """Extract toàn bộ lịch sử sự kiện cho mọi mã, ghi staging Parquet."""
    from stock_lakehouse.ingestion.corporate_events import extract_corporate_events
    from stock_lakehouse.staging.writer import StagingPathBuilder, write_staging_parquet
    from uuid import uuid4

    config = _get_config()
    batch_id = uuid4().hex

    df = extract_corporate_events(_get_symbols(), source="VCI", batch_id=batch_id, processing_date=processing_date)
    staging_uri = StagingPathBuilder(bucket=config.minio.bucket).events(processing_date, batch_id)
    write_staging_parquet(df, staging_uri, config.minio)

    return {"batch_id": batch_id, "staging_uri": staging_uri, "row_count": df.height, "processing_date": processing_date}


@task
def validate_staging(batch_info: dict) -> dict:
    """Validate staging trước khi vào Bronze."""
    from stock_lakehouse.quality import validate_bronze_corporate_events
    from stock_lakehouse.staging.writer import read_staging_parquet

    config = _get_config()
    staging_uri = batch_info["staging_uri"]
    df = read_staging_parquet(staging_uri, config.minio)

    if df.is_empty():
        raise ValueError(f"Staging file is empty: {staging_uri}")
    validate_bronze_corporate_events(df).quarantine_and_raise(
        df, domain="staging_events", processing_date=batch_info["staging_uri"].split("/")[-2], 
        batch_id=batch_info["batch_id"], config=config.minio
    )
    return batch_info


@task
def write_bronze(batch_info: dict) -> dict:
    """Build Bronze + overwrite Iceberg (full snapshot)."""
    from stock_lakehouse.bronze.corporate_events import build_bronze_corporate_events
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.tables import BRONZE_CORPORATE_EVENTS_SCHEMA
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    from stock_lakehouse.staging.writer import read_staging_parquet

    config = _get_config()
    staging_uri = batch_info["staging_uri"]
    staging_df = read_staging_parquet(staging_uri, config.minio)

    bronze = build_bronze_corporate_events(staging_df)
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    write_dataframe(
        ensure_table(catalog, f"{ns}.{BRONZE_TABLE}", BRONZE_CORPORATE_EVENTS_SCHEMA),
        bronze,
        mode="overwrite",
    )
    return {**batch_info, "bronze_rows": bronze.height}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _read_table(table):
    """Helper to read Iceberg table."""
    from stock_lakehouse.iceberg.reader import read_table as _read
    return _read(table)


# ---------------------------------------------------------------------------
# TaskFlow tasks
# ---------------------------------------------------------------------------

@task
def transform_silver(batch_info: dict) -> dict:
    """Bronze → Silver (dedup event_id + suy event_label) + overwrite Iceberg."""
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.tables import SILVER_CORPORATE_EVENTS_SCHEMA
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    from stock_lakehouse.silver.corporate_events import build_silver_corporate_events

    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    bronze = _read_table(catalog.load_table(f"{ns}.{BRONZE_TABLE}"))
    silver = build_silver_corporate_events(bronze)

    write_dataframe(
        ensure_table(catalog, f"{ns}.{SILVER_TABLE}", SILVER_CORPORATE_EVENTS_SCHEMA),
        silver,
        mode="overwrite",
    )
    return {**batch_info, "silver_rows": silver.height}


@task
def validate_silver(batch_info: dict) -> dict:
    """Validate Silver."""
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.quality import validate_silver_corporate_events

    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    silver = _read_table(catalog.load_table(f"{ns}.{SILVER_TABLE}"))
    validate_silver_corporate_events(silver).quarantine_and_raise(
        silver, domain="silver_events", processing_date="latest", batch_id=batch_info["batch_id"], config=config.minio
    )
    return batch_info


@task
def build_gold_fact(batch_info: dict) -> dict:
    """Build factless fact + overwrite Iceberg (FK fail-loud dim_symbol + dim_date)."""
    from stock_lakehouse.gold.fact_corporate_events import build_fact_corporate_events
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.iceberg.tables import FACT_CORPORATE_EVENTS_SCHEMA
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe

    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    silver = _read_table(catalog.load_table(f"{ns}.{SILVER_TABLE}"))
    dim_symbol = read_table(catalog.load_table(f"{ns}.dim_symbol"))
    dim_date = read_table(catalog.load_table(f"{ns}.dim_date"))

    fact = build_fact_corporate_events(silver, dim_symbol, dim_date)
    write_dataframe(
        ensure_table(catalog, f"{ns}.{FACT_TABLE}", FACT_CORPORATE_EVENTS_SCHEMA),
        fact,
        mode="overwrite",
    )
    return {**batch_info, "fact_rows": fact.height}


@task
def validate_gold(batch_info: dict) -> dict:
    """Validate Gold factless fact."""
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.quality import validate_fact_corporate_events

    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    fact = _read_table(catalog.load_table(f"{ns}.{FACT_TABLE}"))
    dim_symbol = read_table(catalog.load_table(f"{ns}.dim_symbol"))
    dim_date = read_table(catalog.load_table(f"{ns}.dim_date"))
    validate_fact_corporate_events(fact, dim_symbol, dim_date).quarantine_and_raise(
        fact, domain="gold_events_fact", processing_date="latest", batch_id=batch_info["batch_id"], config=config.minio
    )
    return batch_info


@task
def sync_clickhouse(batch_info: dict) -> None:
    """Sync factless fact sang ClickHouse (truncate + insert full snapshot)."""
    from stock_lakehouse.clickhouse.loader import sync_corporate_events_to_clickhouse
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table

    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    fact = _read_table(catalog.load_table(f"{ns}.{FACT_TABLE}"))
    sync_corporate_events_to_clickhouse(fact, config.clickhouse)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_corporate_events",
    default_args=default_args,
    description="HOSE corporate events: Staging → Bronze → Silver → Gold (factless fact) → ClickHouse",
    schedule_interval="30 18 * * 1-5",  # 18:30 ICT, T2-T6 (cùng lịch với dag_symbol_metadata)
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "corporate_events", "daily", "taskflow"],
) as dag:

    # Get processing date
    processing_date = "{{ ds }}"

    # Run pipeline
    batch_info = extract_events(processing_date)
    validated_staging = validate_staging(batch_info)
    bronze_info = write_bronze(validated_staging)
    silver_info = transform_silver(bronze_info)
    validated_silver = validate_silver(silver_info)
    gold_info = build_gold_fact(validated_silver)
    validated_gold = validate_gold(gold_info)
    sync_clickhouse(validated_gold)
