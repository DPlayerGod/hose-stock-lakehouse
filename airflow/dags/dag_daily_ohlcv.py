"""DAG: Daily OHLCV Pipeline.

Flow:
    extract_ohlcv
    → write_staging
    → validate_staging
    → write_bronze
    → transform_silver
    → validate_silver
    → build_gold_fact
    → validate_gold
    → sync_clickhouse

Each task delegates to functions in ``src/stock_lakehouse/``.
The DAG runs daily and supports backfill via execution_date.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Timezone — ICT (UTC+7, Đông Nam Á). Cron schedule_interval được hiểu theo
# múi giờ này khi start_date là timezone-aware.
# ---------------------------------------------------------------------------
LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task callables — thin wrappers calling src/ modules
# ---------------------------------------------------------------------------

def _get_config():
    """Build PipelineConfig from environment (Airflow sets env vars)."""
    from stock_lakehouse.config import PipelineConfig
    return PipelineConfig()


def _get_symbols():
    """Return the list of HOSE symbols to ingest."""
    from stock_lakehouse.pipelines.daily_ohlcv import DEFAULT_SYMBOLS
    return list(DEFAULT_SYMBOLS)


def task_extract_ohlcv(**context):
    """Extract OHLCV data from VNStock / VCI for the processing date."""
    from stock_lakehouse.ingestion.ohlcv import OhlcvExtractRequest, extract_ohlcv

    ds = context["ds"]  # YYYY-MM-DD
    symbols = _get_symbols()
    request = OhlcvExtractRequest.daily(ds, symbols=symbols, source="VCI")
    df = extract_ohlcv(request)
    # Push to XCom as serialisable metadata
    context["ti"].xcom_push(key="batch_id", value=request.batch_id)
    context["ti"].xcom_push(key="row_count", value=df.height)
    # Persist df in memory via file so downstream tasks can read from staging
    return {"batch_id": request.batch_id, "rows": df.height}


def task_write_staging(**context):
    """Write extracted OHLCV data to MinIO staging as Parquet."""
    from stock_lakehouse.ingestion.ohlcv import OhlcvExtractRequest, extract_ohlcv
    from stock_lakehouse.staging.writer import StagingPathBuilder, write_staging_parquet
    from stock_lakehouse.utils.dates import format_date

    ds = context["ds"]
    config = _get_config()
    batch_id = context["ti"].xcom_pull(task_ids="extract_ohlcv", key="batch_id")
    symbols = _get_symbols()

    request = OhlcvExtractRequest.daily(ds, symbols=symbols, source="VCI", batch_id=batch_id)
    df = extract_ohlcv(request)
    staging_uri = StagingPathBuilder(bucket=config.minio.bucket).ohlcv(format_date(ds), batch_id)
    write_staging_parquet(df, staging_uri, config.minio)
    context["ti"].xcom_push(key="staging_uri", value=staging_uri)


def task_validate_staging(**context):
    """Validate staging data before writing to Bronze."""
    from stock_lakehouse.quality import validate_bronze_ohlcv
    from stock_lakehouse.staging.writer import read_staging_parquet

    config = _get_config()
    ds = context["ds"]
    batch_id = context["ti"].xcom_pull(task_ids="extract_ohlcv", key="batch_id")
    staging_uri = context["ti"].xcom_pull(task_ids="write_staging", key="staging_uri")
    df = read_staging_parquet(staging_uri, config.minio)

    if df.is_empty():
        raise ValueError(f"Staging file is empty: {staging_uri}")

    result = validate_bronze_ohlcv(df)
    result.quarantine_and_raise(df, domain="staging_ohlcv", processing_date=ds, batch_id=batch_id, config=config.minio)
    context["ti"].xcom_push(key="staging_rows", value=df.height)


def task_write_bronze(**context):
    """Build Bronze OHLCV and write to Iceberg."""
    from stock_lakehouse.bronze.ohlcv import build_bronze_ohlcv
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import try_read_table
    from stock_lakehouse.iceberg.tables import BRONZE_OHLCV_SCHEMA, BRONZE_OHLCV_PARTITION_SPEC
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    from stock_lakehouse.staging.writer import read_staging_parquet
    from stock_lakehouse.utils.dates import format_date

    ds = context["ds"]
    config = _get_config()
    staging_uri = context["ti"].xcom_pull(task_ids="write_staging", key="staging_uri")
    staging_df = read_staging_parquet(staging_uri, config.minio)

    bronze_day = build_bronze_ohlcv(staging_df)
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    existing = try_read_table(catalog, f"{ns}.bronze_hose_ohlcv_daily")
    bronze_all = _replace_by_date(existing, bronze_day, date_column="time", processing_date=format_date(ds))

    write_dataframe(
        ensure_table(catalog, f"{ns}.bronze_hose_ohlcv_daily", BRONZE_OHLCV_SCHEMA, BRONZE_OHLCV_PARTITION_SPEC),
        bronze_all,
        mode="overwrite",
    )
    context["ti"].xcom_push(key="bronze_rows", value=bronze_day.height)


def task_transform_silver(**context):
    """Transform Bronze to Silver and write to Iceberg."""
    from stock_lakehouse.bronze.ohlcv import build_bronze_ohlcv
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import try_read_table
    from stock_lakehouse.iceberg.tables import SILVER_OHLCV_SCHEMA, SILVER_OHLCV_PARTITION_SPEC
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    from stock_lakehouse.silver.ohlcv import build_silver_ohlcv
    from stock_lakehouse.staging.writer import read_staging_parquet
    from stock_lakehouse.utils.dates import format_date

    ds = context["ds"]
    config = _get_config()
    staging_uri = context["ti"].xcom_pull(task_ids="write_staging", key="staging_uri")
    staging_df = read_staging_parquet(staging_uri, config.minio)

    bronze_day = build_bronze_ohlcv(staging_df)
    silver_day = build_silver_ohlcv(bronze_day, processing_date=format_date(ds))

    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    existing = try_read_table(catalog, f"{ns}.silver_hose_ohlcv_daily")
    silver_all = _replace_by_date(existing, silver_day, date_column="trading_date", processing_date=format_date(ds))

    write_dataframe(
        ensure_table(catalog, f"{ns}.silver_hose_ohlcv_daily", SILVER_OHLCV_SCHEMA, SILVER_OHLCV_PARTITION_SPEC),
        silver_all,
        mode="overwrite",
    )
    context["ti"].xcom_push(key="silver_rows", value=silver_day.height)


def task_validate_silver(**context):
    """Validate Silver OHLCV data."""
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.quality import validate_silver_ohlcv
    from stock_lakehouse.utils.dates import format_date

    ds = context["ds"]
    config = _get_config()
    batch_id = context["ti"].xcom_pull(task_ids="extract_ohlcv", key="batch_id")
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    table = catalog.load_table(f"{ns}.silver_hose_ohlcv_daily")
    silver = read_table(table)
    silver_day = silver.filter(silver["trading_date"].cast(str) == format_date(ds))

    result = validate_silver_ohlcv(silver_day, processing_date=format_date(ds))
    result.quarantine_and_raise(silver_day, domain="silver_ohlcv", processing_date=ds, batch_id=batch_id, config=config.minio)


def task_build_gold_fact(**context):
    """Build fact_hose_daily_market and write to Iceberg."""
    from stock_lakehouse.gold.fact_daily_market import build_fact_daily_market, replace_daily_market
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import try_read_table, read_table
    from stock_lakehouse.iceberg.tables import (
        FACT_HOSE_DAILY_MARKET_SCHEMA,
        FACT_HOSE_DAILY_MARKET_PARTITION_SPEC,
    )
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    from stock_lakehouse.utils.dates import format_date

    ds = context["ds"]
    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    silver_all = read_table(catalog.load_table(f"{ns}.silver_hose_ohlcv_daily"))
    dim_symbol = read_table(catalog.load_table(f"{ns}.dim_symbol"))
    dim_date = read_table(catalog.load_table(f"{ns}.dim_date"))

    fact_day = build_fact_daily_market(silver_all, dim_symbol, dim_date, processing_date=format_date(ds))

    existing_fact = try_read_table(catalog, f"{ns}.fact_hose_daily_market")
    fact_all = replace_daily_market(existing_fact, fact_day, format_date(ds))

    write_dataframe(
        ensure_table(catalog, f"{ns}.fact_hose_daily_market", FACT_HOSE_DAILY_MARKET_SCHEMA, FACT_HOSE_DAILY_MARKET_PARTITION_SPEC),
        fact_all,
        mode="overwrite",
    )
    context["ti"].xcom_push(key="fact_rows", value=fact_day.height)


def task_validate_gold(**context):
    """Validate Gold fact_hose_daily_market."""
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.quality import validate_fact_daily_market
    from stock_lakehouse.utils.dates import format_date

    ds = context["ds"]
    config = _get_config()
    batch_id = context["ti"].xcom_pull(task_ids="extract_ohlcv", key="batch_id")
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    fact = read_table(catalog.load_table(f"{ns}.fact_hose_daily_market"))
    dim_symbol = read_table(catalog.load_table(f"{ns}.dim_symbol"))
    dim_date = read_table(catalog.load_table(f"{ns}.dim_date"))

    fact_day = fact.filter(fact["trading_date"].cast(str) == format_date(ds))
    result = validate_fact_daily_market(fact_day, dim_symbol, dim_date)
    result.quarantine_and_raise(fact_day, domain="gold_fact", processing_date=ds, batch_id=batch_id, config=config.minio)


def task_sync_clickhouse(**context):
    """Sync Gold tables to ClickHouse for the processing date."""
    from stock_lakehouse.clickhouse.loader import sync_fact_to_clickhouse
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.utils.dates import format_date

    ds = context["ds"]
    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace

    fact = read_table(catalog.load_table(f"{ns}.fact_hose_daily_market"))
    sync_fact_to_clickhouse(fact, processing_date=format_date(ds), config=config.clickhouse)


# ---------------------------------------------------------------------------
# Helper — reuse the single source of truth (no duplication)
# ---------------------------------------------------------------------------
from stock_lakehouse.pipelines.ohlcv_core import _replace_by_date


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dag_daily_ohlcv",
    default_args=default_args,
    description="Daily HOSE OHLCV pipeline: Staging → Bronze → Silver → Gold → ClickHouse",
    schedule_interval="0 18 * * 1-5",  # 18:00 ICT (UTC+7), các ngày trong tuần, sau giờ đóng cửa
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "ohlcv", "daily"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_ohlcv",
        python_callable=task_extract_ohlcv,
    )

    t_staging = PythonOperator(
        task_id="write_staging",
        python_callable=task_write_staging,
    )

    t_validate_staging = PythonOperator(
        task_id="validate_staging",
        python_callable=task_validate_staging,
    )

    t_bronze = PythonOperator(
        task_id="write_bronze",
        python_callable=task_write_bronze,
    )

    t_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=task_transform_silver,
    )

    t_validate_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=task_validate_silver,
    )

    t_gold = PythonOperator(
        task_id="build_gold_fact",
        python_callable=task_build_gold_fact,
    )

    t_validate_gold = PythonOperator(
        task_id="validate_gold",
        python_callable=task_validate_gold,
    )

    t_clickhouse = PythonOperator(
        task_id="sync_clickhouse",
        python_callable=task_sync_clickhouse,
    )

    # Linear dependency chain
    (
        t_extract
        >> t_staging
        >> t_validate_staging
        >> t_bronze
        >> t_silver
        >> t_validate_silver
        >> t_gold
        >> t_validate_gold
        >> t_clickhouse
    )
