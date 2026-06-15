"""DAG: HOSE Symbol Metadata Pipeline.

Flow (overview approach — small table, full overwrite each run):
    extract_hose_symbols → write_staging_symbols → write_bronze_symbols
    → transform_silver_symbols → validate_silver_symbols
    → upsert_dim_symbol → sync_dim_symbol_to_clickhouse
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# ICT (UTC+7, Đông Nam Á) — cron schedule_interval được hiểu theo múi giờ này.
LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _get_config():
    from stock_lakehouse.config import PipelineConfig
    return PipelineConfig()


def task_extract_hose_symbols(**ctx):
    from stock_lakehouse.ingestion.symbols import extract_hose_symbols
    from uuid import uuid4
    batch_id = uuid4().hex
    df = extract_hose_symbols(batch_id=batch_id)
    ctx["ti"].xcom_push(key="batch_id", value=batch_id)
    ctx["ti"].xcom_push(key="row_count", value=df.height)


def task_write_staging_symbols(**ctx):
    from stock_lakehouse.ingestion.symbols import extract_hose_symbols
    from stock_lakehouse.staging.writer import StagingPathBuilder, StagingPath, write_staging_parquet
    config = _get_config()
    batch_id = ctx["ti"].xcom_pull(task_ids="extract_hose_symbols", key="batch_id")
    df = extract_hose_symbols(batch_id=batch_id)
    uri = StagingPathBuilder(bucket=config.minio.bucket).build(
        StagingPath(domain="symbols", processing_date="latest", batch_id=batch_id)
    )
    write_staging_parquet(df, uri, config.minio)
    ctx["ti"].xcom_push(key="staging_uri", value=uri)


def task_write_bronze_symbols(**ctx):
    from stock_lakehouse.bronze.symbols import build_bronze_symbols
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.tables import BRONZE_SYMBOLS_SCHEMA
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    from stock_lakehouse.staging.writer import read_staging_parquet
    config = _get_config()
    uri = ctx["ti"].xcom_pull(task_ids="write_staging_symbols", key="staging_uri")
    df = read_staging_parquet(uri, config.minio)
    bronze = build_bronze_symbols(df)
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    write_dataframe(
        ensure_table(catalog, f"{ns}.bronze_hose_symbols", BRONZE_SYMBOLS_SCHEMA),
        bronze, mode="overwrite",
    )
    ctx["ti"].xcom_push(key="bronze_rows", value=bronze.height)


def task_transform_silver_symbols(**ctx):
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.iceberg.tables import SILVER_SYMBOLS_SCHEMA
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    from stock_lakehouse.silver.symbols import build_silver_symbols
    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    bronze = read_table(catalog.load_table(f"{ns}.bronze_hose_symbols"))
    silver = build_silver_symbols(bronze)
    write_dataframe(
        ensure_table(catalog, f"{ns}.silver_hose_symbols", SILVER_SYMBOLS_SCHEMA),
        silver, mode="overwrite",
    )
    ctx["ti"].xcom_push(key="silver_rows", value=silver.height)


def task_validate_silver_symbols(**ctx):
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.silver.symbols import validate_silver_symbols
    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    silver = read_table(catalog.load_table(f"{ns}.silver_hose_symbols"))
    validate_silver_symbols(silver).raise_for_errors()


def task_upsert_dim_symbol(**ctx):
    from stock_lakehouse.gold.dim_symbol import build_dim_symbol
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import try_read_table, read_table
    from stock_lakehouse.iceberg.tables import DIM_SYMBOL_SCHEMA
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    silver = read_table(catalog.load_table(f"{ns}.silver_hose_symbols"))
    existing_dim = try_read_table(catalog, f"{ns}.dim_symbol")
    dim_symbol = build_dim_symbol(silver, existing_dim)
    write_dataframe(
        ensure_table(catalog, f"{ns}.dim_symbol", DIM_SYMBOL_SCHEMA),
        dim_symbol, mode="overwrite",
    )
    ctx["ti"].xcom_push(key="dim_symbol_rows", value=dim_symbol.height)


def task_sync_dim_symbol_to_clickhouse(**ctx):
    from stock_lakehouse.clickhouse.loader import sync_dim_symbol_to_clickhouse
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    dim_symbol = read_table(catalog.load_table(f"{ns}.dim_symbol"))
    sync_dim_symbol_to_clickhouse(dim_symbol, config.clickhouse)


with DAG(
    dag_id="dag_symbol_metadata",
    default_args=default_args,
    description="HOSE symbol metadata: Extract → Bronze → Silver → dim_symbol → ClickHouse",
    schedule_interval="0 17 * * 0",  # 17:00 ICT (UTC+7) Chủ nhật hàng tuần
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "symbols", "metadata"],
) as dag:
    t1 = PythonOperator(task_id="extract_hose_symbols", python_callable=task_extract_hose_symbols)
    t2 = PythonOperator(task_id="write_staging_symbols", python_callable=task_write_staging_symbols)
    t3 = PythonOperator(task_id="write_bronze_symbols", python_callable=task_write_bronze_symbols)
    t4 = PythonOperator(task_id="transform_silver_symbols", python_callable=task_transform_silver_symbols)
    t5 = PythonOperator(task_id="validate_silver_symbols", python_callable=task_validate_silver_symbols)
    t6 = PythonOperator(task_id="upsert_dim_symbol", python_callable=task_upsert_dim_symbol)
    t7 = PythonOperator(task_id="sync_dim_symbol_to_clickhouse", python_callable=task_sync_dim_symbol_to_clickhouse)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
