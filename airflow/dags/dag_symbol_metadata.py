"""DAG: Symbol Metadata Pipeline using TaskFlow API.

Flow:
    extract_hose_symbols
    → write_staging_symbols
    → write_bronze_symbols
    → transform_silver_symbols
    → validate_silver_symbols
    → upsert_dim_symbol
    → sync_dim_symbol_to_clickhouse
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


def _get_config():
    from stock_lakehouse.config import PipelineConfig
    return PipelineConfig()


# ---------------------------------------------------------------------------
# TaskFlow tasks
# ---------------------------------------------------------------------------

@task
def extract_hose_symbols() -> dict:
    from stock_lakehouse.ingestion.symbols import extract_hose_symbols
    from uuid import uuid4
    batch_id = uuid4().hex
    df = extract_hose_symbols(batch_id=batch_id)
    return {"batch_id": batch_id, "row_count": df.height}


@task
def write_staging_symbols(batch_info: dict) -> str:
    from stock_lakehouse.ingestion.symbols import extract_hose_symbols
    from stock_lakehouse.staging.writer import StagingPathBuilder, StagingPath, write_staging_parquet
    config = _get_config()
    batch_id = batch_info["batch_id"]
    df = extract_hose_symbols(batch_id=batch_id)
    uri = StagingPathBuilder(bucket=config.minio.bucket).build(
        StagingPath(domain="symbols", processing_date="latest", batch_id=batch_id)
    )
    write_staging_parquet(df, uri, config.minio)
    return uri


@task
def write_bronze_symbols(staging_uri: str) -> int:
    from stock_lakehouse.bronze.symbols import build_bronze_symbols
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.tables import BRONZE_SYMBOLS_SCHEMA
    from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
    from stock_lakehouse.staging.writer import read_staging_parquet
    config = _get_config()
    df = read_staging_parquet(staging_uri, config.minio)
    bronze = build_bronze_symbols(df)
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    write_dataframe(
        ensure_table(catalog, f"{ns}.bronze_hose_symbols", BRONZE_SYMBOLS_SCHEMA),
        bronze, mode="overwrite",
    )
    return bronze.height


@task
def transform_silver_symbols(bronze_rows: int) -> int:
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
    return silver.height


@task
def validate_silver_symbols(silver_rows: int) -> None:
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    from stock_lakehouse.quality import validate_silver_symbols
    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    silver = read_table(catalog.load_table(f"{ns}.silver_hose_symbols"))
    validate_silver_symbols(silver).raise_for_errors()


@task
def upsert_dim_symbol(silver_rows: int) -> int:
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
    return dim_symbol.height


@task
def sync_dim_symbol_to_clickhouse(dim_symbol_rows: int) -> None:
    from stock_lakehouse.clickhouse.loader import sync_dim_symbol_to_clickhouse
    from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
    from stock_lakehouse.iceberg.reader import read_table
    config = _get_config()
    catalog = load_lakehouse_catalog(config.iceberg)
    ns = config.iceberg.namespace
    dim_symbol = read_table(catalog.load_table(f"{ns}.dim_symbol"))
    sync_dim_symbol_to_clickhouse(dim_symbol, config.clickhouse)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_symbol_metadata",
    default_args=default_args,
    description="HOSE symbol metadata: Extract → Bronze → Silver → dim_symbol → ClickHouse",
    schedule_interval="0 17 * * 0",  # 17:00 ICT (UTC+7) Chủ nhật hàng tuần
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "symbols", "metadata", "taskflow"],
) as dag:
    batch_info = extract_hose_symbols()
    staging_uri = write_staging_symbols(batch_info)
    bronze_rows = write_bronze_symbols(staging_uri)
    silver_rows = transform_silver_symbols(bronze_rows)
    validated = validate_silver_symbols(silver_rows)
    dim_rows = upsert_dim_symbol(validated)
    sync_dim_symbol_to_clickhouse(dim_rows)
