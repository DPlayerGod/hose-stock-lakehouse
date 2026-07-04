"""DAG: Daily OHLCV Pipeline using TaskFlow API.

Flow:
    extract_ohlcv
    → validate_staging
    → write_bronze
    → transform_silver
    → validate_silver
    → build_gold_fact
    → validate_gold
    → sync_clickhouse
    → trigger_corporate_events

Note:
    - dag_symbol_metadata must be run manually before this DAG (create dim_symbol table)
    - dag_corporate_events is triggered at the end of this DAG
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

# ---------------------------------------------------------------------------
# Timezone — ICT (UTC+7, Đông Nam Á)
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
# Helper functions (reused across tasks)
# ---------------------------------------------------------------------------

def _get_config():
    """Build PipelineConfig from environment."""
    from stock_lakehouse.config import PipelineConfig
    return PipelineConfig()


def _get_symbols():
    """Return the list of HOSE symbols to ingest."""
    from stock_lakehouse.pipelines.daily_ohlcv import DEFAULT_SYMBOLS
    return list(DEFAULT_SYMBOLS)


def _get_processing_date(data_interval_end=None, logical_date=None):
    """Extract processing date from context."""
    if data_interval_end:
        return data_interval_end.in_timezone(LOCAL_TZ).date().isoformat()
    return logical_date


# ---------------------------------------------------------------------------
# DAG definition using TaskFlow API
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dag_daily_ohlcv",
    default_args=default_args,
    description="Daily HOSE OHLCV pipeline (TaskFlow API): Staging → Bronze → Silver → Gold → ClickHouse",
    schedule_interval="0 18 * * 1-5",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "ohlcv", "daily", "taskflow"],
) as dag:

    # -------------------------------------------------------------------------
    # Task definitions using @task decorator
    # -------------------------------------------------------------------------

    @task
    def extract_ohlcv(data_interval_end=None):
        """Extract OHLCV data from VNStock / VCI and write to staging."""
        from stock_lakehouse.ingestion.ohlcv import OhlcvExtractRequest, extract_ohlcv
        from stock_lakehouse.staging.writer import StagingPathBuilder, write_staging_parquet
        from stock_lakehouse.utils.dates import format_date

        ds = data_interval_end.in_timezone(LOCAL_TZ).date().isoformat()
        symbols = _get_symbols()
        request = OhlcvExtractRequest.daily(ds, symbols=symbols, source="VCI")
        df = extract_ohlcv(request)

        # Write directly to staging
        config = _get_config()
        staging_uri = StagingPathBuilder(bucket=config.minio.bucket).ohlcv(format_date(ds), request.batch_id)
        write_staging_parquet(df, staging_uri, config.minio)

        # Return dict is automatically pushed to XCom
        return {
            "batch_id": request.batch_id,
            "staging_uri": staging_uri,
            "rows": df.height,
            "processing_date": ds,
        }

    @task
    def validate_staging(metadata: dict):
        """Validate staging data before writing to Bronze."""
        from stock_lakehouse.quality import validate_bronze_ohlcv
        from stock_lakehouse.staging.writer import read_staging_parquet

        config = _get_config()
        staging_uri = metadata["staging_uri"]
        batch_id = metadata["batch_id"]
        processing_date = metadata["processing_date"]

        df = read_staging_parquet(staging_uri, config.minio)
        if df.is_empty():
            raise ValueError(f"Staging file is empty: {staging_uri}")

        result = validate_bronze_ohlcv(df)
        result.quarantine_and_raise(
            df,
            domain="staging_ohlcv",
            processing_date=processing_date,
            batch_id=batch_id,
            config=config.minio
        )
        return {**metadata, "staging_rows": df.height}

    @task
    def write_bronze(metadata: dict):
        """Build Bronze OHLCV and write to Iceberg."""
        from stock_lakehouse.bronze.ohlcv import build_bronze_ohlcv
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
        from stock_lakehouse.iceberg.reader import try_read_table
        from stock_lakehouse.iceberg.tables import BRONZE_OHLCV_SCHEMA, BRONZE_OHLCV_PARTITION_SPEC
        from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
        from stock_lakehouse.staging.writer import read_staging_parquet
        from stock_lakehouse.utils.dates import format_date
        from stock_lakehouse.pipelines.ohlcv_core import _replace_by_date

        config = _get_config()
        staging_uri = metadata["staging_uri"]
        processing_date = metadata["processing_date"]

        staging_df = read_staging_parquet(staging_uri, config.minio)
        bronze_day = build_bronze_ohlcv(staging_df)

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        existing = try_read_table(catalog, f"{ns}.bronze_hose_ohlcv_daily")
        bronze_all = _replace_by_date(existing, bronze_day, date_column="time", processing_date=format_date(processing_date))

        write_dataframe(
            ensure_table(catalog, f"{ns}.bronze_hose_ohlcv_daily", BRONZE_OHLCV_SCHEMA, BRONZE_OHLCV_PARTITION_SPEC),
            bronze_all,
            mode="overwrite",
        )
        return {**metadata, "bronze_rows": bronze_day.height}

    @task
    def transform_silver(metadata: dict):
        """Transform Bronze to Silver and write to Iceberg."""
        from stock_lakehouse.bronze.ohlcv import build_bronze_ohlcv
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
        from stock_lakehouse.iceberg.reader import try_read_table
        from stock_lakehouse.iceberg.tables import SILVER_OHLCV_SCHEMA, SILVER_OHLCV_PARTITION_SPEC
        from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
        from stock_lakehouse.silver.ohlcv import build_silver_ohlcv
        from stock_lakehouse.staging.writer import read_staging_parquet
        from stock_lakehouse.utils.dates import format_date
        from stock_lakehouse.pipelines.ohlcv_core import _replace_by_date

        config = _get_config()
        staging_uri = metadata["staging_uri"]
        processing_date = metadata["processing_date"]

        staging_df = read_staging_parquet(staging_uri, config.minio)
        bronze_day = build_bronze_ohlcv(staging_df)
        silver_day = build_silver_ohlcv(bronze_day, processing_date=format_date(processing_date))

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        existing = try_read_table(catalog, f"{ns}.silver_hose_ohlcv_daily")
        silver_all = _replace_by_date(existing, silver_day, date_column="trading_date", processing_date=format_date(processing_date))

        write_dataframe(
            ensure_table(catalog, f"{ns}.silver_hose_ohlcv_daily", SILVER_OHLCV_SCHEMA, SILVER_OHLCV_PARTITION_SPEC),
            silver_all,
            mode="overwrite",
        )
        return {**metadata, "silver_rows": silver_day.height}

    @task
    def validate_silver(metadata: dict):
        """Validate Silver OHLCV data."""
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
        from stock_lakehouse.iceberg.reader import read_table
        from stock_lakehouse.quality import validate_silver_ohlcv
        from stock_lakehouse.utils.dates import format_date

        config = _get_config()
        processing_date = metadata["processing_date"]
        batch_id = metadata["batch_id"]

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        table = catalog.load_table(f"{ns}.silver_hose_ohlcv_daily")
        silver = read_table(table)
        silver_day = silver.filter(silver["trading_date"].cast(str) == format_date(processing_date))

        result = validate_silver_ohlcv(silver_day, processing_date=format_date(processing_date))
        result.quarantine_and_raise(
            silver_day,
            domain="silver_ohlcv",
            processing_date=processing_date,
            batch_id=batch_id,
            config=config.minio
        )
        return metadata

    @task
    def build_gold_fact(metadata: dict):
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

        config = _get_config()
        processing_date = metadata["processing_date"]

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        silver_all = read_table(catalog.load_table(f"{ns}.silver_hose_ohlcv_daily"))
        dim_symbol = read_table(catalog.load_table(f"{ns}.dim_symbol"))
        dim_date = read_table(catalog.load_table(f"{ns}.dim_date"))

        fact_day = build_fact_daily_market(silver_all, dim_symbol, dim_date, processing_date=format_date(processing_date))

        existing_fact = try_read_table(catalog, f"{ns}.fact_hose_daily_market")
        fact_all = replace_daily_market(existing_fact, fact_day, format_date(processing_date))

        write_dataframe(
            ensure_table(catalog, f"{ns}.fact_hose_daily_market", FACT_HOSE_DAILY_MARKET_SCHEMA, FACT_HOSE_DAILY_MARKET_PARTITION_SPEC),
            fact_all,
            mode="overwrite",
        )
        return {**metadata, "fact_rows": fact_day.height}

    @task
    def validate_gold(metadata: dict):
        """Validate Gold fact_hose_daily_market."""
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
        from stock_lakehouse.iceberg.reader import read_table
        from stock_lakehouse.quality import validate_fact_daily_market
        from stock_lakehouse.utils.dates import format_date

        config = _get_config()
        processing_date = metadata["processing_date"]
        batch_id = metadata["batch_id"]

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        fact = read_table(catalog.load_table(f"{ns}.fact_hose_daily_market"))
        dim_symbol = read_table(catalog.load_table(f"{ns}.dim_symbol"))
        dim_date = read_table(catalog.load_table(f"{ns}.dim_date"))

        fact_day = fact.filter(fact["trading_date"].cast(str) == format_date(processing_date))
        result = validate_fact_daily_market(fact_day, dim_symbol, dim_date)
        result.quarantine_and_raise(
            fact_day,
            domain="gold_fact",
            processing_date=processing_date,
            batch_id=batch_id,
            config=config.minio
        )
        return metadata

    @task
    def sync_clickhouse(metadata: dict):
        """Sync Gold tables to ClickHouse."""
        from stock_lakehouse.clickhouse.loader import sync_fact_to_clickhouse
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
        from stock_lakehouse.iceberg.reader import read_table
        from stock_lakehouse.utils.dates import format_date

        config = _get_config()
        processing_date = metadata["processing_date"]

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        fact = read_table(catalog.load_table(f"{ns}.fact_hose_daily_market"))
        sync_fact_to_clickhouse(fact, processing_date=format_date(processing_date), config=config.clickhouse)
        return metadata

    # -------------------------------------------------------------------------
    # TaskFlow — sequential pipeline: extract → validate → bronze → silver → gold → sync
    # -------------------------------------------------------------------------

    extract_result = extract_ohlcv()
    validate_stg_result = validate_staging(extract_result)
    bronze_result = write_bronze(validate_stg_result)
    silver_result = transform_silver(bronze_result)
    validate_silver_result = validate_silver(silver_result)
    gold_result = build_gold_fact(validate_silver_result)
    validate_gold_result = validate_gold(gold_result)
    sync_result = sync_clickhouse(validate_gold_result)
