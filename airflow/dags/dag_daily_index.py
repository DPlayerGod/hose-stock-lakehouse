"""DAG: Daily Market-Index Pipeline using TaskFlow API.

Flow:
    extract_index
    → validate_staging
    → write_bronze
    → transform_silver
    → validate_silver
    → build_gold_fact
    → validate_gold
    → sync_clickhouse

Note:
    - dag_dim_date must be run manually before this DAG (create dim_date table)
    - dag_corporate_events is triggered by dag_daily_ohlcv only (avoid duplicate triggers)
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

# ---------------------------------------------------------------------------
# Timezone — ICT (UTC+7)
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

BRONZE_TABLE = "bronze_hose_index_daily"
SILVER_TABLE = "silver_hose_index_daily"
FACT_TABLE = "fact_hose_index_daily"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _get_config():
    """Build PipelineConfig from environment."""
    from stock_lakehouse.config import PipelineConfig
    return PipelineConfig()


def _get_indices():
    """Return the list of market indices to ingest."""
    from stock_lakehouse.pipelines.daily_index import DEFAULT_INDICES
    return list(DEFAULT_INDICES)


# ---------------------------------------------------------------------------
# DAG definition using TaskFlow API
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dag_daily_index",
    default_args=default_args,
    description="Daily HOSE market-index pipeline (TaskFlow API): Staging → Bronze → Silver → Gold → ClickHouse",
    schedule_interval="0 18 * * 1-5",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "index", "daily", "taskflow"],
) as dag:

    # -------------------------------------------------------------------------
    # Task definitions using @task decorator
    # -------------------------------------------------------------------------

    @task
    def extract_index(data_interval_end=None):
        """Extract index OHLCV from VNStock / VCI and write to staging."""
        from stock_lakehouse.ingestion.ohlcv import OhlcvExtractRequest, extract_ohlcv
        from stock_lakehouse.staging.writer import StagingPathBuilder, write_staging_parquet
        from stock_lakehouse.utils.dates import format_date

        ds = data_interval_end.in_timezone(LOCAL_TZ).date().isoformat()
        request = OhlcvExtractRequest.daily(ds, symbols=_get_indices(), source="VCI")
        df = extract_ohlcv(request)

        # Write directly to staging
        config = _get_config()
        staging_uri = StagingPathBuilder(bucket=config.minio.bucket).index(format_date(ds), request.batch_id)
        write_staging_parquet(df, staging_uri, config.minio)

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
            domain="staging_index",
            processing_date=processing_date,
            batch_id=batch_id,
            config=config.minio
        )
        return {**metadata, "staging_rows": df.height}

    @task
    def write_bronze(metadata: dict):
        """Build Bronze index OHLCV and write to Iceberg."""
        from stock_lakehouse.bronze.ohlcv import build_bronze_ohlcv
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
        from stock_lakehouse.iceberg.tables import BRONZE_OHLCV_SCHEMA, BRONZE_OHLCV_PARTITION_SPEC
        from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
        from stock_lakehouse.staging.writer import read_staging_parquet
        from stock_lakehouse.utils.dates import format_date

        config = _get_config()
        staging_uri = metadata["staging_uri"]
        processing_date = metadata["processing_date"]

        staging_df = read_staging_parquet(staging_uri, config.minio)
        bronze_day = build_bronze_ohlcv(staging_df)

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        write_dataframe(
            ensure_table(catalog, f"{ns}.{BRONZE_TABLE}", BRONZE_OHLCV_SCHEMA, BRONZE_OHLCV_PARTITION_SPEC),
            bronze_day,
            mode="overwrite",
            overwrite_filter=f"time = '{format_date(processing_date)}'",
        )
        return {**metadata, "bronze_rows": bronze_day.height}

    @task
    def transform_silver(metadata: dict):
        """Transform Bronze to Silver index and write to Iceberg."""
        from stock_lakehouse.bronze.ohlcv import build_bronze_ohlcv
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
        from stock_lakehouse.iceberg.tables import SILVER_OHLCV_SCHEMA, SILVER_OHLCV_PARTITION_SPEC
        from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
        from stock_lakehouse.silver.ohlcv import build_silver_ohlcv
        from stock_lakehouse.staging.writer import read_staging_parquet
        from stock_lakehouse.utils.dates import format_date

        config = _get_config()
        staging_uri = metadata["staging_uri"]
        processing_date = metadata["processing_date"]

        staging_df = read_staging_parquet(staging_uri, config.minio)
        bronze_day = build_bronze_ohlcv(staging_df)
        silver_day = build_silver_ohlcv(bronze_day, processing_date=format_date(processing_date))

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        write_dataframe(
            ensure_table(catalog, f"{ns}.{SILVER_TABLE}", SILVER_OHLCV_SCHEMA, SILVER_OHLCV_PARTITION_SPEC),
            silver_day,
            mode="overwrite",
            overwrite_filter=f"trading_date = '{format_date(processing_date)}'",
        )
        return {**metadata, "silver_rows": silver_day.height}

    @task
    def validate_silver(metadata: dict):
        """Validate Silver index OHLCV data."""
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
        from stock_lakehouse.iceberg.reader import read_table
        from stock_lakehouse.quality import validate_silver_ohlcv
        from stock_lakehouse.utils.dates import format_date

        config = _get_config()
        processing_date = metadata["processing_date"]
        batch_id = metadata["batch_id"]

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        silver = read_table(catalog.load_table(f"{ns}.{SILVER_TABLE}"))
        silver_day = silver.filter(silver["trading_date"].cast(str) == format_date(processing_date))

        result = validate_silver_ohlcv(silver_day, processing_date=format_date(processing_date))
        result.quarantine_and_raise(
            silver_day,
            domain="silver_index",
            processing_date=processing_date,
            batch_id=batch_id,
            config=config.minio
        )
        return metadata

    @task
    def build_gold_fact(metadata: dict):
        """Build fact_hose_index_daily and write to Iceberg (dim_date only, no dim_symbol)."""
        from stock_lakehouse.gold.fact_index_daily import build_fact_index_daily
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
        from stock_lakehouse.iceberg.reader import read_table
        from stock_lakehouse.iceberg.tables import (
            FACT_HOSE_INDEX_DAILY_SCHEMA,
            FACT_HOSE_INDEX_DAILY_PARTITION_SPEC,
        )
        from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
        from stock_lakehouse.utils.dates import format_date

        config = _get_config()
        processing_date = metadata["processing_date"]

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        silver_all = read_table(catalog.load_table(f"{ns}.{SILVER_TABLE}"))
        dim_date = read_table(catalog.load_table(f"{ns}.dim_date"))

        fact_day = build_fact_index_daily(silver_all, dim_date, processing_date=format_date(processing_date))

        write_dataframe(
            ensure_table(catalog, f"{ns}.{FACT_TABLE}", FACT_HOSE_INDEX_DAILY_SCHEMA, FACT_HOSE_INDEX_DAILY_PARTITION_SPEC),
            fact_day,
            mode="overwrite",
            overwrite_filter=f"trading_date = '{format_date(processing_date)}'",
        )
        return {**metadata, "fact_rows": fact_day.height}

    @task
    def validate_gold(metadata: dict):
        """Validate Gold fact_hose_index_daily."""
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
        from stock_lakehouse.iceberg.reader import read_table
        from stock_lakehouse.quality import validate_fact_index_daily
        from stock_lakehouse.utils.dates import format_date

        config = _get_config()
        processing_date = metadata["processing_date"]
        batch_id = metadata["batch_id"]

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        fact = read_table(catalog.load_table(f"{ns}.{FACT_TABLE}"))
        dim_date = read_table(catalog.load_table(f"{ns}.dim_date"))

        fact_day = fact.filter(fact["trading_date"].cast(str) == format_date(processing_date))
        result = validate_fact_index_daily(fact_day, dim_date)
        result.quarantine_and_raise(
            fact_day,
            domain="gold_index_fact",
            processing_date=processing_date,
            batch_id=batch_id,
            config=config.minio
        )
        return metadata

    @task
    def sync_clickhouse(metadata: dict):
        """Sync index fact to ClickHouse."""
        from stock_lakehouse.clickhouse.loader import sync_index_fact_to_clickhouse
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
        from stock_lakehouse.iceberg.reader import read_table
        from stock_lakehouse.utils.dates import format_date

        config = _get_config()
        processing_date = metadata["processing_date"]

        catalog = load_lakehouse_catalog(config.iceberg)
        ns = config.iceberg.namespace

        fact = read_table(catalog.load_table(f"{ns}.{FACT_TABLE}"))
        sync_index_fact_to_clickhouse(fact, processing_date=format_date(processing_date), config=config.clickhouse)
        return metadata
    # -------------------------------------------------------------------------
    # TaskFlow — sequential pipeline: extract → validate → bronze → silver → gold → sync
    # -------------------------------------------------------------------------

    extract_result = extract_index()
    validate_stg_result = validate_staging(extract_result)
    bronze_result = write_bronze(validate_stg_result)
    silver_result = transform_silver(bronze_result)
    validate_silver_result = validate_silver(silver_result)
    gold_result = build_gold_fact(validate_silver_result)
    validate_gold_result = validate_gold(gold_result)
    sync_result = sync_clickhouse(validate_gold_result)
