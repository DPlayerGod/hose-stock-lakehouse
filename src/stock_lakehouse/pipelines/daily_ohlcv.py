from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import polars as pl

from stock_lakehouse.bronze.ohlcv import build_bronze_ohlcv
from stock_lakehouse.clickhouse.loader import sync_fact_to_clickhouse
from stock_lakehouse.config import PipelineConfig, SYMBOLS
from stock_lakehouse.gold.fact_daily_market import build_fact_daily_market, replace_daily_market
from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
from stock_lakehouse.iceberg.reader import read_table, try_read_table
from stock_lakehouse.iceberg.tables import (
    BRONZE_OHLCV_PARTITION_SPEC,
    BRONZE_OHLCV_SCHEMA,
    FACT_HOSE_DAILY_MARKET_PARTITION_SPEC,
    FACT_HOSE_DAILY_MARKET_SCHEMA,
    SILVER_OHLCV_PARTITION_SPEC,
    SILVER_OHLCV_SCHEMA,
)
from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
from stock_lakehouse.ingestion.ohlcv import OhlcvExtractRequest, extract_ohlcv
from stock_lakehouse.quality.gold import validate_fact_daily_market
from stock_lakehouse.quality.ohlcv import validate_bronze_ohlcv, validate_silver_ohlcv
from stock_lakehouse.silver.ohlcv import build_silver_ohlcv
from stock_lakehouse.staging.writer import StagingPathBuilder, write_staging_parquet
from stock_lakehouse.utils.dates import format_date, parse_date


DEFAULT_SYMBOLS = SYMBOLS


@dataclass(frozen=True)
class DailyPipelineResult:
    processing_date: str
    batch_id: str
    staging_uri: str
    bronze_rows: int
    silver_rows: int
    fact_rows: int


def run_daily_ohlcv_pipeline(
    processing_date: str,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
    source: str = "VCI",
    batch_id: str | None = None,
    sync_clickhouse: bool = True,
    config: PipelineConfig = PipelineConfig(),
) -> DailyPipelineResult:
    """Run the daily OHLCV pipeline end-to-end.

    Mirrors ``dags.dag_daily_ohlcv`` task chain:
        extract → staging → validate_staging → bronze → silver → validate_silver
        → build_gold_fact → validate_gold → sync_clickhouse

    ``dim_symbol`` and ``dim_date`` are read from existing Iceberg tables (owned by
    ``dag_symbol_metadata`` / ``dag_dim_date``); this pipeline never writes them.
    """
    day = parse_date(processing_date)
    pd_str = format_date(day)

    # 1. Extract → Staging
    request = OhlcvExtractRequest.daily(day, symbols=symbols, source=source, batch_id=batch_id)
    raw = extract_ohlcv(request)
    staging_uri = StagingPathBuilder(bucket=config.minio.bucket).ohlcv(pd_str, request.batch_id)
    write_staging_parquet(raw, staging_uri, config.minio)

    # 2. Validate staging
    if raw.is_empty():
        raise ValueError(f"Staging file is empty: {staging_uri}")
    validate_bronze_ohlcv(raw).quarantine_and_raise(
        raw, domain="staging_ohlcv", processing_date=pd_str, batch_id=request.batch_id, config=config.minio
    )

    catalog = load_lakehouse_catalog(config.iceberg)
    namespace = config.iceberg.namespace

    # 3. Bronze
    bronze_day = build_bronze_ohlcv(raw)
    bronze_all = _replace_by_date(
        try_read_table(catalog, f"{namespace}.bronze_hose_ohlcv_daily"),
        bronze_day,
        date_column="time",
        processing_date=pd_str,
    )
    write_dataframe(
        ensure_table(
            catalog,
            f"{namespace}.bronze_hose_ohlcv_daily",
            BRONZE_OHLCV_SCHEMA,
            BRONZE_OHLCV_PARTITION_SPEC,
        ),
        bronze_all,
        mode="overwrite",
    )

    # 4. Silver
    silver_day = build_silver_ohlcv(bronze_day, processing_date=pd_str)
    silver_all = _replace_by_date(
        try_read_table(catalog, f"{namespace}.silver_hose_ohlcv_daily"),
        silver_day,
        date_column="trading_date",
        processing_date=pd_str,
    )
    write_dataframe(
        ensure_table(
            catalog,
            f"{namespace}.silver_hose_ohlcv_daily",
            SILVER_OHLCV_SCHEMA,
            SILVER_OHLCV_PARTITION_SPEC,
        ),
        silver_all,
        mode="overwrite",
    )

    # 5. Validate silver (day D only)
    validate_silver_ohlcv(silver_day, processing_date=pd_str).quarantine_and_raise(
        silver_day, domain="silver_ohlcv", processing_date=pd_str, batch_id=request.batch_id, config=config.minio
    )

    # 6. Build Gold fact — dim_symbol / dim_date read from existing tables
    dim_symbol = read_table(catalog.load_table(f"{namespace}.dim_symbol"))
    dim_date = read_table(catalog.load_table(f"{namespace}.dim_date"))
    fact_day = build_fact_daily_market(silver_all, dim_symbol, dim_date, processing_date=pd_str)
    existing_fact = try_read_table(catalog, f"{namespace}.fact_hose_daily_market")
    fact_all = fact_day if existing_fact is None else replace_daily_market(existing_fact, fact_day, pd_str)
    write_dataframe(
        ensure_table(
            catalog,
            f"{namespace}.fact_hose_daily_market",
            FACT_HOSE_DAILY_MARKET_SCHEMA,
            FACT_HOSE_DAILY_MARKET_PARTITION_SPEC,
        ),
        fact_all,
        mode="overwrite",
    )

    # 7. Validate gold (day D only)
    validate_fact_daily_market(fact_day, dim_symbol, dim_date).quarantine_and_raise(
        fact_day, domain="gold_fact", processing_date=pd_str, batch_id=request.batch_id, config=config.minio
    )

    # 8. Sync fact to ClickHouse
    if sync_clickhouse:
        sync_fact_to_clickhouse(fact_all, processing_date=pd_str, config=config.clickhouse)

    return DailyPipelineResult(
        processing_date=pd_str,
        batch_id=request.batch_id,
        staging_uri=staging_uri,
        bronze_rows=bronze_day.height,
        silver_rows=silver_day.height,
        fact_rows=fact_day.height,
    )


def _replace_by_date(
    existing: pl.DataFrame | None,
    replacement: pl.DataFrame,
    *,
    date_column: str,
    processing_date: str,
) -> pl.DataFrame:
    if existing is None or existing.is_empty():
        return replacement
    return (
        pl.concat(
            [
                existing.filter(pl.col(date_column).cast(pl.Utf8) != processing_date),
                replacement.filter(pl.col(date_column).cast(pl.Utf8) == processing_date),
            ],
            how="diagonal",
        )
        .select(existing.columns)
        .sort(date_column)
    )
