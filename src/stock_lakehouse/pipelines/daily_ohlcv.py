from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import polars as pl

from stock_lakehouse.bronze.ohlcv import build_bronze_ohlcv
from stock_lakehouse.clickhouse.loader import GoldFrames, sync_gold_to_clickhouse
from stock_lakehouse.config import PipelineConfig, SYMBOLS
from stock_lakehouse.gold.dim_date import build_dim_date
from stock_lakehouse.gold.dim_symbol import build_dim_symbol
from stock_lakehouse.gold.fact_daily_market import build_fact_daily_market, replace_daily_market
from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
from stock_lakehouse.iceberg.reader import try_read_table
from stock_lakehouse.iceberg.tables import (
    BRONZE_OHLCV_PARTITION_SPEC,
    BRONZE_OHLCV_SCHEMA,
    DIM_DATE_SCHEMA,
    DIM_SYMBOL_SCHEMA,
    FACT_HOSE_DAILY_MARKET_PARTITION_SPEC,
    FACT_HOSE_DAILY_MARKET_SCHEMA,
    SILVER_OHLCV_PARTITION_SPEC,
    SILVER_OHLCV_SCHEMA,
)
from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
from stock_lakehouse.ingestion.ohlcv import OhlcvExtractRequest, extract_ohlcv
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
    day = parse_date(processing_date)
    request = OhlcvExtractRequest.daily(day, symbols=symbols, source=source, batch_id=batch_id)
    raw = extract_ohlcv(request)
    staging_uri = StagingPathBuilder(bucket=config.minio.bucket).ohlcv(format_date(day), request.batch_id)
    write_staging_parquet(raw, staging_uri, config.minio)

    catalog = load_lakehouse_catalog(config.iceberg)
    namespace = config.iceberg.namespace

    bronze_day = build_bronze_ohlcv(raw)
    bronze_all = _replace_by_date(
        try_read_table(catalog, f"{namespace}.bronze_hose_ohlcv_daily"),
        bronze_day,
        date_column="time",
        processing_date=format_date(day),
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

    silver_day = build_silver_ohlcv(bronze_day, processing_date=format_date(day))
    silver_all = _replace_by_date(
        try_read_table(catalog, f"{namespace}.silver_hose_ohlcv_daily"),
        silver_day,
        date_column="trading_date",
        processing_date=format_date(day),
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

    dim_symbol_existing = try_read_table(catalog, f"{namespace}.dim_symbol")
    dim_symbol = build_dim_symbol(pl.DataFrame({"symbol": [symbol.upper() for symbol in symbols]}), dim_symbol_existing)
    dim_date = _load_or_build_dim_date(config, format_date(day))
    fact_day = build_fact_daily_market(silver_all, dim_symbol, dim_date, processing_date=format_date(day))
    existing_fact = try_read_table(catalog, f"{namespace}.fact_hose_daily_market")
    fact_all = fact_day if existing_fact is None else replace_daily_market(existing_fact, fact_day, format_date(day))

    write_dataframe(ensure_table(catalog, f"{namespace}.dim_symbol", DIM_SYMBOL_SCHEMA), dim_symbol, mode="overwrite")
    write_dataframe(ensure_table(catalog, f"{namespace}.dim_date", DIM_DATE_SCHEMA), dim_date, mode="overwrite")
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

    if sync_clickhouse:
        sync_gold_to_clickhouse(
            GoldFrames(dim_date=dim_date, dim_symbol=dim_symbol, fact_daily_market=fact_all),
            processing_date=format_date(day),
            config=config.clickhouse,
        )

    return DailyPipelineResult(
        processing_date=format_date(day),
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


def _load_or_build_dim_date(config: PipelineConfig, processing_date: str) -> pl.DataFrame:
    catalog = load_lakehouse_catalog(config.iceberg)
    dim_date = try_read_table(catalog, f"{config.iceberg.namespace}.dim_date")
    if dim_date is not None and not dim_date.is_empty():
        return dim_date
    _ = processing_date
    return build_dim_date("2020-01-01", "2030-12-31")
