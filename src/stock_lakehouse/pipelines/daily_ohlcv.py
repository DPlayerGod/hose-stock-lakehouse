from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from stock_lakehouse.clickhouse.loader import sync_fact_to_clickhouse
from stock_lakehouse.config import PipelineConfig, SYMBOLS
from stock_lakehouse.gold.fact_daily_market import build_fact_daily_market, replace_daily_market
from stock_lakehouse.iceberg.reader import read_table, try_read_table
from stock_lakehouse.iceberg.tables import (
    FACT_HOSE_DAILY_MARKET_PARTITION_SPEC,
    FACT_HOSE_DAILY_MARKET_SCHEMA,
)
from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
from stock_lakehouse.pipelines.ohlcv_core import run_ohlcv_to_silver
from stock_lakehouse.quality import validate_fact_daily_market


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

    The extract→silver leg is shared with ``daily_index`` via ``run_ohlcv_to_silver``.
    ``dim_symbol`` and ``dim_date`` are read from existing Iceberg tables (owned by
    ``dag_symbol_metadata`` / ``dag_dim_date``); this pipeline never writes them.
    """
    core = run_ohlcv_to_silver(
        processing_date=processing_date,
        symbols=symbols,
        source=source,
        batch_id=batch_id,
        config=config,
        staging_domain="ohlcv",
        bronze_table="bronze_hose_ohlcv_daily",
        silver_table="silver_hose_ohlcv_daily",
    )
    catalog, namespace, pd_str = core.catalog, core.namespace, core.processing_date

    # Build Gold fact — dim_symbol / dim_date read from existing tables
    dim_symbol = read_table(catalog.load_table(f"{namespace}.dim_symbol"))
    dim_date = read_table(catalog.load_table(f"{namespace}.dim_date"))
    fact_day = build_fact_daily_market(core.silver_all, dim_symbol, dim_date, processing_date=pd_str)
    existing_fact = try_read_table(catalog, f"{namespace}.fact_hose_daily_market")
    fact_all = replace_daily_market(existing_fact, fact_day, pd_str)
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

    # Validate gold (day D only)
    validate_fact_daily_market(fact_day, dim_symbol, dim_date).quarantine_and_raise(
        fact_day, domain="gold_fact", processing_date=pd_str, batch_id=core.batch_id, config=config.minio
    )

    # Sync fact to ClickHouse
    if sync_clickhouse:
        sync_fact_to_clickhouse(fact_all, processing_date=pd_str, config=config.clickhouse)

    return DailyPipelineResult(
        processing_date=pd_str,
        batch_id=core.batch_id,
        staging_uri=core.staging_uri,
        bronze_rows=core.bronze_day.height,
        silver_rows=core.silver_day.height,
        fact_rows=fact_day.height,
    )
