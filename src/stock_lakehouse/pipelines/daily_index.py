from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from stock_lakehouse.clickhouse.loader import sync_index_fact_to_clickhouse
from stock_lakehouse.config import INDEX_SYMBOLS, PipelineConfig
from stock_lakehouse.gold.fact_index_daily import build_fact_index_daily, replace_index_daily
from stock_lakehouse.iceberg.reader import read_table, try_read_table
from stock_lakehouse.iceberg.tables import (
    FACT_HOSE_INDEX_DAILY_PARTITION_SPEC,
    FACT_HOSE_INDEX_DAILY_SCHEMA,
)
from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
from stock_lakehouse.pipelines.ohlcv_core import run_ohlcv_to_silver
from stock_lakehouse.quality import validate_fact_index_daily


DEFAULT_INDICES = INDEX_SYMBOLS


@dataclass(frozen=True)
class DailyIndexResult:
    processing_date: str
    batch_id: str
    staging_uri: str
    bronze_rows: int
    silver_rows: int
    fact_rows: int


def run_daily_index_pipeline(
    processing_date: str,
    indices: Sequence[str] = DEFAULT_INDICES,
    source: str = "VCI",
    batch_id: str | None = None,
    sync_clickhouse: bool = True,
    config: PipelineConfig = PipelineConfig(),
) -> DailyIndexResult:
    """Run the daily market-index pipeline end-to-end.

    Reuses the entire extract→silver leg from ``run_ohlcv_to_silver`` (indices share the
    OHLCV shape), then builds the Gold index fact: indicators + ``dim_date`` join only —
    **no ``dim_symbol``** (an index is not a listed company). Reads ``dim_date`` from the
    existing Iceberg table (owned by ``dag_dim_date``); never writes it.
    """
    core = run_ohlcv_to_silver(
        processing_date=processing_date,
        symbols=indices,
        source=source,
        batch_id=batch_id,
        config=config,
        staging_domain="index",
        bronze_table="bronze_hose_index_daily",
        silver_table="silver_hose_index_daily",
    )
    catalog, namespace, pd_str = core.catalog, core.namespace, core.processing_date

    # Build Gold index fact — dim_date read from existing table (no dim_symbol)
    dim_date = read_table(catalog.load_table(f"{namespace}.dim_date"))
    fact_day = build_fact_index_daily(core.silver_all, dim_date, processing_date=pd_str)
    write_dataframe(
        ensure_table(
            catalog,
            f"{namespace}.fact_hose_index_daily",
            FACT_HOSE_INDEX_DAILY_SCHEMA,
            FACT_HOSE_INDEX_DAILY_PARTITION_SPEC,
        ),
        fact_day,
        mode="overwrite",
        overwrite_filter=f"trading_date = '{pd_str}'",
    )

    # Validate gold (day D only)
    validate_fact_index_daily(fact_day, dim_date).quarantine_and_raise(
        fact_day, domain="gold_index_fact", processing_date=pd_str, batch_id=core.batch_id, config=config.minio
    )

    # Sync fact to ClickHouse
    if sync_clickhouse:
        sync_index_fact_to_clickhouse(fact_day, processing_date=pd_str, config=config.clickhouse)

    return DailyIndexResult(
        processing_date=pd_str,
        batch_id=core.batch_id,
        staging_uri=core.staging_uri,
        bronze_rows=core.bronze_day.height,
        silver_rows=core.silver_day.height,
        fact_rows=fact_day.height,
    )
