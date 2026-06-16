"""Pipeline orchestrator for HOSE symbol metadata.

Since the symbols table is small, this pipeline processes the full dataset
in one pass (overview approach) — no partitioning needed.

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

from dataclasses import dataclass
from uuid import uuid4

import polars as pl

from stock_lakehouse.bronze.symbols import build_bronze_symbols
from stock_lakehouse.clickhouse.loader import sync_dim_symbol_to_clickhouse
from stock_lakehouse.config import PipelineConfig
from stock_lakehouse.gold.dim_symbol import build_dim_symbol
from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
from stock_lakehouse.iceberg.reader import try_read_table
from stock_lakehouse.iceberg.tables import (
    BRONZE_SYMBOLS_SCHEMA,
    DIM_SYMBOL_SCHEMA,
    SILVER_SYMBOLS_SCHEMA,
)
from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
from stock_lakehouse.ingestion.symbols import extract_hose_symbols
from stock_lakehouse.silver.symbols import build_silver_symbols, validate_silver_symbols
from stock_lakehouse.staging.writer import StagingPathBuilder, write_staging_parquet, read_staging_parquet


@dataclass(frozen=True)
class SymbolPipelineResult:
    batch_id: str
    staging_uri: str
    bronze_rows: int
    silver_rows: int
    dim_symbol_rows: int
    synced_clickhouse: bool


def run_symbol_metadata_pipeline(
    sync_clickhouse: bool = True,
    batch_id: str | None = None,
    config: PipelineConfig = PipelineConfig(),
) -> SymbolPipelineResult:
    """Run the full symbol metadata pipeline end-to-end."""
    batch_id = batch_id or uuid4().hex
    catalog = load_lakehouse_catalog(config.iceberg)
    namespace = config.iceberg.namespace

    # 1. Extract
    raw = extract_hose_symbols(batch_id=batch_id)

    # 2. Write Staging
    staging_uri = _staging_symbols_uri(config, batch_id)
    write_staging_parquet(raw, staging_uri, config.minio)

    # 3. Bronze
    bronze = build_bronze_symbols(raw)
    write_dataframe(
        ensure_table(catalog, f"{namespace}.bronze_hose_symbols", BRONZE_SYMBOLS_SCHEMA),
        bronze,
        mode="overwrite",
    )

    # 4. Silver
    silver = build_silver_symbols(bronze)
    write_dataframe(
        ensure_table(catalog, f"{namespace}.silver_hose_symbols", SILVER_SYMBOLS_SCHEMA),
        silver,
        mode="overwrite",
    )

    # 5. Validate silver
    validate_silver_symbols(silver).quarantine_and_raise(
        silver, domain="silver_symbols", processing_date="latest", batch_id=batch_id, config=config.minio
    )

    # 6. Upsert dim_symbol
    existing_dim = try_read_table(catalog, f"{namespace}.dim_symbol")
    dim_symbol = build_dim_symbol(silver, existing_dim)
    write_dataframe(
        ensure_table(catalog, f"{namespace}.dim_symbol", DIM_SYMBOL_SCHEMA),
        dim_symbol,
        mode="overwrite",
    )

    # 7. Sync to ClickHouse
    if sync_clickhouse:
        sync_dim_symbol_to_clickhouse(dim_symbol, config.clickhouse)

    return SymbolPipelineResult(
        batch_id=batch_id,
        staging_uri=staging_uri,
        bronze_rows=bronze.height,
        silver_rows=silver.height,
        dim_symbol_rows=dim_symbol.height,
        synced_clickhouse=sync_clickhouse,
    )


def _staging_symbols_uri(config: PipelineConfig, batch_id: str) -> str:
    from stock_lakehouse.staging.writer import StagingPath

    builder = StagingPathBuilder(bucket=config.minio.bucket)
    return builder.build(
        StagingPath(
            domain="symbols",
            processing_date="latest",
            batch_id=batch_id,
        )
    )
