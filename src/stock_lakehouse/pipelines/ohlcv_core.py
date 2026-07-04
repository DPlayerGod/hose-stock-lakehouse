"""Phần dùng chung của pipeline OHLCV: extract → staging → bronze → silver.

Cổ phiếu và chỉ số thị trường có *cùng shape* OHLCV tới hết tầng Silver — chỉ khác
tên bảng (và rẽ nhánh ở Gold). Hàm ``run_ohlcv_to_silver`` gói toàn bộ phần chung đó,
nhận tên bảng + staging domain làm tham số. Mỗi pipeline (``daily_ohlcv`` /
``daily_index``) gọi helper này rồi tự lo tầng Gold + ClickHouse riêng — DRY mà
không trộn ``if asset_type`` vào một pipeline khổng lồ.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import polars as pl

from stock_lakehouse.bronze.ohlcv import build_bronze_ohlcv
from stock_lakehouse.config import PipelineConfig
from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
from stock_lakehouse.iceberg.reader import try_read_table, read_table
from stock_lakehouse.iceberg.tables import (
    BRONZE_OHLCV_PARTITION_SPEC,
    BRONZE_OHLCV_SCHEMA,
    SILVER_OHLCV_PARTITION_SPEC,
    SILVER_OHLCV_SCHEMA,
)
from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
from stock_lakehouse.ingestion.ohlcv import OhlcvExtractRequest, extract_ohlcv
from stock_lakehouse.quality import validate_bronze_ohlcv, validate_silver_ohlcv
from stock_lakehouse.silver.ohlcv import build_silver_ohlcv
from stock_lakehouse.staging.writer import StagingPath, StagingPathBuilder, write_staging_parquet
from stock_lakehouse.utils.dates import format_date, parse_date
from stock_lakehouse.utils.frames import replace_rows_for_date


@dataclass(frozen=True)
class OhlcvSilverResult:
    """Kết quả phần chung — đủ cho pipeline gọi tiếp tầng Gold."""

    catalog: Any
    namespace: str
    processing_date: str
    batch_id: str
    staging_uri: str
    bronze_day: pl.DataFrame
    silver_day: pl.DataFrame
    silver_all: pl.DataFrame


def run_ohlcv_to_silver(
    *,
    processing_date: str,
    symbols: Sequence[str],
    source: str,
    batch_id: str | None,
    config: PipelineConfig,
    staging_domain: str,
    bronze_table: str,
    silver_table: str,
) -> OhlcvSilverResult:
    """Extract → staging → validate → bronze → silver, idempotent theo ``processing_date``.

    ``staging_domain`` ("ohlcv" / "index") quyết định layout staging + nhãn quarantine;
    ``bronze_table`` / ``silver_table`` là tên bảng Iceberg (không kèm namespace).
    """
    day = parse_date(processing_date)
    pd_str = format_date(day)

    # 1. Extract → Staging
    request = OhlcvExtractRequest.daily(day, symbols=symbols, source=source, batch_id=batch_id)
    raw = extract_ohlcv(request)
    staging_uri = StagingPathBuilder(bucket=config.minio.bucket).build(
        StagingPath(domain=staging_domain, processing_date=pd_str, batch_id=request.batch_id)
    )
    write_staging_parquet(raw, staging_uri, config.minio)

    # 2. Validate staging
    if raw.is_empty():
        raise ValueError(f"Staging file is empty: {staging_uri}")
    validate_bronze_ohlcv(raw).quarantine_and_raise(
        raw, domain=f"staging_{staging_domain}", processing_date=pd_str, batch_id=request.batch_id, config=config.minio
    )

    catalog = load_lakehouse_catalog(config.iceberg)
    namespace = config.iceberg.namespace

    # 3. Bronze
    bronze_day = build_bronze_ohlcv(raw)
    write_dataframe(
        ensure_table(catalog, f"{namespace}.{bronze_table}", BRONZE_OHLCV_SCHEMA, BRONZE_OHLCV_PARTITION_SPEC),
        bronze_day,
        mode="overwrite",
        overwrite_filter=f"time = '{pd_str}'",
    )

    # 4. Silver
    silver_day = build_silver_ohlcv(bronze_day, processing_date=pd_str)
    write_dataframe(
        ensure_table(catalog, f"{namespace}.{silver_table}", SILVER_OHLCV_SCHEMA, SILVER_OHLCV_PARTITION_SPEC),
        silver_day,
        mode="overwrite",
        overwrite_filter=f"trading_date = '{pd_str}'",
    )

    # 5. Validate silver (day D only)
    validate_silver_ohlcv(silver_day, processing_date=pd_str).quarantine_and_raise(
        silver_day, domain=f"silver_{staging_domain}", processing_date=pd_str, batch_id=request.batch_id, config=config.minio
    )

    silver_all = read_table(catalog.load_table(f"{namespace}.{silver_table}"))

    return OhlcvSilverResult(
        catalog=catalog,
        namespace=namespace,
        processing_date=pd_str,
        batch_id=request.batch_id,
        staging_uri=staging_uri,
        bronze_day=bronze_day,
        silver_day=silver_day,
        silver_all=silver_all,
    )


def _replace_by_date(
    existing: pl.DataFrame | None,
    replacement: pl.DataFrame,
    *,
    date_column: str,
    processing_date: str,
) -> pl.DataFrame:
    """Bronze/Silver: thay rows ngày D, giữ thứ tự cột của bảng + sort theo ``date_column``."""
    return replace_rows_for_date(
        existing, replacement, processing_date, date_column=date_column
    )
