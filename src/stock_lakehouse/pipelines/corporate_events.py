"""Pipeline điều phối sự kiện doanh nghiệp (corporate events).

Flow:
    extract_corporate_events
    → write_staging
    → validate_staging
    → write_bronze   (overwrite full — feed trả toàn bộ lịch sử)
    → transform_silver (dedup event_id + suy event_label)
    → validate_silver
    → build_gold_fact (factless fact: FK fail-loud dim_symbol + dim_date)
    → validate_gold
    → sync_clickhouse

Idempotency: không slice theo ngày D (feed là full snapshot). Mỗi tầng dựng lại từ
toàn bộ dữ liệu rồi **overwrite**; dedup theo natural key ``event_id``. Chạy lại cho
ra cùng tập rows. dim_symbol/dim_date đọc từ bảng có sẵn (không tạo ở đây).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Sequence
from uuid import uuid4

from stock_lakehouse.bronze.corporate_events import build_bronze_corporate_events
from stock_lakehouse.clickhouse.loader import sync_corporate_events_to_clickhouse
from stock_lakehouse.config import SYMBOLS, PipelineConfig
from stock_lakehouse.gold.fact_corporate_events import build_fact_corporate_events
from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog
from stock_lakehouse.iceberg.reader import read_table
from stock_lakehouse.iceberg.tables import (
    BRONZE_CORPORATE_EVENTS_SCHEMA,
    FACT_CORPORATE_EVENTS_SCHEMA,
    SILVER_CORPORATE_EVENTS_SCHEMA,
)
from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe
from stock_lakehouse.ingestion.corporate_events import extract_corporate_events
from stock_lakehouse.quality import (
    validate_bronze_corporate_events,
    validate_silver_corporate_events,
)
from stock_lakehouse.silver.corporate_events import build_silver_corporate_events
from stock_lakehouse.staging.writer import StagingPathBuilder, write_staging_parquet
from stock_lakehouse.utils.dates import format_date, parse_date


BRONZE_TABLE = "bronze_hose_corporate_events"
SILVER_TABLE = "silver_hose_corporate_events"
FACT_TABLE = "fact_corporate_events"


@dataclass(frozen=True)
class CorporateEventsResult:
    processing_date: str
    batch_id: str
    staging_uri: str
    bronze_rows: int
    silver_rows: int
    fact_rows: int
    dropped_out_of_range: int


def run_corporate_events_pipeline(
    processing_date: str | date,
    symbols: Sequence[str] = SYMBOLS,
    source: str = "VCI",
    batch_id: str | None = None,
    sync_clickhouse: bool = True,
    config: PipelineConfig = PipelineConfig(),
) -> CorporateEventsResult:
    """Chạy pipeline sự kiện doanh nghiệp end-to-end."""
    day = parse_date(processing_date)
    pd_str = format_date(day)
    batch_id = batch_id or uuid4().hex

    # 1. Extract (full lịch sử cho mọi mã)
    raw = extract_corporate_events(symbols, source=source, batch_id=batch_id, processing_date=day)

    # 2. Staging
    staging_uri = StagingPathBuilder(bucket=config.minio.bucket).events(pd_str, batch_id)
    write_staging_parquet(raw, staging_uri, config.minio)
    if raw.is_empty():
        raise ValueError(f"Staging file is empty: {staging_uri}")

    # 3. Validate staging
    validate_bronze_corporate_events(raw).quarantine_and_raise(
        raw, domain="staging_events", processing_date=pd_str, batch_id=batch_id, config=config.minio
    )

    catalog = load_lakehouse_catalog(config.iceberg)
    namespace = config.iceberg.namespace

    # 4. Bronze (overwrite full)
    bronze = build_bronze_corporate_events(raw)
    write_dataframe(
        ensure_table(catalog, f"{namespace}.{BRONZE_TABLE}", BRONZE_CORPORATE_EVENTS_SCHEMA),
        bronze,
        mode="overwrite",
    )

    # 5. Silver (overwrite full)
    silver = build_silver_corporate_events(bronze)
    write_dataframe(
        ensure_table(catalog, f"{namespace}.{SILVER_TABLE}", SILVER_CORPORATE_EVENTS_SCHEMA),
        silver,
        mode="overwrite",
    )

    # 6. Validate silver
    validate_silver_corporate_events(silver).quarantine_and_raise(
        silver, domain="silver_events", processing_date=pd_str, batch_id=batch_id, config=config.minio
    )

    # 7. Gold factless fact (FK fail-loud dim_symbol + dim_date)
    dim_symbol = read_table(catalog.load_table(f"{namespace}.dim_symbol"))
    dim_date = read_table(catalog.load_table(f"{namespace}.dim_date"))
    fact = build_fact_corporate_events(silver, dim_symbol, dim_date)
    write_dataframe(
        ensure_table(catalog, f"{namespace}.{FACT_TABLE}", FACT_CORPORATE_EVENTS_SCHEMA),
        fact,
        mode="overwrite",
    )

    # 8. Validate gold
    from stock_lakehouse.quality import validate_fact_corporate_events

    validate_fact_corporate_events(fact, dim_symbol, dim_date).quarantine_and_raise(
        fact, domain="gold_events_fact", processing_date=pd_str, batch_id=batch_id, config=config.minio
    )

    # 9. Sync to ClickHouse
    if sync_clickhouse:
        sync_corporate_events_to_clickhouse(fact, config.clickhouse)

    return CorporateEventsResult(
        processing_date=pd_str,
        batch_id=batch_id,
        staging_uri=staging_uri,
        bronze_rows=bronze.height,
        silver_rows=silver.height,
        fact_rows=fact.height,
        dropped_out_of_range=silver.height - fact.height,
    )
