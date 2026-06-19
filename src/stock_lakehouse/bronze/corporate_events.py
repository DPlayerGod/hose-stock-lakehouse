from __future__ import annotations

import polars as pl

from stock_lakehouse.quality import validate_bronze_corporate_events
from stock_lakehouse.staging.writer import read_staging_parquet
from stock_lakehouse.utils.dates import now_utc


BRONZE_CORPORATE_EVENTS_COLUMNS = (
    "event_id",
    "symbol",
    "event_code",
    "event_title_vi",
    "value_per_share",
    "event_date",
    "source",
    "batch_id",
    "ingested_at",
    "processing_date",
)


def build_bronze_corporate_events(staging_df: pl.DataFrame) -> pl.DataFrame:
    """Bronze = raw đã ép kiểu + lineage (chưa dedup, chưa suy label)."""
    bronze = staging_df.select(BRONZE_CORPORATE_EVENTS_COLUMNS).with_columns(
        pl.col("event_id").cast(pl.Utf8),
        pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
        pl.col("event_code").cast(pl.Utf8).str.to_uppercase(),
        pl.col("event_title_vi").cast(pl.Utf8, strict=False),
        pl.col("value_per_share").cast(pl.Float64, strict=False),
        pl.col("event_date").cast(pl.Date, strict=False),
        pl.col("source").cast(pl.Utf8),
        pl.col("batch_id").cast(pl.Utf8),
        _parse_timestamp("ingested_at"),
        pl.col("processing_date").cast(pl.Date, strict=False),
    )
    validate_bronze_corporate_events(bronze).raise_for_errors()
    return bronze


def build_bronze_corporate_events_from_staging(uri: str) -> pl.DataFrame:
    return build_bronze_corporate_events(read_staging_parquet(uri))


def _parse_timestamp(column: str) -> pl.Expr:
    return (
        pl.when(pl.col(column).is_null())
        .then(pl.lit(None, dtype=pl.Datetime(time_zone="UTC")))
        .otherwise(pl.col(column).cast(pl.Utf8).str.to_datetime(strict=False, time_zone="UTC"))
        .fill_null(pl.lit(now_utc()))
        .alias(column)
    )
