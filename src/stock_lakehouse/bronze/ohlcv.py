from __future__ import annotations

import polars as pl

from stock_lakehouse.quality import validate_bronze_ohlcv
from stock_lakehouse.staging.writer import read_staging_parquet
from stock_lakehouse.utils.dates import now_utc


BRONZE_OHLCV_COLUMNS = (
    "symbol",
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "batch_id",
    "ingested_at",
    "processing_date",
)


def build_bronze_ohlcv(staging_df: pl.DataFrame) -> pl.DataFrame:
    bronze = staging_df.select(BRONZE_OHLCV_COLUMNS).with_columns(
        pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
        pl.col("time").cast(pl.Date, strict=False),
        pl.col("open").cast(pl.Float64, strict=False),
        pl.col("high").cast(pl.Float64, strict=False),
        pl.col("low").cast(pl.Float64, strict=False),
        pl.col("close").cast(pl.Float64, strict=False),
        pl.col("volume").cast(pl.Int64, strict=False),
        pl.col("source").cast(pl.Utf8),
        pl.col("batch_id").cast(pl.Utf8),
        _parse_timestamp("ingested_at"),
        pl.col("processing_date").cast(pl.Date, strict=False),
    )
    validate_bronze_ohlcv(bronze).raise_for_errors()
    return bronze


def build_bronze_ohlcv_from_staging(uri: str) -> pl.DataFrame:
    return build_bronze_ohlcv(read_staging_parquet(uri))


def _parse_timestamp(column: str) -> pl.Expr:
    return (
        pl.when(pl.col(column).is_null())
        .then(pl.lit(None, dtype=pl.Datetime(time_zone="UTC")))
        .otherwise(pl.col(column).cast(pl.Utf8).str.to_datetime(strict=False, time_zone="UTC"))
        .fill_null(pl.lit(now_utc()))
        .alias(column)
    )
