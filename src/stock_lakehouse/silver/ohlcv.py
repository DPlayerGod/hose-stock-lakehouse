from __future__ import annotations

import polars as pl

from stock_lakehouse.quality.ohlcv import validate_silver_ohlcv
from stock_lakehouse.utils.dates import format_date, now_utc


SILVER_OHLCV_COLUMNS = (
    "symbol",
    "trading_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "source",
    "batch_id",
    "ingested_at",
    "updated_at",
)


def build_silver_ohlcv(bronze_df: pl.DataFrame, processing_date: str | None = None) -> pl.DataFrame:
    silver = (
        bronze_df.rename(
            {
                "time": "trading_date",
                "open": "open_price",
                "high": "high_price",
                "low": "low_price",
                "close": "close_price",
            }
        )
        .with_columns(
            pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
            pl.col("trading_date").cast(pl.Date, strict=False),
            pl.col("open_price").cast(pl.Float64, strict=False),
            pl.col("high_price").cast(pl.Float64, strict=False),
            pl.col("low_price").cast(pl.Float64, strict=False),
            pl.col("close_price").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Int64, strict=False),
            pl.lit(now_utc()).alias("updated_at"),
        )
    )

    if processing_date is not None:
        silver = silver.filter(pl.col("trading_date").cast(pl.Utf8) == format_date(processing_date))

    silver = (
        silver.sort("symbol", "trading_date", "ingested_at")
        .unique(subset=["symbol", "trading_date"], keep="last", maintain_order=True)
        .select(SILVER_OHLCV_COLUMNS)
    )
    validate_silver_ohlcv(silver, processing_date=format_date(processing_date) if processing_date else None).raise_for_errors()
    return silver
