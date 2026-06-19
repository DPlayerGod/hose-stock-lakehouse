"""Silver layer for HOSE symbol metadata."""
from __future__ import annotations

import polars as pl

from stock_lakehouse.quality import validate_silver_symbols
from stock_lakehouse.utils.dates import now_utc


SILVER_SYMBOLS_COLUMNS = (
    "symbol",
    "company_name",
    "sector_name",
    "company_profile",
    "listing_date",
    "exchange_code",
    "listed_status",
    "source",
    "batch_id",
    "ingested_at",
    "updated_at",
)


def build_silver_symbols(bronze_df: pl.DataFrame) -> pl.DataFrame:
    """Deduplicate and normalise bronze symbols into silver."""
    silver = (
        bronze_df.with_columns(
            pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
            pl.col("company_name").cast(pl.Utf8, strict=False),
            pl.col("sector_name").cast(pl.Utf8, strict=False),
            pl.col("company_profile").cast(pl.Utf8, strict=False),
            pl.col("listing_date").cast(pl.Date, strict=False),
            pl.col("exchange_code").cast(pl.Utf8, strict=False).fill_null("HOSE"),
            pl.col("listed_status").cast(pl.Utf8, strict=False).fill_null("LISTED"),
            pl.lit(now_utc()).alias("updated_at"),
        )
        .sort("symbol", "ingested_at")
        .unique(subset=["symbol"], keep="last", maintain_order=True)
        .select(SILVER_SYMBOLS_COLUMNS)
    )
    validate_silver_symbols(silver).raise_for_errors()
    return silver
