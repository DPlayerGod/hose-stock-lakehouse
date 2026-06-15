"""Silver layer for HOSE symbol metadata."""
from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from stock_lakehouse.quality.gold import validate_dim_symbol
from stock_lakehouse.quality.ohlcv import ValidationResult


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


def validate_silver_symbols(df: pl.DataFrame) -> ValidationResult:
    """Validate silver symbol data — no duplicate symbols, required fields present."""
    errors: list[str] = []
    required = {"symbol", "exchange_code", "listed_status"}
    missing = sorted(required.difference(df.columns))
    if missing:
        errors.append(f"missing required columns: {missing}")
        return ValidationResult(False, tuple(errors))

    for column in ("symbol", "exchange_code", "listed_status"):
        if df.filter(pl.col(column).is_null()).height:
            errors.append(f"{column} contains null values")

    duplicate_count = df.group_by("symbol").len().filter(pl.col("len") > 1).height
    if duplicate_count:
        errors.append("silver symbols contain duplicate symbol rows")

    return ValidationResult(not errors, tuple(errors))


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
            pl.lit(datetime.now(timezone.utc)).alias("updated_at"),
        )
        .sort("symbol", "ingested_at")
        .unique(subset=["symbol"], keep="last", maintain_order=True)
        .select(SILVER_SYMBOLS_COLUMNS)
    )
    validate_silver_symbols(silver).raise_for_errors()
    return silver
