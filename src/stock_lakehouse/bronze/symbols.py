"""Bronze layer for HOSE symbol metadata."""
from __future__ import annotations

import polars as pl

from stock_lakehouse.quality.ohlcv import ValidationResult


BRONZE_SYMBOLS_COLUMNS = (
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
)


def validate_bronze_symbols(df: pl.DataFrame) -> ValidationResult:
    """Basic quality checks for bronze symbol data."""
    errors: list[str] = []
    required = {"symbol", "source", "batch_id", "ingested_at"}
    missing = sorted(required.difference(df.columns))
    if missing:
        errors.append(f"missing required columns: {missing}")
    else:
        for column in ("symbol", "source", "batch_id", "ingested_at"):
            if df.filter(pl.col(column).is_null()).height:
                errors.append(f"{column} contains null values")
    return ValidationResult(not errors, tuple(errors))


def build_bronze_symbols(staging_df: pl.DataFrame) -> pl.DataFrame:
    """Transform staging symbols into bronze, applying type casts and validation."""
    # Add missing columns with defaults
    defaults = {
        "company_name": None,
        "sector_name": None,
        "company_profile": None,
        "listing_date": None,
        "exchange_code": "HOSE",
        "listed_status": "LISTED",
    }
    df = staging_df
    for column, default in defaults.items():
        if column not in df.columns:
            df = df.with_columns(pl.lit(default).alias(column))

    bronze = df.select(list(BRONZE_SYMBOLS_COLUMNS)).with_columns(
        pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
        pl.col("company_name").cast(pl.Utf8, strict=False),
        pl.col("sector_name").cast(pl.Utf8, strict=False),
        pl.col("company_profile").cast(pl.Utf8, strict=False),
        pl.col("listing_date").cast(pl.Date, strict=False),
        pl.col("exchange_code").cast(pl.Utf8, strict=False).fill_null("HOSE"),
        pl.col("listed_status").cast(pl.Utf8, strict=False).fill_null("LISTED"),
        pl.col("source").cast(pl.Utf8),
        pl.col("batch_id").cast(pl.Utf8),
    )
    validate_bronze_symbols(bronze).raise_for_errors()
    return bronze
