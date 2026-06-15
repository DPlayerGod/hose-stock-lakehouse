from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from stock_lakehouse.quality.gold import validate_dim_symbol


DIM_SYMBOL_COLUMNS = (
    "symbol_key",
    "symbol",
    "company_name",
    "sector_name",
    "company_profile",
    "listing_date",
    "exchange_code",
    "listed_status",
    "updated_at",
)


def build_dim_symbol(symbols_df: pl.DataFrame, existing_dim: pl.DataFrame | None = None) -> pl.DataFrame:
    latest = _normalize_latest_symbols(symbols_df)
    now = datetime.now(timezone.utc)

    if existing_dim is None or existing_dim.is_empty():
        result = (
            latest.sort("symbol")
            .with_row_index("symbol_key", offset=1)
            .with_columns(
                pl.col("symbol_key").cast(pl.Int64),
                pl.lit(now).alias("updated_at"),
            )
            .select(DIM_SYMBOL_COLUMNS)
        )
        validate_dim_symbol(result).raise_for_errors()
        return result

    existing = existing_dim.select(DIM_SYMBOL_COLUMNS).with_columns(
        pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
        pl.col("symbol_key").cast(pl.Int64),
    )
    max_key = existing.get_column("symbol_key").max() or 0
    current = latest.join(existing.select("symbol", "symbol_key"), on="symbol", how="left")
    new_symbols = (
        current.filter(pl.col("symbol_key").is_null())
        .drop("symbol_key")
        .sort("symbol")
        .with_row_index("new_index", offset=1)
        .with_columns((pl.lit(max_key) + pl.col("new_index")).cast(pl.Int64).alias("symbol_key"))
        .drop("new_index")
    )
    old_symbols = current.filter(pl.col("symbol_key").is_not_null()).with_columns(
        pl.col("symbol_key").cast(pl.Int64)
    )
    delisted = (
        existing.join(latest.select("symbol"), on="symbol", how="anti")
        .with_columns(
            pl.lit("DELISTED").alias("listed_status"),
            pl.lit(now).alias("updated_at"),
        )
        .select(DIM_SYMBOL_COLUMNS)
    )
    active = pl.concat([old_symbols, new_symbols], how="diagonal").with_columns(
        pl.lit(now).alias("updated_at")
    )
    result = pl.concat([active.select(DIM_SYMBOL_COLUMNS), delisted], how="diagonal").sort("symbol_key")
    validate_dim_symbol(result).raise_for_errors()
    return result


def _normalize_latest_symbols(symbols_df: pl.DataFrame) -> pl.DataFrame:
    defaults = {
        "company_name": None,
        "sector_name": None,
        "company_profile": None,
        "listing_date": None,
        "exchange_code": "HOSE",
        "listed_status": "LISTED",
    }
    df = symbols_df
    for column, default in defaults.items():
        if column not in df.columns:
            df = df.with_columns(pl.lit(default).alias(column))

    return (
        df.with_columns(
            pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
            pl.col("company_name").cast(pl.Utf8, strict=False),
            pl.col("sector_name").cast(pl.Utf8, strict=False),
            pl.col("company_profile").cast(pl.Utf8, strict=False),
            pl.col("listing_date").cast(pl.Date, strict=False),
            pl.col("exchange_code").cast(pl.Utf8, strict=False).fill_null("HOSE"),
            pl.col("listed_status").cast(pl.Utf8, strict=False).fill_null("LISTED"),
        )
        .sort("symbol")
        .unique(subset=["symbol"], keep="last", maintain_order=True)
        .select([column for column in DIM_SYMBOL_COLUMNS if column not in {"symbol_key", "updated_at"}])
    )
