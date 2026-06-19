from __future__ import annotations

import polars as pl

from stock_lakehouse.gold.indicators import add_indicators
from stock_lakehouse.quality import validate_fact_daily_market
from stock_lakehouse.utils.dates import format_date, now_utc
from stock_lakehouse.utils.frames import replace_rows_for_date


FACT_DAILY_MARKET_COLUMNS = (
    "symbol_key",
    "date_key",
    "trading_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "price_change",
    "pct_change",
    "sma20",
    "ema20",
    "rsi14",
    "macd",
    "avg_volume_20d",
    "updated_at",
)


def build_fact_daily_market(
    silver_ohlcv_df: pl.DataFrame,
    dim_symbol_df: pl.DataFrame,
    dim_date_df: pl.DataFrame,
    processing_date: str | None = None,
) -> pl.DataFrame:
    """Build the daily market fact by recomputing indicators over full history.

    For this project's small symbol universe (a handful of HOSE tickers) a full
    recompute from the complete silver history is cheap and removes any risk of
    incremental-state drift. ``processing_date`` selects which trading day to emit
    after the rolling indicators are computed over the whole series.
    """
    enriched = add_indicators(_prepare_silver_ohlcv(silver_ohlcv_df), entity_col="symbol")

    if processing_date is not None:
        enriched = enriched.filter(pl.col("trading_date").cast(pl.Utf8) == format_date(processing_date))

    fact = (
        _attach_dimension_keys(enriched, dim_symbol_df, dim_date_df)
        .with_columns(pl.lit(now_utc()).alias("updated_at"))
        .select(FACT_DAILY_MARKET_COLUMNS)
        .sort("symbol_key", "trading_date")
    )
    validate_fact_daily_market(fact, dim_symbol_df, dim_date_df).raise_for_errors()
    return fact


def replace_daily_market(
    existing_fact_df: pl.DataFrame | None,
    replacement_fact_df: pl.DataFrame,
    trading_date: str,
) -> pl.DataFrame:
    """Idempotent replace of one trading day: drop date D, append the rebuilt date D."""
    return replace_rows_for_date(
        existing_fact_df,
        replacement_fact_df,
        trading_date,
        columns=FACT_DAILY_MARKET_COLUMNS,
        sort_by=["symbol_key", "trading_date"],
    )


def _prepare_silver_ohlcv(silver_ohlcv_df: pl.DataFrame) -> pl.DataFrame:
    return (
        silver_ohlcv_df.with_columns(
            pl.col("symbol").cast(pl.Utf8).str.to_uppercase(),
            pl.col("trading_date").cast(pl.Date, strict=False),
            pl.col("volume").cast(pl.Int64, strict=False),
        )
        .sort("symbol", "trading_date")
    )


def _attach_dimension_keys(
    df: pl.DataFrame,
    dim_symbol_df: pl.DataFrame,
    dim_date_df: pl.DataFrame,
) -> pl.DataFrame:
    """Left-join surrogate FKs from the dimensions and enforce referential integrity.

    A LEFT join keeps every fact row so that a ``symbol`` missing from ``dim_symbol``
    (or a ``trading_date`` missing from ``dim_date``) surfaces as a null key and fails
    loudly here, instead of an INNER join silently dropping the row.
    """
    joined = df.join(
        dim_symbol_df.select("symbol", "symbol_key"), on="symbol", how="left"
    ).join(
        dim_date_df.select(pl.col("full_date").alias("trading_date"), "date_key"),
        on="trading_date",
        how="left",
    )
    _raise_for_orphan_keys(joined)
    return joined


def _raise_for_orphan_keys(joined: pl.DataFrame) -> None:
    errors: list[str] = []
    orphan_symbols = (
        joined.filter(pl.col("symbol_key").is_null()).get_column("symbol").unique().to_list()
    )
    if orphan_symbols:
        errors.append(f"symbols missing from dim_symbol: {sorted(orphan_symbols)}")
    orphan_dates = (
        joined.filter(pl.col("date_key").is_null()).get_column("trading_date").unique().to_list()
    )
    if orphan_dates:
        errors.append(f"trading_dates missing from dim_date: {sorted(str(d) for d in orphan_dates)}")
    if errors:
        raise ValueError("fact_hose_daily_market FK violation -> " + "; ".join(errors))
