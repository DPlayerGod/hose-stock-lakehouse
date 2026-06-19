from __future__ import annotations

import polars as pl

from stock_lakehouse.gold.indicators import add_indicators
from stock_lakehouse.quality import validate_fact_index_daily
from stock_lakehouse.utils.dates import format_date, now_utc
from stock_lakehouse.utils.frames import replace_rows_for_date


FACT_INDEX_DAILY_COLUMNS = (
    "index_code",
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


def build_fact_index_daily(
    silver_index_df: pl.DataFrame,
    dim_date_df: pl.DataFrame,
    processing_date: str | None = None,
) -> pl.DataFrame:
    """Build the daily index fact by recomputing indicators over full history.

    Mirrors ``build_fact_daily_market`` but for market indices (VN-Index, VN30…):
    same OHLCV shape and indicator engine, **but no ``dim_symbol``**. An index is
    identified by its natural key ``index_code`` (no surrogate), and it is not a
    listed company, so there is no symbol dimension to join. ``dim_date`` is still
    joined with fail-loud referential integrity, exactly like the price fact.

    Silver carries the entity under the generic ``symbol`` column (reusing the OHLCV
    transforms); it is renamed to ``index_code`` here, at the Gold boundary.
    """
    enriched = add_indicators(_prepare_silver_index(silver_index_df), entity_col="index_code")

    if processing_date is not None:
        enriched = enriched.filter(pl.col("trading_date").cast(pl.Utf8) == format_date(processing_date))

    fact = (
        _attach_date_key(enriched, dim_date_df)
        .with_columns(pl.lit(now_utc()).alias("updated_at"))
        .select(FACT_INDEX_DAILY_COLUMNS)
        .sort("index_code", "trading_date")
    )
    validate_fact_index_daily(fact, dim_date_df).raise_for_errors()
    return fact


def replace_index_daily(
    existing_fact_df: pl.DataFrame | None,
    replacement_fact_df: pl.DataFrame,
    trading_date: str,
) -> pl.DataFrame:
    """Idempotent replace of one trading day: drop date D, append the rebuilt date D."""
    return replace_rows_for_date(
        existing_fact_df,
        replacement_fact_df,
        trading_date,
        columns=FACT_INDEX_DAILY_COLUMNS,
        sort_by=["index_code", "trading_date"],
    )


def _prepare_silver_index(silver_index_df: pl.DataFrame) -> pl.DataFrame:
    return (
        silver_index_df.rename({"symbol": "index_code"})
        .with_columns(
            pl.col("index_code").cast(pl.Utf8).str.to_uppercase(),
            pl.col("trading_date").cast(pl.Date, strict=False),
            pl.col("volume").cast(pl.Int64, strict=False),
        )
        .sort("index_code", "trading_date")
    )


def _attach_date_key(df: pl.DataFrame, dim_date_df: pl.DataFrame) -> pl.DataFrame:
    """Left-join ``date_key`` from ``dim_date`` and enforce referential integrity.

    LEFT join keeps every fact row so a ``trading_date`` missing from ``dim_date``
    surfaces as a null key and fails loudly, instead of being silently dropped.
    """
    joined = df.join(
        dim_date_df.select(pl.col("full_date").alias("trading_date"), "date_key"),
        on="trading_date",
        how="left",
    )
    orphan_dates = (
        joined.filter(pl.col("date_key").is_null()).get_column("trading_date").unique().to_list()
    )
    if orphan_dates:
        missing = sorted(str(d) for d in orphan_dates)
        raise ValueError(
            f"fact_hose_index_daily FK violation -> trading_dates missing from dim_date: {missing}"
        )
    return joined
