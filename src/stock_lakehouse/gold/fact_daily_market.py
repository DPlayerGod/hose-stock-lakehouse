from __future__ import annotations

import polars as pl

from stock_lakehouse.quality.gold import validate_fact_daily_market
from stock_lakehouse.utils.dates import format_date, now_utc


RSI_PERIOD = 14

# Warmup cho chỉ báo EMA: seed phiên đầu thiên lệch (ema20 phiên 1 = close, macd phiên 1 = 0),
# chưa phản ánh đủ lịch sử. Để đồng bộ quy ước với sma20/avg_volume_20d/rsi14 ("tránh giá trị
# gây hiểu nhầm"), trả null tới khi đủ N phiên thay vì xuất giá trị seed.
EMA20_WARMUP = 20   # ema20 -> đủ 20 phiên (như sma20)
MACD_WARMUP = 26    # macd = ema12 - ema26 -> leg chậm ema26 quyết định warmup

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
    enriched = (
        _prepare_silver_ohlcv(silver_ohlcv_df)
        .with_columns(
            pl.col("close_price").diff().over("symbol").alias("price_change"),
            pl.col("close_price").pct_change().over("symbol").alias("pct_change"),
            pl.col("close_price").rolling_mean(window_size=20, min_samples=20).over("symbol").alias("sma20"),
            pl.col("close_price").ewm_mean(span=20, adjust=False).over("symbol").alias("ema20"),
            pl.col("volume").rolling_mean(window_size=20, min_samples=20).over("symbol").alias("avg_volume_20d"),
            pl.col("close_price").ewm_mean(span=12, adjust=False).over("symbol").alias("_ema12"),
            pl.col("close_price").ewm_mean(span=26, adjust=False).over("symbol").alias("_ema26"),
            pl.int_range(0, pl.len()).over("symbol").alias("_session_idx"),
        )
        .with_columns((pl.col("_ema12") - pl.col("_ema26")).alias("macd"))
        .with_columns(
            _warmup_null("ema20", EMA20_WARMUP),
            _warmup_null("macd", MACD_WARMUP),
        )
        .drop("_ema12", "_ema26", "_session_idx")
    )
    enriched = _with_rsi(enriched)

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
    existing_fact_df: pl.DataFrame,
    replacement_fact_df: pl.DataFrame,
    trading_date: str,
) -> pl.DataFrame:
    """Idempotent replace of one trading day: drop date D, append the rebuilt date D."""
    target_date = format_date(trading_date)
    return (
        pl.concat(
            [
                existing_fact_df.filter(pl.col("trading_date").cast(pl.Utf8) != target_date),
                replacement_fact_df.filter(pl.col("trading_date").cast(pl.Utf8) == target_date),
            ],
            how="diagonal",
        )
        .select(FACT_DAILY_MARKET_COLUMNS)
        .sort("symbol_key", "trading_date")
    )


def _warmup_null(column: str, min_sessions: int) -> pl.Expr:
    """Trả null cho ``column`` tới khi đủ ``min_sessions`` phiên (``_session_idx`` 0-based),
    đồng bộ quy ước warmup của các chỉ báo cửa sổ cố định (sma20/avg_volume_20d/rsi14)."""
    return (
        pl.when(pl.col("_session_idx") >= min_sessions - 1)
        .then(pl.col(column))
        .otherwise(None)
        .alias(column)
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


def _with_rsi(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by("symbol", maintain_order=True).map_groups(_with_wilder_rsi_for_symbol)


def _with_wilder_rsi_for_symbol(symbol_df: pl.DataFrame) -> pl.DataFrame:
    """Compute Wilder's RSI(14) per symbol; cheap for this project's small universe."""
    rows = symbol_df.sort("trading_date")
    closes = rows.get_column("close_price").to_list()
    rsi_values: list[float | None] = []
    gains: list[float] = []
    losses: list[float] = []
    previous_avg_gain: float | None = None
    previous_avg_loss: float | None = None

    for index, close in enumerate(closes):
        if index == 0:
            gains.append(0.0)
            losses.append(0.0)
            rsi_values.append(None)
            continue

        delta = close - closes[index - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

        if index < RSI_PERIOD:
            rsi_values.append(None)
            continue

        if index == RSI_PERIOD:
            previous_avg_gain = sum(gains[1 : RSI_PERIOD + 1]) / RSI_PERIOD
            previous_avg_loss = sum(losses[1 : RSI_PERIOD + 1]) / RSI_PERIOD
        else:
            if previous_avg_gain is None or previous_avg_loss is None:
                raise ValueError("Wilder RSI state was not initialized before recursive update")
            previous_avg_gain = ((RSI_PERIOD - 1) * previous_avg_gain + gains[index]) / RSI_PERIOD
            previous_avg_loss = ((RSI_PERIOD - 1) * previous_avg_loss + losses[index]) / RSI_PERIOD

        if previous_avg_loss == 0:
            rsi_values.append(100.0)
        else:
            rsi_values.append(100 - (100 / (1 + (previous_avg_gain / previous_avg_loss))))

    return rows.with_columns(pl.Series("rsi14", rsi_values, dtype=pl.Float64))
