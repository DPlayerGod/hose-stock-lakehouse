"""Engine chỉ báo kỹ thuật — *asset-agnostic*, dùng chung cho fact giá & fact index.

Tách khỏi ``fact_daily_market.py`` để áp dụng nguyên lý SRP: phần "tính chỉ báo"
không phụ thuộc vào việc entity là cổ phiếu (``symbol``) hay chỉ số (``index_code``).
Mỗi fact builder chỉ truyền ``entity_col`` của mình rồi tự lo phần join dimension.

Quy ước warmup giữ nguyên như bản gốc: mọi chỉ báo trả ``null`` tới khi đủ số phiên
tối thiểu để tránh giá trị seed gây hiểu nhầm (xem CLAUDE.md).
"""
from __future__ import annotations

import polars as pl


RSI_PERIOD = 14

# Warmup cho chỉ báo EMA: seed phiên đầu thiên lệch (ema20 phiên 1 = close, macd phiên 1 = 0),
# chưa phản ánh đủ lịch sử. Để đồng bộ quy ước với sma20/avg_volume_20d/rsi14, trả null tới khi đủ N phiên thay vì xuất giá trị seed.
EMA20_WARMUP = 20   # ema20 -> đủ 20 phiên (như sma20)
MACD_WARMUP = 26    # macd = ema12 - ema26 -> leg chậm ema26 quyết định warmup


def add_indicators(
    df: pl.DataFrame,
    *,
    entity_col: str,
    close: str = "close_price",
    volume: str = "volume",
) -> pl.DataFrame:
    """Thêm chỉ báo kỹ thuật trên **toàn bộ lịch sử** của mỗi entity.

    Caller phải sort sẵn theo ``(entity_col, trading_date)`` trước khi gọi — các
    cửa sổ rolling/EWM dựa vào thứ tự phiên. Trả về df gốc + các cột:
    ``price_change · pct_change · sma20 · ema20 · rsi14 · macd · avg_volume_20d``.
    """
    enriched = (
        df.with_columns(
            pl.col(close).diff().over(entity_col).alias("price_change"),
            pl.col(close).pct_change().over(entity_col).alias("pct_change"),
            pl.col(close).rolling_mean(window_size=20, min_samples=20).over(entity_col).alias("sma20"),
            pl.col(close).ewm_mean(span=20, adjust=False).over(entity_col).alias("ema20"),
            pl.col(volume).rolling_mean(window_size=20, min_samples=20).over(entity_col).alias("avg_volume_20d"),
            pl.col(close).ewm_mean(span=12, adjust=False).over(entity_col).alias("_ema12"),
            pl.col(close).ewm_mean(span=26, adjust=False).over(entity_col).alias("_ema26"),
            pl.int_range(0, pl.len()).over(entity_col).alias("_session_idx"),
        )
        .with_columns((pl.col("_ema12") - pl.col("_ema26")).alias("macd"))
        .with_columns(
            _warmup_null("ema20", EMA20_WARMUP),
            _warmup_null("macd", MACD_WARMUP),
        )
        .drop("_ema12", "_ema26", "_session_idx")
    )
    return add_wilder_rsi(enriched, entity_col=entity_col, close=close)


def add_wilder_rsi(df: pl.DataFrame, *, entity_col: str, close: str = "close_price") -> pl.DataFrame:
    """Thêm cột ``rsi14`` (Wilder's RSI) tính riêng cho từng entity."""

    def _per_group(group_df: pl.DataFrame) -> pl.DataFrame:
        return _wilder_rsi_for_group(group_df, close=close)

    return df.group_by(entity_col, maintain_order=True).map_groups(_per_group)


def _warmup_null(column: str, min_sessions: int) -> pl.Expr:
    """Trả null cho ``column`` tới khi đủ ``min_sessions`` phiên (``_session_idx`` 0-based),
    đồng bộ quy ước warmup của các chỉ báo cửa sổ cố định (sma20/avg_volume_20d/rsi14)."""
    return (
        pl.when(pl.col("_session_idx") >= min_sessions - 1)
        .then(pl.col(column))
        .otherwise(None)
        .alias(column)
    )


def _wilder_rsi_for_group(group_df: pl.DataFrame, *, close: str) -> pl.DataFrame:
    """Compute Wilder's RSI(14) for one entity; cheap for this project's small universe."""
    rows = group_df.sort("trading_date")
    closes = rows.get_column(close).to_list()
    rsi_values: list[float | None] = []
    gains: list[float] = []
    losses: list[float] = []
    previous_avg_gain: float | None = None
    previous_avg_loss: float | None = None

    for index, value in enumerate(closes):
        if index == 0:
            gains.append(0.0)
            losses.append(0.0)
            rsi_values.append(None)
            continue

        delta = value - closes[index - 1]
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
