"""
RSI — Relative Strength Index calculators.

Two implementations:
- ``compute_rsi``:  Simple average (uses last `period` deltas equally).
                    Suitable for quick heuristic alerts.
- ``compute_wilder_rsi``: Wilder's smoothed RSI — identical to the batch
                    implementation in ``gold/indicators.py``. Use this for
                    signals that must be consistent with the daily fact layer.
"""

import logging
from typing import Optional

logger = logging.getLogger('alerts.indicators.rsi')

RSI_PERIOD = 14


def compute_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    """
    Tính RSI từ danh sách close prices (simple average).

    Args:
        closes: Danh sách giá đóng cửa, **mới nhất ở cuối**.
                Cần ít nhất ``period + 1`` phần tử.
        period: Chu kỳ RSI (mặc định 14).

    Returns:
        RSI (0-100) hoặc None nếu không đủ dữ liệu.
    """
    if len(closes) < period + 1:
        return None

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = deltas[-period:]

    gains = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0
    if avg_gain == 0:
        return 0.0

    rs = avg_gain / avg_loss
    rsi = 100.0 - 100.0 / (1.0 + rs)
    return round(rsi, 2)


def compute_wilder_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    """
    Tính Wilder's RSI từ danh sách close prices.

    Công thức (Wilder smoothing):
        avg_gain(t) = ((period-1) × avg_gain(t-1) + gain(t)) / period
        avg_loss(t) = ((period-1) × avg_loss(t-1) + loss(t)) / period

    Đồng nhất với ``gold/indicators._wilder_rsi_for_group``.

    Args:
        closes: Danh sách giá đóng cửa, **mới nhất ở cuối**.
                Cần ít nhất ``period + 1`` phần tử.
        period: Chu kỳ RSI (mặc định 14).

    Returns:
        RSI (0-100) hoặc None nếu không đủ dữ liệu.
    """
    if len(closes) < period + 1:
        return None

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]

    # Seed: simple average over the first `period` deltas
    first_gains = gains[:period]
    first_losses = losses[:period]

    avg_gain = sum(first_gains) / period
    avg_loss = sum(first_losses) / period

    for i in range(period, len(gains)):
        avg_gain = ((period - 1) * avg_gain + gains[i]) / period
        avg_loss = ((period - 1) * avg_loss + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    if avg_gain == 0:
        return 0.0

    rs = avg_gain / avg_loss
    return round(100.0 - 100.0 / (1.0 + rs), 2)
