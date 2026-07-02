"""
Volume Analyzer — phát hiện khối lượng đột biến.

Công thức:
    volume_ratio = current_volume / avg(volume, lookback)
    Spike khi ratio >= threshold (mặc định 3.0)
"""

import logging
from typing import Optional

logger = logging.getLogger('alerts.indicators.volume')


def compute_volume_ratio(
    volumes: list[int], lookback: int = 20
) -> Optional[float]:
    """
    Tính tỷ lệ volume nến hiện tại / trung bình N nến trước đó.

    Args:
        volumes: Danh sách volume, **mới nhất ở cuối**.
                 Cần ít nhất ``lookback + 1`` phần tử.
        lookback: Số nến trước đó để tính trung bình (mặc định 20).

    Returns:
        Tỷ lệ (float) hoặc None nếu không đủ dữ liệu.
    """
    if len(volumes) < lookback + 1:
        return None

    current = volumes[-1]
    past = volumes[-(lookback + 1):-1]
    avg = sum(past) / len(past)

    if avg <= 0:
        return None

    return round(current / avg, 2)
