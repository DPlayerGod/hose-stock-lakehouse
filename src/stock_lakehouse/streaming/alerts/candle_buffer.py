"""
CandleBuffer — lưu N nến OHLCV gần nhất cho mỗi symbol.

RSI cần ~15 nến close, Volume Spike cần ~21 nến volume.
Buffer mặc định giữ 50 nến cho an toàn.
"""

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Candle:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class CandleBuffer:
    """Quản lý buffer nến gần nhất cho nhiều symbol."""

    def __init__(self, maxlen: int = 50):
        self._maxlen = maxlen
        self._buffers: dict[str, deque[Candle]] = {}

    def push(self, symbol: str, candle: Candle) -> None:
        """Thêm hoặc cập nhật nến trong buffer.

        Nếu nến cuối cùng trong buffer có cùng candle_time → ghi đè
        (intra-candle update). Nếu khác → append nến mới.
        Nếu nến mới thuộc ngày khác → clear buffer trước (qua ngày mới).
        """
        if symbol not in self._buffers:
            self._buffers[symbol] = deque(maxlen=self._maxlen)

        buf = self._buffers[symbol]
        if buf and buf[-1].ts.date() != candle.ts.date():
            buf.clear()
        if buf and buf[-1].ts == candle.ts:
            buf[-1] = candle
        else:
            buf.append(candle)

    def get_closes(self, symbol: str, n: Optional[int] = None) -> list[float]:
        """Lấy n giá close gần nhất (mới nhất ở cuối)."""
        buf = self._buffers.get(symbol)
        if not buf:
            return []
        items = list(buf) if n is None else list(buf)[-n:]
        return [c.close for c in items]

    def get_volumes(self, symbol: str, n: Optional[int] = None) -> list[int]:
        """Lấy n volume gần nhất (mới nhất ở cuối)."""
        buf = self._buffers.get(symbol)
        if not buf:
            return []
        items = list(buf) if n is None else list(buf)[-n:]
        return [c.volume for c in items]

    def size(self, symbol: str) -> int:
        """Số nến hiện tại trong buffer của symbol."""
        buf = self._buffers.get(symbol)
        return len(buf) if buf else 0

    def last(self, symbol: str) -> Optional[Candle]:
        """Lấy nến mới nhất."""
        buf = self._buffers.get(symbol)
        return buf[-1] if buf else None
