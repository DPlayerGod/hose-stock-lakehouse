"""
BaseAlertRule — abstract class cho tất cả alert rules.

Mỗi rule kế thừa class này và implement ``evaluate()``.
Cooldown mechanism tích hợp sẵn: tránh spam alert cho cùng
symbol trong khoảng thời gian ngắn (cooldown key = symbol).
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Optional

from ..models import Alert
from ..candle_buffer import CandleBuffer

ICT = timezone(timedelta(hours=7))


class BaseAlertRule(ABC):
    """Base class cho tất cả alert rules."""

    RULE_NAME: str = ""

    def __init__(self, config, cooldown_sec: int = 300):
        self.config = config
        self.cooldown_sec = cooldown_sec
        self._last_fired: dict[str, datetime] = {}

    @abstractmethod
    def evaluate(
        self,
        symbol: str,
        price: float,
        ts: datetime,
        buffer: CandleBuffer,
    ) -> Optional[Alert]:
        ...

    def _can_fire(self, symbol: str, alert_type: str, ts: datetime) -> bool:
        """Kiểm tra cooldown — trả False nếu đang trong cooldown."""
        last = self._last_fired.get(symbol)
        if last and (ts - last).total_seconds() < self.cooldown_sec:
            return False
        return True

    def _mark_fired(self, symbol: str, alert_type: str, ts: datetime) -> None:
        """Đánh dấu đã fire — bắt đầu cooldown."""
        self._last_fired[symbol] = ts
