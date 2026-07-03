"""
RSIRule — Cảnh báo khi RSI vượt ngưỡng quá mua / quá bán.

RSI > 70 → quá mua (có thể giảm)
RSI < 30 → quá bán (có thể tăng)
RSI > 85 hoặc < 15 → mức cực đoan → severity CRITICAL
"""

import logging
from datetime import datetime
from typing import Optional

from ..models import Alert
from .base import BaseAlertRule

logger = logging.getLogger('alerts.rules.rsi')


class RSIRule(BaseAlertRule):
    """Phát cảnh báo RSI quá mua / quá bán."""

    RULE_NAME = "RSI"

    def __init__(self, config):
        cooldown = getattr(config, 'ALERT_COOLDOWN_SEC', 300)
        super().__init__(config, cooldown_sec=cooldown)
        self.overbought = float(getattr(config, 'RSI_OVERBOUGHT', 70))
        self.oversold = float(getattr(config, 'RSI_OVERSOLD', 30))

    def evaluate(
        self,
        symbol: str,
        price: float,
        ts: datetime,
        rsi: Optional[float] = None,
        volume_ratio: Optional[float] = None,
        vwap: Optional[float] = None,
        sigma: Optional[float] = None,
    ) -> Optional[Alert]:
        if rsi is None:
            return None

        if rsi >= self.overbought:
            severity = 'CRITICAL' if rsi >= 85 else 'WARNING'
            alert_type = 'RSI_OVERBOUGHT'
            if not self._can_fire(symbol, alert_type, ts):
                return None
            self._mark_fired(symbol, alert_type, ts)
            return Alert(
                alert_time=ts, symbol=symbol,
                rule_name=self.RULE_NAME, alert_type=alert_type,
                severity=severity, price=price,
                indicator_value=rsi, threshold=self.overbought,
                deviation_pct=0.0,
                message=f"{symbol} RSI={rsi:.1f} — quá mua"
                        f"{' (cực đoan!)' if severity == 'CRITICAL' else ''}",
            )

        if rsi <= self.oversold:
            severity = 'CRITICAL' if rsi <= 15 else 'WARNING'
            alert_type = 'RSI_OVERSOLD'
            if not self._can_fire(symbol, alert_type, ts):
                return None
            self._mark_fired(symbol, alert_type, ts)
            return Alert(
                alert_time=ts, symbol=symbol,
                rule_name=self.RULE_NAME, alert_type=alert_type,
                severity=severity, price=price,
                indicator_value=rsi, threshold=self.oversold,
                deviation_pct=0.0,
                message=f"{symbol} RSI={rsi:.1f} — quá bán"
                        f"{' (cực đoan!)' if severity == 'CRITICAL' else ''}",
            )

        return None
