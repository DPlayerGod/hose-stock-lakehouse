"""
VolumeSpikeRule — Cảnh báo khi khối lượng giao dịch đột biến.

Volume ratio ≥ 3.0 → WARNING (khối lượng bất thường)
Volume ratio ≥ 5.0 → CRITICAL (khối lượng cực đoan)
"""

import logging
from datetime import datetime
from typing import Optional

from ..models import Alert
from .base import BaseAlertRule

logger = logging.getLogger('alerts.rules.volume_spike')


class VolumeSpikeRule(BaseAlertRule):
    """Phát cảnh báo khi khối lượng giao dịch đột biến."""

    RULE_NAME = "VOLUME_SPIKE"

    def __init__(self, config):
        cooldown = getattr(config, 'ALERT_COOLDOWN_SEC', 300)
        super().__init__(config, cooldown_sec=cooldown)
        self.spike_ratio = float(getattr(config, 'VOLUME_SPIKE_RATIO', 3.0))

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
        if volume_ratio is None:
            return None

        if volume_ratio < self.spike_ratio:
            return None

        alert_type = 'VOLUME_SPIKE'
        severity = 'CRITICAL' if volume_ratio >= 5.0 else 'WARNING'

        if not self._can_fire(symbol, alert_type, ts):
            return None
        self._mark_fired(symbol, alert_type, ts)

        return Alert(
            alert_time=ts, symbol=symbol,
            rule_name=self.RULE_NAME, alert_type=alert_type,
            severity=severity, price=price,
            indicator_value=volume_ratio, threshold=self.spike_ratio,
            deviation_pct=0.0,
            message=f"{symbol} KL đột biến {volume_ratio:.1f}x trung bình"
                    f"{' (cực đoan!)' if severity == 'CRITICAL' else ''}",
        )
