"""
VWAPRule — Cảnh báo khi giá lệch khỏi VWAP ± σ bands.

Refactor từ logic _check() cũ trong detector.py.
"""

import logging
from datetime import datetime
from typing import Optional

from ..models import Alert
from ..candle_buffer import CandleBuffer
from ..vwap import VWAPCalculator
from .base import BaseAlertRule

logger = logging.getLogger('alerts.rules.vwap')


class VWAPRule(BaseAlertRule):
    """Phát cảnh báo khi giá vượt VWAP ± k×σ hoặc % threshold."""

    RULE_NAME = "VWAP"

    def __init__(self, config, vwap_calc: VWAPCalculator):
        cooldown = getattr(config, 'ALERT_COOLDOWN_SEC', 300)
        super().__init__(config, cooldown_sec=cooldown)
        self.calc = vwap_calc

    def evaluate(
        self,
        symbol: str,
        price: float,
        ts: datetime,
        buffer: CandleBuffer,
    ) -> Optional[Alert]:
        s_vwap, s_sigma = self.calc.get_session_vwap_and_sigma(symbol, ts)
        if not s_vwap or s_vwap <= 0:
            return None

        deviation_pct = (price - s_vwap) / s_vwap * 100

        mode = getattr(self.config, 'ALERT_BAND_MODE', 'sigma')
        if mode == 'pct':
            return self._check_pct(symbol, price, s_vwap, deviation_pct, ts)
        return self._check_sigma(symbol, price, s_vwap, s_sigma, deviation_pct, ts)

    def _check_pct(
        self, symbol: str, price: float, vwap: float,
        deviation_pct: float, ts: datetime,
    ) -> Optional[Alert]:
        threshold = self.config.ALERT_THRESHOLD_PCT
        if deviation_pct > threshold:
            return self._make_alert(
                symbol, 'VWAP_BREAKOUT_UP', 'WARNING', price, vwap,
                threshold, deviation_pct, ts,
                f"{symbol} breakout ↑ VWAP +{deviation_pct:.1f}%"
            )
        if deviation_pct < -threshold:
            return self._make_alert(
                symbol, 'VWAP_BREAKDOWN', 'WARNING', price, vwap,
                threshold, deviation_pct, ts,
                f"{symbol} breakdown ↓ VWAP {deviation_pct:.1f}%"
            )
        return None

    def _check_sigma(
        self, symbol: str, price: float, vwap: float,
        sigma: Optional[float], deviation_pct: float, ts: datetime,
    ) -> Optional[Alert]:
        if not sigma or sigma <= 0:
            return None

        k = float(getattr(self.config, 'BAND_SIGMA_MULTIPLIER', 2.0))
        upper = vwap + k * sigma
        lower = vwap - k * sigma

        if price > upper:
            return self._make_alert(
                symbol, 'VWAP_BREAKOUT_UP', 'WARNING', price, vwap,
                upper, deviation_pct, ts,
                f"{symbol} vượt VWAP +{k}σ (giá {price:.2f} > band {upper:.2f})"
            )
        if price < lower:
            return self._make_alert(
                symbol, 'VWAP_BREAKDOWN', 'WARNING', price, vwap,
                lower, deviation_pct, ts,
                f"{symbol} dưới VWAP -{k}σ (giá {price:.2f} < band {lower:.2f})"
            )
        return None

    def _make_alert(
        self, symbol: str, alert_type: str, severity: str,
        price: float, vwap: float, threshold: float,
        deviation_pct: float, ts: datetime, message: str,
    ) -> Optional[Alert]:
        if not self._can_fire(symbol, alert_type, ts):
            return None
        self._mark_fired(symbol, alert_type, ts)
        return Alert(
            alert_time=ts, symbol=symbol,
            rule_name=self.RULE_NAME, alert_type=alert_type,
            severity=severity, price=price,
            indicator_value=vwap, threshold=threshold,
            deviation_pct=deviation_pct, message=message,
        )
