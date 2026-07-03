"""
CombinedSignalRule — Cảnh báo kết hợp VWAP + RSI + Volume Spike.

Chỉ fire khi ≥ 2 tín hiệu đồng thuận → giảm false alarm, tăng chất lượng.

Bảng tổ hợp:
┌────────────────┬──────────┬────────────┬─────────────────────┬──────────┐
│ VWAP           │ RSI      │ Volume     │ Alert               │ Severity │
├────────────────┼──────────┼────────────┼─────────────────────┼──────────┤
│ Breakout ↑     │ > 70     │ Spike ≥ 3x │ COMBINED_PUMP_RISK  │ CRITICAL │
│ Breakdown ↓    │ < 30     │ Spike ≥ 3x │ COMBINED_PANIC_SELL │ CRITICAL │
│ Breakout ↑     │ > 70     │ Bình thường│ COMBINED_OVERBOUGHT │ WARNING  │
│ Breakdown ↓    │ < 30     │ Bình thường│ COMBINED_OVERSOLD   │ WARNING  │
│ Trong band     │ 30-70    │ Spike ≥ 3x │ COMBINED_UNUSUAL_VOL │ WARNING │
└────────────────┴──────────┴────────────┴─────────────────────┴──────────┘
"""

import logging
from datetime import datetime
from typing import Optional

from ..models import Alert
from .base import BaseAlertRule

logger = logging.getLogger('alerts.rules.combined')


class CombinedSignalRule(BaseAlertRule):
    """Cảnh báo kết hợp đa tín hiệu: VWAP × RSI × Volume."""

    RULE_NAME = "COMBINED"

    def __init__(self, config, vwap_calc=None):
        cooldown = getattr(config, 'ALERT_COOLDOWN_SEC', 300)
        super().__init__(config, cooldown_sec=cooldown)

        self.band_mode = getattr(config, 'ALERT_BAND_MODE', 'sigma')
        self.threshold_pct = getattr(config, 'ALERT_THRESHOLD_PCT', 1.5)
        self.sigma_k = float(getattr(config, 'BAND_SIGMA_MULTIPLIER', 2.0))

        self.rsi_overbought = float(getattr(config, 'RSI_OVERBOUGHT', 70))
        self.rsi_oversold = float(getattr(config, 'RSI_OVERSOLD', 30))

        self.vol_spike_ratio = float(getattr(config, 'VOLUME_SPIKE_RATIO', 3.0))

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
        vwap_state = self._get_vwap_state(price, vwap, sigma)

        is_breakout = (vwap_state == 'BREAKOUT')
        is_breakdown = (vwap_state == 'BREAKDOWN')
        is_overbought = (rsi is not None and rsi >= self.rsi_overbought)
        is_oversold = (rsi is not None and rsi <= self.rsi_oversold)
        is_vol_spike = (volume_ratio is not None and volume_ratio >= self.vol_spike_ratio)

        alert = None

        if is_breakout and is_overbought and is_vol_spike:
            alert = self._build(
                symbol, price, ts, vwap,
                alert_type='COMBINED_PUMP_RISK',
                severity='CRITICAL',
                indicator_value=rsi or 0,
                threshold=self.rsi_overbought,
                message=(
                    f"{symbol} RỦI RO ĐẨY GIÁ — "
                    f"Breakout VWAP + RSI={rsi:.0f} (quá mua) + KL {volume_ratio:.1f}x"
                ),
            )

        elif is_breakdown and is_oversold and is_vol_spike:
            alert = self._build(
                symbol, price, ts, vwap,
                alert_type='COMBINED_PANIC_SELL',
                severity='CRITICAL',
                indicator_value=rsi or 0,
                threshold=self.rsi_oversold,
                message=(
                    f"{symbol} BÁN THÁO — "
                    f"Breakdown VWAP + RSI={rsi:.0f} (quá bán) + KL {volume_ratio:.1f}x"
                ),
            )

        elif is_breakout and is_overbought:
            alert = self._build(
                symbol, price, ts, vwap,
                alert_type='COMBINED_OVERBOUGHT_BREAKOUT',
                severity='WARNING',
                indicator_value=rsi or 0,
                threshold=self.rsi_overbought,
                message=(
                    f"{symbol} Breakout VWAP + RSI={rsi:.0f} (quá mua) — cẩn trọng"
                ),
            )

        elif is_breakdown and is_oversold:
            alert = self._build(
                symbol, price, ts, vwap,
                alert_type='COMBINED_OVERSOLD_BREAKDOWN',
                severity='WARNING',
                indicator_value=rsi or 0,
                threshold=self.rsi_oversold,
                message=(
                    f"{symbol} Breakdown VWAP + RSI={rsi:.0f} (quá bán) — có thể là cơ hội"
                ),
            )

        elif is_vol_spike and not is_breakout and not is_breakdown:
            alert = self._build(
                symbol, price, ts, vwap,
                alert_type='COMBINED_UNUSUAL_VOLUME',
                severity='WARNING',
                indicator_value=volume_ratio or 0,
                threshold=self.vol_spike_ratio,
                message=(
                    f"{symbol} KL đột biến {volume_ratio:.1f}x — "
                    f"RSI={f'{rsi:.0f}' if rsi else '?'}, giá trong band VWAP"
                ),
            )

        elif (is_breakout or is_breakdown) and is_vol_spike:
            direction = "Breakout ↑" if is_breakout else "Breakdown ↓"
            alert = self._build(
                symbol, price, ts, vwap,
                alert_type='COMBINED_VOLUME_BREAKOUT' if is_breakout else 'COMBINED_VOLUME_BREAKDOWN',
                severity='WARNING',
                indicator_value=volume_ratio or 0,
                threshold=self.vol_spike_ratio,
                message=(
                    f"{symbol} {direction} VWAP + KL {volume_ratio:.1f}x — "
                    f"RSI={f'{rsi:.0f}' if rsi else '?'}"
                ),
            )

        return alert

    def _get_vwap_state(self, price: float, vwap: Optional[float], sigma: Optional[float]) -> str:
        """Trả về 'BREAKOUT', 'BREAKDOWN', hoặc 'IN_BAND'."""
        if not vwap or vwap <= 0:
            return 'IN_BAND'

        if self.band_mode == 'pct':
            deviation_pct = (price - vwap) / vwap * 100
            if deviation_pct > self.threshold_pct:
                return 'BREAKOUT'
            if deviation_pct < -self.threshold_pct:
                return 'BREAKDOWN'
            return 'IN_BAND'

        if not sigma or sigma <= 0:
            return 'IN_BAND'
        upper = vwap + self.sigma_k * sigma
        lower = vwap - self.sigma_k * sigma
        if price > upper:
            return 'BREAKOUT'
        if price < lower:
            return 'BREAKDOWN'
        return 'IN_BAND'

    def _build(
        self, symbol: str, price: float, ts: datetime,
        vwap: Optional[float],
        alert_type: str, severity: str,
        indicator_value: float, threshold: float,
        message: str,
    ) -> Optional[Alert]:
        if not self._can_fire(symbol, alert_type, ts):
            return None
        self._mark_fired(symbol, alert_type, ts)

        s_vwap = vwap or 0
        deviation_pct = ((price - s_vwap) / s_vwap * 100) if s_vwap > 0 else 0

        return Alert(
            alert_time=ts, symbol=symbol,
            rule_name=self.RULE_NAME, alert_type=alert_type,
            severity=severity, price=price,
            indicator_value=indicator_value, threshold=threshold,
            deviation_pct=deviation_pct, message=message,
        )
