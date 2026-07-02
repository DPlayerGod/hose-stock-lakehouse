"""
Alert models — dataclasses và enums cho hệ thống cảnh báo đa tín hiệu.
"""

from dataclasses import dataclass
from datetime import datetime


class AlertSeverity(str):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertCategory(str):
    TREND = "TREND"
    MOMENTUM = "MOMENTUM"
    VOLUME = "VOLUME"


@dataclass
class Alert:
    alert_time: datetime
    symbol: str
    rule_name: str
    alert_type: str
    severity: str
    price: float
    indicator_value: float
    threshold: float
    deviation_pct: float
    message: str
