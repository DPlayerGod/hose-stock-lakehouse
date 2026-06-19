from __future__ import annotations

from datetime import date, timedelta

import holidays
import polars as pl

from stock_lakehouse.quality import validate_dim_date


DIM_DATE_COLUMNS = (
    "date_key",
    "full_date",
    "day",
    "cal_week",
    "cal_month",
    "cal_quarter",
    "cal_year",
    "is_weekend",
    "event_name",
    "event_type",
    "is_day_off",
)


def build_dim_date(start_date: str | date, end_date: str | date) -> pl.DataFrame:
    start = _to_date(start_date)
    end = _to_date(end_date)
    if end < start:
        raise ValueError("end_date must be greater than or equal to start_date")

    vn_holidays = holidays.country_holidays("VN", years=range(start.year, end.year + 1))
    rows = []
    current = start
    while current <= end:
        event_name = vn_holidays.get(current)
        is_weekend = current.weekday() >= 5
        rows.append(
            {
                "date_key": int(current.strftime("%Y%m%d")),
                "full_date": current,
                "day": current.day,
                "cal_week": current.isocalendar().week,
                "cal_month": current.month,
                "cal_quarter": ((current.month - 1) // 3) + 1,
                "cal_year": current.year,
                "is_weekend": is_weekend,
                "event_name": event_name,
                "event_type": _classify_event_type(event_name),
                "is_day_off": is_weekend or event_name is not None,
            }
        )
        current += timedelta(days=1)

    dim_date = pl.DataFrame(rows).select(DIM_DATE_COLUMNS)
    validate_dim_date(dim_date).raise_for_errors()
    return dim_date


_COMPENSATION_KEYWORDS = ("nghỉ bù", "hoán đổi", "thay cho")


def _classify_event_type(event_name: str | None) -> str | None:
    if event_name is None:
        return None
    name = event_name.lower()
    if any(keyword in name for keyword in _COMPENSATION_KEYWORDS):
        return "COMPENSATION"
    return "HOLIDAY"


def _to_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)
