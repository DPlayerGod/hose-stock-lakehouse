from __future__ import annotations

from datetime import date, datetime


def parse_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def format_date(value: str | date | datetime) -> str:
    return parse_date(value).isoformat()

