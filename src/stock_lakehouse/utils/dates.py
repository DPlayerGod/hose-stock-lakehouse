from __future__ import annotations

from datetime import date, datetime, timezone


def now_utc() -> datetime:
    """Thời điểm hiện tại dạng UTC instant (timezone-aware).

    Chuẩn lưu trữ: mọi cột timestamp lưu UTC instant. Việc quy đổi sang
    giờ địa phương (nếu cần) làm ở tầng hiển thị/serving, không phải khi ghi.
    """
    return datetime.now(timezone.utc)


def parse_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def format_date(value: str | date | datetime) -> str:
    return parse_date(value).isoformat()

