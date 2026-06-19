"""Thao tác DataFrame dùng chung giữa các tầng."""
from __future__ import annotations

from typing import Sequence

import polars as pl

from stock_lakehouse.utils.dates import format_date


def replace_rows_for_date(
    existing: pl.DataFrame | None,
    replacement: pl.DataFrame,
    processing_date: str,
    *,
    date_column: str = "trading_date",
    columns: Sequence[str] | None = None,
    sort_by: Sequence[str] | str | None = None,
) -> pl.DataFrame:
    """Idempotent replace của đúng một ngày: bỏ rows ngày D khỏi ``existing`` rồi ghép
    rows ngày D dựng lại từ ``replacement``. Chạy lại ngày D cho ra cùng tập rows.

    Dùng chung cho cả Bronze/Silver (``_replace_by_date``) lẫn các Gold fact
    (``replace_daily_market`` / ``replace_index_daily``) — chỉ khác ``date_column``,
    ``columns`` (pin schema/thứ tự cột) và ``sort_by``.
    """
    target = format_date(processing_date)
    if existing is None or existing.is_empty():
        merged = replacement
    else:
        merged = pl.concat(
            [
                existing.filter(pl.col(date_column).cast(pl.Utf8) != target),
                replacement.filter(pl.col(date_column).cast(pl.Utf8) == target),
            ],
            how="diagonal",
        )
    out_columns = list(columns) if columns is not None else merged.columns
    return merged.select(out_columns).sort(sort_by if sort_by is not None else date_column)
