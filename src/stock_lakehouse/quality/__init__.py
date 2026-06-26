"""Tầng data-quality — validate bằng Polars theo 6 chiều chất lượng dữ liệu.

Tổ chức:
- ``result``  — kiểu chung: ``Dimension`` (6 chiều), ``Severity``, ``CheckOutcome``,
  ``ValidationResult``.
- ``checks``  — các hàm ``check(df, config)`` tái sử dụng + ``run_suite``.
- ``suites``  — suite khai báo cho từng bảng (các hàm ``validate_*``).

Import gọn từ một chỗ::

    from stock_lakehouse.quality import validate_silver_ohlcv, ValidationResult
"""
from __future__ import annotations

from stock_lakehouse.quality.checks import (
    Check,
    ColumnRelation,
    ForeignKey,
    InRange,
    InSet,
    MatchesDate,
    NotNull,
    Positive,
    RequiredColumns,
    Unique,
    WithinDailyBand,
    run_check,
    run_suite,
)
from stock_lakehouse.quality.result import (
    CheckOutcome,
    Dimension,
    Severity,
    ValidationResult,
)
from stock_lakehouse.quality.suites import (
    validate_bronze_corporate_events,
    validate_bronze_ohlcv,
    validate_bronze_symbols,
    validate_dim_date,
    validate_dim_symbol,
    validate_fact_corporate_events,
    validate_fact_daily_market,
    validate_fact_index_daily,
    validate_silver_corporate_events,
    validate_silver_ohlcv,
    validate_silver_symbols,
)

__all__ = [
    # result
    "Dimension",
    "Severity",
    "CheckOutcome",
    "ValidationResult",
    # checks (config + runner)
    "Check",
    "RequiredColumns",
    "NotNull",
    "Unique",
    "InRange",
    "Positive",
    "InSet",
    "ColumnRelation",
    "ForeignKey",
    "MatchesDate",
    "WithinDailyBand",
    "run_check",
    "run_suite",
    # suites
    "validate_bronze_ohlcv",
    "validate_bronze_symbols",
    "validate_silver_ohlcv",
    "validate_silver_symbols",
    "validate_dim_date",
    "validate_dim_symbol",
    "validate_fact_daily_market",
    "validate_fact_index_daily",
    "validate_bronze_corporate_events",
    "validate_silver_corporate_events",
    "validate_fact_corporate_events",
]
