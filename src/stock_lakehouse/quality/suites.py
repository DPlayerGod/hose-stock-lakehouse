"""Suite chất lượng theo từng bảng — *khai báo*, không lặp logic.

Mỗi bảng = một danh sách config check (xem ``checks.py``) gắn với 6 chiều chất
lượng. Hàm ``validate_*`` chỉ việc dựng danh sách rồi gọi ``run_suite``; toàn bộ
phần "chạy check" dùng chung.
"""
from __future__ import annotations

from typing import Iterable

import polars as pl

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
from stock_lakehouse.quality.result import ValidationResult


def _validate(
    df: pl.DataFrame,
    *,
    suite_name: str,
    required: tuple[str, ...],
    checks: Iterable[Check],
) -> ValidationResult:
    """Chạy suite với "cổng" cột bắt buộc trước (fail-fast, tránh báo lỗi trùng).

    Nếu thiếu cột bắt buộc, các check sau không có ngữ cảnh để chạy ⇒ trả ngay
    lỗi cột thiếu thay vì lặp lại "missing column" ở từng check.
    """
    gate = run_check(df, RequiredColumns(required))
    if not gate.passed:
        return ValidationResult(False, (f"{suite_name}: {gate.message}",), (), (gate,))
    return run_suite(df, checks, suite_name)


# ────────────────────────────────── BRONZE ─────────────────────────────────


def validate_bronze_ohlcv(df: pl.DataFrame) -> ValidationResult:
    """Bronze = raw đã ép kiểu → chỉ kiểm COMPLETENESS (đủ cột + khoá không null)."""
    required = (
        "symbol", "time", "open", "high", "low", "close",
        "volume", "source", "batch_id", "ingested_at",
    )
    return _validate(
        df,
        suite_name="bronze_hose_ohlcv_daily",
        required=required,
        checks=[NotNull(("symbol", "time", "source", "batch_id", "ingested_at"))],
    )


def validate_bronze_symbols(df: pl.DataFrame) -> ValidationResult:
    """Bronze symbols → COMPLETENESS (đủ cột lineage + khoá không null)."""
    required = ("symbol", "source", "batch_id", "ingested_at")
    return _validate(
        df,
        suite_name="bronze_hose_symbols",
        required=required,
        checks=[NotNull(required)],
    )


# ────────────────────────────────── SILVER ─────────────────────────────────


def validate_silver_ohlcv(df: pl.DataFrame, processing_date: str | None = None) -> ValidationResult:
    """Silver OHLCV — trải đủ COMPLETENESS / UNIQUENESS / VALIDITY / CONSISTENCY
    (+ TIMELINESS khi truyền ``processing_date``)."""
    required = (
        "symbol", "trading_date", "open_price", "high_price",
        "low_price", "close_price", "volume",
    )
    checks: list[Check] = [
        # COMPLETENESS
        NotNull(("symbol", "trading_date")),
        # UNIQUENESS
        Unique(("symbol", "trading_date")),
        # VALIDITY — giá > 0, khối lượng >= 0
        Positive(("open_price", "high_price", "low_price", "close_price")),
        InRange("volume", min_value=0),
        # CONSISTENCY — quan hệ OHLC trong cùng phiên
        ColumnRelation("high_price", ">=", "low_price"),
        ColumnRelation("high_price", ">=", "open_price"),
        ColumnRelation("high_price", ">=", "close_price"),
        ColumnRelation("low_price", "<=", "open_price"),
        ColumnRelation("low_price", "<=", "close_price"),
    ]
    if processing_date is not None:
        # TIMELINESS — mọi dòng phải đúng ngày D
        checks.append(MatchesDate("trading_date", processing_date))
    return _validate(df, suite_name="silver_hose_ohlcv_daily", required=required, checks=checks)


def validate_silver_symbols(df: pl.DataFrame) -> ValidationResult:
    """Silver symbols — COMPLETENESS + UNIQUENESS (mỗi mã 1 dòng) + VALIDITY trạng thái."""
    required = ("symbol", "exchange_code", "listed_status")
    return _validate(
        df,
        suite_name="silver_hose_symbols",
        required=required,
        checks=[
            NotNull(required),
            Unique(("symbol",)),
            InSet("listed_status", ("LISTED", "DELISTED")),
        ],
    )


# ─────────────────────────────────── GOLD ──────────────────────────────────


def validate_dim_date(df: pl.DataFrame) -> ValidationResult:
    """dim_date — khoá không null & duy nhất + miền giá trị lịch hợp lệ."""
    required = ("date_key", "full_date")
    return _validate(
        df,
        suite_name="dim_date",
        required=required,
        checks=[
            NotNull(("date_key", "full_date")),
            Unique(("date_key",)),
            Unique(("full_date",)),
            InRange("cal_month", min_value=1, max_value=12),
            InRange("cal_quarter", min_value=1, max_value=4),
            InRange("day", min_value=1, max_value=31),
        ],
    )


def validate_dim_symbol(df: pl.DataFrame) -> ValidationResult:
    """dim_symbol — surrogate key & symbol không null/duy nhất + trạng thái hợp lệ."""
    required = ("symbol_key", "symbol", "exchange_code", "listed_status")
    return _validate(
        df,
        suite_name="dim_symbol",
        required=required,
        checks=[
            NotNull(required),
            Unique(("symbol_key",)),
            Unique(("symbol",)),
            InSet("listed_status", ("LISTED", "DELISTED")),
        ],
    )


def validate_fact_daily_market(
    fact_df: pl.DataFrame,
    dim_symbol_df: pl.DataFrame,
    dim_date_df: pl.DataFrame,
) -> ValidationResult:
    """fact_hose_daily_market — đầy đủ 6 chiều:

    COMPLETENESS (khoá), UNIQUENESS (symbol_key+date_key), VALIDITY (rsi ∈ [0,100],
    giá > 0, volume ≥ 0), CONSISTENCY (quan hệ OHLC + FK tới dim_symbol/dim_date),
    ACCURACY (biên độ giá ±7% — WARN), TIMELINESS được đảm bảo từ tầng trên.
    """
    required = ("symbol_key", "date_key", "trading_date")
    checks: list[Check] = [
        # COMPLETENESS
        NotNull(("symbol_key", "date_key", "trading_date")),
        # UNIQUENESS
        Unique(("symbol_key", "date_key")),
        # VALIDITY
        InRange("rsi14", min_value=0, max_value=100),
        Positive(("open_price", "high_price", "low_price", "close_price")),
        InRange("volume", min_value=0),
        # CONSISTENCY — quan hệ OHLC
        ColumnRelation("high_price", ">=", "low_price"),
        ColumnRelation("low_price", "<=", "close_price"),
        # CONSISTENCY — referential integrity (FK)
        ForeignKey("symbol_key", dim_symbol_df, "symbol_key", name="fact->dim_symbol"),
        ForeignKey("date_key", dim_date_df, "date_key", name="fact->dim_date"),
        # ACCURACY — biên độ giá ngày-trên-ngày hợp lý (WARN, không chặn)
        WithinDailyBand("pct_change", band=0.07),
    ]
    return _validate(
        fact_df, suite_name="fact_hose_daily_market", required=required, checks=checks
    )


def validate_fact_index_daily(
    fact_df: pl.DataFrame,
    dim_date_df: pl.DataFrame,
) -> ValidationResult:
    """fact_hose_index_daily — như fact giá nhưng cho chỉ số thị trường:

    Khác fact cổ phiếu ở 2 điểm:
    - **Không** FK tới ``dim_symbol`` (index không phải doanh nghiệp niêm yết; định
      danh bằng natural key ``index_code``). UNIQUENESS theo ``index_code + date_key``.
    - **Không** check biên độ ±7% (``WithinDailyBand``): trần/sàn ±7% là giới hạn giá
      *cổ phiếu* HOSE, chỉ số tổng hợp không bị ràng buộc này → bỏ để tránh WARN giả.
    """
    required = ("index_code", "date_key", "trading_date")
    checks: list[Check] = [
        # COMPLETENESS
        NotNull(("index_code", "date_key", "trading_date")),
        # UNIQUENESS
        Unique(("index_code", "date_key")),
        # VALIDITY
        InRange("rsi14", min_value=0, max_value=100),
        Positive(("open_price", "high_price", "low_price", "close_price")),
        InRange("volume", min_value=0),
        # CONSISTENCY — quan hệ OHLC
        ColumnRelation("high_price", ">=", "low_price"),
        ColumnRelation("low_price", "<=", "close_price"),
        # CONSISTENCY — referential integrity (chỉ FK tới dim_date)
        ForeignKey("date_key", dim_date_df, "date_key", name="fact_index->dim_date"),
    ]
    return _validate(
        fact_df, suite_name="fact_hose_index_daily", required=required, checks=checks
    )
