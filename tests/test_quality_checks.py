"""Test cho tầng data-quality theo 6 chiều: các hàm check(df, config) + suites."""
from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from stock_lakehouse.quality import (
    ColumnRelation,
    Dimension,
    ForeignKey,
    InRange,
    InSet,
    MatchesDate,
    NotNull,
    Positive,
    RequiredColumns,
    Severity,
    Unique,
    WithinDailyBand,
    run_check,
    run_suite,
    validate_dim_symbol,
    validate_fact_daily_market,
    validate_silver_ohlcv,
)
from stock_lakehouse.quality.checks import (
    check_column_relation,
    check_foreign_key,
    check_in_range,
    check_in_set,
    check_matches_date,
    check_not_null,
    check_positive,
    check_required_columns,
    check_unique,
    check_within_daily_band,
)


# ─────────────────────────── COMPLETENESS ──────────────────────────────────


def test_required_columns_pass_and_fail() -> None:
    df = pl.DataFrame({"a": [1], "b": [2]})
    assert check_required_columns(df, RequiredColumns(("a", "b"))).passed
    out = check_required_columns(df, RequiredColumns(("a", "c")))
    assert not out.passed
    assert out.dimension is Dimension.COMPLETENESS
    assert "c" in (out.message or "")


def test_not_null_detects_nulls_and_missing_column() -> None:
    df = pl.DataFrame({"a": [1, None], "b": [1, 2]})
    assert not check_not_null(df, NotNull(("a",))).passed
    assert check_not_null(df, NotNull(("b",))).passed
    assert not check_not_null(df, NotNull(("missing",))).passed


# ──────────────────────────── UNIQUENESS ───────────────────────────────────


def test_unique_flags_duplicate_composite_key() -> None:
    df = pl.DataFrame({"s": ["A", "A", "B"], "d": [1, 1, 1]})
    fail = check_unique(df, Unique(("s", "d")))
    assert not fail.passed and fail.failing_rows == 1
    assert check_unique(df, Unique(("s",))).passed is False  # A duplicated
    assert check_unique(pl.DataFrame({"s": ["A", "B"]}), Unique(("s",))).passed


# ───────────────────────────── VALIDITY ────────────────────────────────────


def test_in_range_ignores_null_flags_out_of_bounds() -> None:
    df = pl.DataFrame({"rsi": [10.0, 150.0, None]})
    out = check_in_range(df, InRange("rsi", min_value=0, max_value=100))
    assert not out.passed and out.failing_rows == 1
    assert check_in_range(df, InRange("rsi", min_value=0)).passed  # no upper bound


def test_positive_flags_non_positive_only_on_non_null() -> None:
    df = pl.DataFrame({"p": [1.0, 0.0, None], "q": [5.0, 6.0, 7.0]})
    assert not check_positive(df, Positive(("p",))).passed
    assert check_positive(df, Positive(("q",))).passed


def test_in_set_enforces_allowed_values() -> None:
    df = pl.DataFrame({"status": ["LISTED", "WAT", None]})
    out = check_in_set(df, InSet("status", ("LISTED", "DELISTED")))
    assert not out.passed and out.failing_rows == 1  # 'WAT'; null bỏ qua


# ──────────────────────────── CONSISTENCY ──────────────────────────────────


def test_column_relation_cross_field() -> None:
    df = pl.DataFrame({"high": [10.0, 5.0], "low": [1.0, 9.0]})
    out = check_column_relation(df, ColumnRelation("high", ">=", "low"))
    assert not out.passed and out.failing_rows == 1
    assert out.dimension is Dimension.CONSISTENCY


def test_column_relation_rejects_unknown_operator() -> None:
    df = pl.DataFrame({"a": [1], "b": [2]})
    assert not check_column_relation(df, ColumnRelation("a", "<>", "b")).passed


def test_foreign_key_detects_orphans_and_skips_empty() -> None:
    ref = pl.DataFrame({"key": [1, 2, 3]})
    df = pl.DataFrame({"key": [1, 99]})
    out = check_foreign_key(df, ForeignKey("key", ref, "key"))
    assert not out.passed and out.failing_rows == 1
    empty = pl.DataFrame({"key": []}, schema={"key": pl.Int64})
    assert check_foreign_key(empty, ForeignKey("key", ref, "key")).passed


# ───────────────────────────── ACCURACY ────────────────────────────────────


def test_within_daily_band_is_warn_severity() -> None:
    df = pl.DataFrame({"pct_change": [0.05, 0.09, None]})
    out = check_within_daily_band(df, WithinDailyBand("pct_change", band=0.07))
    assert not out.passed
    assert out.severity is Severity.WARN
    assert out.dimension is Dimension.ACCURACY
    assert out.failing_rows == 1


# ──────────────────────────── TIMELINESS ───────────────────────────────────


def test_matches_date_flags_rows_outside_target_day() -> None:
    df = pl.DataFrame({"trading_date": [date(2026, 6, 15), date(2026, 6, 16)]})
    out = check_matches_date(df, MatchesDate("trading_date", "2026-06-15"))
    assert not out.passed and out.failing_rows == 1
    assert out.dimension is Dimension.TIMELINESS


# ─────────────────────────── run_suite engine ──────────────────────────────


def test_run_check_dispatches_by_config_type() -> None:
    df = pl.DataFrame({"x": [1, 1]})
    assert run_check(df, NotNull(("x",))).passed
    assert not run_check(df, Unique(("x",))).passed


def test_run_suite_separates_errors_from_warnings() -> None:
    df = pl.DataFrame({"x": [1, None], "pct_change": [0.2, 0.0]})
    res = run_suite(
        df,
        [NotNull(("x",)), WithinDailyBand("pct_change", band=0.07)],
        suite_name="demo",
    )
    assert res.is_valid is False          # NotNull (ERROR) làm batch fail
    assert len(res.errors) == 1
    assert len(res.warnings) == 1         # band vượt ngưỡng -> chỉ cảnh báo
    assert all(msg.startswith("demo: ") for msg in res.errors)


def test_run_suite_all_pass() -> None:
    df = pl.DataFrame({"x": [1, 2]})
    res = run_suite(df, [NotNull(("x",)), Unique(("x",))])
    assert res.is_valid and not res.errors and not res.warnings


# ─────────────────────────── suites (end-to-end) ───────────────────────────


def _valid_silver() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["FPT", "VCB"],
            "trading_date": [date(2026, 6, 15), date(2026, 6, 15)],
            "open_price": [10.0, 20.0],
            "high_price": [12.0, 22.0],
            "low_price": [9.0, 19.0],
            "close_price": [11.0, 21.0],
            "volume": [1000, 2000],
        }
    )


def test_validate_silver_ohlcv_pass_and_consistency_fail() -> None:
    assert validate_silver_ohlcv(_valid_silver()).is_valid

    broken = _valid_silver().with_columns(pl.lit(1.0).alias("high_price"))  # high < low
    res = validate_silver_ohlcv(broken)
    assert not res.is_valid
    assert any("high_price >= low_price" in e for e in res.errors)


def test_validate_silver_ohlcv_timeliness() -> None:
    res = validate_silver_ohlcv(_valid_silver(), processing_date="2026-06-16")
    assert not res.is_valid
    assert any("expected date 2026-06-16" in e for e in res.errors)


def test_validate_dim_symbol_rejects_bad_status() -> None:
    dim = pl.DataFrame(
        {
            "symbol_key": [1],
            "symbol": ["FPT"],
            "exchange_code": ["HOSE"],
            "listed_status": ["FROZEN"],
        }
    )
    res = validate_dim_symbol(dim)
    assert not res.is_valid
    assert any("listed_status" in e for e in res.errors)


def test_validate_fact_daily_market_foreign_key() -> None:
    fact = pl.DataFrame(
        {
            "symbol_key": [1, 99],
            "date_key": [20260615, 20260615],
            "trading_date": [date(2026, 6, 15), date(2026, 6, 15)],
            "open_price": [10.0, 10.0],
            "high_price": [12.0, 12.0],
            "low_price": [9.0, 9.0],
            "close_price": [11.0, 11.0],
            "volume": [100, 100],
            "rsi14": [50.0, 50.0],
            "pct_change": [0.01, 0.01],
        }
    )
    dim_symbol = pl.DataFrame({"symbol_key": [1, 2]})
    dim_date = pl.DataFrame({"date_key": [20260615]})

    res = validate_fact_daily_market(fact, dim_symbol, dim_date)
    assert not res.is_valid
    assert any("dim_symbol" in e for e in res.errors)  # symbol_key=99 orphan
