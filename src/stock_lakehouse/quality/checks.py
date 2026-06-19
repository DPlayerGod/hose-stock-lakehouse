"""Các check chất lượng dữ liệu **tái sử dụng**, viết bằng Polars.

Triết lý (theo gợi ý của mentor): mỗi check là một hàm thuần
``check(df, config) -> CheckOutcome`` — nhận đúng 2 tham số là *dataframe* và
*config*. ``config`` là một dataclass nhỏ mô tả tham số của check (cột nào,
ngưỡng bao nhiêu, ...). Nhờ vậy cùng một hàm dùng lại được cho mọi bảng, chỉ
khác config.

Các check được nhóm theo **6 chiều chất lượng dữ liệu** (xem ``result.Dimension``):

| Chiều         | Config / hàm check                                   |
|---------------|------------------------------------------------------|
| COMPLETENESS  | ``RequiredColumns`` · ``NotNull``                    |
| UNIQUENESS    | ``Unique``                                           |
| VALIDITY      | ``InRange`` · ``Positive`` · ``InSet``               |
| CONSISTENCY   | ``ColumnRelation`` (cross-field) · ``ForeignKey``    |
| ACCURACY      | ``WithinDailyBand`` (biên độ giá hợp lý)             |
| TIMELINESS    | ``MatchesDate``                                      |

Một "suite" cho một bảng chỉ là list các config; ``run_suite`` dispatch mỗi
config tới đúng hàm check rồi gộp kết quả thành ``ValidationResult``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

import polars as pl

from stock_lakehouse.quality.result import (
    CheckOutcome,
    Dimension,
    Severity,
    ValidationResult,
)


# ───────────────────────── Config objects (1 dataclass / 1 loại check) ──────


@dataclass(frozen=True)
class RequiredColumns:
    """COMPLETENESS — các cột bắt buộc phải tồn tại trong DataFrame."""

    columns: tuple[str, ...]
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class NotNull:
    """COMPLETENESS — các cột không được chứa giá trị null."""

    columns: tuple[str, ...]
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class Unique:
    """UNIQUENESS — tổ hợp ``columns`` phải duy nhất (khoá nghiệp vụ/chính)."""

    columns: tuple[str, ...]
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class InRange:
    """VALIDITY — giá trị nằm trong ``[min_value, max_value]`` (bao gồm 2 đầu)."""

    column: str
    min_value: float | None = None
    max_value: float | None = None
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class Positive:
    """VALIDITY — các cột phải > 0 (giá trị null được bỏ qua, xét ở COMPLETENESS)."""

    columns: tuple[str, ...]
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class InSet:
    """VALIDITY — giá trị phải nằm trong tập cho phép (vd enum trạng thái)."""

    column: str
    allowed: tuple[object, ...]
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class ColumnRelation:
    """CONSISTENCY — quan hệ giữa 2 cột trong cùng một dòng (cross-field).

    ``op`` ∈ {">=", "<=", ">", "<", "==", "!="}. Vd ``ColumnRelation("high_price",
    ">=", "low_price")`` đòi mọi dòng có ``high_price >= low_price``.
    """

    left: str
    op: str
    right: str
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class ForeignKey:
    """CONSISTENCY (referential) — mọi giá trị ``column`` phải tồn tại trong
    ``reference[reference_column]`` (giá trị null được bỏ qua)."""

    column: str
    reference: pl.DataFrame
    reference_column: str
    name: str | None = None
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class MatchesDate:
    """TIMELINESS — mọi dòng phải đúng kỳ kỳ vọng (vd ``trading_date == ds``)."""

    column: str
    expected: str
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class WithinDailyBand:
    """ACCURACY — biến động ngày-trên-ngày phải nằm trong biên hợp lý.

    HOSE giới hạn biên độ giá ±7%/phiên; một ``pct_change`` (phân số, vd 0.07 =
    +7%) vượt biên gợi ý dữ liệu *có thể* sai (hoặc do sự kiện doanh nghiệp).
    Vì lý do thứ hai là hợp lệ, mặc định để ``WARN`` — cảnh báo chứ không chặn.
    """

    column: str
    band: float = 0.07
    severity: Severity = Severity.WARN


# Union các loại config — "suite" của một bảng là một Iterable các giá trị này.
Check = (
    RequiredColumns
    | NotNull
    | Unique
    | InRange
    | Positive
    | InSet
    | ColumnRelation
    | ForeignKey
    | MatchesDate
    | WithinDailyBand
)


# ─────────────────────────────── Helpers ────────────────────────────────────


def _missing_columns(df: pl.DataFrame, columns: Iterable[str]) -> list[str]:
    return sorted(set(columns).difference(df.columns))


def _ok(dimension: Dimension, check: str, severity: Severity) -> CheckOutcome:
    return CheckOutcome(dimension=dimension, check=check, passed=True, severity=severity)


def _fail(
    dimension: Dimension, check: str, severity: Severity, message: str, failing_rows: int = 0
) -> CheckOutcome:
    return CheckOutcome(
        dimension=dimension,
        check=check,
        passed=False,
        severity=severity,
        message=message,
        failing_rows=failing_rows,
    )


def _count_violations(df: pl.DataFrame, violation: pl.Expr) -> int:
    """Số dòng thỏa biểu thức ``violation`` (đã loại null bằng ``& not_null``)."""
    return df.filter(violation).height


# ──────────────────────────── Check functions ──────────────────────────────
# Mỗi hàm nhận đúng (df, config) và trả về một CheckOutcome.


def check_required_columns(df: pl.DataFrame, config: RequiredColumns) -> CheckOutcome:
    missing = _missing_columns(df, config.columns)
    if missing:
        return _fail(
            Dimension.COMPLETENESS,
            "required_columns",
            config.severity,
            f"missing required columns: {missing}",
        )
    return _ok(Dimension.COMPLETENESS, "required_columns", config.severity)


def check_not_null(df: pl.DataFrame, config: NotNull) -> CheckOutcome:
    missing = _missing_columns(df, config.columns)
    if missing:
        return _fail(
            Dimension.COMPLETENESS, "not_null", config.severity, f"missing required columns: {missing}"
        )
    offending = [c for c in config.columns if df.filter(pl.col(c).is_null()).height]
    if offending:
        joined = ", ".join(offending)
        return _fail(
            Dimension.COMPLETENESS,
            "not_null",
            config.severity,
            f"{joined} contains null values",
        )
    return _ok(Dimension.COMPLETENESS, "not_null", config.severity)


def check_unique(df: pl.DataFrame, config: Unique) -> CheckOutcome:
    missing = _missing_columns(df, config.columns)
    if missing:
        return _fail(
            Dimension.UNIQUENESS, "unique", config.severity, f"missing required columns: {missing}"
        )
    duplicate_rows = df.group_by(*config.columns).len().filter(pl.col("len") > 1).height
    if duplicate_rows:
        joined = " + ".join(config.columns)
        return _fail(
            Dimension.UNIQUENESS,
            "unique",
            config.severity,
            f"duplicate {joined} rows",
            failing_rows=duplicate_rows,
        )
    return _ok(Dimension.UNIQUENESS, "unique", config.severity)


def check_in_range(df: pl.DataFrame, config: InRange) -> CheckOutcome:
    if config.column not in df.columns:
        return _fail(
            Dimension.VALIDITY, "in_range", config.severity, f"missing required columns: ['{config.column}']"
        )
    violation = pl.lit(False)
    if config.min_value is not None:
        violation = violation | (pl.col(config.column) < config.min_value)
    if config.max_value is not None:
        violation = violation | (pl.col(config.column) > config.max_value)
    bad = _count_violations(df, pl.col(config.column).is_not_null() & violation)
    if bad:
        return _fail(
            Dimension.VALIDITY,
            "in_range",
            config.severity,
            f"{config.column} outside [{config.min_value}, {config.max_value}]",
            failing_rows=bad,
        )
    return _ok(Dimension.VALIDITY, "in_range", config.severity)


def check_positive(df: pl.DataFrame, config: Positive) -> CheckOutcome:
    missing = _missing_columns(df, config.columns)
    if missing:
        return _fail(
            Dimension.VALIDITY, "positive", config.severity, f"missing required columns: {missing}"
        )
    offending = [
        c for c in config.columns if _count_violations(df, pl.col(c).is_not_null() & (pl.col(c) <= 0))
    ]
    if offending:
        joined = ", ".join(offending)
        return _fail(Dimension.VALIDITY, "positive", config.severity, f"{joined} must be > 0")
    return _ok(Dimension.VALIDITY, "positive", config.severity)


def check_in_set(df: pl.DataFrame, config: InSet) -> CheckOutcome:
    if config.column not in df.columns:
        return _fail(
            Dimension.VALIDITY, "in_set", config.severity, f"missing required columns: ['{config.column}']"
        )
    bad = _count_violations(
        df,
        pl.col(config.column).is_not_null() & ~pl.col(config.column).is_in(list(config.allowed)),
    )
    if bad:
        return _fail(
            Dimension.VALIDITY,
            "in_set",
            config.severity,
            f"{config.column} has values outside {list(config.allowed)}",
            failing_rows=bad,
        )
    return _ok(Dimension.VALIDITY, "in_set", config.severity)


_RELATION_VIOLATION: dict[str, Callable[[pl.Expr, pl.Expr], pl.Expr]] = {
    ">=": lambda left, right: left < right,
    "<=": lambda left, right: left > right,
    ">": lambda left, right: left <= right,
    "<": lambda left, right: left >= right,
    "==": lambda left, right: left != right,
    "!=": lambda left, right: left == right,
}


def check_column_relation(df: pl.DataFrame, config: ColumnRelation) -> CheckOutcome:
    missing = _missing_columns(df, (config.left, config.right))
    if missing:
        return _fail(
            Dimension.CONSISTENCY,
            "column_relation",
            config.severity,
            f"missing required columns: {missing}",
        )
    if config.op not in _RELATION_VIOLATION:
        return _fail(
            Dimension.CONSISTENCY, "column_relation", config.severity, f"unsupported operator '{config.op}'"
        )
    violated = _RELATION_VIOLATION[config.op](pl.col(config.left), pl.col(config.right))
    both_present = pl.col(config.left).is_not_null() & pl.col(config.right).is_not_null()
    bad = _count_violations(df, both_present & violated)
    if bad:
        return _fail(
            Dimension.CONSISTENCY,
            "column_relation",
            config.severity,
            f"rule violated: {config.left} {config.op} {config.right}",
            failing_rows=bad,
        )
    return _ok(Dimension.CONSISTENCY, "column_relation", config.severity)


def check_foreign_key(df: pl.DataFrame, config: ForeignKey) -> CheckOutcome:
    label = config.name or f"{config.column}->{config.reference_column}"
    if config.column not in df.columns:
        return _fail(
            Dimension.CONSISTENCY,
            "foreign_key",
            config.severity,
            f"missing required columns: ['{config.column}']",
        )
    if config.reference_column not in config.reference.columns:
        return _fail(
            Dimension.CONSISTENCY,
            "foreign_key",
            config.severity,
            f"reference missing column '{config.reference_column}'",
        )
    if df.is_empty():
        return _ok(Dimension.CONSISTENCY, "foreign_key", config.severity)
    known = set(config.reference.get_column(config.reference_column).to_list())
    orphans = (
        df.filter(pl.col(config.column).is_not_null())
        .filter(~pl.col(config.column).is_in(list(known)))
    )
    if orphans.height:
        return _fail(
            Dimension.CONSISTENCY,
            "foreign_key",
            config.severity,
            f"{label}: keys missing from reference",
            failing_rows=orphans.height,
        )
    return _ok(Dimension.CONSISTENCY, "foreign_key", config.severity)


def check_matches_date(df: pl.DataFrame, config: MatchesDate) -> CheckOutcome:
    if config.column not in df.columns:
        return _fail(
            Dimension.TIMELINESS,
            "matches_date",
            config.severity,
            f"missing required columns: ['{config.column}']",
        )
    bad = _count_violations(df, pl.col(config.column).cast(pl.Utf8) != config.expected)
    if bad:
        return _fail(
            Dimension.TIMELINESS,
            "matches_date",
            config.severity,
            f"{config.column} has rows outside expected date {config.expected}",
            failing_rows=bad,
        )
    return _ok(Dimension.TIMELINESS, "matches_date", config.severity)


def check_within_daily_band(df: pl.DataFrame, config: WithinDailyBand) -> CheckOutcome:
    if config.column not in df.columns:
        return _fail(
            Dimension.ACCURACY,
            "within_daily_band",
            config.severity,
            f"missing required columns: ['{config.column}']",
        )
    bad = _count_violations(
        df, pl.col(config.column).is_not_null() & (pl.col(config.column).abs() > config.band)
    )
    if bad:
        return _fail(
            Dimension.ACCURACY,
            "within_daily_band",
            config.severity,
            f"{config.column} exceeds ±{config.band:.0%} band on {bad} row(s)",
            failing_rows=bad,
        )
    return _ok(Dimension.ACCURACY, "within_daily_band", config.severity)


# ──────────────────────────── Dispatch + runner ────────────────────────────

_DISPATCH: dict[type, Callable[..., CheckOutcome]] = {
    RequiredColumns: check_required_columns,
    NotNull: check_not_null,
    Unique: check_unique,
    InRange: check_in_range,
    Positive: check_positive,
    InSet: check_in_set,
    ColumnRelation: check_column_relation,
    ForeignKey: check_foreign_key,
    MatchesDate: check_matches_date,
    WithinDailyBand: check_within_daily_band,
}


def run_check(df: pl.DataFrame, config: Check) -> CheckOutcome:
    """Chạy một check đơn lẻ — dispatch theo loại ``config``."""
    try:
        fn = _DISPATCH[type(config)]
    except KeyError:  # pragma: no cover - lập trình sai mới chạm tới
        raise TypeError(f"unknown check config: {type(config).__name__}") from None
    return fn(df, config)


def run_suite(df: pl.DataFrame, checks: Iterable[Check], suite_name: str = "") -> ValidationResult:
    """Chạy cả suite (list config) trên ``df`` và gộp thành ``ValidationResult``.

    - ``errors``   : message của các check FAIL ở mức ERROR (làm batch không hợp lệ).
    - ``warnings`` : message của các check FAIL ở mức WARN (không chặn pipeline).
    """
    outcomes = tuple(run_check(df, check) for check in checks)
    prefix = f"{suite_name}: " if suite_name else ""
    errors = tuple(
        f"{prefix}{o.message}" for o in outcomes if not o.passed and o.severity is Severity.ERROR
    )
    warnings = tuple(
        f"{prefix}{o.message}" for o in outcomes if not o.passed and o.severity is Severity.WARN
    )
    return ValidationResult(
        is_valid=not errors, errors=errors, warnings=warnings, outcomes=outcomes
    )
