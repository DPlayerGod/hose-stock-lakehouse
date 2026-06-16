from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import polars as pl

from stock_lakehouse.quality.ohlcv import ValidationResult


@dataclass(frozen=True)
class NotNullExpectation:
    column: str


@dataclass(frozen=True)
class UniqueColumnsExpectation:
    columns: tuple[str, ...]


@dataclass(frozen=True)
class BetweenExpectation:
    column: str
    min_value: float | None = None
    max_value: float | None = None


DataFrameExpectation = NotNullExpectation | UniqueColumnsExpectation | BetweenExpectation


def validate_polars_expectations(
    df: pl.DataFrame,
    expectations: Iterable[DataFrameExpectation],
    suite_name: str,
) -> ValidationResult:
    errors: list[str] = []

    for expectation in expectations:
        if isinstance(expectation, NotNullExpectation):
            if expectation.column not in df.columns:
                errors.append(f"missing required columns: ['{expectation.column}']")
            elif df.filter(pl.col(expectation.column).is_null()).height:
                errors.append(f"{expectation.column} contains null values")
        elif isinstance(expectation, UniqueColumnsExpectation):
            missing = sorted(set(expectation.columns).difference(df.columns))
            if missing:
                errors.append(f"missing required columns: {missing}")
                continue
            duplicate_count = df.group_by(*expectation.columns).len().filter(pl.col("len") > 1).height
            if duplicate_count:
                joined = " + ".join(expectation.columns)
                errors.append(f"{suite_name} contains duplicate {joined} rows")
        elif isinstance(expectation, BetweenExpectation):
            if expectation.column not in df.columns:
                errors.append(f"missing required columns: ['{expectation.column}']")
                continue
            rule = pl.lit(False)
            if expectation.min_value is not None:
                rule = rule | (pl.col(expectation.column) < expectation.min_value)
            if expectation.max_value is not None:
                rule = rule | (pl.col(expectation.column) > expectation.max_value)
            if df.filter(pl.col(expectation.column).is_not_null() & rule).height:
                errors.append(
                    f"{expectation.column} is outside "
                    f"{expectation.min_value}..{expectation.max_value}"
                )

    return ValidationResult(not errors, tuple(errors))
