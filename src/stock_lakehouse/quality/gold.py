from __future__ import annotations

import polars as pl

from stock_lakehouse.quality.great_expectations import (
    BetweenExpectation,
    NotNullExpectation,
    UniqueColumnsExpectation,
    validate_polars_expectations,
)
from stock_lakehouse.quality.ohlcv import ValidationResult


def validate_dim_date(df: pl.DataFrame) -> ValidationResult:
    return validate_polars_expectations(
        df,
        (
            NotNullExpectation("date_key"),
            NotNullExpectation("full_date"),
            UniqueColumnsExpectation(("date_key",)),
            UniqueColumnsExpectation(("full_date",)),
        ),
        suite_name="dim_date",
    )


def validate_dim_symbol(df: pl.DataFrame) -> ValidationResult:
    return validate_polars_expectations(
        df,
        (
            NotNullExpectation("symbol_key"),
            NotNullExpectation("symbol"),
            NotNullExpectation("exchange_code"),
            NotNullExpectation("listed_status"),
            UniqueColumnsExpectation(("symbol_key",)),
            UniqueColumnsExpectation(("symbol",)),
        ),
        suite_name="dim_symbol",
    )


def validate_fact_daily_market(
    fact_df: pl.DataFrame,
    dim_symbol_df: pl.DataFrame,
    dim_date_df: pl.DataFrame,
) -> ValidationResult:
    result = validate_polars_expectations(
        fact_df,
        (
            NotNullExpectation("symbol_key"),
            NotNullExpectation("date_key"),
            NotNullExpectation("trading_date"),
            UniqueColumnsExpectation(("symbol_key", "date_key")),
            BetweenExpectation("rsi14", min_value=0, max_value=100),
        ),
        suite_name="fact_hose_daily_market",
    )
    errors = list(result.errors)

    if not fact_df.is_empty():
        missing_symbol_keys = set(fact_df.get_column("symbol_key").to_list()).difference(
            set(dim_symbol_df.get_column("symbol_key").to_list())
        )
        if missing_symbol_keys:
            errors.append("fact_hose_daily_market contains symbol_key values missing from dim_symbol")

        missing_date_keys = set(fact_df.get_column("date_key").to_list()).difference(
            set(dim_date_df.get_column("date_key").to_list())
        )
        if missing_date_keys:
            errors.append("fact_hose_daily_market contains date_key values missing from dim_date")

    return ValidationResult(not errors, tuple(errors))
