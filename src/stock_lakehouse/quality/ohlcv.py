from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    errors: tuple[str, ...]

    def raise_for_errors(self) -> None:
        if not self.is_valid:
            raise ValueError("; ".join(self.errors))

    def quarantine_and_raise(
        self,
        df: pl.DataFrame,
        *,
        domain: str,
        processing_date: str,
        batch_id: str,
        config=None,
    ) -> None:
        """Write failed batch to quarantine then raise. No-op when valid."""
        if not self.is_valid:
            from stock_lakehouse.staging.quarantine import write_quarantine
            from stock_lakehouse.config import MinioConfig
            uri = write_quarantine(
                df,
                self.errors,
                domain=domain,
                processing_date=processing_date,
                batch_id=batch_id,
                config=config or MinioConfig(),
            )
            raise ValueError(f"[quarantined={uri}] " + "; ".join(self.errors))


def validate_bronze_ohlcv(df: pl.DataFrame) -> ValidationResult:
    errors = _required_columns(
        df,
        ("symbol", "time", "open", "high", "low", "close", "volume", "source", "batch_id", "ingested_at"),
    )
    errors.extend(_no_nulls(df, ("symbol", "time", "source", "batch_id", "ingested_at")))
    return ValidationResult(not errors, tuple(errors))


def validate_silver_ohlcv(df: pl.DataFrame, processing_date: str | None = None) -> ValidationResult:
    errors = _required_columns(
        df,
        ("symbol", "trading_date", "open_price", "high_price", "low_price", "close_price", "volume"),
    )
    if errors:
        return ValidationResult(False, tuple(errors))

    errors.extend(_no_nulls(df, ("symbol", "trading_date")))
    duplicate_count = df.group_by("symbol", "trading_date").len().filter(pl.col("len") > 1).height
    if duplicate_count:
        errors.append("silver OHLCV contains duplicate symbol + trading_date rows")

    invalid_rules = df.filter(
        (pl.col("open_price") <= 0)
        | (pl.col("high_price") <= 0)
        | (pl.col("low_price") <= 0)
        | (pl.col("close_price") <= 0)
        | (pl.col("high_price") < pl.col("low_price"))
        | (pl.col("high_price") < pl.col("open_price"))
        | (pl.col("high_price") < pl.col("close_price"))
        | (pl.col("low_price") > pl.col("open_price"))
        | (pl.col("low_price") > pl.col("close_price"))
        | (pl.col("volume") < 0)
    )
    if invalid_rules.height:
        errors.append("silver OHLCV violates price or volume rules")

    if processing_date is not None:
        invalid_dates = df.filter(pl.col("trading_date").cast(pl.Utf8) != processing_date)
        if invalid_dates.height:
            errors.append(f"silver OHLCV contains rows outside processing_date={processing_date}")

    return ValidationResult(not errors, tuple(errors))


def _required_columns(df: pl.DataFrame, columns: tuple[str, ...]) -> list[str]:
    missing = sorted(set(columns).difference(df.columns))
    return [f"missing required columns: {missing}"] if missing else []


def _no_nulls(df: pl.DataFrame, columns: tuple[str, ...]) -> list[str]:
    errors: list[str] = []
    for column in columns:
        if column in df.columns and df.filter(pl.col(column).is_null()).height:
            errors.append(f"{column} contains null values")
    return errors

