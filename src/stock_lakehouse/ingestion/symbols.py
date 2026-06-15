"""Extract HOSE symbol metadata from VNStock."""
from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date, datetime, timezone
from uuid import uuid4

import polars as pl

from stock_lakehouse.config import SYMBOLS


SymbolFetcher = Callable[[], object]
MetadataFetcher = Callable[[str], dict[str, object]]


def fetch_vnstock_symbols() -> object:
    """Fetch all HOSE-listed symbols from vnstock."""
    from vnstock import Listing

    listing = Listing()
    return listing.all_symbols(exchange="HOSE")


def fetch_company_metadata(symbol: str) -> dict[str, object]:
    """Fetch per-symbol metadata (sector, profile, listing date) via vnstock Company.

    Mirrors the enrichment used in ``notebook/01_api_feasibility_check_batch.ipynb``:
    the listing only carries ``symbol``/``company_name``, so the descriptive
    columns must come from the Company endpoint. Returns an empty dict on failure.
    """
    from vnstock.api.company import Company

    company = Company(symbol=symbol, source="VCI")
    merged: dict[str, object] = {}
    for method in ("overview", "profile"):
        if not hasattr(company, method):
            continue
        try:
            row = _first_row(getattr(company, method)())
        except (Exception, SystemExit):  # vnstock raises SystemExit on rate limit
            continue
        merged.update({key: value for key, value in row.items() if value is not None})

    return {
        "company_name": merged.get("organ_name") or merged.get("organ_short_name"),
        "sector_name": merged.get("sector") or merged.get("icb_name3") or merged.get("icb_name4"),
        "company_profile": merged.get("company_profile") or merged.get("profile"),
        "listing_date": _to_date(merged.get("listing_date")),
    }


def _to_date(value: object) -> date | None:
    """Normalise a date-like value (e.g. ``'2006-12-13T00:00:00'``) to ``date``."""
    if isinstance(value, datetime):
        return value.date()
    if value is None or isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        return None


def extract_hose_symbols(
    fetcher: SymbolFetcher = fetch_vnstock_symbols,
    metadata_fetcher: MetadataFetcher | None = fetch_company_metadata,
    symbols: Sequence[str] | None = SYMBOLS,
    batch_id: str | None = None,
) -> pl.DataFrame:
    """Extract and normalise HOSE symbol metadata into a Polars DataFrame.

    The returned DataFrame always contains at least the ``symbol`` column
    and pipeline metadata columns (``source``, ``batch_id``, ``ingested_at``).
    When ``metadata_fetcher`` is set, each symbol is enriched with Company
    metadata so descriptive columns are populated instead of all-null.

    ``symbols`` restricts the listing to the project's tracked tickers
    (default :data:`stock_lakehouse.config.SYMBOLS`) so per-symbol Company
    enrichment stays within vnstock's free-tier rate limit. Pass ``None`` to
    keep every HOSE symbol.
    """
    batch_id = batch_id or uuid4().hex
    raw = fetcher()
    df = _to_polars(raw)
    if df.is_empty():
        return _empty_symbols_frame()

    df = _normalise_columns(df)
    if "symbol" not in df.columns:
        raise ValueError("Symbol response missing required column: symbol")

    df = df.with_columns(pl.col("symbol").cast(pl.Utf8).str.to_uppercase())
    if symbols is not None:
        df = df.filter(pl.col("symbol").is_in([symbol.upper() for symbol in symbols]))
    if metadata_fetcher is not None:
        df = _enrich_with_metadata(df, metadata_fetcher)

    ingested_at = datetime.now(timezone.utc)
    return df.with_columns(
        pl.lit("VCI").alias("source"),
        pl.lit(batch_id).alias("batch_id"),
        pl.lit(ingested_at).alias("ingested_at"),
    )


def _enrich_with_metadata(df: pl.DataFrame, metadata_fetcher: MetadataFetcher) -> pl.DataFrame:
    metadata = pl.DataFrame(
        [{"symbol": symbol, **metadata_fetcher(symbol)} for symbol in df.get_column("symbol")]
    )
    if metadata.is_empty():
        return df
    overlap = [column for column in metadata.columns if column != "symbol" and column in df.columns]
    return df.drop(overlap).join(metadata, on="symbol", how="left")


def _first_row(result: object) -> dict[str, object]:
    if result is None:
        return {}
    if isinstance(result, dict):
        return result
    df = result if isinstance(result, pl.DataFrame) else pl.from_pandas(result)  # type: ignore[arg-type]
    return df.head(1).to_dicts()[0] if not df.is_empty() else {}


def _empty_symbols_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "symbol": pl.Utf8,
            "company_name": pl.Utf8,
            "sector_name": pl.Utf8,
            "company_profile": pl.Utf8,
            "listing_date": pl.Date,
            "exchange_code": pl.Utf8,
            "listed_status": pl.Utf8,
            "source": pl.Utf8,
            "batch_id": pl.Utf8,
            "ingested_at": pl.Datetime(time_zone="UTC"),
        }
    )


_COLUMN_ALIASES = {
    "ticker": "symbol",
    "organ_name": "company_name",
    "organ_short_name": "company_name",
    "icb_name3": "sector_name",
    "icb_name4": "sector_name",
    "com_group_code": "exchange_code",
}


def _normalise_columns(df: pl.DataFrame) -> pl.DataFrame:
    normalised = {name: name.strip().lower().replace(" ", "_") for name in df.columns}
    df = df.rename(normalised)
    aliases = {
        source: target
        for source, target in _COLUMN_ALIASES.items()
        if source in df.columns and target not in df.columns
    }
    return df.rename(aliases)


def _to_polars(raw: object) -> pl.DataFrame:
    if isinstance(raw, pl.DataFrame):
        return raw
    return pl.from_pandas(raw)  # type: ignore[arg-type]
