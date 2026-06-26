"""Tests for stock_lakehouse.iceberg.writer module.

Unit tests run without any infrastructure (mocking the Catalog/Table).
Integration tests (marked with @pytest.mark.integration) require Docker Compose services.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import cast
from unittest.mock import MagicMock

import polars as pl
import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    LongType,
    NestedField,
    StringType,
)

from stock_lakehouse.iceberg.writer import ensure_table, write_dataframe


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIMPLE_SCHEMA = Schema(
    NestedField(field_id=1, name="symbol", field_type=StringType(), required=True),  # type: ignore[call-arg]
    NestedField(field_id=2, name="value", field_type=LongType(), required=False),  # type: ignore[call-arg]
)


def _sample_df() -> pl.DataFrame:
    return pl.DataFrame({"symbol": ["FPT", "VNM"], "value": [100, 200]})


def _scan_to_dataframe(table: Table) -> pl.DataFrame:
    return cast(pl.DataFrame, pl.from_arrow(table.scan().to_arrow()))


# ---------------------------------------------------------------------------
# Unit tests — ensure_table
# ---------------------------------------------------------------------------

class TestEnsureTable:
    def test_returns_existing_table(self) -> None:
        """If the table already exists, just load and return it."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table

        result = ensure_table(mock_catalog, "lakehouse.bronze_ohlcv", SIMPLE_SCHEMA)

        assert result is mock_table
        mock_catalog.load_table.assert_called_once_with("lakehouse.bronze_ohlcv")
        mock_catalog.create_table.assert_not_called()

    def test_creates_table_when_not_found(self) -> None:
        """If load_table raises, creates namespace and table."""
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("NoSuchTableException")
        mock_new_table = MagicMock()
        mock_catalog.create_table.return_value = mock_new_table

        result = ensure_table(mock_catalog, "lakehouse.bronze_ohlcv", SIMPLE_SCHEMA)

        assert result is mock_new_table
        mock_catalog.create_namespace.assert_called_once_with("lakehouse")
        mock_catalog.create_table.assert_called_once_with(
            "lakehouse.bronze_ohlcv", schema=SIMPLE_SCHEMA
        )

    def test_creates_table_with_partition_spec(self) -> None:
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("not found")
        mock_partition_spec = MagicMock()

        ensure_table(
            mock_catalog,
            "lakehouse.bronze_ohlcv",
            SIMPLE_SCHEMA,
            partition_spec=cast(PartitionSpec, mock_partition_spec),
        )

        mock_catalog.create_table.assert_called_once_with(
            "lakehouse.bronze_ohlcv",
            schema=SIMPLE_SCHEMA,
            partition_spec=mock_partition_spec,
        )

    def test_namespace_already_exists_does_not_fail(self) -> None:
        """If namespace already exists, the exception is swallowed."""
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("not found")
        mock_catalog.create_namespace.side_effect = Exception("NamespaceAlreadyExists")
        mock_catalog.create_table.return_value = MagicMock()

        # Should not raise
        result = ensure_table(mock_catalog, "lakehouse.bronze_ohlcv", SIMPLE_SCHEMA)
        assert result is not None

    def test_top_level_table_no_namespace(self) -> None:
        """An identifier without a dot means no namespace to create."""
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("not found")
        mock_catalog.create_table.return_value = MagicMock()

        ensure_table(mock_catalog, "simple_table", SIMPLE_SCHEMA)

        mock_catalog.create_namespace.assert_not_called()


# ---------------------------------------------------------------------------
# Unit tests — write_dataframe
# ---------------------------------------------------------------------------

class TestWriteDataframe:
    @pytest.fixture
    def mock_table(self) -> MagicMock:
        """Create a mock Table that returns SIMPLE_SCHEMA from .schema()."""
        table = MagicMock()
        table.schema.return_value = SIMPLE_SCHEMA
        return table

    def test_append_mode(self, mock_table: MagicMock) -> None:
        df = _sample_df()

        write_dataframe(mock_table, df, mode="append")

        mock_table.append.assert_called_once()
        mock_table.overwrite.assert_not_called()
        # Verify it received a PyArrow table
        arrow_arg = mock_table.append.call_args[0][0]
        assert isinstance(arrow_arg, pa.Table)

    def test_overwrite_mode(self, mock_table: MagicMock) -> None:
        df = _sample_df()

        write_dataframe(mock_table, df, mode="overwrite")

        mock_table.overwrite.assert_called_once()
        mock_table.append.assert_not_called()

    def test_default_mode_is_append(self, mock_table: MagicMock) -> None:
        df = _sample_df()

        write_dataframe(mock_table, df)

        mock_table.append.assert_called_once()

    def test_invalid_mode_raises(self, mock_table: MagicMock) -> None:
        df = _sample_df()

        with pytest.raises(ValueError, match="Unsupported write mode"):
            write_dataframe(mock_table, df, mode="delete")  # type: ignore[arg-type]

    def test_arrow_conversion_preserves_data(self, mock_table: MagicMock) -> None:
        df = _sample_df()

        write_dataframe(mock_table, df, mode="append")

        arrow_table = mock_table.append.call_args[0][0]
        roundtrip = cast(pl.DataFrame, pl.from_arrow(arrow_table))
        assert roundtrip.to_dicts() == df.to_dicts()


# ---------------------------------------------------------------------------
# Integration tests — Iceberg REST Catalog + MinIO
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestIcebergWriterIntegration:
    """End-to-end tests against the real Iceberg REST catalog and MinIO."""

    @pytest.fixture(autouse=True)
    def _check_services(self) -> None:
        """Skip if Iceberg REST catalog is not reachable."""
        import urllib.request

        from stock_lakehouse.config import IcebergConfig

        config = IcebergConfig()
        try:
            urllib.request.urlopen(f"{config.uri}/v1/config", timeout=3)
        except Exception:
            pytest.skip("Iceberg REST catalog not reachable — run `docker compose up -d`")

    @pytest.fixture
    def catalog(self):
        from stock_lakehouse.iceberg.catalog import load_lakehouse_catalog

        return load_lakehouse_catalog()

    @pytest.fixture
    def test_table_id(self) -> str:
        import uuid

        return f"lakehouse.test_writer_{uuid.uuid4().hex[:8]}"

    def test_ensure_table_creates_new(self, catalog, test_table_id: str) -> None:
        table = ensure_table(catalog, test_table_id, SIMPLE_SCHEMA)
        assert table is not None
        # Should be loadable now
        loaded = catalog.load_table(test_table_id)
        assert loaded is not None

    def test_write_and_scan(self, catalog, test_table_id: str) -> None:
        """Write a DataFrame and scan it back via Iceberg."""
        table = ensure_table(catalog, test_table_id, SIMPLE_SCHEMA)

        df = _sample_df()
        write_dataframe(table, df, mode="append")

        # Read back via Iceberg scan
        result = _scan_to_dataframe(table)
        assert result.height == 2
        assert set(result.get_column("symbol").to_list()) == {"FPT", "VNM"}

    def test_append_adds_rows(self, catalog, test_table_id: str) -> None:
        table = ensure_table(catalog, test_table_id, SIMPLE_SCHEMA)

        df1 = pl.DataFrame({"symbol": ["FPT"], "value": [100]})
        df2 = pl.DataFrame({"symbol": ["VNM"], "value": [200]})

        write_dataframe(table, df1, mode="append")
        write_dataframe(table, df2, mode="append")

        result = _scan_to_dataframe(table)
        assert result.height == 2

    def test_overwrite_replaces_data(self, catalog, test_table_id: str) -> None:
        table = ensure_table(catalog, test_table_id, SIMPLE_SCHEMA)

        df1 = pl.DataFrame({"symbol": ["FPT"], "value": [100]})
        df2 = pl.DataFrame({"symbol": ["VNM"], "value": [200]})

        write_dataframe(table, df1, mode="append")
        write_dataframe(table, df2, mode="overwrite")

        result = _scan_to_dataframe(table)
        assert result.height == 1
        assert result.get_column("symbol").to_list() == ["VNM"]

    def test_ohlcv_schema_write(self, catalog) -> None:
        """Test with the real BRONZE_OHLCV_SCHEMA from tables.py."""
        import uuid

        from stock_lakehouse.iceberg.tables import BRONZE_OHLCV_SCHEMA

        table_id = f"lakehouse.test_ohlcv_{uuid.uuid4().hex[:8]}"
        table = ensure_table(catalog, table_id, BRONZE_OHLCV_SCHEMA)

        df = pl.DataFrame({
            "symbol": ["FPT"],
            "time": [date(2026, 6, 14)],
            "open": [10.0],
            "high": [12.0],
            "low": [9.0],
            "close": [11.0],
            "volume": [1000],
            "source": ["VCI"],
            "batch_id": ["batch-1"],
            "ingested_at": [datetime.now(timezone.utc)],
            "processing_date": [date(2026, 6, 14)],
        }).cast({
            "volume": pl.Int64,
            "ingested_at": pl.Datetime(time_zone="UTC"),
        })

        write_dataframe(table, df, mode="append")

        result = _scan_to_dataframe(table)
        assert result.height == 1
        assert result.get_column("symbol").to_list() == ["FPT"]
