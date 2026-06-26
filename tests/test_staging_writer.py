"""Tests for stock_lakehouse.staging.writer and staging.quarantine modules.

Unit tests run without any infrastructure.
Integration tests (marked with @pytest.mark.integration) require MinIO running via Docker Compose.
"""
from __future__ import annotations

import io
import json
import os
from contextlib import contextmanager
from unittest.mock import MagicMock
from pathlib import Path

import polars as pl
import pytest

from stock_lakehouse.config import MinioConfig
from stock_lakehouse.quality import ValidationResult
from stock_lakehouse.staging.quarantine import write_quarantine
from stock_lakehouse.staging.writer import (
    StagingPath,
    StagingPathBuilder,
    read_staging_parquet,
    write_staging_parquet,
)


# ---------------------------------------------------------------------------
# Unit tests — StagingPath / StagingPathBuilder
# ---------------------------------------------------------------------------

class TestStagingPathBuilder:
    def test_ohlcv_default_bucket_and_root(self) -> None:
        builder = StagingPathBuilder()
        result = builder.ohlcv("2026-06-14", batch_id="b1")
        assert result == (
            "s3://lakehouse/staging/ohlcv/"
            "processing_date=2026-06-14/batch_id=b1/part-00000.parquet"
        )

    def test_ohlcv_custom_bucket_and_root(self) -> None:
        builder = StagingPathBuilder(bucket="my-bucket", root="/raw/")
        result = builder.ohlcv("2026-01-01", batch_id="xyz")
        assert result == (
            "s3://my-bucket/raw/ohlcv/"
            "processing_date=2026-01-01/batch_id=xyz/part-00000.parquet"
        )

    def test_build_custom_staging_path(self) -> None:
        builder = StagingPathBuilder()
        path = StagingPath(
            domain="custom_domain",
            processing_date="2026-03-15",
            batch_id="batch-99",
            filename="data.parquet",
        )
        result = builder.build(path)
        assert result == (
            "s3://lakehouse/staging/custom_domain/"
            "processing_date=2026-03-15/batch_id=batch-99/data.parquet"
        )

    def test_root_stripping_slashes(self) -> None:
        builder = StagingPathBuilder(root="///staging///")
        result = builder.ohlcv("2026-06-14", batch_id="b1")
        assert "/staging/" in result
        assert "///staging///" not in result


# ---------------------------------------------------------------------------
# Unit tests — local file roundtrip
# ---------------------------------------------------------------------------

class TestLocalParquetRoundtrip:
    def test_write_and_read_local_parquet(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "symbol": ["FPT", "VNM"],
            "volume": [100, 200],
        })
        uri = str(tmp_path / "test_output" / "part.parquet")

        returned = write_staging_parquet(df, uri)
        assert Path(returned).exists()

        actual = read_staging_parquet(returned)
        assert actual.to_dicts() == df.to_dicts()

    def test_write_creates_parent_directories(self, tmp_path: Path) -> None:
        df = pl.DataFrame({"a": [1]})
        uri = str(tmp_path / "deep" / "nested" / "dir" / "file.parquet")
        write_staging_parquet(df, uri)
        assert Path(uri).exists()

    def test_roundtrip_preserves_types(self, tmp_path: Path) -> None:
        """Ensure numeric and date types survive a Parquet roundtrip."""
        df = pl.DataFrame({
            "symbol": ["FPT"],
            "price": [25.5],
            "volume": [1000],
        }).cast({"price": pl.Float64, "volume": pl.Int64})

        uri = str(tmp_path / "typed.parquet")
        write_staging_parquet(df, uri)
        actual = read_staging_parquet(uri)

        assert actual.dtypes == df.dtypes
        assert actual.to_dicts() == df.to_dicts()

    def test_write_empty_dataframe(self, tmp_path: Path) -> None:
        df = pl.DataFrame({"symbol": [], "volume": []}).cast(
            {"symbol": pl.Utf8, "volume": pl.Int64}
        )
        uri = str(tmp_path / "empty.parquet")
        write_staging_parquet(df, uri)
        actual = read_staging_parquet(uri)
        assert actual.height == 0
        assert actual.columns == ["symbol", "volume"]


# ---------------------------------------------------------------------------
# Integration tests — S3 (MinIO) roundtrip
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestS3ParquetRoundtrip:
    """Require a running MinIO instance (docker compose up -d)."""

    @pytest.fixture(autouse=True)
    def _check_minio(self) -> None:
        """Skip tests if MinIO is not reachable."""
        import urllib.request

        config = MinioConfig()
        try:
            urllib.request.urlopen(f"{config.endpoint}/minio/health/live", timeout=3)
        except Exception:
            pytest.skip("MinIO not reachable — run `docker compose up -d`")

    @pytest.fixture
    def s3_uri(self) -> str:
        import uuid

        batch_id = uuid.uuid4().hex[:8]
        return (
            f"s3://lakehouse/staging/test_staging_writer/"
            f"processing_date=2026-06-14/batch_id={batch_id}/part-00000.parquet"
        )

    def test_write_and_read_s3(self, s3_uri: str) -> None:
        df = pl.DataFrame({
            "symbol": ["FPT", "VNM"],
            "volume": [100, 200],
        })
        config = MinioConfig()

        returned = write_staging_parquet(df, s3_uri, config=config)
        assert returned == s3_uri

        actual = read_staging_parquet(returned, config=config)
        assert actual.to_dicts() == df.to_dicts()

    def test_write_s3_preserves_types(self, s3_uri: str) -> None:
        df = pl.DataFrame({
            "symbol": ["FPT"],
            "price": [25.5],
            "volume": [1000],
        }).cast({"price": pl.Float64, "volume": pl.Int64})
        config = MinioConfig()

        write_staging_parquet(df, s3_uri, config=config)
        actual = read_staging_parquet(s3_uri, config=config)

        assert actual.dtypes == df.dtypes
        assert actual.to_dicts() == df.to_dicts()

    def test_staging_path_builder_s3_roundtrip(self) -> None:
        """End-to-end: build path → write → read."""
        import uuid

        builder = StagingPathBuilder()
        batch_id = f"test-{uuid.uuid4().hex[:8]}"
        uri = builder.ohlcv("2026-06-14", batch_id=batch_id)

        df = pl.DataFrame({
            "symbol": ["FPT"],
            "open": [10.0],
            "close": [11.0],
            "volume": [500],
        })
        config = MinioConfig()

        write_staging_parquet(df, uri, config=config)
        actual = read_staging_parquet(uri, config=config)
        assert actual.to_dicts() == df.to_dicts()


# ---------------------------------------------------------------------------
# Shared fixture — in-memory fake S3 for quarantine unit tests
# ---------------------------------------------------------------------------

@pytest.fixture
def quarantine_fake_s3(monkeypatch):
    """Replace s3fs in quarantine module with an in-memory filesystem.

    Returns a dict mapping URI → buffer (BytesIO or StringIO) for assertions.
    """
    buffers: dict = {}

    class _FakeFS:
        @contextmanager
        def open(self, uri, mode="rb"):
            buf = io.BytesIO() if "b" in mode else io.StringIO()
            buffers[uri] = buf
            yield buf

    monkeypatch.setattr(
        "stock_lakehouse.staging.quarantine.s3fs.S3FileSystem",
        lambda **_: _FakeFS(),
    )
    return buffers


# ---------------------------------------------------------------------------
# Unit tests — write_quarantine URI and file content
# ---------------------------------------------------------------------------

class TestWriteQuarantine:
    def test_returns_correct_base_uri(self, quarantine_fake_s3) -> None:
        df = pl.DataFrame({"symbol": ["FPT"], "close": [25.0]})
        result = write_quarantine(
            df, ("col x is null",),
            domain="silver_ohlcv",
            processing_date="2026-06-16",
            batch_id="abc123",
        )
        assert result == (
            "s3://lakehouse/quarantine/silver_ohlcv"
            "/processing_date=2026-06-16/batch_id=abc123"
        )

    def test_custom_bucket(self, quarantine_fake_s3) -> None:
        df = pl.DataFrame({"a": [1]})
        result = write_quarantine(
            df, (),
            domain="gold_fact",
            processing_date="2026-01-01",
            batch_id="b1",
            bucket="my-lake",
        )
        assert result.startswith("s3://my-lake/quarantine/")

    def test_writes_parquet_roundtrip(self, quarantine_fake_s3) -> None:
        df = pl.DataFrame({"symbol": ["FPT", "VNM"], "close": [25.0, 80.0]})
        base = write_quarantine(
            df, ("some error",),
            domain="silver_ohlcv",
            processing_date="2026-06-16",
            batch_id="test-b",
        )
        buf = quarantine_fake_s3[f"{base}/data.parquet"]
        buf.seek(0)
        assert pl.read_parquet(buf).to_dicts() == df.to_dicts()

    def test_writes_errors_json_content(self, quarantine_fake_s3) -> None:
        df = pl.DataFrame({"symbol": ["FPT"]})
        errors = ("col x is null", "duplicate symbol+date")
        base = write_quarantine(
            df, errors,
            domain="staging_ohlcv",
            processing_date="2026-06-16",
            batch_id="batch-99",
        )
        content = json.loads(quarantine_fake_s3[f"{base}/errors.json"].getvalue())
        assert content["errors"] == list(errors)
        assert content["domain"] == "staging_ohlcv"
        assert content["processing_date"] == "2026-06-16"
        assert content["batch_id"] == "batch-99"

    def test_empty_errors_written_as_empty_list(self, quarantine_fake_s3) -> None:
        df = pl.DataFrame({"a": [1]})
        base = write_quarantine(
            df, (),
            domain="test",
            processing_date="2026-01-01",
            batch_id="b1",
        )
        content = json.loads(quarantine_fake_s3[f"{base}/errors.json"].getvalue())
        assert content["errors"] == []


# ---------------------------------------------------------------------------
# Unit tests — ValidationResult.quarantine_and_raise
# ---------------------------------------------------------------------------

class TestValidationResultQuarantineAndRaise:
    @pytest.fixture
    def df(self) -> pl.DataFrame:
        return pl.DataFrame({"symbol": ["FPT"], "close": [25.0]})

    @pytest.fixture
    def mock_write(self, monkeypatch):
        monkeypatch.setattr(
            "stock_lakehouse.staging.quarantine.write_quarantine",
            MagicMock(return_value="s3://lakehouse/quarantine/test/processing_date=2026-06-16/batch_id=b1"),
        )

    def test_no_op_when_valid(self, df: pl.DataFrame) -> None:
        ValidationResult(True, ()).quarantine_and_raise(
            df, domain="test", processing_date="2026-06-16", batch_id="b1"
        )

    def test_raises_when_invalid(self, df: pl.DataFrame, mock_write) -> None:
        with pytest.raises(ValueError, match="col x is null"):
            ValidationResult(False, ("col x is null",)).quarantine_and_raise(
                df, domain="test", processing_date="2026-06-16", batch_id="b1"
            )

    def test_error_message_contains_quarantine_uri(self, df: pl.DataFrame, mock_write) -> None:
        with pytest.raises(ValueError) as exc_info:
            ValidationResult(False, ("error",)).quarantine_and_raise(
                df, domain="test", processing_date="2026-06-16", batch_id="b1"
            )
        msg = str(exc_info.value)
        assert "quarantined=" in msg
        assert "s3://lakehouse/quarantine/test" in msg

    def test_all_errors_present_in_message(self, df: pl.DataFrame, mock_write) -> None:
        with pytest.raises(ValueError) as exc_info:
            ValidationResult(False, ("err_a", "err_b", "err_c")).quarantine_and_raise(
                df, domain="test", processing_date="2026-06-16", batch_id="b1"
            )
        msg = str(exc_info.value)
        assert "err_a" in msg
        assert "err_b" in msg
        assert "err_c" in msg


# ---------------------------------------------------------------------------
# Integration tests — write_quarantine against real MinIO
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestWriteQuarantineS3:
    """Require a running MinIO instance (docker compose up -d)."""

    @pytest.fixture(autouse=True)
    def _check_minio(self) -> None:
        import urllib.request

        config = MinioConfig()
        try:
            urllib.request.urlopen(f"{config.endpoint}/minio/health/live", timeout=3)
        except Exception:
            pytest.skip("MinIO not reachable — run `docker compose up -d`")

    def test_writes_data_and_errors_to_s3(self) -> None:
        import uuid
        import s3fs as _s3fs

        config = MinioConfig()
        df = pl.DataFrame({"symbol": ["FPT", "VNM"], "close": [25.0, 80.0]})
        errors = ("col x is null", "duplicate rows")
        batch_id = f"test-{uuid.uuid4().hex[:8]}"

        base = write_quarantine(
            df, errors,
            domain="test_quarantine",
            processing_date="2026-06-16",
            batch_id=batch_id,
            config=config,
        )

        assert "quarantine/test_quarantine" in base
        assert "processing_date=2026-06-16" in base
        assert batch_id in base

        fs = _s3fs.S3FileSystem(
            key=config.access_key,
            secret=config.secret_key,
            client_kwargs={"endpoint_url": config.endpoint, "region_name": config.region},
        )

        with fs.open(f"{base}/data.parquet", "rb") as f:
            actual_df = pl.read_parquet(io.BytesIO(f.read()))  # type: ignore[arg-type]
        assert actual_df.to_dicts() == df.to_dicts()

        with fs.open(f"{base}/errors.json", "r") as f:
            meta = json.load(f)
        assert meta["errors"] == list(errors)
        assert meta["domain"] == "test_quarantine"
        assert meta["batch_id"] == batch_id

