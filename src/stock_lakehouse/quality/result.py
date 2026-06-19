"""Kiểu dữ liệu chung cho tầng data-quality.

Toàn bộ check trong project được tổ chức quanh **6 chiều chất lượng dữ liệu**
(DAMA DMBOK / ISO 8000) — xem ``Dimension`` bên dưới. Mỗi check trả về một
``CheckOutcome``; gộp nhiều outcome lại thành ``ValidationResult`` để pipeline
quyết định dừng (fail-fast) hay chỉ ghi cảnh báo.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import polars as pl


class Dimension(str, Enum):
    """6 chiều chất lượng dữ liệu.

    - COMPLETENESS — *đầy đủ*: giá trị/cột bắt buộc có mặt (không thiếu, không null).
    - UNIQUENESS   — *duy nhất*: không trùng lặp ở khoá nghiệp vụ / khoá chính.
    - VALIDITY     — *hợp lệ*: giá trị đúng kiểu/miền/khoảng/dấu/tập cho phép.
    - CONSISTENCY  — *nhất quán*: các cột trong 1 dòng và các bảng khớp logic
      với nhau (vd ``high >= low``, FK tồn tại trong dim).
    - ACCURACY     — *chính xác*: phản ánh đúng thực tế / bất biến nghiệp vụ
      (vd biên độ giá HOSE ±7%). Khó đo nội tại nên thường dùng kiểm tra
      hợp-lý (plausibility) hoặc đối chiếu nguồn tin cậy.
    - TIMELINESS   — *kịp thời*: dữ liệu thuộc đúng kỳ kỳ vọng & còn mới
      (vd mọi dòng đúng ngày D, độ trễ trong ngưỡng).
    """

    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    TIMELINESS = "timeliness"


class Severity(str, Enum):
    """Mức nghiêm trọng của một check.

    - ERROR: vi phạm ⇒ dữ liệu không hợp lệ ⇒ pipeline dừng (fail-fast).
    - WARN : vi phạm ⇒ chỉ ghi nhận cảnh báo, không dừng pipeline. Dùng cho
      các chiều "mờ" như ACCURACY (vd biên độ giá có thể vượt ngưỡng vì sự
      kiện doanh nghiệp hợp lệ).
    """

    ERROR = "error"
    WARN = "warn"


@dataclass(frozen=True)
class CheckOutcome:
    """Kết quả của *một* check trên *một* DataFrame."""

    dimension: Dimension
    check: str
    passed: bool
    severity: Severity = Severity.ERROR
    message: str | None = None
    failing_rows: int = 0

    @property
    def is_blocking_failure(self) -> bool:
        """Có phải lỗi đủ nặng để dừng pipeline không."""
        return not self.passed and self.severity is Severity.ERROR


@dataclass(frozen=True)
class ValidationResult:
    """Tổng hợp nhiều ``CheckOutcome`` thành một phán quyết cho cả batch."""

    is_valid: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...] = ()
    outcomes: tuple[CheckOutcome, ...] = ()

    def raise_for_errors(self) -> None:
        """Raise nếu có lỗi ERROR. Dùng trong build-function (không có MinIO context)."""
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
        """Ghi batch lỗi vào quarantine rồi raise. No-op khi hợp lệ.

        Dùng ở tầng pipeline/DAG nơi có ``MinioConfig`` để ghi quarantine.
        """
        if not self.is_valid:
            from stock_lakehouse.config import MinioConfig
            from stock_lakehouse.staging.quarantine import write_quarantine

            uri = write_quarantine(
                df,
                self.errors,
                domain=domain,
                processing_date=processing_date,
                batch_id=batch_id,
                config=config or MinioConfig(),
            )
            raise ValueError(f"[quarantined={uri}] " + "; ".join(self.errors))
