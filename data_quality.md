# Data Quality — Stock Lakehouse (HOSE)

Tài liệu giải thích **vì sao** và **bằng cách nào** project kiểm tra chất lượng
dữ liệu sau khi gỡ Great Expectations. Toàn bộ validate giờ chạy bằng **Polars
thuần**, tổ chức quanh **6 chiều chất lượng dữ liệu** (DAMA DMBOK / ISO 8000).

> Gói code: [`src/stock_lakehouse/quality/`](src/stock_lakehouse/quality/)
> — `result.py` (kiểu chung) · `checks.py` (hàm tái sử dụng) · `suites.py` (suite từng bảng).

---

## Vì sao bỏ Great Expectations?

GX trước đây chỉ "hình thức": dựng `ExpectationSuite` rồi bỏ, validate thật vẫn
bằng Polars; không có Data Context / Checkpoint / Data Docs. Với universe ~5 mã,
GX là gánh nặng phụ thuộc mà không đem lại observability thực. Thay vào đó ta tự
viết các **hàm validate tái sử dụng** — nhẹ, dễ test, dễ đọc, đúng nhu cầu.

---

## 6 chiều chất lượng dữ liệu

| # | Chiều | Câu hỏi | Ví dụ trong project |
|---|-------|---------|---------------------|
| 1 | **Completeness** (đầy đủ) | Giá trị/cột bắt buộc có mặt không? | `symbol`, `trading_date` không null; đủ cột OHLCV |
| 2 | **Uniqueness** (duy nhất) | Có trùng khoá không? | 1 dòng / `symbol + trading_date`; `symbol_key` duy nhất |
| 3 | **Validity** (hợp lệ) | Giá trị đúng kiểu/miền/dấu/tập? | giá > 0; `rsi14 ∈ [0,100]`; `listed_status ∈ {LISTED, DELISTED}` |
| 4 | **Consistency** (nhất quán) | Các cột/bảng có khớp logic? | `high ≥ low/open/close`; FK fact → dim tồn tại |
| 5 | **Accuracy** (chính xác) | Có khớp thực tế/bất biến nghiệp vụ? | biên độ giá HOSE ±7%/phiên (kiểm tra hợp-lý) |
| 6 | **Timeliness** (kịp thời) | Dữ liệu đúng kỳ & còn mới? | mọi dòng đúng `processing_date` (ngày D) |

> **Accuracy khó đo nội tại.** Nó cần "nguồn sự thật" để đối chiếu. Khi chỉ có 1
> nguồn (VCI), ta xấp xỉ accuracy bằng **kiểm tra hợp-lý** (plausibility): một
> biến động giá > ±7% là *bất thường* — hoặc dữ liệu sai, hoặc do sự kiện doanh
> nghiệp hợp lệ. Vì khả năng thứ hai, check này để mức **WARN** (cảnh báo, không
> chặn pipeline) thay vì ERROR.

---

## Thiết kế: hàm validate tái sử dụng `check(df, config)`

Theo gợi ý của mentor — **mỗi check là một hàm thuần nhận đúng 2 tham số**:
`check(df, config) -> CheckOutcome`. `config` là một dataclass nhỏ mô tả tham số
(cột nào, ngưỡng bao nhiêu). Cùng một hàm dùng lại cho mọi bảng, chỉ khác config.

```python
from stock_lakehouse.quality import InRange, run_check

run_check(fact_df, InRange("rsi14", min_value=0, max_value=100))
# -> CheckOutcome(dimension=VALIDITY, check='in_range', passed=True/False, ...)
```

### Bảng config ↔ hàm check ↔ chiều

| Chiều | Config | Hàm | Ý nghĩa |
|-------|--------|-----|---------|
| Completeness | `RequiredColumns(columns)` | `check_required_columns` | đủ cột |
| Completeness | `NotNull(columns)` | `check_not_null` | không null |
| Uniqueness | `Unique(columns)` | `check_unique` | tổ hợp cột duy nhất |
| Validity | `InRange(column, min, max)` | `check_in_range` | trong khoảng |
| Validity | `Positive(columns)` | `check_positive` | > 0 |
| Validity | `InSet(column, allowed)` | `check_in_set` | thuộc tập cho phép |
| Consistency | `ColumnRelation(left, op, right)` | `check_column_relation` | quan hệ 2 cột (cross-field) |
| Consistency | `ForeignKey(column, ref, ref_col)` | `check_foreign_key` | FK tồn tại trong dim |
| Accuracy | `WithinDailyBand(column, band)` | `check_within_daily_band` | biên độ hợp lý (WARN) |
| Timeliness | `MatchesDate(column, expected)` | `check_matches_date` | đúng ngày D |

Mỗi config có thêm trường `severity` (mặc định `ERROR`, riêng `WithinDailyBand`
là `WARN`). Giá trị **null được bỏ qua** ở các check miền/quan hệ — null thuộc
trách nhiệm của `NotNull` (tránh phạt 2 lần một vấn đề).

### Chạy cả "suite" khai báo

Một suite của bảng chỉ là **list config**; `run_suite` dispatch từng config tới
đúng hàm rồi gộp thành `ValidationResult`:

```python
from stock_lakehouse.quality import run_suite, NotNull, Unique, Positive

result = run_suite(
    silver_df,
    [NotNull(("symbol", "trading_date")),
     Unique(("symbol", "trading_date")),
     Positive(("open_price", "high_price", "low_price", "close_price"))],
    suite_name="silver_hose_ohlcv_daily",
)
result.is_valid     # False nếu có lỗi ERROR
result.errors       # message các lỗi ERROR (làm batch không hợp lệ)
result.warnings     # message các lỗi WARN (chỉ cảnh báo)
result.outcomes     # chi tiết từng CheckOutcome (để log/quan sát)
```

`ValidationResult` giữ nguyên 2 phương thức cũ:
- `raise_for_errors()` — dùng trong build-function (fail-fast nội bộ).
- `quarantine_and_raise(df, domain=…, processing_date=…, batch_id=…)` — ghi batch
  lỗi vào `s3://lakehouse/quarantine/…` rồi raise (dùng ở pipeline/DAG).

---

## Suite áp cho từng bảng

Định nghĩa trong [`quality/suites.py`](src/stock_lakehouse/quality/suites.py).
Mỗi suite chạy "cổng" `RequiredColumns` trước (thiếu cột ⇒ trả lỗi ngay, không
báo trùng).

| Bảng | Completeness | Uniqueness | Validity | Consistency | Accuracy | Timeliness |
|------|:---:|:---:|:---:|:---:|:---:|:---:|
| `bronze_hose_ohlcv_daily` | ✓ | | | | | |
| `bronze_hose_symbols` | ✓ | | | | | |
| `silver_hose_ohlcv_daily` | ✓ | ✓ | ✓ | ✓ | | ✓¹ |
| `silver_hose_symbols` | ✓ | ✓ | ✓ | | | |
| `dim_date` | ✓ | ✓ | ✓ | | | |
| `dim_symbol` | ✓ | ✓ | ✓ | | | |
| `fact_hose_daily_market` | ✓ | ✓ | ✓ | ✓ | ✓² | |

> ¹ Timeliness ở silver chỉ bật khi truyền `processing_date` (mọi dòng = ngày D).
> ² Accuracy ở fact là `WithinDailyBand("pct_change", 0.07)` mức **WARN**.

Bronze cố tình chỉ kiểm Completeness — đúng tinh thần "Bronze = raw đã ép kiểu",
chưa áp rule nghiệp vụ (để dành Silver).

---

## Thêm một check mới

1. Thêm dataclass config + hàm `check_*` trong `checks.py` (nhớ gắn `Dimension`).
2. Đăng ký vào `_DISPATCH` và export ở `__init__.py`.
3. Thêm config vào suite tương ứng trong `suites.py`.
4. Viết test trong `tests/test_quality_checks.py`.
