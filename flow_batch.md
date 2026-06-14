# Batch Pipeline Flow

## 1. Mục tiêu

Xây dựng batch pipeline cho dữ liệu chứng khoán HOSE theo kiến trúc Lakehouse:

```text
VNStock / VCI
→ Airflow
→ Python + Polars
→ Staging Parquet trên MinIO
→ Great Expectations
→ PyIceberg
→ Bronze / Silver / Gold Iceberg
→ ClickHouse
→ Streamlit
```

Pipeline cần:

- Chạy theo ngày và hỗ trợ backfill.
- Retry không tạo dữ liệu trùng.
- Tách rõ Staging, Bronze, Silver và Gold.
- Kiểm tra chất lượng trước khi ghi sang layer tiếp theo.
- Load dữ liệu Gold sang ClickHouse để phục vụ dashboard.

## 2. Vai trò của từng thành phần

| Thành phần | Vai trò |
|---|---|
| VNStock / VCI | Nguồn dữ liệu OHLCV và metadata cổ phiếu |
| Python | Điều khiển luồng xử lý và gọi API |
| Polars | Làm sạch, biến đổi và tính chỉ báo |
| MinIO | Object Storage cho Staging và Iceberg |
| PyArrow | Chuyển dữ liệu giữa Polars và PyIceberg |
| PyIceberg | Tạo, đọc và ghi bảng Iceberg |
| Iceberg REST Catalog | Quản lý catalog và metadata bảng Iceberg |
| Great Expectations | Kiểm tra chất lượng dữ liệu |
| PostgreSQL | Metadata database cho Airflow và có thể cho catalog |
| ClickHouse | Serving database cho truy vấn phân tích |
| Airflow | Lập lịch, retry và điều phối pipeline |
| Streamlit | Hiển thị dashboard batch |

Polars, PyArrow, PyIceberg và Great Expectations là thư viện Python, không phải service Docker độc lập.

## 3. Cấu trúc thư mục đề xuất

```text
MiniProject/
├── requirements.txt
├── docker-compose.yml
├── .env
├── .env.example
├── .gitignore
├── src/
│   └── stock_lakehouse/
│       ├── __init__.py
│       ├── config.py
│       ├── ingestion/
│       │   ├── ohlcv.py
│       │   └── symbols.py
│       ├── staging/
│       │   └── writer.py
│       ├── quality/
│       │   ├── bronze_checks.py
│       │   ├── silver_checks.py
│       │   └── gold_checks.py
│       ├── bronze/
│       │   ├── ohlcv.py
│       │   └── symbols.py
│       ├── silver/
│       │   ├── ohlcv.py
│       │   └── symbols.py
│       ├── gold/
│       │   ├── dim_date.py
│       │   ├── dim_symbol.py
│       │   └── fact_daily_market.py
│       ├── iceberg/
│       │   ├── catalog.py
│       │   ├── tables.py
│       │   └── writer.py
│       ├── clickhouse/
│       │   ├── client.py
│       │   └── loader.py
│       └── utils/
│           ├── dates.py
│           └── logging.py
├── dags/
│   ├── dag_daily_ohlcv.py
│   ├── dag_symbol_metadata.py
│   └── dag_dim_date.py
├── streamlit_app/
├── tests/
└── notebooks/
    ├── 01_smoke_test_minio.ipynb
    ├── 02_smoke_test_iceberg.ipynb
    ├── 03_smoke_test_clickhouse.ipynb
    └── feasibility/
```

Quy ước:

- `src/`: code nghiệp vụ chạy thật.
- `dags/`: chỉ chứa logic điều phối Airflow.
- `notebooks/`: smoke test, lệnh chạy thủ công, thử nghiệm API và khám phá dữ liệu.
- `tests/`: unit test và integration test.

## 4. Chuẩn bị môi trường Python

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Kiểm tra:

```bash
python --version
python -c "import polars, pyarrow, pyiceberg; print('Environment OK')"
```

Nên dùng Python 3.12 hoặc 3.13 để tránh lỗi tương thích PyIceberg trên Windows.

## 5. Infrastructure cần dựng

### Giai đoạn đầu

```text
MinIO
MinIO initialization
Iceberg REST Catalog
ClickHouse
PostgreSQL
```

### Giai đoạn sau

```text
Airflow Webserver
Airflow Scheduler
Streamlit
```

Lệnh kiểm tra:

```bash
docker compose up -d
docker compose ps
docker compose logs -f
```

## 6. Cấu trúc dữ liệu trên MinIO

Bucket chính:

```text
s3://lakehouse/
```

Các vùng:

```text
s3://lakehouse/staging/
s3://lakehouse/warehouse/
s3://lakehouse/quarantine/
```

- `staging/`: Parquet ngay sau khi gọi API.
- `warehouse/`: dữ liệu và metadata Iceberg.
- `quarantine/`: batch không đạt kiểm tra chất lượng.

Không tự quản lý thủ công partition bên trong `warehouse/`; Iceberg sẽ quản lý data file, manifest và metadata.

## 7. Luồng xử lý tổng thể

```text
1. Airflow khởi tạo DAG theo lịch
2. Extract dữ liệu từ VNStock / VCI
3. Chuyển dữ liệu thành Polars DataFrame
4. Ghi Parquet vào MinIO Staging
5. Validate dữ liệu Staging
6. Ghi dữ liệu hợp lệ vào Bronze Iceberg
7. Đọc Bronze và transform sang Silver
8. Validate Silver
9. Ghi Silver Iceberg
10. Tạo hoặc cập nhật dim_date
11. Tạo hoặc cập nhật dim_symbol
12. Tính chỉ báo kỹ thuật
13. Tạo fact_hose_daily_market
14. Validate Gold
15. Ghi Gold Iceberg
16. Load Gold sang ClickHouse
17. Streamlit đọc ClickHouse để hiển thị dashboard
```

## 8. Flow theo từng layer

### 8.1 Extract

Đầu vào:

```text
processing_date
symbol list
source
batch_id
```

Xử lý:

- Gọi API.
- Chuẩn hóa response tối thiểu.
- Bổ sung `source`, `batch_id`, `ingested_at`, `processing_date`.

Đầu ra là Polars DataFrame.

### 8.2 Staging

Đường dẫn đề xuất:

```text
staging/ohlcv/processing_date=YYYY-MM-DD/batch_id=<id>/
staging/symbols/batch_id=<id>/
```

Mục đích:

- Giữ input của batch.
- Retry không cần gọi lại API.
- Hỗ trợ debug.
- Tách extract khỏi commit Iceberg.

```text
API response
→ Polars DataFrame
→ Parquet
→ MinIO Staging
```

### 8.3 Bronze

Các bảng:

```text
bronze_hose_ohlcv_daily
bronze_hose_symbols
```

Kiểm tra cơ bản:

- `symbol` không null.
- `time` không null.
- Các cột OHLCV có kiểu hợp lệ.
- `source`, `batch_id`, `ingested_at` không null.

```text
Staging Parquet
→ Validate Bronze
→ Polars
→ Arrow Table
→ PyIceberg
→ Bronze Iceberg
```

### 8.4 Silver

Các bảng:

```text
silver_hose_ohlcv_daily
silver_hose_symbols
```

Xử lý chính:

- Chuẩn hóa tên cột và kiểu dữ liệu.
- Đổi `time` thành `trading_date`.
- Chuẩn hóa mã cổ phiếu.
- Lọc đúng ngày yêu cầu.
- Loại duplicate theo `symbol + trading_date`.
- Kiểm tra quy tắc OHLCV.
- Chuẩn hóa metadata doanh nghiệp.

Các rule quan trọng:

```text
open_price > 0
high_price >= low_price
high_price >= open_price
high_price >= close_price
low_price <= open_price
low_price <= close_price
volume >= 0
```

### 8.5 Gold

Các bảng:

```text
dim_date
dim_symbol
fact_hose_daily_market
```

#### dim_date

Grain: một dòng cho một ngày lịch.

Nguồn:

- Sinh trực tiếp bằng Python.
- Dùng `holidays` để bổ sung ngày lễ Việt Nam.

Các cột chính:

```text
date_key
full_date
day
cal_week
cal_month
cal_quarter
cal_year
is_weekend
event_name
event_type
is_day_off
```

`dim_date` được tạo trực tiếp ở Gold, không cần đi qua Bronze và Silver.

#### dim_symbol

Grain: một dòng cho một mã cổ phiếu.

Các cột chính:

```text
symbol_key
symbol
company_name
sector_name
company_profile
listing_date
issued_share
exchange_code
listed_status
updated_at
```

Nguyên tắc cập nhật:

- Symbol cũ giữ nguyên `symbol_key`.
- Symbol mới được cấp key mới.
- Symbol hủy niêm yết cập nhật trạng thái, không xóa cứng.
- Không append mù.

#### fact_hose_daily_market

Grain: một mã cổ phiếu trong một ngày giao dịch.

Các cột chính:

```text
symbol_key
date_key
trading_date
open_price
high_price
low_price
close_price
volume
price_change
pct_change
sma20
ema20
rsi14
macd
avg_volume_20d
updated_at
```

```text
Silver OHLCV
+ dim_symbol
+ dim_date
→ Polars join
→ Tính chỉ báo
→ Validate Gold
→ PyIceberg commit
→ Gold Iceberg
```

## 9. Tính chỉ báo bằng Polars

Dữ liệu phải sắp xếp theo:

```text
symbol
trading_date
```

Các chỉ báo:

- `price_change`
- `pct_change`
- `sma20`
- `ema20`
- `rsi14`
- `macd`
- `avg_volume_20d`

Khi xử lý ngày D, cần đọc thêm dữ liệu lịch sử trước ngày D để tính rolling indicator chính xác, không chỉ đọc riêng ngày D.

## 10. Data Quality

### Bronze

- Không null ở cột bắt buộc.
- Kiểu dữ liệu đúng.
- Có đủ metadata pipeline.

### Silver

- Không duplicate theo `symbol + trading_date`.
- OHLCV hợp lệ.
- Volume không âm.
- Batch chỉ chứa ngày cần xử lý.

### Gold

- `symbol_key` và `date_key` không null.
- Không duplicate theo `symbol_key + date_key`.
- `rsi14` trong khoảng 0–100.
- Foreign key tồn tại trong dimension.
- Số lượng symbol đạt mức kỳ vọng.

Nếu validation thất bại:

```text
Dừng pipeline
→ Không commit sang layer tiếp theo
→ Ghi dữ liệu lỗi vào quarantine
→ Task trả về failed
```

## 11. Idempotency

Mỗi batch cần có:

```text
processing_date
batch_id
source
```

Nguyên tắc:

- Không append mù.
- Chỉ thay thế dữ liệu của ngày đang xử lý.
- Không ảnh hưởng ngày khác.
- Giữ snapshot Iceberg để time travel hoặc rollback.
- Kiểm tra batch đầy đủ trước khi overwrite.

```text
Đọc đầy đủ dữ liệu ngày D
→ Transform
→ Validate
→ Overwrite partition ngày D
```

Nếu batch ngày D thiếu symbol mà vẫn overwrite, dữ liệu cũ của symbol bị thiếu có thể biến mất khỏi snapshot mới.

## 12. Load sang ClickHouse

Các bảng serving:

```text
dim_date
dim_symbol
fact_hose_daily_market
```

```text
Gold Iceberg
→ PyIceberg scan
→ Arrow / Polars
→ clickhouse-connect
→ ClickHouse
```

Load phải idempotent. Khi retry ngày D, cần thay thế dữ liệu ngày D trước khi insert lại.

## 13. Airflow DAG

### DAG daily OHLCV

```text
extract_ohlcv
→ write_staging
→ validate_staging
→ write_bronze
→ transform_silver
→ validate_silver
→ build_gold_fact
→ validate_gold
→ sync_clickhouse
```

### DAG symbol metadata

```text
extract_hose_symbols
→ write_staging_symbols
→ write_bronze_symbols
→ transform_silver_symbols
→ validate_silver_symbols
→ upsert_dim_symbol
→ sync_dim_symbol_to_clickhouse
```

### DAG dim_date

```text
generate_calendar
→ validate_dim_date
→ write_dim_date_iceberg
→ sync_dim_date_to_clickhouse
```

Airflow DAG chỉ gọi function hoặc module trong `src/`; không đặt toàn bộ logic transform trực tiếp trong file DAG.

## 14. Thứ tự triển khai

```text
[x] Tạo môi trường Python
[x] Cài requirements.txt
[ ] Chuẩn hóa cấu trúc repository
[x] Dựng MinIO
[x] Tạo bucket lakehouse
[x] Smoke test ghi và đọc Parquet trên MinIO
[x] Dựng Iceberg REST Catalog
[x] Smoke test tạo bảng Iceberg bằng PyIceberg
[x] Dựng ClickHouse
[x] Smoke test kết nối ClickHouse
[ ] Viết module extract OHLCV
[ ] Viết module Staging
[ ] Tạo Bronze OHLCV
[ ] Tạo Silver OHLCV
[ ] Tạo dim_date
[ ] Tạo dim_symbol
[ ] Tạo fact_hose_daily_market
[ ] Tích hợp Great Expectations
[ ] Kiểm thử idempotency
[ ] Load Gold sang ClickHouse
[ ] Dựng Airflow
[ ] Viết DAG
[ ] Xây dựng Streamlit dashboard
```

## 15. Việc cần làm ngay

```text
1. Dựng MinIO bằng Docker Compose
2. Tạo bucket lakehouse
3. Tạo notebook `01_smoke_test_minio.ipynb`
4. Ghi một Polars DataFrame thành Parquet lên MinIO
5. Đọc lại file để xác nhận kết nối
6. Dựng Iceberg REST Catalog
7. Tạo notebook `02_smoke_test_iceberg.ipynb`
8. Tạo và đọc thử một bảng Iceberg
```

Chỉ sau khi MinIO và Iceberg hoạt động ổn định mới tiếp tục xây dựng Bronze, Silver và Gold.
