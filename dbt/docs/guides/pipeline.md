# Hướng dẫn dbt Pipeline: STAG → CURATED → INSIGHT

## 1. Tổng quan

Pipeline dbt này triển khai luồng xử lý dữ liệu Oracle 19c theo 3 lớp:

```
DBT_STAG_CUSTOMER ─────────┐
DBT_STAG_ACCOUNT ──────────┼──> DBT_CURATED_TRANSACTION ──> DBT_INSIGHT_HIGH_VALUE_TXN
DBT_STAG_TRANSACTION ──────┘
```

- **STAG**: Bảng nguồn đã có sẵn trong DB (dbt đọc, không tạo).
- **CURATED**: dbt JOIN 3 bảng STAG, enrich thêm `data_quality_status`.
- **INSIGHT**: dbt lọc giao dịch thành công, khớp đủ dữ liệu, > 500 triệu VND.

## 2. Cấu trúc project

```
models/
├── sources.yml                          # Khai báo 3 bảng STAG nguồn
├── stag/
│   ├── dbt_stag_customer.sql            # ephemeral - lọc theo business_date
│   ├── dbt_stag_account.sql             # ephemeral - lọc theo business_date
│   ├── dbt_stag_transaction.sql         # ephemeral - lọc theo business_date
│   └── schema.yml                       # Tests cho stag models
├── curated/
│   ├── dbt_curated_transaction.sql      # incremental (delete+insert)
│   └── schema.yml                       # Tests cho curated
└── insight/
    ├── dbt_insight_high_value_txn.sql   # incremental (delete+insert)
    └── schema.yml                       # Tests cho insight
```

### Giải thích materialization

| Layer | Materialization | Ý nghĩa |
|-------|----------------|----------|
| stag | `ephemeral` | Không tạo bảng/view. SQL được inline vào model gọi nó (như CTE). |
| curated | `incremental` (`delete+insert`) | Lần đầu: CREATE TABLE AS SELECT. Lần sau: DELETE dữ liệu cũ của business_date rồi INSERT lại. |
| insight | `incremental` (`delete+insert`) | Tương tự curated. |

## 3. Yêu cầu trước khi chạy

### 3.1. Cài dbt

```bash
pip install dbt-core dbt-oracle
```

Kiểm tra:

```bash
dbt --version
```

### 3.2. Kiểm tra profiles.yml

File: `C:\Users\ducph7\.dbt\profiles.yml`

Nội dung hiện tại:

```yaml
test:
  outputs:
    uat:
      host: 10.9.193.201
      password: <password>
      port: 1521
      protocol: tcp
      schema: ofsaafccm
      database: amldb
      service: amldb
      threads: 1
      type: oracle
      user: ofsaafccm
  target: uat
```

`dbt_project.yml` dùng `profile: 'test'` → dbt tìm profile tên `test` → target mặc định là `uat`.

### 3.3. Kiểm tra bảng STAG đã tồn tại

Dùng sqlcli:

```bash
sqlcli sql execute -p DBTTEST -f check_tables.sql > output.txt
```

Hoặc kiểm tra trực tiếp bằng SQL:

```sql
SELECT table_name FROM user_tables WHERE table_name LIKE 'DBT%' ORDER BY table_name;
```

Phải có 5 bảng:
- `DBT_STAG_CUSTOMER`
- `DBT_STAG_ACCOUNT`
- `DBT_STAG_TRANSACTION`
- `DBT_CURATED_TRANSACTION`
- `DBT_INSIGHT_HIGH_VALUE_TXN`

Nếu chưa có, chạy file DDL:

```sql
@oracle19_dbt_pipeline_01_ddl_seed.sql
```

## 4. Chạy pipeline

### 4.1. Kiểm tra kết nối

```bash
dbt debug
```

Tất cả check phải hiện `OK`. Nếu lỗi `Could not find profile named 'test'`, kiểm tra lại `profiles.yml`.

### 4.2. Compile (xem SQL mà không chạy)

```bash
dbt compile --vars "{business_date: '20260920'}"
```

SQL đã compile nằm trong `target/compiled/aml_transaction_pipeline/models/`.

### 4.3. Chạy ngày 2026-09-20

```bash
dbt run --vars "{business_date: '20260920'}"
```

Lần chạy đầu tiên, dbt tạo bảng CURATED và INSIGHT bằng `CREATE TABLE AS SELECT`.

### 4.4. Chạy ngày 2026-09-21 (incremental)

```bash
dbt run --vars "{business_date: '20260921'}"
```

Lần này dbt dùng `DELETE + INSERT` chỉ cho dữ liệu ngày 21, không đụng dữ liệu ngày 20.

### 4.5. Chạy lại một ngày (idempotent)

```bash
dbt run --vars "{business_date: '20260920'}"
```

Chạy lại cùng ngày 20 hoàn toàn an toàn. dbt xóa dữ liệu ngày 20 rồi insert lại, kết quả giống hệt.

### 4.6. Chạy với target khác

```bash
dbt run --target sit --vars "{business_date: '20260920'}"
```

Sẽ dùng kết nối `sit` thay vì `uat` (nếu đã cấu hình trong profiles.yml).

### 4.7. Full refresh (tạo lại bảng từ đầu)

```bash
dbt run --full-refresh --vars "{business_date: '20260920'}"
```

Bỏ qua incremental, DROP bảng cũ và CREATE lại từ đầu.

## 5. Kiểm tra kết quả

### 5.1. Bằng sqlcli

Tạo file `verify_pipeline.sql`:

```sql
SELECT 'STAG_TXN' AS layer, business_date, COUNT(*) AS cnt
FROM dbt_stag_transaction GROUP BY business_date
UNION ALL
SELECT 'CURATED', business_date, COUNT(*)
FROM dbt_curated_transaction GROUP BY business_date
UNION ALL
SELECT 'INSIGHT', business_date, COUNT(*)
FROM dbt_insight_high_value_txn GROUP BY business_date
ORDER BY 1, 2;
```

```bash
sqlcli sql execute -p DBTTEST -f verify_pipeline.sql > result.txt
```

### 5.2. Kết quả mong đợi

**Ngày 2026-09-20:**

| Layer | Số bản ghi |
|-------|-----------|
| STAG transaction | 6 |
| CURATED | 6 |
| INSIGHT | 1 (T20260920002, 600 triệu) |

Data quality trong CURATED:

| Trạng thái | Số bản ghi |
|------------|-----------|
| MATCHED | 4 |
| CUSTOMER_NOT_FOUND | 1 |
| ACCOUNT_NOT_FOUND | 1 |

**Ngày 2026-09-21:**

| Layer | Số bản ghi |
|-------|-----------|
| STAG transaction | 3 |
| CURATED | 3 |
| INSIGHT | 2 (T20260921001 550tr, T20260921003 1.2 tỷ) |

### 5.3. Kiểm tra data quality

```sql
SELECT business_date, data_quality_status, COUNT(*) AS cnt
FROM dbt_curated_transaction
GROUP BY business_date, data_quality_status
ORDER BY business_date, data_quality_status;
```

### 5.4. Kiểm tra INSIGHT chi tiết

```sql
SELECT transaction_id, customer_name, amount_vnd, business_date
FROM dbt_insight_high_value_txn
ORDER BY business_date, transaction_id;
```

## 6. Chạy dbt test

```bash
dbt test --vars "{business_date: '20260920'}"
```

Các test đã cấu hình:
- **stag**: `not_null` trên khóa chính, `accepted_values` cho `transaction_status`.
- **curated**: `not_null` trên khóa, `accepted_values` cho `data_quality_status` và `transaction_status`.
- **insight**: `not_null` trên `transaction_id`, `business_date`, `amount_vnd`.

## 7. Các lỗi thường gặp

| Lỗi | Nguyên nhân | Xử lý |
|-----|-------------|-------|
| `Could not find profile named 'test'` | profiles.yml thiếu hoặc sai tên | Kiểm tra `C:\Users\ducph7\.dbt\profiles.yml` |
| `Compilation Error: source stag.dbt_stag_customer not found` | sources.yml bị xóa hoặc sai tên | Kiểm tra `models/sources.yml` |
| `ORA-00942: table or view does not exist` | Bảng STAG chưa được tạo | Chạy `oracle19_dbt_pipeline_01_ddl_seed.sql` |
| `delete+insert strategy not supported` | dbt-oracle chưa hỗ trợ strategy này | Xem mục 8 bên dưới |
| `var 'business_date' not found` | Quên truyền `--vars` | Thêm `--vars "{business_date: '20260920'}"` |

## 8. Lưu ý về dbt-oracle adapter

Adapter `dbt-oracle` có thể không hỗ trợ `incremental_strategy='delete+insert'` tùy phiên bản. Nếu gặp lỗi:

**Cách 1**: Đổi sang `merge` strategy — sửa config trong `dbt_curated_transaction.sql` và `dbt_insight_high_value_txn.sql`:

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='transaction_id'
    )
}}
```

**Cách 2**: Dùng `table` materialization (đơn giản nhất, nhưng recreate toàn bộ mỗi lần chạy):

```sql
{{ config(materialized='table') }}
```

## 9. Tóm tắt lệnh nhanh

```bash
# Kiểm tra kết nối
dbt debug

# Compile xem SQL
dbt compile --vars "{business_date: '20260920'}"

# Chạy ngày 20
dbt run --vars "{business_date: '20260920'}"

# Chạy ngày 21
dbt run --vars "{business_date: '20260921'}"

# Test
dbt test --vars "{business_date: '20260920'}"

# Full refresh
dbt run --full-refresh --vars "{business_date: '20260920'}"

# Xem docs
dbt docs generate
dbt docs serve
```
