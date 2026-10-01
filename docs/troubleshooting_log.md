# Troubleshooting Log — dagster-dbt (Oracle)

Ghi lại các lỗi gặp phải trong quá trình chạy `dg dev` và cách đã fix, theo thứ tự thời gian.

---

## 1. `DagsterInvalidDefinitionError: Duplicate asset key: AssetKey(['raw_customers'])`

**Nguyên nhân:**
Có 3 nguồn cùng khai báo asset key `raw_customers` (và tương tự `raw_orders`):

1. dbt seed `raw_customers.csv` trong `dbt/seeds/` — dbt tự coi seed là asset, key = tên seed.
2. dbt source `raw.raw_customers` khai báo trong `dbt/models/sources.yml`, với `meta.dagster.asset_key: ["raw_customers"]` (theo đúng pattern "Step 7 — Connect upstream assets to dbt sources" của tài liệu Dagster).
3. Python asset `raw_customers` trong `src/dagster_dbt_demo/defs/assets.py`, dùng để insert data vào Oracle.

Seed (1) và source (2) + Python asset (3) đều claim cùng một key `raw_customers` → Dagster báo trùng khi load defs.

**Cách fix:**
- Đổi tên/loại bỏ seed cũ `raw_customers.csv`, `raw_orders.csv` (không cần seed giả nữa vì đã có Python asset ingest thật vào Oracle).
- Sửa model `stg_customers.sql`/`stg_orders.sql` để dùng `source()` thay cho `ref()` (chi tiết code ở mục 2, vì đây cũng là phần sửa chung).
- Giữ `sources.yml` với `meta.dagster.asset_key` khớp key của Python asset — đây là cấu hình đúng theo tài liệu Dagster, không phải nguyên nhân gây lỗi.

**Code — trước (seed còn tồn tại):**
```
dbt/seeds/
├── raw_customers.csv   <-- dbt coi là asset "raw_customers"
├── raw_orders.csv       <-- dbt coi là asset "raw_orders"
└── raw_payments.csv
```

**Code — sau (đổi tên để dbt không còn nhận seed này là asset):**
```
dbt/seeds/
├── raw_customers_bk.csv   <-- đã đổi tên, không còn được dbt parse
├── raw_orders_bk.csv       <-- đã đổi tên, không còn được dbt parse
└── raw_payments.csv
```

---

## 2. `Compilation Error: Model 'stg_orders' depends on a node named 'raw_orders' which was not found`

**Nguyên nhân:**
Sau khi đổi tên seed `raw_orders.csv` → `raw_orders_bk.csv` (loại seed khỏi dbt), `stg_orders.sql`/`stg_customers.sql` vẫn còn gọi `{{ ref('raw_orders') }}` / `{{ ref('raw_customers') }}`. `ref()` trong dbt chỉ tìm trong model/seed của chính dbt project — không biết gì về Python asset bên Dagster. Vì seed đã bị đổi tên nên dbt không còn thấy node `raw_orders` nữa.

**Cách fix:**
Đổi `ref(...)` → `source('raw', ...)` để nối đúng tới source đã khai báo trong `sources.yml` (và qua đó, nối tới Python asset bên Dagster).

**Code trước (`stg_customers.sql`):**
```sql
with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_customers') }}

),
...
```

**Code sau (`stg_customers.sql`):**
```sql
with source as (

    select * from {{ source('raw', 'raw_customers') }}

),
...
```

**Code trước (`stg_orders.sql`):**
```sql
with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_orders') }}

),
...
```

**Code sau (`stg_orders.sql`):**
```sql
with source as (

    select * from {{ source('raw', 'raw_orders') }}

),
...
```

---

## 3. `oracledb.exceptions.DatabaseError: ORA-00933: SQL command not properly ended`

**Nguyên nhân:**
Trong `assets.py`, câu `INSERT` dùng cú pháp multi-row kiểu MySQL/Postgres (nhiều tuple sau một `VALUES`). Oracle **không hỗ trợ** cú pháp này — Oracle parser gặp dấu phẩy sau tuple đầu tiên thì không hiểu cú pháp → `ORA-00933`.

**Cách fix:**
Đổi sang 1 câu `INSERT ... VALUES (:1, :2, ...)` dùng bind parameters, kết hợp `cursor.executemany(statement, rows)` với `rows` là list of tuple — cách chuẩn để insert nhiều dòng trong Oracle qua `oracledb`, đồng thời an toàn hơn (tránh SQL injection, tránh lỗi escape ký tự đặc biệt như dấu `'`).

**Code trước (`raw_customers`):**
```python
connection = get_connection()
statement = """
    INSERT INTO RAW_CUSTOMERS (ID,FIRST_NAME,LAST_NAME) VALUES
        (201,'ducph7','P.'),
        (202,'Shawn','M.'),
        (203,'Kathleen','P.'),
        (204,'Jimmy','C.'),
        (205,'Katherine','R.'),
        (206,'Sarah','R.'),
        (207,'Martin','M.'),
        (208,'Frank','R.'),
        (209,'Jennifer','F.'),
        (210,'Henry','W.');
"""
cursor = connection.cursor()
cursor.execute(statement)
connection.commit()
context.add_output_metadata({"num_rows": 10})
```

**Code sau (`raw_customers`):**
```python
connection = get_connection()
insert_statement = "INSERT INTO RAW_CUSTOMERS (ID, FIRST_NAME, LAST_NAME) VALUES (:1, :2, :3)"
rows = [
    (201, "ducph7", "P."),
    (202, "Shawn", "M."),
    (203, "Kathleen", "P."),
    (204, "Jimmy", "C."),
    (205, "Katherine", "R."),
    (206, "Sarah", "R."),
    (207, "Martin", "M."),
    (208, "Frank", "R."),
    (209, "Jennifer", "F."),
    (210, "Henry", "W."),
]
cursor = connection.cursor()
cursor.executemany(insert_statement, rows)
connection.commit()
context.add_output_metadata({"num_rows": len(rows)})
```

**Code trước (`raw_orders`):**
```python
connection = get_connection()
statement = """
    INSERT INTO RAW_ORDERS (ID,USER_ID,ORDER_DATE,STATUS) VALUES
    (201,201,TIMESTAMP'2018-01-01 00:00:00','returned'),
    (202,202,TIMESTAMP'2018-01-02 00:00:00','completed'),
    (203,203,TIMESTAMP'2018-01-04 00:00:00','completed'),
    (204,204,TIMESTAMP'2018-01-05 00:00:00','completed'),
    (205,205,TIMESTAMP'2018-01-05 00:00:00','completed'),
    (206,206,TIMESTAMP'2018-01-07 00:00:00','completed'),
    (207,207,TIMESTAMP'2018-01-09 00:00:00','completed'),
    (208,208,TIMESTAMP'2018-01-11 00:00:00','returned'),
    (209,209,TIMESTAMP'2018-01-12 00:00:00','completed'),
    (210,210,TIMESTAMP'2018-01-14 00:00:00','completed');
"""
cursor = connection.cursor()
cursor.execute(statement)
connection.commit()
context.add_output_metadata({"num_rows": 10})
```

**Code sau (`raw_orders`):**
```python
connection = get_connection()
insert_statement = (
    "INSERT INTO RAW_ORDERS (ID, USER_ID, ORDER_DATE, STATUS) "
    "VALUES (:1, :2, TO_DATE(:3, 'YYYY-MM-DD HH24:MI:SS'), :4)"
)
rows = [
    (201, 201, "2018-01-01 00:00:00", "returned"),
    (202, 202, "2018-01-02 00:00:00", "completed"),
    (203, 203, "2018-01-04 00:00:00", "completed"),
    (204, 204, "2018-01-05 00:00:00", "completed"),
    (205, 205, "2018-01-05 00:00:00", "completed"),
    (206, 206, "2018-01-07 00:00:00", "completed"),
    (207, 207, "2018-01-09 00:00:00", "completed"),
    (208, 208, "2018-01-11 00:00:00", "returned"),
    (209, 209, "2018-01-12 00:00:00", "completed"),
    (210, 210, "2018-01-14 00:00:00", "completed"),
]
cursor = connection.cursor()
cursor.executemany(insert_statement, rows)
connection.commit()
context.add_output_metadata({"num_rows": len(rows)})
```

---

## 4. `dbt build` lỗi `ORA-00903: invalid table name` khi build `stg_customers`/`stg_orders`

**Nguyên nhân:**
`sources.yml` khai báo source `raw` nhưng không set `schema`/`database`. dbt mặc định dùng tên source (`raw`) làm **schema name**, nên SQL sinh ra select từ schema `raw` — schema này không tồn tại trong Oracle (bảng thật nằm ở schema `vpbcustom`, theo profile Oracle active tại `C:\Users\<user>\.dbt\profiles.yml`, target `dev`).

Lưu ý phụ: `dbt/profiles.yml` trong repo khai báo `type: duckdb`, nhưng **không phải file được dùng thực tế** — dbt ưu tiên `~/.dbt/profiles.yml` (user-level) nếu không chỉ định `--profiles-dir` khác. File profile thật đang dùng Oracle (`type: oracle`, `schema: vpbcustom`), khớp với log lỗi `vpbcustom.stg_customers`.

**Cách fix:**
Thêm `schema: vpbcustom` vào source `raw`.

**Code trước (`dbt/models/sources.yml`):**
```yaml
version: 2
sources:
  - name: raw
    tables:
      - name: raw_customers
        meta:
          dagster:
            asset_key: ["raw_customers"]
      - name: raw_orders
        meta:
          dagster:
            asset_key: ["raw_orders"]
```

**Code sau (`dbt/models/sources.yml`):**
```yaml
version: 2
sources:
  - name: raw
    schema: vpbcustom
    tables:
      - name: raw_customers
        meta:
          dagster:
            asset_key: ["raw_customers"]
      - name: raw_orders
        meta:
          dagster:
            asset_key: ["raw_orders"]
```

---

## 5. Bổ sung: TRUNCATE trước khi INSERT (chủ động, tránh lỗi unique constraint)

**Vấn đề phòng ngừa:**
Mỗi lần materialize lại asset `raw_customers`/`raw_orders`, `executemany` insert lại cùng bộ `ID` cố định (201-210) → nếu bảng đã có data từ lần chạy trước, lần insert sau sẽ gặp `ORA-00001: unique constraint violated`.

**Cách fix:**
Thêm `cursor.execute("TRUNCATE TABLE ...")` ngay trước `executemany`, để mỗi lần materialize đều xóa sạch bảng rồi insert lại từ đầu.

> Lưu ý: `TRUNCATE` là DDL trong Oracle, tự động commit ngay, không thể rollback. Chỉ phù hợp vì đây là bảng raw dùng cho mục đích ingest demo/dev — không nên áp dụng cách này cho bảng chứa dữ liệu quan trọng không thể mất.

**Code trước (`raw_customers`, tiếp theo mục 3):**
```python
cursor = connection.cursor()
cursor.executemany(insert_statement, rows)
connection.commit()
context.add_output_metadata({"num_rows": len(rows)})
```

**Code sau (`raw_customers`):**
```python
cursor = connection.cursor()
cursor.execute("TRUNCATE TABLE RAW_CUSTOMERS")
cursor.executemany(insert_statement, rows)
connection.commit()
context.add_output_metadata({"num_rows": len(rows)})
```

**Code trước (`raw_orders`, tiếp theo mục 3):**
```python
cursor = connection.cursor()
cursor.executemany(insert_statement, rows)
connection.commit()
context.add_output_metadata({"num_rows": len(rows)})
```

**Code sau (`raw_orders`):**
```python
cursor = connection.cursor()
cursor.execute("TRUNCATE TABLE RAW_ORDERS")
cursor.executemany(insert_statement, rows)
connection.commit()
context.add_output_metadata({"num_rows": len(rows)})
```

---

## Tổng kết kiến trúc sau khi fix

- `src/dagster_dbt_demo/defs/assets.py`: 2 Python asset `raw_customers`, `raw_orders` — truncate rồi insert data mẫu vào Oracle schema `vpbcustom` bằng bind params + `executemany`.
- `dbt/models/sources.yml`: khai báo source `raw` (schema `vpbcustom`) với `meta.dagster.asset_key` khớp key của 2 Python asset trên, để Dagster nối đúng dependency giữa dbt model và asset Python.
- `dbt/models/staging/stg_customers.sql`, `stg_orders.sql`: dùng `{{ source('raw', ...) }}` để đọc từ bảng do Python asset insert, thay vì seed/ref cũ.
- `dbt/seeds/raw_customers.csv`, `raw_orders.csv`: không còn dùng (đã đổi tên thành `*_bk.csv`), vì Python asset đã thay thế vai trò ingest raw data.
