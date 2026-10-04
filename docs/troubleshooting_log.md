# Troubleshooting Log — dagster-dbt (Oracle → Apache Doris)

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

## 6. Dagster UI báo `stg_customers` liên tục "Unsynced (1 change since last materialization / 1 upstream data version change / has a new materialization)" dù đã chạy lại full luồng

**Hiện tượng:**
Materialize `raw_customers` → `stg_customers` đúng thứ tự, kể cả chạy lại full pipeline, nhưng `stg_customers` trong UI vẫn luôn hiện "Unsynced" ngay sau khi build xong.

**Nguyên nhân:**
Asset Python `raw_customers`/`raw_orders` không khai báo `code_version` và không trả về `data_version` tường minh (hàm chỉ `return None`, dùng `context.add_output_metadata` để log metadata). Theo cơ chế mặc định của Dagster, khi asset không tự báo cáo `DataVersion`, **mỗi lần materialize sẽ tự sinh một data version khác nhau** (gắn theo run), bất kể nội dung data insert ra có giống lần trước hay không.

Hệ quả: `stg_customers` (dbt model, phụ thuộc `raw_customers` qua `source()`) luôn thấy "upstream có data version mới" ngay sau khi `raw_customers` chạy lại — dù dữ liệu insert là y hệt — nên liên tục bị đánh dấu "Unsynced" dù bạn chạy đúng thứ tự.

Tham khảo: [Asset versioning and caching — Dagster docs](https://docs.dagster.io/guides/dagster/asset-versioning-and-caching) — "By default, Dagster automatically computes a data version for each materialization of an asset. It does this by hashing a code version together with the data versions of any input assets."

**Cách fix:**
- Thêm `code_version="v1"` cố định cho asset — version của logic code, chỉ bump tay khi sửa logic ingest.
- Đổi asset trả về `dg.MaterializeResult(data_version=..., metadata=...)`, với `data_version` được hash từ nội dung `rows` thực tế (không phải random/theo run). Nhờ vậy `data_version` chỉ đổi khi nội dung data ingest thực sự thay đổi, không đổi mỗi lần chạy lại với cùng data.

**Code trước (`raw_customers`):**
```python
@dg.asset(key="raw_customers", compute_kind="python", group_name="raw")
def raw_customers(context: dg.AssetExecutionContext) -> None:
    """Ingest raw customer data from an external source."""
    connection = get_connection()
    insert_statement = "INSERT INTO RAW_CUSTOMERS (ID, FIRST_NAME, LAST_NAME) VALUES (:1, :2, :3)"
    rows = [
        (201, "ducph7", "P."),
        ...
    ]
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE RAW_CUSTOMERS")
    cursor.executemany(insert_statement, rows)
    connection.commit()
    context.add_output_metadata({"num_rows": len(rows)})
```

**Code sau (`raw_customers`):**
```python
import hashlib

def _data_version_for(rows: list[tuple]) -> dg.DataVersion:
    """Build a stable DataVersion from the row contents.

    Hashing the actual data means the data version only changes when the
    ingested rows change, instead of a new version being generated on every
    materialization (which would make downstream dbt assets always show up
    as "Unsynced" even when nothing really changed).
    """
    digest = hashlib.sha256(repr(rows).encode("utf-8")).hexdigest()
    return dg.DataVersion(digest)


@dg.asset(key="raw_customers", compute_kind="python", group_name="raw", code_version="v1")
def raw_customers(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Ingest raw customer data from an external source."""
    connection = get_connection()
    insert_statement = "INSERT INTO RAW_CUSTOMERS (ID, FIRST_NAME, LAST_NAME) VALUES (:1, :2, :3)"
    rows = [
        (201, "ducph7", "P."),
        ...
    ]
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE RAW_CUSTOMERS")
    cursor.executemany(insert_statement, rows)
    connection.commit()
    return dg.MaterializeResult(
        data_version=_data_version_for(rows),
        metadata={"num_rows": len(rows)},
    )
```

Áp dụng tương tự cho `raw_orders` (cùng pattern, hash trên `rows` của orders).

**Lưu ý nếu sau này đổi sang ingest từ nguồn thật (API/file/DB khác) thay vì data hard-code:**
`_data_version_for` phải hash trên **data thực tế lấy được** tại runtime, không phải hard-code — nếu không, cơ chế theo dõi "data version" sẽ không còn phản ánh đúng việc data nguồn có thay đổi hay không.

**Phân biệt nhanh `code_version` vs `data_version`:**

| | `code_version` | `data_version` |
|---|---|---|
| Trả lời câu hỏi | Code đổi chưa? | Data đổi chưa? |
| Ai set | Bạn (tay, bump khi sửa logic) | Bạn (qua `MaterializeResult`) hoặc Dagster tự sinh random mỗi run (mặc định) |
| Ảnh hưởng | Theo dõi lịch sử version code qua các run | Quyết định downstream có bị đánh dấu "Unsynced" hay không |

---

## 7. `raw_payments` (seed) hiển thị group là `raw_payments` thay vì `raw` trên Dagster UI

**Hiện tượng:**
Group name của các model/seed trong dbt được quy định bởi `src/dagster_dbt_demo/defs/dbt_ingest/template_vars.py` (hàm `group_from_fqn`, lấy `fqn[1]` — folder đầu tiên sau tên project). Với model nằm trong subfolder (`models/staging/...`) thì ra đúng group `staging`. Nhưng seed `raw_payments` nằm thẳng ở `seeds/` (không có subfolder), nên fqn chỉ có `["jaffle_shop", "raw_payments"]` → `fqn[1]` trả về `"raw_payments"` (tên seed) thay vì group mong muốn (`raw`).

**Nguyên nhân (thật):**
Logic `group_from_fqn` dựa vào cấu trúc folder, nên chỉ đúng với node nằm trong subfolder — seed ở root `seeds/` không có folder con nên bị tính sai.

Lần fix đầu tiên thử dùng `meta.dagster.group_name: raw` khai báo riêng trong `dbt/seeds/schema.yml` (property file cho seed), nhưng **không có hiệu lực**. Nguyên nhân: trong `dbt_ingest/defs.yaml`, field `translation.group_name` đang được set bằng 1 template Jinja cố định `"{{ group_from_fqn(node.fqn) }}"` áp dụng cho **toàn bộ** node của component. Khi `translation.group_name` được khai báo tường minh ở cấp component như vậy, nó sẽ luôn được tính và áp dụng cho mọi node — override hoàn toàn bất kỳ `meta.dagster.group_name` khai báo riêng ở từng node trong YAML của dbt (seed/model). Vì vậy `schema.yml` không sai cú pháp, chỉ là không bao giờ được dùng tới.

**Cách fix (không đổi logic Python trong `template_vars.py`):**
Sửa trực tiếp biểu thức Jinja trong `translation.group_name` ở `dbt_ingest/defs.yaml`, thêm điều kiện đặc biệt cho `raw_payments`, fallback về `group_from_fqn(node.fqn)` cho các node khác.

**Code trước (`dbt_ingest/defs.yaml`):**
```yaml
  translation:
    group_name: "{{ group_from_fqn(node.fqn) }}"
    description: "Transforms data using dbt model {{ node.name }}"
```

**Code sau (`dbt_ingest/defs.yaml`):**
```yaml
  translation:
    group_name: "{{ 'raw' if node.name == 'raw_payments' else group_from_fqn(node.fqn) }}"
    description: "Transforms data using dbt model {{ node.name }}"
```

**Lưu ý:** `dbt/seeds/schema.yml` (khai báo `meta.dagster.group_name: raw` cho `raw_payments`) vẫn được giữ lại trong repo vì không gây hại, nhưng thực tế không có tác dụng do bị `translation.group_name` ở `defs.yaml` override — nguồn group_name thật sự nằm ở `defs.yaml`, không phải ở property file của dbt.

---

## 8. Bổ sung: dùng subgroup (nested group) để tách rõ "raw qua dbt seed" và "raw qua Python asset"

**Nhu cầu:**
Sau khi gom các asset raw (`raw_customers`, `raw_orders`, `raw_payments`) vào cùng group `raw`, muốn tách rõ hơn: asset nào do dbt seed tạo ra, asset nào do Python ingest tạo ra — mà vẫn giữ chung 1 group cha `raw` trên UI.

**Cách làm:**
Dagster cho phép group name lồng nhau (nested/hierarchical group) bằng cách dùng dấu `/` trong `group_name`. UI sẽ hiển thị dạng box lồng trong box theo cấp, và asset selection hỗ trợ wildcard theo cấp (ví dụ `group:"raw/*"` chọn hết asset trong các subgroup con của `raw`). Tham khảo: [Asset metadata and tags — Dagster docs](https://docs.dagster.io/guides/build/assets/metadata-and-tags).

Áp dụng:
- `raw_payments` (dbt seed) → group `raw/seeds`
- `raw_customers`, `raw_orders` (Python asset) → group `raw/python`

**Code trước (`dbt_ingest/defs.yaml`):**
```yaml
  translation:
    group_name: "{{ 'raw' if node.name == 'raw_payments' else group_from_fqn(node.fqn) }}"
```

**Code sau (`dbt_ingest/defs.yaml`):**
```yaml
  translation:
    group_name: "{{ 'raw/seeds' if node.name == 'raw_payments' else group_from_fqn(node.fqn) }}"
```

**Code trước (`assets.py`):**
```python
@dg.asset(key="raw_customers", compute_kind="python", group_name="raw", code_version="v1")
...
@dg.asset(key="raw_orders", compute_kind="python", group_name="raw", code_version="v1")
```

**Code sau (`assets.py`):**
```python
@dg.asset(key="raw_customers", compute_kind="python", group_name="raw/python", code_version="v1")
...
@dg.asset(key="raw_orders", compute_kind="python", group_name="raw/python", code_version="v1")
```

---

## 9. `KeyError: 'seed.jaffle_shop.raw_customers'` khi materialize — Dagster chạy bản dbt project cũ

**Nguyên nhân:**
`DbtProjectComponent` cache một bản copy của dbt project + manifest trong `src/dagster_dbt_demo/defs/.local_defs_state/`. Sau khi sửa `dbt_project.yml` (disable seed), Dagster vẫn dùng bản copy cũ, nên dbt trong Dagster vẫn load seed đã bị disable, trong khi chạy `dbt build` trực tiếp thì bình thường.

**Cách fix:**
Xóa state cache để Dagster dựng lại:
```bash
rm -rf src/dagster_dbt_demo/defs/.local_defs_state
# hoặc
dg utils refresh-defs-state
```
Mỗi khi sửa dbt project mà Dagster không thấy thay đổi → làm bước này. Nên thêm `.local_defs_state/` vào `.gitignore`.

---

## 10. `dbt parse` / `dg dev` treo vô thời hạn (0% CPU) — file trong `.venv` bị iCloud offload

**Nguyên nhân:**
Project nằm trong `~/Desktop`, thư mục được iCloud "Desktop & Documents" sync. iCloud đã offload hơn 4000 file trong `.venv` (flag `dataless`: file còn tên nhưng nội dung không còn trên đĩa). Khi Python import các file này, nó chờ iCloud tải về — nhưng iCloud sync đang tắt (`brctl status` → `SYNC DISABLED`) nên treo mãi. Stack dump (`PYTHONFAULTHANDLER=1` + `kill -ABRT`) dừng ở `importlib ... get_data` khi import `dateutil`.

Kiểm tra:
```bash
find .venv -flags +dataless | wc -l      # > 0 là bị
ls -lO .venv/lib/python3.12/site-packages/dateutil/   # thấy "compressed,dataless"
```

**Cách fix:**
Dựng lại venv ở chỗ iCloud không sync, ví dụ:
```bash
rm -rf .venv
mkdir .venv.nosync && ln -s .venv.nosync .venv   # iCloud bỏ qua tên *.nosync
uv sync
```
Triệt để nhất: dời cả project ra khỏi `~/Desktop` / `~/Documents`.

---

## 11. `sqlite3.DatabaseError: file is not a database` khi chạy `dg dev`

**Nguyên nhân:**
Không set `DAGSTER_HOME` nên `dg dev` dùng thư mục tạm `.tmp_dagster_home_*` trong project (nằm trên Desktop). Các file SQLite trong đó (`schedules.db`, `history/runs/*.db`) bị iCloud offload (`dataless`) giống mục 10 → SQLite đọc ra dữ liệu rác.

**Cách fix:**
Set `DAGSTER_HOME` cố định, nằm ngoài thư mục iCloud sync, và xóa thư mục tạm cũ:
```bash
rm -rf .tmp_dagster_home_*
mkdir -p ~/.dagster_home && touch ~/.dagster_home/dagster.yaml
echo 'export DAGSTER_HOME=~/.dagster_home' >> ~/.zshrc
```

---

## 12. `sqlite3.OperationalError: attempt to write a readonly database` (`PRAGMA journal_mode=WAL;`)

**Nguyên nhân:**
`DAGSTER_HOME=/Volumes/Data/.dagster_home` nằm trên ổ ngoài format **exFAT**. Test 1 process bật WAL trên exFAT vẫn được, nhưng `dg dev` chạy nhiều process (webserver, daemon, code server) cùng mở SQLite ở chế độ WAL — cần file lock + shared memory (`-shm`), thứ exFAT (mount qua fskit) không hỗ trợ ổn định → lỗi readonly. (Nguyên nhân suy luận, chưa tái hiện được đúng kịch bản nhiều process.)

**Cách fix:**
Để `DAGSTER_HOME` trên ổ APFS nội bộ, ngoài thư mục iCloud: `~/.dagster_home` (xem mục 11). Không đặt SQLite storage của Dagster trên exFAT/FAT.

---

## 13. `Required var 'start_date' not found in config` khi load definitions (`dbt compile --quiet` exit code 2)

**Nguyên nhân:**
Khi load definitions, `DbtProjectComponent` chạy `prepare_project_cli_args: ['compile', '--quiet']` **không có `--vars`**. Biến `start_date`, `end_date`, `partition_date` chỉ được truyền ở `cli_args` lúc chạy `build` theo partition. Model `orders.sql` (`var("start_date")`) và `customers.sql` (`var("partition_date")`) không có default → compile lỗi → code location `dagster-dbt` load lỗi.

**Cách fix:**
Khai báo giá trị mặc định trong `dbt_project.yml`. Lúc materialize partition, `--vars` trong `defs.yaml` sẽ override.

**Code sau (`dbt/dbt_project.yml`):**
```yaml
# Defaults so `dbt compile`/`parse` works without --vars (e.g. Dagster's prepare step).
# Partitioned runs from Dagster override these via `--vars` in defs.yaml.
vars:
  start_date: "1900-01-01"
  end_date: "1900-01-02"
  partition_date: "1900-01-01"
```

Lưu ý: description của asset trên UI hiển thị SQL lúc compile, nên sẽ thấy `business_date = '1900-01-01'` — bình thường.

---

## 14. Bổ sung: tự tạo bảng `RAW_CUSTOMERS`/`RAW_ORDERS` nếu chưa tồn tại (chủ động, tránh `ORA-00942`)

**Nguyên nhân:**
Asset Python `TRUNCATE` + `INSERT` vào bảng có sẵn; với schema/DB mới chưa có bảng sẽ lỗi `ORA-00942: table or view does not exist`. Oracle 19c không có `CREATE TABLE IF NOT EXISTS` (chỉ có từ 23ai).

**Cách fix:**
Thêm helper chạy `CREATE TABLE` trong khối PL/SQL, bỏ qua `ORA-00955` (tên đã được dùng), các lỗi khác vẫn raise.

**Code sau (`src/dagster_dbt_demo/defs/assets.py`):**
```python
def _create_table_if_not_exists(cursor, create_statement: str) -> None:
    cursor.execute(
        """
        BEGIN
            EXECUTE IMMEDIATE :ddl;
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """,
        ddl=create_statement,
    )

# trong raw_customers, trước TRUNCATE:
_create_table_if_not_exists(
    cursor,
    "CREATE TABLE RAW_CUSTOMERS ("
    "ID NUMBER PRIMARY KEY, "
    "FIRST_NAME VARCHAR2(100), "
    "LAST_NAME VARCHAR2(100))",
)
# raw_orders tương tự: ID NUMBER PRIMARY KEY, USER_ID NUMBER, ORDER_DATE DATE, STATUS VARCHAR2(50)
```

Bảng được tạo trong schema của user đăng nhập (`ORACLE_USER`) — phải khớp `schema` của source `raw` trong `sources.yml`.

---

## 15. `dg dev`: code server bị kill với `DagsterExecutionInterruptedError` trong `refresh_state` (treo ~2.5 phút)

**Nguyên nhân:**
Khi load definitions, `DbtProjectComponent.refresh_state` copy cả thư mục `dbt/` vào `src/dagster_dbt_demo/defs/.local_defs_state/` rồi chạy `dbt compile`. File `dbt/seeds/.gitkeep` (rỗng, 0 byte) bị iCloud offload (`dataless`) và **không tải về được** — đọc file là treo vô hạn (`cat` timeout). Bước copy treo → quá thời gian chờ code server → `dg dev` kill process (`DagsterExecutionInterruptedError`), để lại file `state` 0 byte.

Không phải lỗi dbt/Oracle: chạy `dbt compile --profiles-dir .` trực tiếp xong trong ~5s, exit 0; `nc -vz 192.168.102.4 1521` thông.

Ngoài ra còn 31 file `dataless` khác (`README.md`, `pack_project.py`, nhiều file trong `.git/`), nhưng các file này iCloud tải về được khi đọc.

**Cách fix:**
```bash
# .gitkeep rỗng nên tạo lại là y hệt
rm -f dbt/seeds/.gitkeep && touch dbt/seeds/.gitkeep

# ép iCloud tải về các file dataless còn lại
find . -path ./.venv.nosync -prune -o -flags +dataless -print > /tmp/dl.txt
while IFS= read -r f; do cat "$f" > /dev/null & done < /tmp/dl.txt; wait

# xóa state hỏng của lần bị kill
rm -rf src/dagster_dbt_demo/defs/.local_defs_state
```
Kiểm tra: `dg check defs` (venv đã activate) → `All definitions loaded successfully.` trong ~9s.

Lưu ý: chừng nào project còn nằm trong `~/Desktop`, iCloud có thể offload file tiếp. Gặp treo khó hiểu → chạy lại lệnh `find ... -flags +dataless` ở trên trước tiên. Triệt để: dời project ra khỏi `~/Desktop`/`~/Documents`.

---

## 16. Bổ sung: chuyển engine dbt từ Oracle sang Apache Doris (nhánh `doris`)

**Bối cảnh:**
Nhánh `doris` tách từ `oracle`. dbt đọc source `raw.raw_customers`/`raw.raw_orders` do Python asset ghi vào, nên đổi engine dbt thì Python asset và script export cũng phải chuyển sang Doris (cùng một database), nếu không lineage `raw_* → stg_*` sẽ trỏ sang bảng không tồn tại.

Server lab: Doris 4.1.3 (1 FE + **1 BE**) tại `192.168.102.4:19030` (MySQL protocol), database `superapp_semantic`, user `root`, không password. Database này còn chứa bảng khác (`customer_360`, `rt_*`, ...), không trùng tên với bảng jaffle_shop.

**Cách làm:**
- `pyproject.toml`: bỏ `dbt-oracle`, thêm `dbt-doris>=1.0.0` (chạy được với `dbt-core 1.12.5`) và `mysql-connector-python` (driver Python asset dùng trực tiếp). Chạy `uv sync`.
- `dbt/profiles.yml`: thay target `oracle` bằng `doris`, đọc từ biến môi trường `DORIS_*`. Doris không có tầng schema: **schema = database**, `database` phải bỏ trống hoặc trùng `schema`.
- `dbt/models/sources.yml`: `schema: dev` → `schema: "{{ target.schema }}"` để source luôn nằm cùng database với target dbt (cũng là database mà Python asset ghi vào).
- `dbt/dbt_project.yml`: Doris mặc định 3 replica/tablet, nên cluster chỉ có 1 BE sẽ lỗi khi `CREATE TABLE`. Set `+properties.replication_num` (mặc định `1`, đổi qua `DORIS_REPLICATION_NUM`) cho models và seeds. `dbt-doris` không nhận config `replication_num` ở cấp project (không có trong `DorisConfig`), phải đi qua `properties`.
- `src/dagster_dbt_demo/defs/utils.py`: `get_connection()` dùng `mysql.connector` với `autocommit=True`. Nếu database chưa có (errno 1049) thì tự `CREATE DATABASE IF NOT EXISTS`. Lỗi không kết nối được (errno 2003/2005) được bọc thành `ConnectionFailedError`. Lưu ý: C-extension của mysql-connector trả về `DatabaseError` chứ không phải `InterfaceError` cho errno 2003, nên phải bắt theo errno.
- `src/dagster_dbt_demo/defs/assets.py`: Doris có `CREATE TABLE IF NOT EXISTS`, nên bỏ khối PL/SQL bắt `ORA-00955` (mục 14). Bảng tạo dạng `UNIQUE KEY (id)` (tương đương primary key), `DISTRIBUTED BY HASH (id) BUCKETS 1`. Tên bảng viết thường vì Doris phân biệt hoa thường tên bảng (`lower_case_table_names=0`). Bind params đổi từ `:1` sang `%s`. `ORDER_DATE DATE` (Oracle, có giờ) đổi thành `order_date DATETIME`, bỏ `TO_DATE`.
- `src/dagster_dbt_demo/defs/my_python_script/export_customers.py`: đọc `customers` từ Doris. Biến `ORACLE_TABLE`/`ORACLE_OUTPUT` đổi thành `DORIS_TABLE`/`DORIS_OUTPUT`.
- `.env.example` (và `.env` local): biến `DORIS_HOST`, `DORIS_PORT`, `DORIS_USER`, `DORIS_PASSWORD`, `DORIS_SCHEMA`, `DORIS_REPLICATION_NUM`.

**Code sau (`dbt/profiles.yml`):**
```yaml
jaffle_shop:
  target: doris
  outputs:
    doris:
      type: doris
      host: "{{ env_var('DORIS_HOST', 'localhost') }}"
      port: "{{ env_var('DORIS_PORT', '9030') | as_number }}"
      username: "{{ env_var('DORIS_USER', 'root') }}"
      password: "{{ env_var('DORIS_PASSWORD', '') }}"
      schema: "{{ env_var('DORIS_SCHEMA', 'jaffle_shop') }}"
      threads: 4
```

**Code sau (`dbt/dbt_project.yml`):**
```yaml
models:
  jaffle_shop:
      materialized: table
      +properties:
        replication_num: "{{ env_var('DORIS_REPLICATION_NUM', '1') }}"
      staging:
        materialized: view

seeds:
  jaffle_shop:
    +properties:
      replication_num: "{{ env_var('DORIS_REPLICATION_NUM', '1') }}"
```

**Code trước / sau (`src/dagster_dbt_demo/defs/assets.py`):**
```python
# Trước (Oracle)
insert_statement = "INSERT INTO RAW_CUSTOMERS (ID, FIRST_NAME, LAST_NAME) VALUES (:1, :2, :3)"
_create_table_if_not_exists(cursor, "CREATE TABLE RAW_CUSTOMERS (ID NUMBER PRIMARY KEY, ...)")  # PL/SQL bỏ qua ORA-00955

# Sau (Doris)
insert_statement = "INSERT INTO raw_customers (id, first_name, last_name) VALUES (%s, %s, %s)"
_create_table_if_not_exists(
    cursor,
    "raw_customers",
    "id INT NOT NULL, first_name VARCHAR(100), last_name VARCHAR(100)",
    key="id",
)  # -> CREATE TABLE IF NOT EXISTS ... UNIQUE KEY (id) DISTRIBUTED BY HASH (id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
```

**Kiểm tra (đã verify end-to-end trên `192.168.102.4:19030/superapp_semantic`):**
```bash
source .venv/bin/activate          # dg cần `dbt` trong PATH
export DAGSTER_HOME=~/.dagster_home
rm -rf src/dagster_dbt_demo/defs/.local_defs_state
dg check defs                       # All definitions loaded successfully.
dg launch --assets 'group:"raw/python" or kind:"dbt"' --partition 2026-10-02
# -> dbt: Done. PASS=26 ERROR=0, RUN_SUCCESS
```
Python asset tự đọc `.env` qua `load_dotenv()` trong `utils.py`, và dbt subprocess do Dagster gọi thừa hưởng các biến đó. Riêng khi chạy `dbt` tay trong `dbt/`, phải nạp env trước: `set -a; source ../.env; set +a`.

---

## 17. Bổ sung: escape dấu `'` trong `COMMENT` của bảng Doris (chủ động)

**Vấn đề phòng ngừa:**
`dbt-doris` 1.0.0 (macro `doris__table_comment`) chèn thẳng description của model vào `COMMENT '...'`, không escape. Description của `customers` có `customer's orders`, nên câu `CREATE TABLE` sẽ hỏng cú pháp ngay tại dấu `'`.

**Cách fix:**
Override macro trong project (macro của root project được ưu tiên hơn macro của adapter), escape `\` và `'`.

**Code sau (`dbt/macros/doris_overrides.sql`):**
```sql
{% macro doris__table_comment() -%}
  {% set description = model.get('description', "") %}
  COMMENT '{{ description | replace("\\", "\\\\") | replace("'", "\\'") }}'
{%- endmacro %}
```
Đã verify: `information_schema.tables.table_comment` của `customers` lưu đúng `... a customer's orders`.

---

## 18. `1105 (HY000): errCode = 2, detailMessage = Unknown column 'total_order_amount' in 'table list' in PROJECT clause` khi build `customers`

**Nguyên nhân:**
Khi model có khai báo `columns:` trong `schema.yml`, `dbt-doris` 1.0.0 (macro `doris__table_colume_type`) luôn bọc SQL thành `select <các cột trong schema.yml> from (<sql model>)`, kể cả khi **không** bật `contract.enforced`. dbt-core chỉ làm vậy khi có contract. Hệ quả:
- `customers`: `schema.yml` mô tả cột `total_order_amount`, nhưng SQL thực sinh ra `customer_lifetime_value`, nên CTAS lỗi `Unknown column`.
- `orders`: build "OK" nhưng **âm thầm mất cột `business_date`**, vì cột này không có trong `schema.yml`. `customers` cũng sẽ mất `customer_lifetime_value` và `business_date`.

Trên Oracle không gặp, vì `dbt-oracle` không dùng danh sách cột trong `schema.yml` để tạo bảng.

**Cách fix:**
Override macro để chỉ project/cast theo danh sách cột khi contract được bật, giống dbt-core. Không sửa `schema.yml`: cột `total_order_amount` chỉ là tài liệu (lệch sẵn từ jaffle_shop gốc), không ảnh hưởng nữa.

**Code trước (macro gốc của `dbt-doris`):**
```sql
{% macro doris__table_colume_type(sql) -%}
    {% set cols = model.get('columns') %}
    {% if cols %}
        select {{get_table_columns_and_constraints()}} from (
            {{sql}}
        ) `_table_colume_type_name`
    {% else %}
        {{sql}}
    {%- endif -%}
{%- endmacro %}
```

**Code sau (`dbt/macros/doris_overrides.sql`):**
```sql
{% macro doris__table_colume_type(sql) -%}
    {% if config.get('contract', {}).enforced %}
        select {{ get_table_columns_and_constraints() }} from (
            {{ sql }}
        ) `_table_colume_type_name`
    {% else %}
        {{ sql }}
    {%- endif -%}
{%- endmacro %}
```
Kiểm tra: `dbt build` → `PASS=26 ERROR=0`. Bảng `customers` có đủ 8 cột (gồm `customer_lifetime_value`, `business_date`), `orders` có `business_date`. Chạy lại lần 2 (nhánh `ALTER TABLE ... REPLACE WITH TABLE` khi bảng đã tồn tại) cũng pass, không còn bảng `__dbt_tmp`.

---

## 19. `DagsterDbtManifestNotFoundError: .../dbt/target/manifest.json does not exist` khi `dg check defs` / `pytest` trên clone mới

**Nguyên nhân:**
`src/dagster_dbt_demo/defs/my_python_script/defs.yaml` khai báo dependency bằng `{{ context.load_component('dbt_ingest').asset_key_for_model('customers') }}`. Hàm `asset_key_for_model` đọc `dbt/target/manifest.json` trong thư mục dbt gốc, **không** đọc bản cache ở `.local_defs_state`. `dbt/target/` bị gitignore nên clone mới (hoặc git worktree mới) chưa có file này → load definitions lỗi ở component `my_python_script`, dù `dbt_ingest` đã load xong.

Thư mục làm việc cũ không gặp vì dbt đã từng chạy ở đó. Với `dg dev` có lẽ cũng không gặp, vì ở chế độ dev dagster-dbt chạy bước prepare (`dbt compile`) trong thư mục dbt gốc trước (suy ra từ code dagster-dbt, chưa kiểm chứng trên clone mới).

Gặp khi đồng bộ code từ nhánh `doris` sang nhánh `oracle` trong một git worktree mới (2026-10-05). Lỗi có sẵn từ trước trên cả 3 nhánh, không do việc đồng bộ.

**Cách fix:**
Chạy `dbt parse` một lần (không cần kết nối database):
```bash
cd dbt && set -a && source ../.env && set +a && dbt parse --profiles-dir .
```
Đã ghi vào `AGENTS.md` (Known environment gotchas), docstring `tests/conftest.py` và mục 5 của `docs/scheduler_guide.md`. Chưa đổi code: có thể bỏ phụ thuộc manifest bằng cách khai báo dependency tĩnh (`deps: [customers]`), nhưng giữ cách `load_component(...)` theo tài liệu Dagster.

---

## Tổng kết kiến trúc sau khi fix

- `src/dagster_dbt_demo/defs/assets.py`: 2 Python asset `raw_customers`, `raw_orders` — truncate rồi insert data mẫu vào Doris database `DORIS_SCHEMA` (lab: `superapp_semantic`) bằng bind params `%s` + `executemany`, trả về `MaterializeResult` với `data_version` hash theo nội dung data và `code_version` cố định để tránh báo "Unsynced" giả.
- `dbt/models/sources.yml`: khai báo source `raw` (schema `{{ target.schema }}`, tức cùng database với target dbt, xem mục 16) với `meta.dagster.asset_key` khớp key của 2 Python asset trên, để Dagster nối đúng dependency giữa dbt model và asset Python.
- `dbt/models/staging/stg_customers.sql`, `stg_orders.sql`: dùng `{{ source('raw', ...) }}` để đọc từ bảng do Python asset insert, thay vì seed/ref cũ.
- `dbt/seeds/raw_customers.csv`, `raw_orders.csv`: không còn dùng (đã đổi tên thành `*_bk.csv`), vì Python asset đã thay thế vai trò ingest raw data.
- `src/dagster_dbt_demo/defs/dbt_ingest/defs.yaml`: thêm điều kiện đặc biệt trong Jinja template của `translation.group_name` để seed `raw_payments` thuộc subgroup `raw/seeds`, fallback `group_from_fqn(node.fqn)` cho các node khác — không cần đổi `template_vars.py`.
- `src/dagster_dbt_demo/defs/assets.py`: asset Python `raw_customers`, `raw_orders` thuộc subgroup `raw/python`.
- `dbt/seeds/schema.yml`: khai báo `meta.dagster.group_name: raw` cho seed `raw_payments` — giữ lại cho rõ ý định, nhưng không có hiệu lực thực tế vì bị `translation.group_name` ở `defs.yaml` override (xem mục 7).
- `src/dagster_dbt_demo/defs/assets.py`: asset `raw_customers`, `raw_orders` tự `CREATE TABLE IF NOT EXISTS` (Doris `UNIQUE KEY (id)`, `replication_num` theo `DORIS_REPLICATION_NUM`) trước khi truncate + insert (xem mục 14, 16).
- `dbt/dbt_project.yml`: khai báo default cho `vars` (`start_date`, `end_date`, `partition_date`) để bước prepare `dbt compile` của Dagster chạy được không cần `--vars` (xem mục 13).
- Môi trường: `DAGSTER_HOME=~/.dagster_home` (APFS, ngoài iCloud); `.venv` không nằm trong thư mục bị iCloud offload (xem mục 10–12).
- Engine (nhánh `doris`, xem mục 16): dbt target `doris` (`dbt-doris` 1.0.0, MySQL protocol) đọc `DORIS_*` từ `.env`. Python asset (`utils.get_connection`, `mysql.connector`, autocommit) và `export_customers.py` dùng cùng database đó. `dbt_project.yml` set `+properties.replication_num` cho models/seeds.
- `dbt/macros/doris_overrides.sql`: override 2 macro của `dbt-doris`: escape `COMMENT` (mục 17), và chỉ project theo cột trong `schema.yml` khi `contract.enforced` (mục 18).
- Lịch chạy khai báo bằng YAML: `src/dagster_dbt_demo/defs/schedules/defs.yaml` dùng component tự viết `CronSchedulesComponent` (`src/dagster_dbt_demo/components/cron_schedules_component.py`). Mỗi phần tử (`name`, `cron`, `selection`, `partition_offset_days`, `timezone`, `running`) sinh job `<name>_job` + schedule `<name>_schedule`; partition key = ngày tick (theo `timezone`) trừ `partition_offset_days`. Hiện có `customers_daily` (`0 1 * * *`, `+key:customers+`) và `orders_daily` (`0 2 * * *`, `+key:orders+`), giờ `Asia/Ho_Chi_Minh`, partition T-1; lịch toàn bộ pipeline `daily_pipeline` (`0 0 * * *`) để dạng comment. Không dùng `build_schedule_from_partitioned_job` vì hàm này không nhận `cron_schedule` cho job partition theo thời gian. Lịch chỉ fire khi daemon chạy (`dg dev`). Cách đặt lịch bằng code (`@dg.schedule` / `build_schedule_from_partitioned_job`) giữ dạng comment ở `src/dagster_dbt_demo/defs/schedules_code.py`. Hướng dẫn cả 2 cách: `docs/scheduler_guide.md`.
- `tests/` (pytest, cấu hình trong `pyproject.toml`): test lịch chạy, cú pháp selection, component YAML, code lịch mẫu đang comment (không cần Doris), và `test_db_integration.py` kiểm tra dữ liệu thật trên Doris (marker `integration`, tự skip nếu không kết nối được). Chạy: `pytest` hoặc `pytest -m "not integration"`.
- `src/dagster_dbt_demo/defs/dbt_ingest/defs.yaml` dùng component tự viết `DbtProjectWithVarsComponent` (`src/dagster_dbt_demo/components/dbt_project_with_vars_component.py`, kế thừa `DbtProjectComponent`): ngoài `cli_args`, gộp thêm vào `--vars` biến từ hàm Python `vars_fn` (`dbt_ingest/dbt_vars.py`) và từ tag `dbt/vars` của run (lịch khai báo bằng field `dbt_vars`). Thứ tự ghi đè: `cli_args` < `vars_fn` < tag. State dbt vẫn dùng key `DbtProjectComponent[...]` nên đổi `type` không cần xoá `.local_defs_state`. Hướng dẫn: `docs/dbt_vars_guide.md`.
- Đồng bộ 3 nhánh `doris` / `oracle` / `postgres` (2026-10-05): file dùng chung (components, lịch, tests trừ `tests/test_db_integration.py`, docs trừ log này, models/seeds dbt, `dbt_ingest/defs.yaml` trừ 1 dòng ví dụ biến môi trường) giống hệt nhau giữa các nhánh. Chỉ khác ở lớp engine: `.env.example`, `pyproject.toml`/`uv.lock` (adapter + driver), `dbt/profiles.yml`, `dbt/dbt_project.yml`, `dbt/models/sources.yml`, `dbt/macros/` (Doris), `defs/utils.py`, `defs/assets.py`, `defs/my_python_script/export_customers.py`, `defs/dbt_ingest/dbt_vars.py` (tên bảng raw), nội dung `tests/test_db_integration.py`, dòng target trong `AGENTS.md`, vài dòng ví dụ trong docs, và log này. Kiểm tra: `git diff doris oracle --stat`.
