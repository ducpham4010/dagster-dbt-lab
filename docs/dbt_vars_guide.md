# Hướng dẫn truyền biến xuống dbt — dagster-dbt

Mỗi lần Dagster chạy dbt, nó gọi `dbt build --vars {...} --select ...`. Trong model dbt, biến được đọc bằng `{{ var('tên_biến') }}`. Tài liệu này mô tả 4 cách đưa thêm biến vào `--vars`, ngoài 3 biến mặc định `start_date`, `end_date`, `partition_date`.

| Mục | Giá trị lấy từ | Khai báo ở | Cần viết code |
|---|---|---|---|
| [1](#1-biến-suy-ra-từ-partition) | Partition của run (ngày, tháng, ngày trước...) | `dbt_ingest/defs.yaml` → `cli_args` | Không |
| [2](#2-hằng-số-và-biến-môi-trường) | Hằng số, biến môi trường (`.env`) | `dbt_ingest/defs.yaml` → `cli_args` | Không |
| [3](#3-biến-riêng-theo-từng-lịch--từng-lần-chạy-tay) | Từng lịch / từng lần chạy tay | `schedules/defs.yaml` → `dbt_vars`, hoặc tag `dbt/vars` trên UI | Không (component đã viết sẵn) |
| [4](#4-biến-tính-bằng-python) | Hàm Python: logic tuỳ ý, đọc database | `dbt_ingest/dbt_vars.py` + `vars_fn` | Có (chỉ viết hàm) |

Ví dụ thật của cả 4 mục đang bật trong project. Các model dbt **chưa dùng** những biến ví dụ này; dbt chấp nhận biến thừa nên chúng không ảnh hưởng gì. Một run của lịch `customers_daily` cho partition `2026-10-02` gửi xuống dbt đúng lệnh sau (đã chạy thật, dbt pass 16/16):

```
dbt build --vars {"start_date": "2026-10-02", "end_date": "2026-10-03", "partition_date": "2026-10-02",
                  "partition_month": "2026-10", "prev_date": "2026-10-01",                          <- mục 1
                  "source_system": "superapp", "target_schema": "superapp_semantic",               <- mục 2
                  "triggered_by": "customers_daily_schedule", "prev_business_date": "2026-10-01",  <- mục 4
                  "is_month_end": false, "quarter": "2026-Q4", "raw_max_order_date": "2018-01-14",
                  "lookback_days": 7}                                                               <- mục 3
          --select ...
```

Các file liên quan:
- `src/dagster_dbt_demo/defs/dbt_ingest/defs.yaml`: cấu hình dbt, `cli_args` (mục 1, 2), `vars_fn` (mục 4).
- `src/dagster_dbt_demo/defs/schedules/defs.yaml`: lịch chạy, `dbt_vars` (mục 3).
- `src/dagster_dbt_demo/defs/dbt_ingest/dbt_vars.py`: hàm Python tính biến (mục 4).
- `src/dagster_dbt_demo/components/dbt_project_with_vars_component.py`: component `DbtProjectWithVarsComponent`, kế thừa `DbtProjectComponent` của dagster-dbt, làm nhiệm vụ gộp mục 3 và 4 vào `--vars`.

---

## 1. Biến suy ra từ partition

Viết bằng Jinja trong `--vars` của `cli_args`. Mỗi run render lại theo partition của run đó.

```yaml
# src/dagster_dbt_demo/defs/dbt_ingest/defs.yaml
  cli_args:
    - build
    - --vars:
        start_date: "{{ partition_time_window.start.strftime('%Y-%m-%d') }}"
        end_date: "{{ partition_time_window.end.strftime('%Y-%m-%d') }}"
        partition_date: "{{ partition_key }}"
        partition_month: "{{ partition_time_window.start.strftime('%Y-%m') }}"                                # 2026-10
        prev_date: "{{ (partition_time_window.start - datetime.timedelta(days=1)).strftime('%Y-%m-%d') }}"    # 2026-10-01
```

Lúc chạy, Jinja trong `cli_args` **chỉ** thấy những biến sau:

| Biến | Ý nghĩa |
|---|---|
| `partition_key` | Chuỗi partition, vd `2026-10-02` |
| `partition_time_window.start` / `.end` | `datetime` đầu/cuối khoảng thời gian của partition (giờ VN) |
| `partition_key_range` | Khoảng partition (khi một run chạy nhiều partition) |
| `datetime` | Chỉ có `datetime.datetime` và `datetime.timedelta` |
| `env` | Biến môi trường, xem mục 2 |

Hàm tự viết trong `template_vars.py` **không** dùng được ở đây, vì chúng chỉ có lúc load chứ không có lúc chạy. Dùng sẽ báo:
`UndefinedError: 'shift_date' not found in scope, available scope is: env, dg, datetime, automation_condition, partition_key, partition_key_range, partition_time_window`.
Logic phức tạp hơn mức trên thì chuyển sang [mục 4](#4-biến-tính-bằng-python).

---

## 2. Hằng số và biến môi trường

```yaml
        source_system: superapp                                    # hằng số
        target_schema: "{{ env('DORIS_SCHEMA', 'jaffle_shop') }}"  # biến môi trường, có giá trị mặc định
```

- `env('TÊN', 'mặc định')` đọc biến môi trường của process chạy run, tức các biến trong `.env`. Thiếu biến mà không có giá trị mặc định thì run lỗi.
- Hằng số dùng chung cho mọi lần chạy cũng có thể khai báo trực tiếp trong `vars:` của `dbt/dbt_project.yml`, không cần qua Dagster.

---

## 3. Biến riêng theo từng lịch / từng lần chạy tay

Dùng khi các lịch cần giá trị khác nhau. Ví dụ lịch `customers` cần `lookback_days: 7`, còn lịch khác không cần, hoặc một lần chạy tay cần `lookback_days: 30`.

Cơ chế: run mang tag **`dbt/vars`** (chuỗi JSON object). `DbtProjectWithVarsComponent` đọc tag này và gộp vào `--vars`.

### Theo từng lịch: field `dbt_vars`

```yaml
# src/dagster_dbt_demo/defs/schedules/defs.yaml
    - name: customers_daily
      cron: "0 1 * * *"
      selection: "+key:customers+"
      partition_offset_days: 1
      dbt_vars:
        lookback_days: 7
```

Mỗi tick của lịch tạo run có tag `dbt/vars = {"lookback_days": 7}`. Lịch `orders_daily` không khai báo `dbt_vars` nên run của nó không có tag này. Giá trị có thể là chuỗi, số hoặc `true`/`false`.

### Từng lần chạy tay trên UI

Trên trang asset hoặc job, mở **Launchpad** (bấm mũi tên cạnh nút `Materialize` → `Open launchpad`, hoặc Shift + bấm `Materialize`). Chọn **Edit tags**, thêm:

| Key | Value |
|---|---|
| `dbt/vars` | `{"lookback_days": 30}` |

Value phải là JSON object hợp lệ (dấu nháy kép). Sai định dạng thì step dbt báo `ValueError: Tag dbt/vars phải là JSON object, nhận được: ...`.

---

## 4. Biến tính bằng Python

Dùng khi giá trị cần logic Python (lịch làm việc, quý tài chính, điều kiện...) hoặc phải **đọc dữ liệu** (watermark, cấu hình trong bảng...).

Khai báo hàm trong `defs.yaml`:

```yaml
# src/dagster_dbt_demo/defs/dbt_ingest/defs.yaml
type: dagster_dbt_demo.components.DbtProjectWithVarsComponent
...
attributes:
  ...
  vars_fn: dagster_dbt_demo.defs.dbt_ingest.dbt_vars.compute_dbt_vars
```

Hàm nhận `context` của step dbt và trả về `dict`. Ví dụ đang dùng:

```python
# src/dagster_dbt_demo/defs/dbt_ingest/dbt_vars.py
def compute_dbt_vars(context: dg.AssetExecutionContext) -> dict[str, Any]:
    dbt_vars = {
        # Lịch nào kích hoạt run ("manual" nếu chạy tay / dg launch).
        "triggered_by": context.run.tags.get("dagster/schedule_name", "manual"),
    }
    if context.has_partition_key:
        day = date.fromisoformat(context.partition_key)
        dbt_vars["prev_business_date"] = previous_business_day(day).isoformat()  # bỏ thứ 7, CN
        dbt_vars["is_month_end"] = (day + timedelta(days=1)).day == 1
        dbt_vars["quarter"] = f"{day.year}-Q{(day.month - 1) // 3 + 1}"
    # Đọc Doris: ngày đặt hàng mới nhất trong bảng raw (vd làm watermark cho model incremental).
    dbt_vars["raw_max_order_date"] = raw_max_order_date()
    return dbt_vars
```

Những gì dùng được trong `context`:

| Thuộc tính | Ví dụ |
|---|---|
| `context.has_partition_key`, `context.partition_key` | `True`, `"2026-10-02"` |
| `context.partition_time_window.start` / `.end` | `datetime` đầu/cuối partition |
| `context.run.tags` | `{"dagster/schedule_name": "customers_daily_schedule", "dbt/vars": "...", ...}` |
| `context.run.run_id` | id của run, vd để ghi audit |
| `context.log.info(...)` | ghi log lên UI |

Giải thích đầy đủ về `context`: `docs/asset_execution_context.md`.

Lưu ý:
- Hàm chạy **mỗi lần** step dbt chạy, bên trong run, sau khi các asset raw trong cùng run đã chạy xong. Vì vậy `raw_max_order_date` thấy dữ liệu vừa nạp.
- Hàm lỗi (vd mất kết nối Doris) thì step dbt lỗi theo.
- `vars_fn` chỉ được import lúc run chạy. Gõ sai đường dẫn thì `dg check defs` vẫn qua, đến lúc chạy mới lỗi. Test `tests/test_dbt_vars.py::test_defs_yaml_vars_fn_is_importable` bắt lỗi này sớm.
- Log của step dbt có dòng `Biến dbt bổ sung: {...}` liệt kê các biến từ mục 3 và 4.

---

## Thứ tự ghi đè

Khi nhiều nguồn có cùng tên biến, nguồn sau ghi đè nguồn trước:

```
cli_args trong defs.yaml (mục 1, 2)  <  vars_fn (mục 4)  <  tag dbt/vars (mục 3)
```

Ví dụ `defs.yaml` có `prev_date: 2026-10-01` mà chạy tay với tag `dbt/vars = {"prev_date": "2026-09-30"}`, thì dbt nhận `2026-09-30`. Tag đứng cuối để người chạy tay (hoặc từng lịch) luôn có quyền quyết định cuối cùng. Thứ tự này được kiểm tra bởi `tests/test_dbt_vars.py::test_override_order`.

---

## Dùng biến trong model dbt

```sql
-- dbt/models/analystic/customers.sql (ví dụ)
select ...,
    '{{ var("quarter") }}' as quarter,
    {{ var("lookback_days") }} as lookback_days
from ...
where order_date >= date_sub('{{ var("partition_date") }}', interval {{ var("lookback_days") }} day)
```

**Bắt buộc:** mọi biến model dùng phải có giá trị mặc định trong `dbt/dbt_project.yml`. Lúc load, Dagster chạy `dbt compile` **không** kèm `--vars`, và chạy `dbt` tay cũng vậy:

```yaml
# dbt/dbt_project.yml
vars:
  start_date: "1900-01-01"
  end_date: "1900-01-02"
  partition_date: "1900-01-01"
  quarter: "1900-Q1"
  lookback_days: 1
```

Thiếu mặc định thì lỗi `Required var 'quarter' not found in config` (xem mục 13 trong `docs/troubleshooting_log.md`). Có thể thay bằng `{{ var("quarter", "1900-Q1") }}` ngay trong model.

---

## Kiểm tra

- **Xem dbt nhận biến gì:** trên UI mở run → log của step `jaffle_shop`, tìm dòng `dbt build --vars {...}` và `Biến dbt bổ sung: {...}`.
- **Chạy test** (không cần Doris):
  ```bash
  pytest tests/test_dbt_vars.py -v
  ```
  Bộ test kiểm tra việc gộp `--vars`, tag `dbt/vars`, `compute_dbt_vars` (ngày làm việc trước, cuối tháng, quý), thứ tự ghi đè, `vars_fn` import được, lịch `customers_daily` có tag còn `orders_daily` thì không. `tests/test_doris_integration.py::test_raw_max_order_date_reads_doris` kiểm tra hàm đọc Doris.
- **Chạy thử một partition:** `dg launch --job orders_daily_job --partition 2026-10-02`. Chạy theo cách này không có tag của lịch, nên `triggered_by = "manual"` và không có `lookback_days`. Lưu ý: partition của hôm nay chưa tồn tại (ngày chưa kết thúc), chạy sẽ báo `DagsterUnknownPartitionError`.

---

## Lỗi thường gặp

| Lỗi | Nguyên nhân | Cách xử lý |
|---|---|---|
| `UndefinedError: '...' not found in scope, available scope is: env, dg, datetime, ...` | Dùng template var / hàm tự viết trong `cli_args` | Chỉ dùng các biến ở bảng mục 1; logic phức tạp chuyển sang `vars_fn` (mục 4) |
| `Required var '...' not found in config` | Model dùng biến chưa có mặc định | Thêm vào `vars:` của `dbt_project.yml` hoặc `var('x', mặc_định)` |
| `ValueError: Tag dbt/vars phải là JSON object, nhận được: ...` | Tag `dbt/vars` sai định dạng | Dùng JSON object: `{"lookback_days": 30}` |
| `ModuleNotFoundError` / `AttributeError` ở step dbt | `vars_fn` sai đường dẫn | Sửa `vars_fn`; chạy `pytest tests/test_dbt_vars.py` |
| `Environment variable X is not set and no default value was provided` | `env('X')` không có giá trị mặc định và thiếu biến | Thêm `X` vào `.env` hoặc dùng `env('X', 'mặc định')` |
