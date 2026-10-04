# AssetExecutionContext — dagster-dbt

`AssetExecutionContext` là object Dagster truyền vào hàm asset **lúc asset đang chạy**. Nó cho biết đang chạy asset nào, trong run nào, cho partition nào, và cung cấp công cụ để ghi log, metadata lên UI.

Tài liệu dựa trên Dagster **1.13.24** (bản đang cài). Source: `.venv/lib/python3.12/site-packages/dagster/_core/execution/context/asset_execution_context.py`. Mọi ví dụ bên dưới đều đã chạy thử.

```python
import dagster as dg

@dg.asset
def raw_customers(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info("Bắt đầu nạp raw_customers")
    ...
```

- Khai báo bằng tham số `context` (kèm type hint) ở đầu hàm, Dagster tự truyền vào.
- Mỗi step của run có một context riêng.
- Không cần dùng thì bỏ tham số này đi cũng được.

---

## 1. Ba loại context trong project

Dễ nhầm vì đều tên `context`, nhưng mỗi loại xuất hiện ở một thời điểm khác nhau:

| Context | Có khi nào | Dùng để | Ở project này |
|---|---|---|---|
| `ComponentLoadContext` | Lúc **load** definitions (`dg dev`, `dg check defs`) | Dựng asset/job/schedule | `build_defs(self, context)` trong `components/cron_schedules_component.py` |
| `ScheduleEvaluationContext` | Lúc lịch **tick** | Quyết định có chạy không, chạy partition nào | `context.scheduled_execution_time` trong lịch |
| **`AssetExecutionContext`** | Lúc asset **đang chạy** trong một run | Đọc partition, tag của run; ghi log, metadata | `assets.py`, `export_ui.py`, `dbt_ingest/dbt_vars.py`, component dbt |

Hệ quả: những gì chỉ biết lúc chạy (partition của run, tag `dbt/vars`, lịch nào kích hoạt) phải đọc từ `AssetExecutionContext`. Đây là lý do biến dbt tính bằng Python phải đặt trong `vars_fn` (xem `docs/dbt_vars_guide.md`, mục 4).

---

## 2. Log lên UI: `context.log`

```python
context.log.info("Đã nạp 10 dòng")
context.log.warning("Bảng nguồn rỗng")
context.log.error("Export thất bại:\n" + traceback.format_exc())
```

Log hiện trong trang run trên UI, gắn với đúng step. Trong project:
- `defs/my_python_script/export_ui.py`: `context.log.error(...)` khi export lỗi.
- `components/dbt_project_with_vars_component.py`: `context.log.info(f"Biến dbt bổ sung: {extra_vars}")`.

---

## 3. Thông tin run: `context.run`

`context.run` là một `DagsterRun`:

| Thuộc tính | Ví dụ |
|---|---|
| `context.run.run_id` | `"1f0c..."` |
| `context.run.tags` | `{"dagster/schedule_name": "customers_daily_schedule", "dagster/partition": "2026-10-02", "dbt/vars": "{\"lookback_days\": 7}"}` |
| `context.run.run_config` | config của run |
| `context.job_name` | `"customers_daily_job"` (nằm trực tiếp trên context, không qua `run`) |

Tag hay gặp:

| Tag | Ai gắn | Ý nghĩa |
|---|---|---|
| `dagster/schedule_name` | Dagster, khi run do lịch tạo | Tên lịch. Chạy tay thì không có tag này |
| `dagster/partition` | Dagster | Partition của run |
| `dbt/vars` | Lịch có `dbt_vars`, hoặc người chạy tay thêm trên UI | Biến dbt riêng của run (`docs/dbt_vars_guide.md`, mục 3) |

Ví dụ trong `defs/dbt_ingest/dbt_vars.py`:

```python
"triggered_by": context.run.tags.get("dagster/schedule_name", "manual"),
```

---

## 4. Partition

| Thuộc tính | Ý nghĩa |
|---|---|
| `context.has_partition_key` | `True` nếu run chạy cho đúng 1 partition |
| `context.partition_key` | Vd `"2026-10-02"` |
| `context.partition_time_window.start` / `.end` | `datetime` đầu/cuối partition, vd `2026-10-02T00:00:00+07:00` → `2026-10-03T00:00:00+07:00`. Chỉ có với partition theo thời gian |
| `context.has_partition_key_range`, `context.partition_key_range`, `context.partition_keys` | Khi một run chạy nhiều partition cùng lúc (backfill gộp) |

**Luôn kiểm `has_partition_key` trước khi đọc `partition_key`.** Run không có partition (asset không partition, chạy tay không chọn partition) mà vẫn đọc `partition_key` thì sẽ lỗi:
`CheckError: Failure condition: Tried to access partition_key for a non-partitioned run`

```python
# defs/dbt_ingest/dbt_vars.py
if context.has_partition_key:
    day = date.fromisoformat(context.partition_key)
    ...
```

---

## 5. Asset đang chạy

| Thuộc tính | Ý nghĩa |
|---|---|
| `context.asset_key` | Key của asset (dùng với asset đơn), vd `AssetKey(["raw_customers"])` |
| `context.selected_asset_keys` | Các asset **được chọn chạy** trong step này |
| `context.assets_def` | Toàn bộ định nghĩa asset (`partitions_def`, deps...) |

`selected_asset_keys` quan trọng với dbt. Toàn bộ model dbt là **một** multi-asset (step `jaffle_shop`). `dagster-dbt` đọc `context.selected_asset_keys` để sinh `--select`. Vì vậy `orders_daily_job` chỉ build `stg_orders`, `stg_payments`, `orders`, chứ không build cả project dbt.

---

## 6. Ghi metadata

Có 2 cách, kết quả như nhau trên UI (tab metadata của lần materialize):

**Trả về `MaterializeResult`.** Đây là cách `defs/assets.py` đang dùng, không cần `context`:

```python
return dg.MaterializeResult(
    data_version=_data_version_for(rows),
    metadata={"num_rows": len(rows)},
)
```

**Gọi `context.add_output_metadata`** khi hàm không trả về gì:

```python
@dg.asset
def plain_asset(context: dg.AssetExecutionContext):
    ...
    context.add_output_metadata({"num_rows": 10})
```

Ngoài ra còn có:
- `context.add_asset_metadata(...)`: gắn metadata cho một asset cụ thể trong multi-asset.
- `context.log_event(...)`: phát sự kiện tuỳ ý, vd `dg.AssetObservation`.

---

## 7. Ví dụ đầy đủ

```python
import dagster as dg

daily = dg.DailyPartitionsDefinition(start_date="2026-09-01", timezone="Asia/Ho_Chi_Minh")


@dg.asset(partitions_def=daily)
def example_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    window = context.partition_time_window
    triggered_by = context.run.tags.get("dagster/schedule_name", "manual")
    context.log.info(f"run={context.run.run_id} partition={context.partition_key} by={triggered_by}")
    # asset_key = example_asset, window = 2026-10-02T00:00+07:00 -> 2026-10-03T00:00+07:00
    return dg.MaterializeResult(metadata={"rows": 10, "triggered_by": triggered_by})


# Chạy thử ngay trong Python, giả lập run do lịch tạo:
result = dg.materialize(
    [example_asset],
    partition_key="2026-10-02",
    tags={"dagster/schedule_name": "customers_daily_schedule"},
)
assert result.success   # metadata: {"rows": 10, "triggered_by": "customers_daily_schedule"}
```

---

## 8. Lấy context trong hàm phụ: `AssetExecutionContext.get()`

Không cần truyền `context` qua nhiều tầng hàm: trong lúc asset đang chạy, hàm phụ có thể tự lấy context hiện tại.

```python
def helper() -> str:
    context = dg.AssetExecutionContext.get()
    context.log.info("log từ hàm phụ")
    return context.asset_key.to_user_string()
```

- Gọi ngoài lúc asset chạy (vd trong test, lúc load) sẽ lỗi: `DagsterInvariantViolationError: No current AssetExecutionContext in scope.`
- Trong source, hàm này **không** được đánh dấu `@public`. Nên ưu tiên truyền `context` qua tham số; chỉ dùng `get()` khi truyền quá phiền.

---

## 9. Các thuộc tính khác

| Thuộc tính | Ý nghĩa |
|---|---|
| `context.resources` | Resource đã gắn cho asset |
| `context.instance` | Dagster instance (lưu trữ ở `DAGSTER_HOME`) |
| `context.retry_number` | Lần retry thứ mấy của step |
| `context.pdb.set_trace()` | Dừng để debug trong lúc asset chạy (debug ở terminal đang chạy `dg dev`) |
| `context.op_execution_context` | Context của op bên dưới, dùng cho các thuộc tính cũ ở mục 10 |

---

## 10. Thuộc tính cũ đã deprecated

`AssetExecutionContext` thực chất bọc ngoài `OpExecutionContext` (context của op, khái niệm cũ hơn asset). Các thuộc tính cũ vẫn chạy nhưng sẽ cảnh báo và bị bỏ trong bản sau:

| Cũ | Dùng thay |
|---|---|
| `context.run_id` | `context.run.run_id` |
| `context.dagster_run` | `context.run` |
| `context.run_config` | `context.run.run_config` |
| `context.run_tags` | `context.run.tags` |
| `context.get_tag(k)` / `context.has_tag(k)` | `context.run.tags.get(k)` / `k in context.run.tags` |
| `context.asset_partition_key_for_output()` | `context.partition_key` |
| `context.asset_partitions_time_window_for_output()` | `context.partition_time_window` |
| `context.asset_partition_key_range_for_output()` | `context.partition_key_range` |
| `context.asset_partition_keys_for_output()` | `context.partition_keys` |
| `context.asset_partitions_def_for_output()` | `context.assets_def.partitions_def` |
| `context.op_config`, `context.op`, `context.op_handle`, `context.get_mapping_key()`... | `context.op_execution_context.<tên cũ>` |

---

## 11. Context trong test

`dg.build_asset_context(...)` tạo context để gọi thẳng hàm asset mà không cần chạy run:

```python
context = dg.build_asset_context(partition_key="2026-10-02", run_tags={"dbt/vars": '{"a": 1}'})
context.partition_key                        # "2026-10-02"
context.op_execution_context.run_tags        # {"dbt/vars": '{"a": 1}'}
```

Hạn chế: context này **không có `context.run`**. Truy cập vào sẽ báo:
`DagsterInvalidPropertyError: The dagster_run property is not set on the context when an asset or op is directly invoked.`

Vì `compute_dbt_vars` đọc `context.run.tags`, `tests/test_dbt_vars.py` dùng context giả tự dựng, chỉ có đúng những thuộc tính hàm cần:

```python
from types import SimpleNamespace

def fake_context(partition_key=None, tags=None):
    return SimpleNamespace(
        has_partition_key=partition_key is not None,
        partition_key=partition_key,
        run=SimpleNamespace(tags=tags or {}),
        log=SimpleNamespace(info=lambda message: None),
    )
```

Muốn có `context.run` thật thì chạy asset qua `dg.materialize([...], partition_key=..., tags=...)` như ở mục 7.

---

## 12. Bản đồ dùng `context` trong project

| File | Dùng gì |
|---|---|
| `src/dagster_dbt_demo/defs/assets.py` | Nhận `context` nhưng chưa dùng; metadata trả qua `MaterializeResult` |
| `src/dagster_dbt_demo/defs/my_python_script/export_ui.py` | `context.log.error` khi export lỗi |
| `src/dagster_dbt_demo/defs/dbt_ingest/dbt_vars.py` | `context.run.tags`, `context.has_partition_key`, `context.partition_key` |
| `src/dagster_dbt_demo/components/dbt_project_with_vars_component.py` | `context.run.tags` (tag `dbt/vars`), `context.log.info`; truyền `context` cho dbt |
| `dagster-dbt` (thư viện) | `context.selected_asset_keys` → `--select`; partition → biến trong `cli_args` |

Tài liệu liên quan: `docs/dbt_vars_guide.md` (truyền biến xuống dbt), `docs/scheduler_guide.md` (lịch chạy).
