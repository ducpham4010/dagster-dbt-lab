# Hướng dẫn đặt lịch chạy (scheduler) — dagster-dbt

Project có 2 cách đặt lịch cho pipeline:

| Cách | File | Trạng thái |
|---|---|---|
| **1. Khai báo bằng YAML** | `src/dagster_dbt_demo/defs/schedules/defs.yaml` | **Đang dùng** |
| **2. Viết bằng code Python** (2 biến thể A/B) | `src/dagster_dbt_demo/defs/schedules_code.py` | Đang tắt (toàn bộ là comment) |

> Với cùng một nhóm asset, **chỉ đặt lịch ở một nơi**. Trùng tên sẽ lỗi `Duplicate definition found for ..._schedule`; khác tên thì asset bị chạy 2 lần.

Lịch hiện tại: **mỗi model analystic một giờ riêng**, đều chạy cho **partition của ngày hôm trước** (giờ Việt Nam):

| Schedule | Cron | Chạy các asset |
|---|---|---|
| `customers_daily_schedule` | `0 1 * * *` (01:00) | raw + staging cần cho `customers`, `customers`, `customers_export` |
| `orders_daily_schedule` | `0 2 * * *` (02:00) | `raw_orders`, `raw_payments`, `stg_orders`, `stg_payments`, `orders` |

Lịch cũ `daily_pipeline` (toàn bộ pipeline lúc 00:00) đang để dạng comment trong YAML. Chi tiết ở [mục 2](#mỗi-model-analystic-một-giờ-riêng-đang-dùng).

---

## 1. Khái niệm cần nắm

- **Job**: tập asset được chạy cùng nhau (vd `orders_daily_job` = `orders` và các asset phía trước nó).
- **Schedule**: lịch cron, mỗi lần tới giờ (một **tick**) thì gửi yêu cầu chạy job.
- **Partition**: các asset dbt được chia partition **theo ngày**, timezone `Asia/Ho_Chi_Minh`, bắt đầu từ `2026-09-01`. Cấu hình ở `src/dagster_dbt_demo/defs/dbt_ingest/template_vars.py`. Mỗi run chạy cho đúng 1 partition (1 ngày).

### Tick lúc 0h ngày T chạy partition nào?

Partition ngày `X` là khoảng `[X 00:00, X+1 00:00)` giờ VN. Mặc định Dagster chỉ tạo partition cho những ngày **đã kết thúc**, nên lúc 0h (hay 1h, 2h...) ngày T, partition mới nhất là **T-1**.

| Tick | Partition chạy | `partition_date` | `start_date` | `end_date` |
|---|---|---|---|---|
| 2026-10-05 00:00 | `2026-10-04` | 2026-10-04 | 2026-10-04 | 2026-10-05 |
| 2026-10-05 01:00 | `2026-10-04` | 2026-10-04 | 2026-10-04 | 2026-10-05 |

`partition_date`, `start_date`, `end_date` là các biến dbt mà Dagster truyền qua `--vars` (xem `dbt_ingest/defs.yaml`). `customers.business_date` lấy `partition_date`, `orders.business_date` lấy `start_date`.

### Muốn chạy dữ liệu của chính ngày T

Cần **2 bước**:

1. Bật `end_offset=1` trong `template_vars.py` (đang để sẵn dạng comment) để partition của ngày đang diễn ra tồn tại ngay từ 00:00.
2. Bảo schedule chạy partition T: YAML đặt `partition_offset_days: 0`, code cách A đổi `days=1` → `days=0`, còn code cách B thì không cần sửa gì vì nó tự theo `end_offset`.

Lưu ý: lúc 1h ngày T thì ngày T mới có khoảng 1 giờ dữ liệu. Ngoài ra `end_offset` áp dụng cho **toàn bộ** asset dbt, nên partition hôm nay luôn hiện "missing" trên UI cho tới khi chạy. Thiếu bước 1 thì tick lỗi: `DagsterUnknownPartitionError: Could not find a partition with key 2026-10-05`.

---

## 2. Cách 1 — Khai báo bằng YAML (đang dùng)

- Lịch: `src/dagster_dbt_demo/defs/schedules/defs.yaml`
- Component đọc YAML (viết một lần, không cần sửa khi thêm lịch): `src/dagster_dbt_demo/components/cron_schedules_component.py`

```yaml
type: dagster_dbt_demo.components.CronSchedulesComponent

attributes:
  schedules:
    - name: customers_daily
      cron: "0 1 * * *"
      selection: "+key:customers+"
      partition_offset_days: 1

    - name: orders_daily
      cron: "0 2 * * *"
      selection: "+key:orders+"
      partition_offset_days: 1
```

Mỗi phần tử trong `schedules` sinh ra **1 job `<name>_job` + 1 schedule `<name>_schedule`**. Ví dụ trên tạo ra `customers_daily_job` / `customers_daily_schedule` và `orders_daily_job` / `orders_daily_schedule`.

### Các field

| Field | Bắt buộc | Mặc định | Ý nghĩa |
|---|---|---|---|
| `name` | có | | Tên gốc của job/schedule. Không trùng giữa các phần tử. |
| `cron` | có | | Crontab 5 trường `phút giờ ngày tháng thứ`, tính theo `timezone`. Nên để trong dấu `"..."`. |
| `selection` | | `"*"` | Asset cần chạy, theo cú pháp chọn asset của Dagster (bảng bên dưới). |
| `partition_offset_days` | | `1` | Partition chạy = ngày tick − N ngày. `1` = T-1, `0` = T (cần `end_offset=1`). Đặt `null` nếu `selection` không có asset nào chia partition. |
| `timezone` | | `Asia/Ho_Chi_Minh` | Timezone của cron và của việc tính ngày. Nên trùng timezone của partition. |
| `running` | | `true` | `true` = lịch tự bật khi load; `false` = để tắt, bật tay trên UI. |
| `description` | | | Mô tả hiện trên UI. |
| `dbt_vars` | | | Biến dbt riêng cho lịch này, vd `{lookback_days: 7}`. Gắn vào run qua tag `dbt/vars`, ghi đè biến cùng tên. Xem mục 3 trong `docs/dbt_vars_guide.md`. |

### Cú pháp `selection` hay dùng

Kết quả dưới đây là danh sách asset thực tế của project này.

| `selection` | Chọn được |
|---|---|
| `"*"` | toàn bộ 9 asset |
| `"group:staging"` | `stg_customers`, `stg_orders`, `stg_payments` |
| `'group:"raw/python"'` | `raw_customers`, `raw_orders` (tên group có `/` phải đặt trong `"..."`) |
| `"key:customers"` | chỉ `customers` |
| `"key:customers+"` | `customers` + các asset phía sau (`customers_export`) |
| `"+key:customers"` | `customers` + toàn bộ asset phía trước (raw, seed, staging) |
| `"kind:dbt"` | toàn bộ asset dbt (không gồm Python asset và export) |
| `"group:staging or group:analystic"` | staging + analystic |
| `"kind:dbt and not group:staging"` | `customers`, `orders`, `raw_payments` |
| `"+key:customers+"` | `customers` + toàn bộ asset phía trước + phía sau (`customers_export`) |
| `"+key:orders+"` | `orders` + `raw_orders`, `raw_payments`, `stg_orders`, `stg_payments` (`orders` không có asset phía sau) |

### Mỗi model analystic một giờ riêng (đang dùng)

Mỗi model là một phần tử riêng, có `cron` riêng. `selection` dạng `+key:<model>+` để **mỗi lịch tự chạy đủ** những gì model đó cần: raw → staging → model → asset phía sau. Hai lịch không phụ thuộc nhau, nên lịch này lỗi hay trễ thì lịch kia vẫn đọc dữ liệu mới.

Đổi giờ thì chỉ sửa `cron`. Thêm model analystic mới thì thêm một phần tử y hệt, đổi `name`, `cron`, `selection`.

Lưu ý:
- **Không đặt 2 lịch cùng giờ.** Cả hai cùng nạp lại `raw_orders`, `raw_payments` (truncate rồi insert). Chạy cùng lúc thì một run có thể đọc đúng lúc run kia vừa truncate. Mỗi lần chạy mất khoảng 10–20 giây, nên đặt cách nhau vài phút là đủ.
- Raw và staging bị chạy lại ở mỗi lịch. Với dữ liệu nhỏ như hiện tại thì không đáng kể.
- Đã chạy thử: job `customers` chỉ build `stg_*` + `customers`; job `orders` chỉ build `stg_orders`, `stg_payments` + `orders`. Mỗi bảng mang `business_date` của partition run tương ứng.

**Cách khác: tách tầng.** Một lịch nạp raw + staging (`"+group:staging"`) lúc 00:00, rồi mỗi model chỉ chạy chính nó (`"key:customers+"` lúc 01:00, `"key:orders"` lúc 02:00). Cách này không chạy lặp raw/staging, nhưng phụ thuộc thời gian: nếu lịch 00:00 lỗi hoặc chạy quá 1 tiếng, model vẫn chạy trên dữ liệu cũ mà vẫn gắn `business_date` mới.

### Thêm một lịch mới

Thêm phần tử vào `schedules`, sau đó chạy kiểm tra ở [mục 5](#5-kiểm-tra-trước-khi-chạy-thật). Một số ví dụ:

```yaml
    # 01:00 mỗi ngày, chỉ chạy các model dbt, partition T-1
    - name: dbt_nightly
      cron: "0 1 * * *"
      selection: "kind:dbt"

    # 06:30 thứ 2 -> thứ 6
    - name: weekday_morning
      cron: "30 6 * * 1-5"
      selection: "*"

    # Nạp lại 2 bảng raw mỗi 6 tiếng. Asset không chia partition -> partition_offset_days: null.
    # Tạo xong để tắt, muốn chạy thì bật trên UI.
    - name: raw_refresh
      cron: "0 */6 * * *"
      selection: 'group:"raw/python"'
      partition_offset_days: null
      running: false
```

- **Sửa giờ chạy:** sửa `cron`.
- **Xoá lịch:** xoá phần tử đó (job đi kèm cũng mất theo).
- **Tắt tạm:** đặt `running: false`, hoặc tắt trên UI (xem [mục 6](#6-vận-hành)).

---

## 3. Cách 2 — Viết bằng code Python (đang tắt)

File `src/dagster_dbt_demo/defs/schedules_code.py` chứa sẵn code của cả 2 biến thể dưới dạng comment.

### Bật cách này

1. Trong `defs/schedules/defs.yaml`, comment các lịch đang bật (`customers_daily`, `orders_daily`), hoặc đổi `schedules:` thành `schedules: []`. Code mẫu chạy toàn bộ pipeline, nên để lịch YAML bật song song thì `customers`/`orders` sẽ bị chạy 2 lần.
2. Trong `schedules_code.py`, bỏ comment khối **"Dùng chung"** và **chỉ một** trong hai khối **Cách A** hoặc **Cách B**. Bôi đen khối rồi bấm `Cmd+/`.
3. Chạy kiểm tra ở [mục 5](#5-kiểm-tra-trước-khi-chạy-thật).

Khối "Dùng chung" định nghĩa job:

```python
daily_pipeline_job = dg.define_asset_job(
    name="daily_pipeline_job",
    selection=dg.AssetSelection.all(),
)
```

### Cách A — `@dg.schedule` + chuỗi crontab

Viết được crontab bất kỳ, tự tính partition từ giờ tick. Đây cũng chính là cách component YAML làm bên trong.

```python
TIMEZONE = "Asia/Ho_Chi_Minh"

@dg.schedule(
    cron_schedule="0 0 * * *",
    job=daily_pipeline_job,
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def daily_pipeline_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    run_date = context.scheduled_execution_time.astimezone(ZoneInfo(TIMEZONE))
    partition_key = (run_date - timedelta(days=1)).strftime("%Y-%m-%d")
    return dg.RunRequest(partition_key=partition_key)
```

- Tên schedule = tên hàm: `daily_pipeline_schedule`.
- `.astimezone(...)` đổi giờ tick về giờ VN trước khi trừ ngày. Thiếu bước này thì nếu Dagster truyền giờ UTC, kết quả có thể lệch 1 ngày.

### Cách B — `build_schedule_from_partitioned_job`

Để Dagster tự sinh lịch từ partition. Giờ chạy khai báo bằng tham số chứ **không** bằng chuỗi cron:

```python
daily_pipeline_schedule = dg.build_schedule_from_partitioned_job(
    daily_pipeline_job,
    hour_of_day=0,       # 0..23
    minute_of_hour=0,    # 0..59
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
```

- Nhịp chạy lấy theo partition (daily nghĩa là mỗi ngày một lần). Chỉ chỉnh được độ lệch giờ bằng `hour_of_day`, `minute_of_hour` (và `day_of_week`, `day_of_month` cho partition tuần/tháng). Ví dụ trên sinh ra cron `0 0 * * *`.
- Timezone lấy từ partition (`Asia/Ho_Chi_Minh`).
- Mỗi tick chạy partition mới nhất đã có: mặc định T-1, bật `end_offset=1` thì tự thành T.
- Tên schedule sinh ra: `daily_pipeline_job_schedule`.
- Signature có tham số `cron_schedule`, nhưng **chỉ dùng được cho partition tĩnh** (`StaticPartitionsDefinition`). Với partition theo ngày sẽ báo:
  `CheckError: Cannot provide cron_schedule or execution_timezone to build_schedule_from_partitioned_job for a time-partitioned job.`

---

## 4. So sánh

| | YAML (cách 1) | Code A (`@dg.schedule`) | Code B (`build_schedule_from_partitioned_job`) |
|---|---|---|---|
| Viết giờ chạy | chuỗi cron | chuỗi cron | `hour_of_day` / `minute_of_hour` |
| Thêm lịch mới | thêm vài dòng YAML | viết thêm job + hàm | viết thêm job + lời gọi hàm |
| Lịch không theo ngày (6 tiếng/lần, thứ 2–6...) | được | được | không, bị khoá theo nhịp partition |
| Chạy partition T thay vì T-1 | `end_offset=1` + `partition_offset_days: 0` | `end_offset=1` + `days=0` | chỉ cần `end_offset=1` |
| Asset không chia partition | `partition_offset_days: null` | sửa code bỏ `partition_key` | không dùng được (job phải có partition) |
| Tên schedule | `<name>_schedule` | tên hàm | `<tên job>_schedule` |
| Nên dùng khi | mặc định, nhiều lịch, người không viết Python | cần logic riêng (bỏ qua ngày lễ, điều kiện...) | chỉ cần "mỗi ngày, lệch vài giờ" và muốn tự theo partition |

---

## 5. Kiểm tra trước khi chạy thật

```bash
cd ~/gitlab/dagster-dbt
source .venv/bin/activate              # dg gọi dbt, phải có dbt trong PATH
export DAGSTER_HOME=~/.dagster_home

dg check defs                          # -> All definitions loaded successfully.
dg list defs                           # xem bảng Schedules: tên + cron
```

**Chạy bộ test** (`tests/`, khoảng 3 giây). Các test này là phiên bản cố định của những lệnh kiểm tra nhanh bên dưới, đọc code test cũng là cách xem mẫu:

```bash
pytest                          # toàn bộ
pytest -m "not integration"     # bỏ test cần Doris
pytest tests/test_schedules.py -v
```

| File | Kiểm tra gì | Cần Doris |
|---|---|---|
| `tests/test_schedules.py` | Lịch đang bật: tên, cron, timezone, tự bật; asset của từng job; tick chạy partition nào (kể cả qua tháng, giờ UTC); 3 giờ chạy tiếp theo | Không |
| `tests/test_asset_selection.py` | Bảng cú pháp `selection` ở mục 2 chọn đúng asset. Thử selection mới: thêm 1 dòng vào bảng trong test | Không |
| `tests/test_cron_schedules_component.py` | Component YAML trong sandbox với asset giả: mặc định, `partition_offset_days` 1/0/null, `end_offset`, `timezone`, `running`; lỗi cron sai, trùng tên, gõ sai tên field | Không |
| `tests/test_schedules_code.py` | Code mẫu đang comment trong `schedules_code.py` (cách A, B) vẫn chạy được khi bỏ comment; cách B từ chối `cron_schedule` | Không |
| `tests/test_dbt_vars.py` | Truyền biến xuống dbt (`docs/dbt_vars_guide.md`): gộp `--vars`, tag `dbt/vars`, `compute_dbt_vars`, thứ tự ghi đè | Không |
| `tests/test_doris_integration.py` | Dữ liệu thật trên Doris (chỉ đọc): đủ bảng/view, raw có dữ liệu, không sót `__dbt_tmp`, `customers` đủ cột, comment giữ dấu `'`, `business_date` hợp lệ. Không kết nối được thì tự skip | Có |

Đổi lịch trong YAML (giờ, selection, thêm/bớt lịch) thì sửa các bảng `EXPECTED_*` đầu file `tests/test_schedules.py` cho khớp.

**Mô phỏng tick** để xem lịch sẽ chạy partition nào, mà không cần chờ tới giờ (helper dùng lại được ở `tests/utils.py`):

```bash
python - <<'EOF'
from datetime import datetime
from zoneinfo import ZoneInfo
import dagster as dg
from dagster_dbt_demo.definitions import defs

d = defs()
s = d.get_schedule_def("customers_daily_schedule")   # đổi tên nếu cần
t = datetime(2026, 10, 5, 1, 0, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))
with dg.build_schedule_context(scheduled_execution_time=t, repository_def=d.get_repository_def()) as ctx:
    print([r.partition_key for r in s.evaluate_tick(ctx).run_requests])   # ['2026-10-04']
EOF
```

**Chạy thử một partition** (chạy thật trên Doris):

```bash
# DORIS_OUTPUT để file export không ghi đè export/customers.csv
DORIS_OUTPUT=/tmp/customers_test.csv dg launch --job customers_daily_job --partition 2026-10-03
dg launch --job orders_daily_job --partition 2026-10-03
```

---

## 6. Vận hành

- **Lịch chỉ chạy khi daemon chạy.** Ở máy local, `dg dev` gồm cả webserver lẫn daemon. Muốn chạy ổn định hằng đêm thì để `dagster-daemon` và webserver chạy thường trực trên server.
- **`DAGSTER_HOME` phải cố định** (`~/.dagster_home`). Dagster lưu trạng thái bật/tắt và lần tick cuối của schedule ở đây. Không đặt biến này thì `dg dev` dùng thư mục tạm, mỗi lần khởi động là mất trạng thái.
- **Bật/tắt trên UI**: vào `Automation`, chọn schedule rồi bấm toggle. Sau khi đã bật/tắt trên UI, **trạng thái trên UI được ưu tiên** hơn `running` / `default_status` trong code hoặc YAML.
- **Lỡ tick** (máy sleep, daemon tắt lúc 0h): khi daemon chạy lại, Dagster **chỉ chạy bù 1 lượt gần nhất**. Nếu lỡ nhiều ngày thì các ngày cũ hơn không được chạy bù. Theo code Dagster 1.13.24 (`dagster/_scheduler/scheduler.py`), các schedule này không gắn partition set nên rơi vào nhánh "not trying to catch up". Điều này đọc từ code, chưa chạy thử. Muốn chạy bù nhiều ngày:
  - Trên UI: vào asset hoặc job, `Materialize` → chọn khoảng partition để chạy backfill, mỗi ngày một run.
  - Hoặc chạy lần lượt từng ngày:
    ```bash
    for d in 2026-10-01 2026-10-02 2026-10-03; do
      dg launch --job customers_daily_job --partition "$d"
      dg launch --job orders_daily_job --partition "$d"
    done
    ```
- Partition trước `2026-09-01` (ngày bắt đầu trong `template_vars.py`) không tồn tại, nên không chạy được.

---

## 7. Crontab nhanh

Cú pháp: `phút giờ ngày tháng thứ` (thứ: `0` hoặc `7` = Chủ nhật, `1` = thứ 2).

| Cron | Ý nghĩa |
|---|---|
| `0 0 * * *` | 00:00 mỗi ngày |
| `0 1 * * *` | 01:00 mỗi ngày |
| `30 6 * * 1-5` | 06:30 thứ 2 → thứ 6 |
| `0 */6 * * *` | mỗi 6 tiếng (0h, 6h, 12h, 18h) |
| `*/15 * * * *` | mỗi 15 phút |
| `0 2 * * 0` | 02:00 Chủ nhật hằng tuần |
| `0 3 1 * *` | 03:00 ngày 1 hằng tháng |
| `0 8,20 * * *` | 08:00 và 20:00 mỗi ngày |

Kiểm tra nhanh một chuỗi cron: <https://crontab.guru>. Lưu ý trang này không biết timezone; ở project này cron tính theo giờ VN.

---

## 8. Lỗi thường gặp

| Lỗi | Nguyên nhân | Cách xử lý |
|---|---|---|
| `Found invalid cron schedule '0 25 * * *' for schedule '...'` | Chuỗi cron sai (giờ 0–23, phút 0–59...) | Sửa `cron` |
| `Duplicate definition found for daily_pipeline_schedule` | Bật cả YAML lẫn code, hoặc 2 phần tử YAML trùng `name` | Chỉ giữ một nơi định nghĩa |
| `DagsterUnknownPartitionError: Could not find a partition with key ...` | `partition_offset_days: 0` / `days=0` mà chưa bật `end_offset=1`, hoặc ngày trước `2026-09-01` | Bật `end_offset=1`, hoặc quay về offset `1` |
| `CheckError: Cannot provide cron_schedule ... for a time-partitioned job` | Truyền `cron_schedule` vào `build_schedule_from_partitioned_job` | Dùng `hour_of_day`/`minute_of_hour`, hoặc chuyển sang YAML / cách A |
| `The dbt executable 'dbt' does not exist` | Chạy `dg` khi chưa activate `.venv` | `source .venv/bin/activate` |
| Tới giờ mà lịch không chạy | Daemon không chạy, schedule đang tắt trên UI, hoặc `running: false` | Chạy `dg dev` / daemon; bật schedule trên UI |
