"""Biến dbt tính bằng Python lúc chạy (mục 4 trong docs/dbt_vars_guide.md).

Khai báo trong defs.yaml: `vars_fn: dagster_dbt_demo.defs.dbt_ingest.dbt_vars.compute_dbt_vars`.
Hàm chạy ngay trước mỗi lần gọi dbt, trong step dbt của run (sau khi các asset raw cùng run đã chạy xong).
Dùng khi giá trị cần logic Python hoặc phải đọc dữ liệu, không viết được bằng Jinja trong YAML.
Model dbt chưa dùng các biến này, đây là ví dụ; dùng thì `{{ var('prev_business_date') }}` và khai báo giá trị
mặc định trong dbt/dbt_project.yml.
"""

from datetime import date, timedelta
from typing import Any

import dagster as dg

from dagster_dbt_demo.defs.utils import get_connection


def compute_dbt_vars(context: dg.AssetExecutionContext) -> dict[str, Any]:
    dbt_vars: dict[str, Any] = {
        # Lịch nào kích hoạt run ("manual" nếu chạy tay / dg launch).
        "triggered_by": context.run.tags.get("dagster/schedule_name", "manual"),
    }
    if context.has_partition_key:
        day = date.fromisoformat(context.partition_key)
        dbt_vars["prev_business_date"] = previous_business_day(day).isoformat()
        dbt_vars["is_month_end"] = (day + timedelta(days=1)).day == 1
        dbt_vars["quarter"] = f"{day.year}-Q{(day.month - 1) // 3 + 1}"
    # Đọc dữ liệu: ngày đặt hàng mới nhất trong bảng raw (vd làm watermark cho model incremental).
    dbt_vars["raw_max_order_date"] = raw_max_order_date()
    return dbt_vars


def previous_business_day(day: date) -> date:
    """Ngày làm việc liền trước `day` (bỏ thứ 7, chủ nhật)."""
    day -= timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def raw_max_order_date() -> str | None:
    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute("select max(order_date) from raw.raw_orders")
        value = cursor.fetchone()[0]
        return value.date().isoformat() if value is not None else None
    finally:
        connection.close()
