from collections.abc import Callable, Sequence
from typing import Optional

import dagster as dg


@dg.template_var
def group_from_fqn() -> Callable[[Sequence[str]], str | None]:
    """Returns a function that extracts the group name from a dbt model's fqn.

    The fqn (fully qualified name) contains the directory structure, e.g.:
    ["jaffle_shop", "staging", "stg_customers"] -> returns "staging"
    ["jaffle_shop", "marts", "customers"] -> returns "marts"
    """

    def _get_group(fqn: Sequence[str]) -> str | None:
        # fqn structure: [project_name, folder, ..., model_name]
        # We want the first folder after the project name
        if len(fqn) >= 2:
            return fqn[1]  # Returns "staging", "intermediate", "marts", etc.
        return None

    return _get_group

@dg.template_var
def daily_partitions_def() -> dg.DailyPartitionsDefinition:
    return dg.DailyPartitionsDefinition(
        start_date="2026-09-01",
        timezone="Asia/Ho_Chi_Minh",
        # Mặc định (end_offset=0) chỉ có partition của những ngày đã kết thúc: lúc 0h/1h ngày T,
        # partition mới nhất là T-1. Bật end_offset=1 để partition của ngày đang diễn ra tồn tại ngay
        # từ 00:00 -> chạy được partition T (start_date=T, end_date=T+1). Muốn schedule chạy partition T
        # thì đặt thêm partition_offset_days: 0 trong defs/schedules/defs.yaml. Lưu ý: lúc đó ngày T mới có vài giờ
        # dữ liệu, và partition hôm nay luôn hiện "missing" trên UI. Áp dụng cho toàn bộ asset dbt.
        # end_offset=1,
    )
