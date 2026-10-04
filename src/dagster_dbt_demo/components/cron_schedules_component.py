import json
from collections.abc import Sequence
from datetime import timedelta
from typing import Any
from zoneinfo import ZoneInfo

import dagster as dg

from .dbt_project_with_vars_component import DBT_VARS_TAG


class CronSchedule(dg.Model, dg.Resolvable):
    """Một lịch chạy khai báo trong defs.yaml."""

    # Tên gốc: sinh ra job `<name>_job` và schedule `<name>_schedule`.
    name: str
    # Crontab "phút giờ ngày tháng thứ", tính theo `timezone`.
    cron: str
    # Cú pháp chọn asset của Dagster: "*", "group:staging", "key:customers+", 'group:"raw/python"', ...
    selection: str = "*"
    # Partition (theo ngày) cần chạy = ngày chạy - partition_offset_days: 1 = hôm trước (T-1),
    # 0 = chính ngày T (phải bật end_offset=1 trong defs/dbt_ingest/template_vars.py).
    # Để null nếu selection không có asset nào chia partition.
    partition_offset_days: int | None = 1
    # Nên trùng timezone của partitions_def trong defs/dbt_ingest/template_vars.py.
    timezone: str = "Asia/Ho_Chi_Minh"
    # true = lịch tự bật khi load; false = để tắt, bật tay trên UI.
    running: bool = True
    description: str | None = None
    # Biến dbt riêng cho lịch này, vd {lookback_days: 7}. Gắn vào run qua tag `dbt/vars`, ghi đè biến cùng tên
    # trong dbt_ingest/defs.yaml. Xem mục 3 trong docs/dbt_vars_guide.md.
    dbt_vars: dict[str, Any] | None = None


class CronSchedulesComponent(dg.Component, dg.Model, dg.Resolvable):
    """Khai báo lịch chạy bằng crontab trong YAML.

    Mỗi phần tử trong `schedules` sinh ra 1 asset job + 1 schedule, nên thêm/sửa lịch chỉ cần sửa defs.yaml.
    Không dùng build_schedule_from_partitioned_job vì với partition theo thời gian nó không nhận
    cron_schedule (chỉ nhận hour_of_day/minute_of_hour), nên partition key được tự tính từ giờ tick.
    """

    schedules: Sequence[CronSchedule]

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        jobs = []
        schedules = []
        for spec in self.schedules:
            job = dg.define_asset_job(name=f"{spec.name}_job", selection=spec.selection)
            jobs.append(job)
            schedules.append(_build_schedule(spec, job))
        return dg.Definitions(jobs=jobs, schedules=schedules)


def _build_schedule(spec: CronSchedule, job) -> dg.ScheduleDefinition:
    @dg.schedule(
        name=f"{spec.name}_schedule",
        cron_schedule=spec.cron,
        job=job,
        execution_timezone=spec.timezone,
        default_status=(
            dg.DefaultScheduleStatus.RUNNING if spec.running else dg.DefaultScheduleStatus.STOPPED
        ),
        description=spec.description,
    )
    def _schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
        tags = {DBT_VARS_TAG: json.dumps(spec.dbt_vars)} if spec.dbt_vars else {}
        if spec.partition_offset_days is None:
            return dg.RunRequest(tags=tags)
        # Đổi về giờ `timezone` trước khi trừ ngày, tránh lệch 1 ngày nếu Dagster truyền giờ UTC.
        run_date = context.scheduled_execution_time.astimezone(ZoneInfo(spec.timezone))
        partition_key = (run_date - timedelta(days=spec.partition_offset_days)).strftime("%Y-%m-%d")
        return dg.RunRequest(partition_key=partition_key, tags=tags)

    return _schedule
