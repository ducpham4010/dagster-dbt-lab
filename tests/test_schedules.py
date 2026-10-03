"""Kiểm tra các lịch đang bật trong src/dagster_dbt_demo/defs/schedules/defs.yaml, trên definitions thật.

Chạy: pytest tests/test_schedules.py -v
Đổi lịch trong YAML (giờ, selection, thêm/bớt lịch) thì sửa các bảng EXPECTED_* bên dưới cho khớp.
"""

from datetime import datetime, timezone

import dagster as dg
import pytest

from tests.utils import job_asset_names, next_tick_times, tick_partition_keys, vn_time

# Tên schedule -> cron.
EXPECTED_SCHEDULES = {
    "customers_daily_schedule": "0 1 * * *",
    "orders_daily_schedule": "0 2 * * *",
}

# Tên job -> các asset job đó chạy.
EXPECTED_JOB_ASSETS = {
    # +key:customers+ : toàn bộ raw + staging phía trước, customers, customers_export phía sau.
    "customers_daily_job": {
        "raw_customers", "raw_orders", "raw_payments",
        "stg_customers", "stg_orders", "stg_payments",
        "customers", "customers_export",
    },
    # +key:orders+ : chỉ những gì orders cần (không có raw_customers, stg_customers).
    "orders_daily_job": {
        "raw_orders", "raw_payments",
        "stg_orders", "stg_payments",
        "orders",
    },
}


def test_active_schedules(project_defs):
    repo = project_defs.get_repository_def()
    assert {s.name: s.cron_schedule for s in repo.schedule_defs} == EXPECTED_SCHEDULES


def test_active_jobs(project_defs):
    repo = project_defs.get_repository_def()
    # Bỏ các job nội bộ của Dagster (tên bắt đầu bằng "__").
    jobs = {job.name for job in repo.get_all_jobs() if not job.name.startswith("__")}
    assert jobs == set(EXPECTED_JOB_ASSETS)


@pytest.mark.parametrize("schedule_name", EXPECTED_SCHEDULES)
def test_schedule_timezone_and_default_status(project_defs, schedule_name):
    schedule = project_defs.resolve_schedule_def(schedule_name)
    assert schedule.execution_timezone == "Asia/Ho_Chi_Minh"
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING


@pytest.mark.parametrize(("job_name", "expected_assets"), EXPECTED_JOB_ASSETS.items())
def test_job_assets(project_defs, job_name, expected_assets):
    assert job_asset_names(project_defs, job_name) == expected_assets


@pytest.mark.parametrize("job_name", EXPECTED_JOB_ASSETS)
def test_job_is_daily_partitioned(project_defs, job_name):
    partitions_def = project_defs.resolve_job_def(job_name).partitions_def
    assert isinstance(partitions_def, dg.DailyPartitionsDefinition)
    assert partitions_def.timezone == "Asia/Ho_Chi_Minh"


@pytest.mark.parametrize(
    ("schedule_name", "when", "expected_partition"),
    [
        # Tick lúc 01:00 / 02:00 ngày T chạy partition T-1.
        ("customers_daily_schedule", vn_time(2026, 10, 5, 1, 0), "2026-10-04"),
        ("orders_daily_schedule", vn_time(2026, 10, 5, 2, 0), "2026-10-04"),
        # Qua tháng: 01:00 ngày 01/11 chạy ngày 31/10.
        ("customers_daily_schedule", vn_time(2026, 11, 1, 1, 0), "2026-10-31"),
        # Dagster truyền giờ dạng UTC: 18:00 UTC ngày 04/10 = 01:00 giờ VN ngày 05/10 -> vẫn là 2026-10-04.
        ("customers_daily_schedule", datetime(2026, 10, 4, 18, 0, tzinfo=timezone.utc), "2026-10-04"),
    ],
)
def test_tick_runs_previous_day_partition(project_defs, schedule_name, when, expected_partition):
    assert tick_partition_keys(project_defs, schedule_name, when) == [expected_partition]


@pytest.mark.parametrize(
    ("schedule_name", "expected_ticks"),
    [
        ("customers_daily_schedule", ["2026-10-05T01:00:00+07:00", "2026-10-06T01:00:00+07:00", "2026-10-07T01:00:00+07:00"]),
        ("orders_daily_schedule", ["2026-10-05T02:00:00+07:00", "2026-10-06T02:00:00+07:00", "2026-10-07T02:00:00+07:00"]),
    ],
)
def test_next_tick_times(project_defs, schedule_name, expected_ticks):
    """3 lần chạy tiếp theo tính từ 12:00 trưa ngày 04/10 (giờ VN)."""
    ticks = next_tick_times(project_defs, schedule_name, after=vn_time(2026, 10, 4, 12, 0))
    assert [t.isoformat() for t in ticks] == expected_ticks
