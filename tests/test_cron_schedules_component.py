"""Test component CronSchedulesComponent (src/dagster_dbt_demo/components/cron_schedules_component.py).

Khai báo lịch giống hệt trong defs.yaml (dạng dict), load qua sandbox của Dagster, ghép với vài asset giả rồi
giả lập tick. Không cần dbt, không cần database.

Chạy: pytest tests/test_cron_schedules_component.py -v
"""

import json
from collections.abc import Iterator
from contextlib import contextmanager

import dagster as dg
import pytest
from dagster.components.testing import create_defs_folder_sandbox

from dagster_dbt_demo.components import CronSchedulesComponent
from tests.utils import error_chain_text, job_asset_names, tick_partition_keys, tick_requests, vn_time

# Partition giống project: theo ngày, giờ VN. Bản có end_offset=1 thì partition hôm nay tồn tại luôn.
DAILY = dg.DailyPartitionsDefinition(start_date="2026-09-01", timezone="Asia/Ho_Chi_Minh")
DAILY_WITH_TODAY = dg.DailyPartitionsDefinition(
    start_date="2026-09-01", timezone="Asia/Ho_Chi_Minh", end_offset=1
)


@contextmanager
def load_schedules(schedules: list[dict], partitions_def=DAILY) -> Iterator[dg.Definitions]:
    """Load component với danh sách `schedules` (y như phần `attributes.schedules` trong defs.yaml).

    Ghép thêm 2 asset giả: `daily_asset` (chia partition theo `partitions_def`) và `plain_asset` (không partition).
    """

    @dg.asset(partitions_def=partitions_def)
    def daily_asset(): ...

    @dg.asset
    def plain_asset(): ...

    with create_defs_folder_sandbox() as sandbox:
        defs_path = sandbox.scaffold_component(
            component_cls=CronSchedulesComponent,
            defs_yaml_contents={
                "type": "dagster_dbt_demo.components.CronSchedulesComponent",
                "attributes": {"schedules": schedules},
            },
        )
        with sandbox.load_component_and_build_defs(defs_path=defs_path) as (_, component_defs):
            defs = dg.Definitions.merge(component_defs, dg.Definitions(assets=[daily_asset, plain_asset]))
            # Ép Dagster kiểm tra toàn bộ definitions (trùng tên, cron sai...) ngay tại đây.
            defs.get_repository_def()
            yield defs


def test_each_entry_creates_job_and_schedule():
    with load_schedules([
        {"name": "a", "cron": "0 1 * * *"},
        {"name": "b", "cron": "0 2 * * *"},
    ]) as defs:
        repo = defs.get_repository_def()
        assert {s.name: s.cron_schedule for s in repo.schedule_defs} == {
            "a_schedule": "0 1 * * *",
            "b_schedule": "0 2 * * *",
        }
        assert {"a_job", "b_job"} <= {job.name for job in repo.get_all_jobs()}


def test_defaults():
    """Chỉ khai báo name + cron: selection="*", partition T-1, giờ VN, lịch tự bật."""
    with load_schedules([{"name": "a", "cron": "0 1 * * *"}]) as defs:
        schedule = defs.resolve_schedule_def("a_schedule")
        assert schedule.execution_timezone == "Asia/Ho_Chi_Minh"
        assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING
        assert job_asset_names(defs, "a_job") == {"daily_asset", "plain_asset"}
        assert tick_partition_keys(defs, "a_schedule", vn_time(2026, 10, 5, 1, 0)) == ["2026-10-04"]


def test_selection_limits_job_assets():
    with load_schedules([{"name": "a", "cron": "0 1 * * *", "selection": "key:daily_asset"}]) as defs:
        assert job_asset_names(defs, "a_job") == {"daily_asset"}


def test_offset_1_runs_previous_day():
    with load_schedules([{"name": "a", "cron": "0 1 * * *", "partition_offset_days": 1}]) as defs:
        assert tick_partition_keys(defs, "a_schedule", vn_time(2026, 10, 5, 1, 0)) == ["2026-10-04"]


def test_offset_0_with_end_offset_runs_same_day():
    """Chạy dữ liệu của chính ngày T: partition_offset_days: 0 + end_offset=1."""
    with load_schedules(
        [{"name": "a", "cron": "0 1 * * *", "partition_offset_days": 0}], partitions_def=DAILY_WITH_TODAY
    ) as defs:
        assert tick_partition_keys(defs, "a_schedule", vn_time(2026, 10, 5, 1, 0)) == ["2026-10-05"]


def test_offset_0_without_end_offset_fails():
    """Thiếu end_offset=1 thì partition ngày T chưa tồn tại -> tick lỗi."""
    with load_schedules([{"name": "a", "cron": "0 1 * * *", "partition_offset_days": 0}]) as defs:
        with pytest.raises(dg.DagsterUnknownPartitionError, match="2026-10-05"):
            tick_partition_keys(defs, "a_schedule", vn_time(2026, 10, 5, 1, 0))


def test_null_offset_for_unpartitioned_assets():
    """Asset không chia partition: partition_offset_days: null -> run không có partition."""
    with load_schedules([
        {"name": "a", "cron": "0 */6 * * *", "selection": "key:plain_asset", "partition_offset_days": None},
    ]) as defs:
        assert tick_partition_keys(defs, "a_schedule", vn_time(2026, 10, 5, 6, 0)) == [None]


def test_custom_timezone():
    """Đổi timezone sang UTC: tick 00:00 UTC ngày 05/10 -> partition 2026-10-04."""
    with load_schedules([{"name": "a", "cron": "0 0 * * *", "timezone": "UTC"}]) as defs:
        assert defs.resolve_schedule_def("a_schedule").execution_timezone == "UTC"
        when = vn_time(2026, 10, 5, 7, 0)  # = 00:00 UTC ngày 05/10
        assert tick_partition_keys(defs, "a_schedule", when) == ["2026-10-04"]


def test_running_false_creates_stopped_schedule():
    with load_schedules([{"name": "a", "cron": "0 1 * * *", "running": False}]) as defs:
        assert defs.resolve_schedule_def("a_schedule").default_status == dg.DefaultScheduleStatus.STOPPED



def test_dbt_vars_become_run_tag():
    """dbt_vars của lịch -> tag `dbt/vars` (JSON) trên run, kể cả lịch không partition."""
    with load_schedules([
        {"name": "a", "cron": "0 1 * * *", "dbt_vars": {"lookback_days": 7, "region": "north"}},
        {"name": "b", "cron": "0 */6 * * *", "selection": "key:plain_asset", "partition_offset_days": None,
         "dbt_vars": {"full_refresh": True}},
    ]) as defs:
        [a] = tick_requests(defs, "a_schedule", vn_time(2026, 10, 5, 1, 0))
        assert json.loads(a.tags["dbt/vars"]) == {"lookback_days": 7, "region": "north"}
        [b] = tick_requests(defs, "b_schedule", vn_time(2026, 10, 5, 6, 0))
        assert json.loads(b.tags["dbt/vars"]) == {"full_refresh": True}


def test_no_dbt_vars_no_tag():
    with load_schedules([{"name": "a", "cron": "0 1 * * *"}]) as defs:
        [request] = tick_requests(defs, "a_schedule", vn_time(2026, 10, 5, 1, 0))
        assert "dbt/vars" not in request.tags

def test_invalid_cron_fails():
    with pytest.raises(Exception) as excinfo:
        with load_schedules([{"name": "a", "cron": "0 25 * * *"}]):
            pass
    assert "Found invalid cron schedule '0 25 * * *' for schedule 'a_schedule'" in error_chain_text(excinfo.value)


def test_duplicate_name_fails():
    with pytest.raises(Exception) as excinfo:
        with load_schedules([
            {"name": "a", "cron": "0 1 * * *"},
            {"name": "a", "cron": "0 2 * * *"},
        ]):
            pass
    assert "Duplicate definition found for a_schedule" in error_chain_text(excinfo.value)


def test_misspelled_field_fails():
    """Gõ sai tên field (crom thay vì cron) thì báo lỗi lúc load, không âm thầm bỏ qua."""
    with pytest.raises(Exception) as excinfo:
        with load_schedules([{"name": "a", "crom": "0 1 * * *"}]):
            pass
    errors = error_chain_text(excinfo.value)
    assert "crom\n  Extra inputs are not permitted" in errors
    assert "cron\n  Field required" in errors
