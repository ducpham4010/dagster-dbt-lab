"""Đảm bảo code đặt lịch mẫu đang comment trong src/dagster_dbt_demo/defs/schedules_code.py vẫn chạy được.

Test bỏ comment (trong bộ nhớ, không sửa file) khối "Dùng chung" + cách A hoặc B, chạy code đó rồi ghép với
definitions thật của project để giả lập tick. Sửa code mẫu mà làm hỏng nó thì test này fail.

Chạy: pytest tests/test_schedules_code.py -v
"""

import re
from pathlib import Path

import dagster as dg
import pytest
from dagster_shared.check import CheckError

from tests.utils import tick_partition_keys, vn_time

SCHEDULES_CODE = Path(__file__).parents[1] / "src/dagster_dbt_demo/defs/schedules_code.py"


def uncomment_variant(variant: str) -> str:
    """Bỏ comment khối "Dùng chung" + khối "Cách <variant>", giống bôi đen rồi Cmd+/ trong editor."""
    blocks = re.split(r"(?m)^(?=# ---- )", SCHEDULES_CODE.read_text())
    out = [blocks[0]]
    for block in blocks[1:]:
        header, _, body = block.partition("\n")
        if header.startswith(("# ---- Dùng chung", f"# ---- Cách {variant}")):
            body = "\n".join(
                line[2:] if line.startswith("# ") else ("" if line == "#" else line)
                for line in body.split("\n")
            )
        out.append(header + "\n" + body)
    return "".join(out)


def run_code(source: str) -> dict:
    namespace: dict = {}
    exec(compile(source, str(SCHEDULES_CODE), "exec"), namespace)
    return namespace


def test_file_is_disabled_by_default():
    """File để nguyên (toàn comment) thì không định nghĩa job/schedule nào."""
    namespace = run_code(SCHEDULES_CODE.read_text())
    assert [name for name in namespace if not name.startswith("__")] == []


@pytest.mark.parametrize(
    ("variant", "schedule_name"),
    [
        ("A", "daily_pipeline_schedule"),  # @dg.schedule: tên schedule = tên hàm
        ("B", "daily_pipeline_job_schedule"),  # build_schedule_from_partitioned_job: <tên job>_schedule
    ],
)
def test_variant_runs_previous_day_at_midnight(project_defs, variant, schedule_name):
    namespace = run_code(uncomment_variant(variant))
    # Ghép vào definitions thật (lịch YAML có tên khác nên không trùng).
    defs = dg.Definitions.merge(
        project_defs,
        dg.Definitions(jobs=[namespace["daily_pipeline_job"]], schedules=[namespace["daily_pipeline_schedule"]]),
    )
    schedule = defs.resolve_schedule_def(schedule_name)
    assert schedule.cron_schedule == "0 0 * * *"
    assert schedule.execution_timezone == "Asia/Ho_Chi_Minh"
    assert tick_partition_keys(defs, schedule_name, vn_time(2026, 10, 5, 0, 0)) == ["2026-10-04"]


def test_build_schedule_from_partitioned_job_rejects_cron():
    """Cách B không nhận chuỗi cron với job partition theo thời gian (lý do phải dùng cách A hoặc YAML).

    Job khai báo y như code mẫu (partition suy ra từ asset dbt lúc load).
    """
    job = dg.define_asset_job("tmp_job", selection="*")
    with pytest.raises(CheckError, match="Cannot provide cron_schedule"):
        dg.build_schedule_from_partitioned_job(job, cron_schedule="0 0 * * *")
