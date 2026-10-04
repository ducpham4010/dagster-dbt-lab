"""Test cách truyền biến xuống dbt (docs/dbt_vars_guide.md): gộp `--vars`, tag `dbt/vars` (mục 3),
hàm Python compute_dbt_vars (mục 4) và thứ tự ghi đè giữa các nguồn. Không cần database.

Chạy: pytest tests/test_dbt_vars.py -v
"""

import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from dagster_dbt import DbtProjectComponent

from dagster_dbt_demo.components import DbtProjectWithVarsComponent
from dagster_dbt_demo.components.dbt_project_with_vars_component import (
    DBT_VARS_TAG,
    _import_function,
    merge_dbt_vars,
    run_tag_vars,
)
from dagster_dbt_demo.defs.dbt_ingest import dbt_vars
from tests.utils import tick_requests, vn_time

DBT_DEFS_YAML = Path(__file__).parents[1] / "src/dagster_dbt_demo/defs/dbt_ingest/defs.yaml"


def fake_context(partition_key: str | None = None, tags: dict | None = None) -> SimpleNamespace:
    """Context giả, chỉ có những thuộc tính compute_dbt_vars / get_cli_args dùng."""
    return SimpleNamespace(
        has_partition_key=partition_key is not None,
        partition_key=partition_key,
        run=SimpleNamespace(tags=tags or {}),
        log=SimpleNamespace(info=lambda message: None),
    )


def vars_of(cli_args: list[str]) -> dict:
    """Lấy dict biến từ tham số `--vars` của lệnh dbt."""
    return json.loads(cli_args[cli_args.index("--vars") + 1])


# ---- gộp --vars -----------------------------------------------------------------------------------


def test_merge_into_existing_vars_overrides_same_name():
    args = ["build", "--vars", json.dumps({"a": 1, "b": 2})]
    merged = merge_dbt_vars(args, {"b": 20, "c": 3})
    assert merged[:2] == ["build", "--vars"]
    assert vars_of(merged) == {"a": 1, "b": 20, "c": 3}


def test_merge_appends_vars_when_missing():
    assert merge_dbt_vars(["build"], {"a": 1}) == ["build", "--vars", '{"a": 1}']


def test_merge_without_extra_vars_keeps_args():
    args = ["build", "--vars", '{"a": 1}']
    assert merge_dbt_vars(args, {}) == args


# ---- mục 3: tag dbt/vars --------------------------------------------------------------------------


def test_run_tag_vars():
    assert run_tag_vars({}) == {}
    assert run_tag_vars({DBT_VARS_TAG: '{"lookback_days": 7}'}) == {"lookback_days": 7}


@pytest.mark.parametrize("bad_value", ["lookback_days=7", "[1, 2]"])
def test_run_tag_vars_rejects_non_json_object(bad_value):
    with pytest.raises(ValueError, match="phải là JSON object"):
        run_tag_vars({DBT_VARS_TAG: bad_value})


def test_customers_schedule_sets_dbt_vars_tag(project_defs):
    """Lịch customers_daily khai báo dbt_vars -> run mang tag dbt/vars."""
    [request] = tick_requests(project_defs, "customers_daily_schedule", vn_time(2026, 10, 5, 1, 0))
    assert json.loads(request.tags[DBT_VARS_TAG]) == {"lookback_days": 7}


def test_orders_schedule_has_no_dbt_vars_tag(project_defs):
    [request] = tick_requests(project_defs, "orders_daily_schedule", vn_time(2026, 10, 5, 2, 0))
    assert DBT_VARS_TAG not in request.tags


# ---- mục 4: hàm Python ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        ("2026-10-05", "2026-10-02"),  # thứ 2 -> thứ 6 tuần trước
        ("2026-10-04", "2026-10-02"),  # chủ nhật -> thứ 6
        ("2026-10-07", "2026-10-06"),  # thứ 4 -> thứ 3
    ],
)
def test_previous_business_day(day, expected):
    assert dbt_vars.previous_business_day(date.fromisoformat(day)).isoformat() == expected


def test_compute_dbt_vars(monkeypatch):
    # Không đọc database trong test này.
    monkeypatch.setattr(dbt_vars, "raw_max_order_date", lambda: "2018-01-14")
    context = fake_context("2026-10-31", tags={"dagster/schedule_name": "customers_daily_schedule"})
    assert dbt_vars.compute_dbt_vars(context) == {
        "triggered_by": "customers_daily_schedule",
        "prev_business_date": "2026-10-30",
        "is_month_end": True,
        "quarter": "2026-Q4",
        "raw_max_order_date": "2018-01-14",
    }


def test_compute_dbt_vars_manual_run_without_partition(monkeypatch):
    monkeypatch.setattr(dbt_vars, "raw_max_order_date", lambda: None)
    assert dbt_vars.compute_dbt_vars(fake_context()) == {"triggered_by": "manual", "raw_max_order_date": None}


def test_defs_yaml_vars_fn_is_importable():
    """`vars_fn` trong dbt_ingest/defs.yaml trỏ đúng tới một hàm import được."""
    path = yaml.safe_load(DBT_DEFS_YAML.read_text())["attributes"]["vars_fn"]
    assert _import_function(path) is dbt_vars.compute_dbt_vars


# ---- thứ tự ghi đè: cli_args (YAML) < vars_fn < tag dbt/vars ----------------------------------------


def fake_vars_fn(context) -> dict:
    return {"from_fn": "fn", "shared": "fn"}


def test_override_order(monkeypatch):
    # Giả lập phần cli_args đã render từ defs.yaml (mục 1, 2).
    yaml_args = ["build", "--vars", json.dumps({"from_yaml": "yaml", "shared": "yaml"})]
    monkeypatch.setattr(DbtProjectComponent, "get_cli_args", lambda self, context: yaml_args)
    # Tạo component không qua __init__ (không cần dbt project thật), chỉ gán vars_fn.
    component = object.__new__(DbtProjectWithVarsComponent)
    component.vars_fn = f"{__name__}.fake_vars_fn"

    without_tag = component.get_cli_args(fake_context("2026-10-02"))
    assert vars_of(without_tag) == {"from_yaml": "yaml", "from_fn": "fn", "shared": "fn"}

    with_tag = component.get_cli_args(fake_context("2026-10-02", tags={DBT_VARS_TAG: '{"shared": "tag"}'}))
    assert vars_of(with_tag) == {"from_yaml": "yaml", "from_fn": "fn", "shared": "tag"}
