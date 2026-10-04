import importlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import dagster as dg
from dagster_dbt import DbtProjectComponent

# Tag của run chứa biến dbt riêng cho run đó (chuỗi JSON). Lịch trong CronSchedulesComponent gắn tag này
# khi khai báo `dbt_vars`; chạy tay trên UI thì thêm tag này trong Launchpad.
DBT_VARS_TAG = "dbt/vars"


@dataclass
class DbtProjectWithVarsComponent(DbtProjectComponent):
    """DbtProjectComponent + gộp thêm biến dbt lúc chạy vào `--vars`. Xem docs/dbt_vars_guide.md.

    Thứ tự ghi đè khi trùng tên biến (sau ghi đè trước):
      1. `cli_args` trong defs.yaml (biến suy từ partition, hằng số, env)
      2. `vars_fn`: hàm Python tự tính biến
      3. tag `dbt/vars` của run (biến riêng theo từng lịch / từng lần chạy tay)
    """

    # Đường dẫn import tới hàm Python tính biến, vd "dagster_dbt_demo.defs.dbt_ingest.dbt_vars.compute_dbt_vars".
    # Hàm nhận AssetExecutionContext, trả về dict {tên biến: giá trị}. Bỏ trống = không dùng.
    vars_fn: str | None = None

    def get_cli_args(self, context: dg.AssetExecutionContext) -> list[str]:
        extra_vars: dict[str, Any] = {}
        if self.vars_fn:
            extra_vars.update(_import_function(self.vars_fn)(context))
        extra_vars.update(run_tag_vars(context.run.tags))
        if extra_vars:
            context.log.info(f"Biến dbt bổ sung: {extra_vars}")
        return merge_dbt_vars(super().get_cli_args(context), extra_vars)


def run_tag_vars(tags: Mapping[str, str]) -> dict[str, Any]:
    """Đọc biến dbt từ tag `dbt/vars` (chuỗi JSON object) của run."""
    raw = tags.get(DBT_VARS_TAG)
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Tag {DBT_VARS_TAG} phải là JSON object, nhận được: {raw!r}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Tag {DBT_VARS_TAG} phải là JSON object, nhận được: {raw!r}")
    return value


def merge_dbt_vars(cli_args: list[str], extra_vars: Mapping[str, Any]) -> list[str]:
    """Gộp `extra_vars` vào tham số `--vars` (ghi đè biến trùng tên); chưa có `--vars` thì thêm mới."""
    if not extra_vars:
        return cli_args
    args = list(cli_args)
    if "--vars" in args:
        index = args.index("--vars") + 1
        args[index] = json.dumps({**json.loads(args[index]), **extra_vars})
    else:
        args += ["--vars", json.dumps(dict(extra_vars))]
    return args


def _import_function(path: str) -> Callable[[dg.AssetExecutionContext], Mapping[str, Any]]:
    module_name, _, function_name = path.rpartition(".")
    return getattr(importlib.import_module(module_name), function_name)
