"""Hàm dùng chung cho các test: giả lập tick của schedule, liệt kê asset của job/selection."""

from datetime import datetime
from zoneinfo import ZoneInfo

import dagster as dg
from dagster._utils.schedules import cron_string_iterator

VN = ZoneInfo("Asia/Ho_Chi_Minh")


def vn_time(*args: int) -> datetime:
    """datetime theo giờ VN, vd vn_time(2026, 10, 5, 1, 0) = 01:00 ngày 05/10/2026."""
    return datetime(*args, tzinfo=VN)


def tick_requests(defs: dg.Definitions, schedule_name: str, when: datetime) -> list[dg.RunRequest]:
    """Giả lập 1 tick của schedule tại thời điểm `when`. Chỉ tính RunRequest, không chạy job thật."""
    schedule = defs.resolve_schedule_def(schedule_name)
    with dg.build_schedule_context(
        scheduled_execution_time=when, repository_def=defs.get_repository_def()
    ) as context:
        return list(schedule.evaluate_tick(context).run_requests)


def tick_partition_keys(defs: dg.Definitions, schedule_name: str, when: datetime) -> list[str | None]:
    """Partition mà tick tại `when` sẽ chạy (None = run không có partition)."""
    return [request.partition_key for request in tick_requests(defs, schedule_name, when)]


def job_asset_names(defs: dg.Definitions, job_name: str) -> set[str]:
    """Tên các asset mà job sẽ chạy."""
    job = defs.resolve_job_def(job_name)
    return {key.to_user_string() for key in job.asset_layer.executable_asset_keys}


def selection_asset_names(defs: dg.Definitions, selection: str) -> set[str]:
    """Tên các asset mà chuỗi selection (cú pháp chọn asset của Dagster) chọn được."""
    asset_graph = defs.get_repository_def().asset_graph
    return {key.to_user_string() for key in dg.AssetSelection.from_string(selection).resolve(asset_graph)}


def next_tick_times(defs: dg.Definitions, schedule_name: str, after: datetime, count: int = 3) -> list[datetime]:
    """`count` lần chạy tiếp theo của schedule sau thời điểm `after`.

    Dùng cron_string_iterator (hàm nội bộ của Dagster, chính là hàm daemon dùng để tính giờ tick).
    """
    schedule = defs.resolve_schedule_def(schedule_name)
    ticks = cron_string_iterator(after.timestamp(), schedule.cron_schedule, schedule.execution_timezone)
    return [next(ticks) for _ in range(count)]


def error_chain_text(exc: BaseException) -> str:
    """Gộp thông báo của exception và mọi exception gốc (__cause__/__context__).

    Lỗi khi load component bị Dagster bọc trong ComponentTreeException, thông báo thật nằm ở exception gốc.
    """
    messages = []
    while exc is not None:
        messages.append(f"{type(exc).__name__}: {exc}")
        exc = exc.__cause__ or exc.__context__
    return "\n".join(messages)
