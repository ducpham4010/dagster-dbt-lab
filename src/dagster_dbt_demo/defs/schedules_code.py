# =================================================================================================
# ĐẶT LỊCH BẰNG CODE (Python) - ĐANG TẮT, giữ lại để tham khảo.
# Project đang dùng lịch khai báo bằng YAML: defs/schedules/defs.yaml.
# Hướng dẫn chi tiết cả 2 cách: docs/scheduler_guide.md
#
# Muốn chuyển sang đặt lịch bằng code:
#   1. Comment phần tử `daily_pipeline` trong defs/schedules/defs.yaml
#      (không thì lỗi trùng tên: "Duplicate definition found for daily_pipeline_schedule").
#   2. Bỏ comment khối "Dùng chung" + MỘT trong hai cách A hoặc B bên dưới
#      (bỏ cả hai thì có 2 lịch cùng chạy 1 job). Bôi đen khối rồi Cmd+/ để bỏ comment.
# =================================================================================================


# ---- Dùng chung ---------------------------------------------------------------------------------
# from datetime import timedelta  # chỉ cách A cần
# from zoneinfo import ZoneInfo  # chỉ cách A cần
#
# import dagster as dg
#
# # Toàn bộ pipeline: Python asset raw -> dbt seed/staging/analystic -> customers_export.
# # partitions_def tự lấy từ asset dbt (theo ngày, Asia/Ho_Chi_Minh, xem dbt_ingest/template_vars.py).
# daily_pipeline_job = dg.define_asset_job(
#     name="daily_pipeline_job",
#     selection=dg.AssetSelection.all(),
# )


# ---- Cách A: @dg.schedule + chuỗi crontab -------------------------------------------------------
# # Tự tính partition key từ giờ tick nên viết được crontab bất kỳ.
# # Trùng timezone của partitions_def trong dbt_ingest/template_vars.py.
# TIMEZONE = "Asia/Ho_Chi_Minh"
#
#
# # Crontab "phút giờ ngày tháng thứ", tính theo giờ TIMEZONE: "0 0 * * *" = 00:00 mỗi ngày.
# @dg.schedule(
#     cron_schedule="0 0 * * *",
#     job=daily_pipeline_job,
#     execution_timezone=TIMEZONE,
#     default_status=dg.DefaultScheduleStatus.RUNNING,
# )
# def daily_pipeline_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
#     # Chạy partition của ngày hôm trước (ngày vừa kết thúc), vd: tick 00:00 ngày 2026-10-05 chạy
#     # partition 2026-10-04. Muốn chạy partition của chính ngày T: đổi days=1 thành days=0 VÀ bật
#     # end_offset=1 trong dbt_ingest/template_vars.py (không bật thì partition ngày T chưa tồn tại -> tick lỗi).
#     run_date = context.scheduled_execution_time.astimezone(ZoneInfo(TIMEZONE))
#     partition_key = (run_date - timedelta(days=1)).strftime("%Y-%m-%d")
#     return dg.RunRequest(partition_key=partition_key)


# ---- Cách B: build_schedule_from_partitioned_job ------------------------------------------------
# # Để Dagster tự sinh lịch từ partitions_def. Giờ chạy khai báo bằng hour_of_day/minute_of_hour
# # (KHÔNG nhận chuỗi cron với partition theo ngày: CheckError "Cannot provide cron_schedule ...
# # for a time-partitioned job"), timezone lấy từ partitions_def. Mỗi tick tự chạy partition mới nhất
# # đã có: mặc định là T-1, bật end_offset=1 trong dbt_ingest/template_vars.py thì thành T.
# # Tên schedule sinh ra: daily_pipeline_job_schedule.
# daily_pipeline_schedule = dg.build_schedule_from_partitioned_job(
#     daily_pipeline_job,
#     hour_of_day=0,
#     minute_of_hour=0,
#     default_status=dg.DefaultScheduleStatus.RUNNING,
# )
