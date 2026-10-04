import dagster as dg
from datetime import datetime
from zoneinfo import ZoneInfo
from dagster_dbt_demo.definitions import defs
d = defs(); repo = d.get_repository_def()
print(" jobs:", [j.name for j in repo.get_all_jobs() if not j.name.startswith("__")])
for name, t in [("customers_daily_schedule", (2026,10,5,1,0)), ("orders_daily_schedule", (2026,10,5,2,0))]:
    s = d.get_schedule_def(name)
    job = d.get_job_def(name.replace("_schedule", "_job"))
    with dg.build_schedule_context(scheduled_execution_time=datetime(*t, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh")), repository_def=repo) as ctx:
        pk = [r.partition_key for r in s.evaluate_tick(ctx).run_requests]
    print(f" {name:25} {s.cron_schedule:10} {s.execution_timezone} {str(s.default_status).split('.')[-1]} -> {pk}")
    print(f"   assets: {sorted(k.to_user_string() for k in job.asset_layer.executable_asset_keys)}")