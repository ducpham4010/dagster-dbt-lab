import dagster as dg
import pytest

from dagster_dbt_demo.definitions import defs as load_project_defs


@pytest.fixture(scope="session")
def project_defs() -> dg.Definitions:
    """Definitions thật của project (load 1 lần cho cả phiên test).

    Cần có dbt/target/manifest.json vì my_python_script/defs.yaml đọc nó (asset_key_for_model). Clone mới thì
    chạy `dbt parse` trong dbt/ hoặc `dg dev` một lần trước (xem AGENTS.md).
    Phần dbt dùng bản đã cache ở src/dagster_dbt_demo/defs/.local_defs_state nên không cần database; chưa có
    cache thì lần load đầu chạy `dbt compile`, lúc đó cần kết nối database.
    """
    return load_project_defs()
