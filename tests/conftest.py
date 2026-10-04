import dagster as dg
import pytest

from dagster_dbt_demo.definitions import defs as load_project_defs


@pytest.fixture(scope="session")
def project_defs() -> dg.Definitions:
    """Definitions thật của project (load 1 lần cho cả phiên test).

    Dùng bản dbt đã cache ở src/dagster_dbt_demo/defs/.local_defs_state nên không cần database. Clone mới
    (chưa có cache) thì lần load đầu chạy `dbt compile`, lúc đó cần kết nối database.
    """
    return load_project_defs()
