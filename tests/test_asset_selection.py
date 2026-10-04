"""Kiểm tra các chuỗi `selection` hay dùng (bảng trong docs/scheduler_guide.md) chọn đúng asset của project.

Chạy: pytest tests/test_asset_selection.py -v
Muốn thử một selection mới trước khi đưa vào defs.yaml: thêm 1 dòng vào bảng bên dưới rồi chạy test.
"""

import pytest

from tests.utils import selection_asset_names

RAW = {"raw_customers", "raw_orders", "raw_payments"}
STAGING = {"stg_customers", "stg_orders", "stg_payments"}


@pytest.mark.parametrize(
    ("selection", "expected_assets"),
    [
        ("*", RAW | STAGING | {"customers", "orders", "customers_export"}),
        ("group:staging", STAGING),
        # Tên group có "/" phải đặt trong dấu "...".
        ('group:"raw/python"', {"raw_customers", "raw_orders"}),
        ("key:customers", {"customers"}),
        # "+" phía sau = thêm các asset phía sau (downstream).
        ("key:customers+", {"customers", "customers_export"}),
        # "+" phía trước = thêm toàn bộ asset phía trước (upstream).
        ("+key:customers", RAW | STAGING | {"customers"}),
        ("+key:customers+", RAW | STAGING | {"customers", "customers_export"}),
        ("+key:orders+", {"raw_orders", "raw_payments", "stg_orders", "stg_payments", "orders"}),
        ("+group:staging", RAW | STAGING),
        ("kind:dbt", {"raw_payments"} | STAGING | {"customers", "orders"}),
        ("group:staging or group:analystic", STAGING | {"customers", "orders"}),
        ("kind:dbt and not group:staging", {"raw_payments", "customers", "orders"}),
    ],
)
def test_selection(project_defs, selection, expected_assets):
    assert selection_asset_names(project_defs, selection) == expected_assets
