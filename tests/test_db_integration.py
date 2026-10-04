"""Kiểm tra dữ liệu thật trên Oracle sau khi pipeline đã chạy ít nhất 1 lần. CHỈ ĐỌC, không ghi gì.

Kết nối theo biến ORACLE_* trong .env (giống dbt và Python asset). Không kết nối được thì cả file tự skip.
Object nằm trong schema của ORACLE_USER, tên viết hoa (Oracle tự viết hoa tên không đặt trong "...").
Chạy:        pytest tests/test_db_integration.py -v
Bỏ qua file: pytest -m "not integration"
"""

from datetime import date

import oracledb
import pytest

from dagster_dbt_demo.defs.dbt_ingest import dbt_vars
from dagster_dbt_demo.defs.utils import ConnectionFailedError, get_connection

pytestmark = pytest.mark.integration

CUSTOMERS_COLUMNS = [
    "customer_id", "first_name", "last_name", "first_order", "most_recent_order",
    "number_of_orders", "customer_lifetime_value", "business_date",
]


@pytest.fixture(scope="module")
def cursor():
    try:
        connection = get_connection()
    except (ConnectionFailedError, oracledb.Error, KeyError) as exc:
        # KeyError: thiếu biến ORACLE_* trong môi trường / .env.
        pytest.skip(f"Không kết nối được Oracle: {exc!r}")
    # Giữ biến `connection` tới hết module, không thì connection bị thu hồi.
    with connection.cursor() as cur:
        yield cur
    connection.close()


def query(cursor, sql: str, params: tuple = ()) -> list[tuple]:
    cursor.execute(sql, params)
    return cursor.fetchall()


def test_server_is_oracle(cursor):
    banner = query(cursor, "select banner from v$version where rownum = 1")[0][0]
    assert "oracle" in banner.lower()


@pytest.mark.parametrize(
    ("name", "dictionary_view"),
    [
        # Python asset ghi vào.
        ("RAW_CUSTOMERS", "user_tables"),
        ("RAW_ORDERS", "user_tables"),
        # dbt seed.
        ("RAW_PAYMENTS", "user_tables"),
        # dbt staging (materialized: view).
        ("STG_CUSTOMERS", "user_views"),
        ("STG_ORDERS", "user_views"),
        ("STG_PAYMENTS", "user_views"),
        # dbt analystic (materialized: table).
        ("CUSTOMERS", "user_tables"),
        ("ORDERS", "user_tables"),
    ],
)
def test_pipeline_objects_exist(cursor, name, dictionary_view):
    name_column = "view_name" if dictionary_view == "user_views" else "table_name"
    rows = query(cursor, f"select count(*) from {dictionary_view} where {name_column} = :1", (name,))
    assert rows == [(1,)], f"{name} chưa có trong {dictionary_view}, pipeline đã chạy chưa?"


@pytest.mark.parametrize("table", ["raw_customers", "raw_orders", "raw_payments"])
def test_raw_tables_not_empty(cursor, table):
    assert query(cursor, f"select count(*) from {table}")[0][0] > 0


def test_no_leftover_dbt_tmp_tables(cursor):
    """dbt build bảng tạm `<model>__dbt_tmp` rồi đổi tên; build xong không được sót lại."""
    assert query(cursor, "select table_name from user_tables where table_name like '%DBT_TMP%'") == []


def test_customers_keeps_all_model_columns(cursor):
    rows = query(
        cursor, "select column_name from user_tab_columns where table_name = 'CUSTOMERS' order by column_id"
    )
    assert [r[0].lower() for r in rows] == CUSTOMERS_COLUMNS


def test_orders_has_business_date(cursor):
    rows = query(cursor, "select column_name from user_tab_columns where table_name = 'ORDERS'")
    assert "business_date" in {r[0].lower() for r in rows}


def test_one_customers_row_per_raw_customer(cursor):
    """customers = stg_customers left join ..., nên số dòng phải bằng raw_customers."""
    assert query(cursor, "select count(*) from customers") == query(cursor, "select count(*) from raw_customers")


@pytest.mark.parametrize("table", ["customers", "orders"])
def test_business_date_is_single_valid_date(cursor, table):
    """Mỗi lần build gắn 1 business_date (= partition của run), dạng YYYY-MM-DD."""
    dates = [r[0] for r in query(cursor, f"select distinct business_date from {table}")]
    assert len(dates) == 1
    assert date.fromisoformat(dates[0].strip())


def test_raw_max_order_date_reads_db(cursor):
    """Biến raw_max_order_date (mục 4, dbt_ingest/dbt_vars.py) đọc được từ Oracle và là ngày ISO.
    Nhận fixture `cursor` để tự skip khi không kết nối được Oracle."""
    value = dbt_vars.raw_max_order_date()
    expected = query(cursor, "select max(order_date) from raw_orders")[0][0].date()
    assert date.fromisoformat(value) == expected
