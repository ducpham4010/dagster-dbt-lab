"""Kiểm tra dữ liệu thật trên Doris sau khi pipeline đã chạy ít nhất 1 lần. CHỈ ĐỌC, không ghi gì.

Kết nối theo biến DORIS_* trong .env (giống dbt và Python asset). Không kết nối được thì cả file tự skip.
Chạy:        pytest tests/test_doris_integration.py -v
Bỏ qua file: pytest -m "not integration"
"""

import os

import mysql.connector
import pytest

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
    except (ConnectionFailedError, mysql.connector.Error) as exc:
        pytest.skip(f"Không kết nối được Doris: {exc}")
    # Giữ biến `connection` tới hết module, không thì connection bị thu hồi và cursor báo "not connected".
    with connection.cursor() as cur:
        yield cur
    connection.close()


def query(cursor, sql: str, params: tuple = ()) -> list[tuple]:
    cursor.execute(sql, params)
    return cursor.fetchall()


@pytest.fixture(scope="module")
def schema() -> str:
    return os.environ.get("DORIS_SCHEMA", "jaffle_shop")


def test_server_is_doris_with_alive_backend(cursor):
    version = query(cursor, "show variables like 'version_comment'")[0][1]
    assert "doris" in version.lower()
    cursor.execute("show backends")
    columns = [d[0] for d in cursor.description]
    alive = [dict(zip(columns, row))["Alive"] for row in cursor.fetchall()]
    assert "true" in alive


@pytest.mark.parametrize(
    ("table", "expected_type"),
    [
        # Python asset ghi vào.
        ("raw_customers", "BASE TABLE"),
        ("raw_orders", "BASE TABLE"),
        # dbt seed.
        ("raw_payments", "BASE TABLE"),
        # dbt staging (materialized: view).
        ("stg_customers", "VIEW"),
        ("stg_orders", "VIEW"),
        ("stg_payments", "VIEW"),
        # dbt analystic (materialized: table).
        ("customers", "BASE TABLE"),
        ("orders", "BASE TABLE"),
    ],
)
def test_pipeline_objects_exist(cursor, schema, table, expected_type):
    rows = query(
        cursor,
        "select table_type from information_schema.tables where table_schema = %s and table_name = %s",
        (schema, table),
    )
    assert rows == [(expected_type,)], f"{schema}.{table} chưa có hoặc sai loại, pipeline đã chạy chưa?"


@pytest.mark.parametrize("table", ["raw_customers", "raw_orders", "raw_payments"])
def test_raw_tables_not_empty(cursor, table):
    assert query(cursor, f"select count(*) from {table}")[0][0] > 0


def test_no_leftover_dbt_tmp_tables(cursor, schema):
    """dbt-doris build bảng tạm `<model>__dbt_tmp` rồi đổi tên; build xong không được sót lại."""
    rows = query(
        cursor,
        "select table_name from information_schema.tables where table_schema = %s and table_name like %s",
        (schema, "%dbt_tmp%"),
    )
    assert rows == []


def test_customers_keeps_all_model_columns(cursor, schema):
    """Macro override doris__table_colume_type: bảng giữ đủ cột của SQL model, kể cả cột không khai báo
    trong schema.yml (customer_lifetime_value, business_date). Xem troubleshooting_log mục 18."""
    rows = query(
        cursor,
        "select column_name from information_schema.columns where table_schema = %s and table_name = 'customers' "
        "order by ordinal_position",
        (schema,),
    )
    assert [r[0] for r in rows] == CUSTOMERS_COLUMNS


def test_orders_has_business_date(cursor, schema):
    rows = query(
        cursor,
        "select column_name from information_schema.columns where table_schema = %s and table_name = 'orders'",
        (schema,),
    )
    assert "business_date" in {r[0] for r in rows}


def test_customers_comment_keeps_apostrophe(cursor, schema):
    """Macro override doris__table_comment: escape dấu ' trong description. Xem troubleshooting_log mục 17."""
    comment = query(
        cursor,
        "select table_comment from information_schema.tables where table_schema = %s and table_name = 'customers'",
        (schema,),
    )[0][0]
    assert "customer's orders" in comment


def test_one_customers_row_per_raw_customer(cursor):
    """customers = stg_customers left join ..., nên số dòng phải bằng raw_customers."""
    assert query(cursor, "select count(*) from customers") == query(cursor, "select count(*) from raw_customers")


@pytest.mark.parametrize("table", ["customers", "orders"])
def test_business_date_is_single_valid_date(cursor, table):
    """Mỗi lần build gắn 1 business_date (= partition của run), dạng YYYY-MM-DD."""
    dates = [r[0] for r in query(cursor, f"select distinct business_date from {table}")]
    assert len(dates) == 1
    assert len(dates[0]) == 10 and dates[0][4] == "-" and dates[0][7] == "-"
