"""Kiểm tra dữ liệu thật trên Postgres sau khi pipeline đã chạy ít nhất 1 lần. CHỈ ĐỌC, không ghi gì.

Kết nối theo biến POSTGRES_* trong .env (giống dbt và Python asset). Không kết nối được thì cả file tự skip.
Bảng raw do Python asset ghi nằm ở schema `raw`; model + seed dbt ở schema POSTGRES_SCHEMA (mặc định analytics).
Chạy:        pytest tests/test_db_integration.py -v
Bỏ qua file: pytest -m "not integration"
"""

import os
from datetime import date

import psycopg2
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
    except (ConnectionFailedError, psycopg2.Error) as exc:
        pytest.skip(f"Không kết nối được Postgres: {exc}")
    # Giữ biến `connection` tới hết module, không thì connection bị thu hồi.
    with connection.cursor() as cur:
        yield cur
    connection.close()


def query(cursor, sql: str, params: tuple = ()) -> list[tuple]:
    cursor.execute(sql, params)
    return cursor.fetchall()


@pytest.fixture(scope="module")
def schema() -> str:
    """Schema chứa model dbt."""
    return os.environ.get("POSTGRES_SCHEMA", "analytics")


def test_server_is_postgres(cursor):
    assert "postgresql" in query(cursor, "select version()")[0][0].lower()


@pytest.mark.parametrize(
    ("table_schema", "table", "expected_type"),
    [
        # Python asset ghi vào.
        ("raw", "raw_customers", "BASE TABLE"),
        ("raw", "raw_orders", "BASE TABLE"),
        # dbt seed.
        (None, "raw_payments", "BASE TABLE"),
        # dbt staging (materialized: view).
        (None, "stg_customers", "VIEW"),
        (None, "stg_orders", "VIEW"),
        (None, "stg_payments", "VIEW"),
        # dbt analystic (materialized: table).
        (None, "customers", "BASE TABLE"),
        (None, "orders", "BASE TABLE"),
    ],
)
def test_pipeline_objects_exist(cursor, schema, table_schema, table, expected_type):
    table_schema = table_schema or schema
    rows = query(
        cursor,
        "select table_type from information_schema.tables where table_schema = %s and table_name = %s",
        (table_schema, table),
    )
    assert rows == [(expected_type,)], f"{table_schema}.{table} chưa có hoặc sai loại, pipeline đã chạy chưa?"


@pytest.mark.parametrize("table", ["raw.raw_customers", "raw.raw_orders", "{schema}.raw_payments"])
def test_raw_tables_not_empty(cursor, schema, table):
    assert query(cursor, f"select count(*) from {table.format(schema=schema)}")[0][0] > 0


def test_no_leftover_dbt_tmp_tables(cursor, schema):
    """dbt build bảng tạm `<model>__dbt_tmp` rồi đổi tên; build xong không được sót lại."""
    rows = query(
        cursor,
        "select table_name from information_schema.tables where table_schema = %s and table_name like %s",
        (schema, "%dbt_tmp%"),
    )
    assert rows == []


def test_customers_keeps_all_model_columns(cursor, schema):
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


def test_one_customers_row_per_raw_customer(cursor, schema):
    """customers = stg_customers left join ..., nên số dòng phải bằng raw_customers."""
    assert query(cursor, f"select count(*) from {schema}.customers") == query(
        cursor, "select count(*) from raw.raw_customers"
    )


@pytest.mark.parametrize("table", ["customers", "orders"])
def test_business_date_is_single_valid_date(cursor, schema, table):
    """Mỗi lần build gắn 1 business_date (= partition của run), dạng YYYY-MM-DD."""
    dates = [r[0] for r in query(cursor, f"select distinct business_date from {schema}.{table}")]
    assert len(dates) == 1
    assert date.fromisoformat(str(dates[0]))


def test_raw_max_order_date_reads_db(cursor):
    """Biến raw_max_order_date (mục 4, dbt_ingest/dbt_vars.py) đọc được từ Postgres và là ngày ISO.
    Nhận fixture `cursor` để tự skip khi không kết nối được Postgres."""
    value = dbt_vars.raw_max_order_date()
    expected = query(cursor, "select max(order_date) from raw.raw_orders")[0][0].date()
    assert date.fromisoformat(value) == expected
