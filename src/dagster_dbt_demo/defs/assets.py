import os

import duckdb
import pandas as pd
from .utils import get_connection

import dagster as dg


@dg.asset(key="raw_customers", compute_kind="python", group_name="raw")
def raw_customers(context: dg.AssetExecutionContext) -> None:
    """Ingest raw customer data from an external source."""
    connection = get_connection()
    insert_statement = "INSERT INTO RAW_CUSTOMERS (ID, FIRST_NAME, LAST_NAME) VALUES (:1, :2, :3)"
    rows = [
        (201, "ducph7", "P."),
        (202, "Shawn", "M."),
        (203, "Kathleen", "P."),
        (204, "Jimmy", "C."),
        (205, "Katherine", "R."),
        (206, "Sarah", "R."),
        (207, "Martin", "M."),
        (208, "Frank", "R."),
        (209, "Jennifer", "F."),
        (210, "Henry", "W."),
    ]
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE RAW_CUSTOMERS")
    cursor.executemany(insert_statement, rows)
    connection.commit()
    context.add_output_metadata({"num_rows": len(rows)})


@dg.asset(key="raw_orders", compute_kind="python", group_name="raw")
def raw_orders(context: dg.AssetExecutionContext) -> None:
    """Ingest raw orders data from an external source."""
    connection = get_connection()
    insert_statement = (
        "INSERT INTO RAW_ORDERS (ID, USER_ID, ORDER_DATE, STATUS) "
        "VALUES (:1, :2, TO_DATE(:3, 'YYYY-MM-DD HH24:MI:SS'), :4)"
    )
    rows = [
        (201, 201, "2018-01-01 00:00:00", "returned"),
        (202, 202, "2018-01-02 00:00:00", "completed"),
        (203, 203, "2018-01-04 00:00:00", "completed"),
        (204, 204, "2018-01-05 00:00:00", "completed"),
        (205, 205, "2018-01-05 00:00:00", "completed"),
        (206, 206, "2018-01-07 00:00:00", "completed"),
        (207, 207, "2018-01-09 00:00:00", "completed"),
        (208, 208, "2018-01-11 00:00:00", "returned"),
        (209, 209, "2018-01-12 00:00:00", "completed"),
        (210, 210, "2018-01-14 00:00:00", "completed"),
    ]
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE RAW_ORDERS")
    cursor.executemany(insert_statement, rows)
    connection.commit()
    context.add_output_metadata({"num_rows": len(rows)})
