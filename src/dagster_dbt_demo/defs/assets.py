import hashlib
import os

import duckdb
import pandas as pd
from .utils import get_connection

import dagster as dg


def _data_version_for(rows: list[tuple]) -> dg.DataVersion:
    """Build a stable DataVersion from the row contents.

    Hashing the actual data means the data version only changes when the
    ingested rows change, instead of a new version being generated on every
    materialization (which would make downstream dbt assets always show up
    as "Unsynced" even when nothing really changed).
    """
    digest = hashlib.sha256(repr(rows).encode("utf-8")).hexdigest()
    return dg.DataVersion(digest)


def _create_table_if_not_exists(cursor, table: str, columns: str, key: str) -> None:
    """Create a Doris UNIQUE KEY table (the equivalent of a primary key) if it is missing."""
    replication_num = os.environ.get("DORIS_REPLICATION_NUM", "1")
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {table} ({columns}) "
        f"UNIQUE KEY ({key}) "
        f"DISTRIBUTED BY HASH ({key}) BUCKETS 1 "
        f'PROPERTIES ("replication_num" = "{replication_num}")'
    )


@dg.asset(key="raw_customers", compute_kind="python", group_name="raw/python", code_version="v1")
def raw_customers(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Ingest raw customer data from an external source."""
    connection = get_connection()
    insert_statement = "INSERT INTO raw_customers (id, first_name, last_name) VALUES (%s, %s, %s)"
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
    _create_table_if_not_exists(
        cursor,
        "raw_customers",
        "id INT NOT NULL, first_name VARCHAR(100), last_name VARCHAR(100)",
        key="id",
    )
    cursor.execute("TRUNCATE TABLE raw_customers")
    cursor.executemany(insert_statement, rows)
    connection.close()
    return dg.MaterializeResult(
        data_version=_data_version_for(rows),
        metadata={"num_rows": len(rows)},
    )


@dg.asset(key="raw_orders", compute_kind="python", group_name="raw/python", code_version="v1")
def raw_orders(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Ingest raw orders data from an external source."""
    connection = get_connection()
    insert_statement = (
        "INSERT INTO raw_orders (id, user_id, order_date, status) VALUES (%s, %s, %s, %s)"
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
    _create_table_if_not_exists(
        cursor,
        "raw_orders",
        "id INT NOT NULL, user_id INT, order_date DATETIME, status VARCHAR(50)",
        key="id",
    )
    cursor.execute("TRUNCATE TABLE raw_orders")
    cursor.executemany(insert_statement, rows)
    connection.close()
    return dg.MaterializeResult(
        data_version=_data_version_for(rows),
        metadata={"num_rows": len(rows)},
    )
