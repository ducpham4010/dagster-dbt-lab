#!/usr/bin/env python3
"""Export Postgres `customers` (built by dbt) to CSV.

Connection: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
(same variables and defaults as the dbt `postgres` target).
Optional: POSTGRES_TABLE (default <POSTGRES_SCHEMA>.customers, schema mặc định analytics), POSTGRES_OUTPUT.
Default output: <project_root>/export/customers.csv.
Uses psycopg2. Run: python export_customers.py
"""

import csv
import os
import re
import tempfile
from contextlib import closing
from datetime import date, datetime
from pathlib import Path

import psycopg2

BATCH_SIZE = 10_000


def csv_value(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return value


def export_customer() -> tuple[Path, int]:
    schema = os.environ.get("POSTGRES_SCHEMA", "analytics")
    table = os.environ.get("POSTGRES_TABLE", f"{schema}.customers").strip()
    # Supports unquoted TABLE or SCHEMA.TABLE names.
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z][A-Za-z0-9_]*)?", table):
        raise ValueError("POSTGRES_TABLE must be TABLE or SCHEMA.TABLE.")

    project_root = Path(__file__).parents[4]
    default_output = project_root / "export" / "customers.csv"
    output = Path(
        os.environ.get("POSTGRES_OUTPUT", str(default_output))
    ).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    temporary_path = None

    try:
        # `with psycopg2.connect(...)` chỉ commit/rollback chứ không đóng kết nối, nên dùng closing().
        with closing(psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            user=os.environ.get("POSTGRES_USER", "dbt"),
            password=os.environ.get("POSTGRES_PASSWORD", "dbt"),
            dbname=os.environ.get("POSTGRES_DB", "jaffle_shop"),
            connect_timeout=10,
        )) as connection:
            # NUMERIC columns come back as Decimal.
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table}")

                with tempfile.NamedTemporaryFile(
                    mode="w",
                    newline="",
                    encoding="utf-8-sig",
                    dir=output.parent,
                    prefix=output.name + ".",
                    suffix=".tmp",
                    delete=False,
                ) as file:
                    temporary_path = Path(file.name)
                    writer = csv.writer(file)
                    writer.writerow([column[0] for column in cursor.description])

                    while True:
                        rows = cursor.fetchmany(BATCH_SIZE)
                        if not rows:
                            break
                        writer.writerows(
                            [csv_value(value) for value in row] for row in rows
                        )
                        row_count += len(rows)
                        print(f"Exported {row_count:,} rows", flush=True)

        # Replace the final file only when the entire export succeeds.
        temporary_path.replace(output)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    print(f"Done: {table} -> {output} ({row_count:,} rows)", flush=True)
    return output, row_count


if __name__ == "__main__":
    export_customer()
