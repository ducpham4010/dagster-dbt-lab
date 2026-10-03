#!/usr/bin/env python3
"""Export Doris `customers` (built by dbt) to CSV.

Connection: DORIS_HOST, DORIS_PORT, DORIS_USER, DORIS_PASSWORD, DORIS_SCHEMA
(same variables and defaults as the dbt `doris` target).
Optional: DORIS_TABLE (default customers), DORIS_OUTPUT.
Default output: <project_root>/export/customers.csv.
Uses mysql-connector-python (Doris FE speaks the MySQL protocol). Run: python export_customers.py
"""

import csv
import os
import re
import tempfile
from datetime import date, datetime
from pathlib import Path

import mysql.connector

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
    table = os.environ.get("DORIS_TABLE", "customers").strip()
    # Supports unquoted TABLE or DATABASE.TABLE names (Doris table names are case-sensitive).
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z][A-Za-z0-9_]*)?", table):
        raise ValueError("DORIS_TABLE must be TABLE or DATABASE.TABLE.")

    project_root = Path(__file__).parents[4]
    default_output = project_root / "export" / "customers.csv"
    output = Path(
        os.environ.get("DORIS_OUTPUT", str(default_output))
    ).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    temporary_path = None

    try:
        with mysql.connector.connect(
            host=os.environ.get("DORIS_HOST", "localhost"),
            port=int(os.environ.get("DORIS_PORT", "9030")),
            user=os.environ.get("DORIS_USER", "root"),
            password=os.environ.get("DORIS_PASSWORD", ""),
            database=os.environ.get("DORIS_SCHEMA", "jaffle_shop"),
        ) as connection:
            # Unbuffered cursor streams rows; DECIMAL columns come back as Decimal.
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
