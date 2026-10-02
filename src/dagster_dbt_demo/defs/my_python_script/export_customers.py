#!/usr/bin/env python3
"""Export Oracle CUSTOMER to CSV.

Required environment variables: ORACLE_USER, ORACLE_PASSWORD, ORACLE_DNS.
Optional: ORACLE_TABLE (default CUSTOMER), ORACLE_OUTPUT.
Default output: <project_root>/export/customers.csv.
Uses python-oracledb Thin mode. Run: python export_customer.py
"""

import csv
import os
import re
import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import oracledb

BATCH_SIZE = 10_000


def number_as_decimal(cursor, metadata):
    # Preserve Oracle NUMBER precision, including amounts and long identifiers.
    if metadata.type_code == oracledb.DB_TYPE_NUMBER:
        return cursor.var(Decimal, arraysize=cursor.arraysize)
    return None


def csv_value(value):
    if isinstance(value, oracledb.LOB):
        value = value.read()
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
    required = ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DNS")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError("Missing environment variables: " + ", ".join(missing))

    table = os.environ.get("ORACLE_TABLE", "CUSTOMERS").strip()
    # Supports standard, unquoted TABLE or SCHEMA.TABLE names.
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_$#]*(\.[A-Za-z][A-Za-z0-9_$#]*)?", table):
        raise ValueError("ORACLE_TABLE must be TABLE or SCHEMA.TABLE.")

    project_root = Path(__file__).parents[4]
    default_output = project_root / "export" / "customers.csv"
    output = Path(
        os.environ.get("ORACLE_OUTPUT", str(default_output))
    ).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    temporary_path = None

    try:
        with oracledb.connect(
            user=os.environ["ORACLE_USER"],
            password=os.environ["ORACLE_PASSWORD"],
            dsn=os.environ["ORACLE_DNS"],
        ) as connection:
            with connection.cursor() as cursor:
                cursor.arraysize = BATCH_SIZE
                cursor.outputtypehandler = number_as_decimal
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
