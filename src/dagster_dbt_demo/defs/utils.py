import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


class ConnectionFailedError(Exception):
    """Raised when the database host cannot be reached."""


def get_connection() -> "psycopg2.extensions.connection":
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    try:
        return psycopg2.connect(
            host=host,
            port=port,
            user=os.environ.get("POSTGRES_USER", "dbt"),
            password=os.environ.get("POSTGRES_PASSWORD", "dbt"),
            dbname=os.environ.get("POSTGRES_DB", "jaffle_shop"),
            # Không đặt thì host không phản hồi sẽ treo rất lâu (vd test tích hợp khi không có Postgres).
            connect_timeout=10,
        )
    except psycopg2.OperationalError as exc:
        raise ConnectionFailedError(f"Cannot reach Postgres at '{host}:{port}'") from exc
