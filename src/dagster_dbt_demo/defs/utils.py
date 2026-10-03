import os

import mysql.connector
from dotenv import load_dotenv
from mysql.connector import errorcode

load_dotenv()

class ConnectionFailedError(Exception):
    """Raised when the database host cannot be reached."""


def get_connection() -> mysql.connector.MySQLConnection:
    """Connect to the Doris FE over the MySQL protocol, using DORIS_SCHEMA as the database.

    Same database as the dbt `doris` target (Doris has no schema level: schema = database),
    created on first use so a fresh cluster works without manual setup.
    """
    database = os.environ.get("DORIS_SCHEMA", "jaffle_shop")
    params = {
        "host": os.environ.get("DORIS_HOST", "localhost"),
        "port": int(os.environ.get("DORIS_PORT", "9030")),
        "user": os.environ.get("DORIS_USER", "root"),
        "password": os.environ.get("DORIS_PASSWORD", ""),
        # Doris loads each INSERT as its own transaction; don't wrap them in client-side ones.
        "autocommit": True,
    }
    try:
        try:
            return mysql.connector.connect(database=database, **params)
        except mysql.connector.Error as exc:
            if exc.errno != errorcode.ER_BAD_DB_ERROR:
                raise
            connection = mysql.connector.connect(**params)
            connection.cursor().execute(f"CREATE DATABASE IF NOT EXISTS `{database}`")
            connection.database = database
            return connection
    except mysql.connector.Error as exc:
        if exc.errno not in (errorcode.CR_CONN_HOST_ERROR, errorcode.CR_UNKNOWN_HOST):
            raise
        raise ConnectionFailedError(
            f"Cannot reach Doris FE at '{params['host']}:{params['port']}'"
        ) from exc
