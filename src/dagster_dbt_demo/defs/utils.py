import os
import oracledb
from dotenv import load_dotenv

load_dotenv()

class ConnectionFailedError(Exception):
    """Raised when the database host cannot be reached."""


def get_connection() -> oracledb.Connection:
    try:
        return oracledb.connect(
            user=os.environ["ORACLE_USER"],
            password=os.environ["ORACLE_PASSWORD"],
            dsn=os.environ["ORACLE_DNS"],
        )
    except oracledb.Error:
        raise
    except OSError as exc:
        # DNS failures and refused sockets can surface as bare OSError
        # (e.g. socket.gaierror) rather than an oracledb.Error.
        raise ConnectionFailedError(
            f"Cannot reach Oracle at '{os.environ['DSN']}' "
        ) from exc
