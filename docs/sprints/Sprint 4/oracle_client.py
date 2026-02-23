"""
Oracle connection management for the Apollo ingestion pipeline.

``oracledb`` is imported at module level — if it is not installed this
module will fail loudly on import with a clear ``ModuleNotFoundError``.
Install it with:  pip install python-oracledb

Usage:
    from src.discovery.oracle_client import connect, OracleSession

    # One-shot connection
    conn = connect(dsn="host:1521/service", user="scott", password="tiger")
    conn.close()

    # Context manager (auto-closes)
    with OracleSession(dsn="...", user="...", password="...") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
"""

from __future__ import annotations

import oracledb  # hard import — fails loudly if python-oracledb is not installed

from src.configs.exceptions import IngestionError

# ---------------------------------------------------------------------------
# Default session settings applied to every new connection
# ---------------------------------------------------------------------------
_SESSION_SQL = [
    # Always interpret DATE bind variables as Oracle DATE (not TIMESTAMP)
    "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
    # Always interpret TIMESTAMP bind variables with full precision
    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'",
]


def connect(
    dsn: str,
    user: str,
    password: str,
    apply_session_settings: bool = True,
    **kwargs,
):
    """
    Open a new ``oracledb`` connection and apply standard session settings.

    Args:
        dsn:                    Oracle DSN string (``host:port/service_name``).
        user:                   Oracle username.
        password:               Oracle password.
        apply_session_settings: If True, execute ``_SESSION_SQL`` statements
                                immediately after connecting.
        **kwargs:               Additional keyword args forwarded to
                                ``oracledb.connect()``.

    Returns:
        An open ``oracledb.Connection`` object.

    Raises:
        IngestionError: If the connection or session setup fails.
    """
    try:
        conn = oracledb.connect(dsn=dsn, user=user, password=password, **kwargs)
    except oracledb.Error as e:
        raise IngestionError(f"Failed to connect to Oracle ({dsn}): {e}") from e

    if apply_session_settings:
        _apply_session(conn)

    return conn


def _apply_session(conn) -> None:
    """Execute standard session-level SQL on an open connection."""
    try:
        with conn.cursor() as cur:
            for stmt in _SESSION_SQL:
                cur.execute(stmt)
    except oracledb.Error as e:
        raise IngestionError(f"Failed to apply session settings: {e}") from e


class OracleSession:
    """
    Context manager that opens and closes an Oracle connection.

    Args:
        dsn:      Oracle DSN string.
        user:     Oracle username.
        password: Oracle password.
        **kwargs: Forwarded to ``connect()``.

    Example::

        with OracleSession(dsn="host/svc", user="u", password="p") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
    """

    def __init__(self, dsn: str, user: str, password: str, **kwargs) -> None:
        self._dsn = dsn
        self._user = user
        self._password = password
        self._kwargs = kwargs
        self._conn = None

    def __enter__(self):
        self._conn = connect(self._dsn, self._user, self._password, **self._kwargs)
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass  # best-effort close
            self._conn = None
