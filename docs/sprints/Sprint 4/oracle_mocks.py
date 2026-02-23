"""
Reusable Oracle mock objects for Sprint 4+ unit tests.

Provides:
  - ``install_mock_oracledb()``  — inject a fake ``oracledb`` module into
                                   ``sys.modules`` BEFORE importing oracle_client.
  - ``MockCursor``               — tracks executed SQL and returns configurable results.
  - ``MockConnection``           — provides ``MockCursor``, tracks commits/closes.
  - ``make_tab_columns_rows()``  — helper to build ALL_TAB_COLUMNS-shaped tuples.

Usage in test files (must come before any src.discovery.oracle_client import)::

    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
    from tests.fixtures.oracle_mocks import install_mock_oracledb
    install_mock_oracledb()

    from src.discovery.oracle_client import OracleSession  # now safe
"""

from __future__ import annotations

import sys
import types
from typing import Any


# ---------------------------------------------------------------------------
# oracledb module mock
# ---------------------------------------------------------------------------

def install_mock_oracledb() -> None:
    """
    Inject a minimal fake ``oracledb`` module into ``sys.modules``.

    Safe to call multiple times.  Must be called before the first import
    of any module that does ``import oracledb``.
    """
    if "oracledb" in sys.modules:
        return  # already installed (real or mock)

    mock_mod = types.ModuleType("oracledb")

    class _OracleError(Exception):
        pass

    mock_mod.Error = _OracleError
    mock_mod.DatabaseError = _OracleError
    mock_mod.InterfaceError = _OracleError

    # Default connect raises — tests override this via MockConnection directly
    def _connect(**kwargs):
        raise _OracleError("Mock oracledb: use MockConnection directly in tests.")

    mock_mod.connect = _connect
    sys.modules["oracledb"] = mock_mod


# ---------------------------------------------------------------------------
# MockCursor
# ---------------------------------------------------------------------------

class MockCursor:
    """
    Fake Oracle cursor for unit tests.

    Configure ``query_results`` to control what ``fetchall()`` / ``fetchone()``
    return.  Results are consumed in the order they are queued — one entry
    per ``execute()`` call.

    Attributes:
        executed:       List of ``(sql, params)`` tuples for all calls to
                        ``execute()``.
        query_results:  List of result sets to return from ``fetchall()``.
                        Each entry is consumed once per ``execute()`` call.
        closed:         True once ``close()`` has been called.
    """

    def __init__(self, query_results: list[list[tuple]] | None = None) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.query_results: list[list[tuple]] = list(query_results or [])
        self._current_results: list[tuple] = []
        self.closed: bool = False
        self.bindarraysize: int = 1000

    def execute(self, sql: str, params=None) -> None:
        if self.closed:
            raise RuntimeError("MockCursor: execute() called on closed cursor.")
        self.executed.append((sql.strip(), params))
        # Consume the next queued result set
        if self.query_results:
            self._current_results = self.query_results.pop(0)
        else:
            self._current_results = []

    def executemany(self, sql: str, data, batcherrors: bool = False) -> None:
        if self.closed:
            raise RuntimeError("MockCursor: executemany() called on closed cursor.")
        rows = list(data)
        self.executed.append((sql.strip(), rows))
        self._current_results = []

    def fetchall(self) -> list[tuple]:
        return list(self._current_results)

    def fetchone(self) -> tuple | None:
        if self._current_results:
            return self._current_results[0]
        return None

    def getbatcherrors(self) -> list:
        return []

    def setinputsizes(self, **kwargs) -> None:
        pass  # no-op in mock

    def close(self) -> None:
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # Convenience: list of just the SQL strings (no params)
    @property
    def executed_sql(self) -> list[str]:
        return [sql for sql, _ in self.executed]


# ---------------------------------------------------------------------------
# MockConnection
# ---------------------------------------------------------------------------

class MockConnection:
    """
    Fake Oracle connection for unit tests.

    Args:
        query_results: Passed through to the internal ``MockCursor``.
                       One list per ``cursor.execute()`` call, in order.

    Attributes:
        committed:  Number of times ``commit()`` was called.
        closed:     True once ``close()`` has been called.
        cursor_obj: The single ``MockCursor`` instance (inspect after test).
    """

    def __init__(self, query_results: list[list[tuple]] | None = None) -> None:
        self.cursor_obj = MockCursor(query_results)
        self.committed: int = 0
        self.closed: bool = False

    def cursor(self) -> MockCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.committed += 1

    def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# ALL_TAB_COLUMNS row builder
# ---------------------------------------------------------------------------

def make_tab_columns_rows(
    columns: list[dict],
) -> list[tuple]:
    """
    Build a list of ``ALL_TAB_COLUMNS``-shaped tuples for use in mock results.

    Each dict in ``columns`` may contain:
      - ``column_name`` (str, required)
      - ``data_type``   (str, default ``'VARCHAR2'``)
      - ``char_length`` (int, default ``100``)
      - ``data_length`` (int, default ``400``)  — bytes
      - ``data_precision`` (int|None, default ``None``)
      - ``data_scale``     (int|None, default ``None``)
      - ``nullable``    (str ``'Y'``/``'N'``, default ``'Y'``)
      - ``char_used``   (str ``'C'``/``'B'``, default ``'C'``)
      - ``column_id``   (int, auto-incremented if not provided)

    Returns:
        List of 9-tuples matching the ``_COLUMNS_SQL`` SELECT order in
        ``remote_discovery.py``:
        (COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, DATA_LENGTH,
         DATA_PRECISION, DATA_SCALE, NULLABLE, CHAR_USED, COLUMN_ID)
    """
    rows = []
    for i, col in enumerate(columns, start=1):
        rows.append((
            col["column_name"],
            col.get("data_type", "VARCHAR2"),
            col.get("char_length", 100),
            col.get("data_length", 400),
            col.get("data_precision", None),
            col.get("data_scale", None),
            col.get("nullable", "Y"),
            col.get("char_used", "C"),
            col.get("column_id", i),
        ))
    return rows
