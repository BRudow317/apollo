"""
Reusable Oracle mock objects for Sprint 4+ unit tests.

Provides:
  - ``install_mock_oracledb()``  — inject a fake ``oracledb`` module into
                                   ``sys.modules`` BEFORE importing oracle_client.
  - ``MockCursor``               — tracks executed SQL, returns configurable results,
                                   supports getbatcherrors().
  - ``MockConnection``           — provides ``MockCursor``, tracks commits/closes.
  - ``MockBatchError``           — simulates a single row-level batch error.
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
# Sentinel DB_TYPE objects
# ---------------------------------------------------------------------------

class _DBType:
    """Lightweight stand-in for oracledb.DB_TYPE_* constants."""
    def __init__(self, name: str) -> None:
        self.name = name
    def __repr__(self) -> str:
        return f"DB_TYPE_{self.name}"


DB_TYPE_VARCHAR   = _DBType("VARCHAR")
DB_TYPE_NUMBER    = _DBType("NUMBER")
DB_TYPE_DATE      = _DBType("DATE")
DB_TYPE_TIMESTAMP = _DBType("TIMESTAMP")


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
        return

    mock_mod = types.ModuleType("oracledb")

    class _OracleError(Exception):
        pass

    mock_mod.Error          = _OracleError
    mock_mod.DatabaseError  = _OracleError
    mock_mod.InterfaceError = _OracleError

    # DB_TYPE constants used by binds.py
    mock_mod.DB_TYPE_VARCHAR   = DB_TYPE_VARCHAR
    mock_mod.DB_TYPE_NUMBER    = DB_TYPE_NUMBER
    mock_mod.DB_TYPE_DATE      = DB_TYPE_DATE
    mock_mod.DB_TYPE_TIMESTAMP = DB_TYPE_TIMESTAMP

    def _connect(**kwargs):
        raise _OracleError("Mock oracledb: use MockConnection directly in tests.")

    mock_mod.connect = _connect
    sys.modules["oracledb"] = mock_mod


# ---------------------------------------------------------------------------
# MockBatchError
# ---------------------------------------------------------------------------

class MockBatchError:
    """
    Simulates a single row-level error from ``cursor.getbatcherrors()``.

    Args:
        offset:  0-based index of the row that failed in the batch.
        message: Oracle error message string (e.g. ``"ORA-12899: value too large"``).
    """

    def __init__(self, offset: int, message: str) -> None:
        self.offset = offset
        self.message = message

    def __repr__(self) -> str:
        return f"MockBatchError(offset={self.offset}, message={self.message!r})"


# ---------------------------------------------------------------------------
# MockCursor
# ---------------------------------------------------------------------------

class MockCursor:
    """
    Fake Oracle cursor for unit tests.

    Attributes:
        executed:       List of ``(sql, params)`` tuples.
        query_results:  Queued result sets for fetchall()/fetchone().
        batch_errors:   Returned by getbatcherrors() after executemany().
        closed:         True once close() has been called.
        input_sizes:    Captured kwargs from setinputsizes().
        bindarraysize:  Captured value set by caller.
    """

    def __init__(
        self,
        query_results: list[list[tuple]] | None = None,
        batch_errors: list[MockBatchError] | None = None,
    ) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.query_results: list[list[tuple]] = list(query_results or [])
        self._current_results: list[tuple] = []
        self.batch_errors: list[MockBatchError] = list(batch_errors or [])
        self.closed: bool = False
        self.bindarraysize: int = 1000
        self.input_sizes: dict = {}

    def execute(self, sql: str, params=None) -> None:
        if self.closed:
            raise RuntimeError("MockCursor: execute() called on closed cursor.")
        self.executed.append((sql.strip(), params))
        if self.query_results:
            self._current_results = self.query_results.pop(0)
        else:
            self._current_results = []

    def executemany(self, sql: str, data, batcherrors: bool = False) -> None:
        if self.closed:
            raise RuntimeError("MockCursor: executemany() called on closed cursor.")
        row_list = list(data)
        self.executed.append((sql.strip(), row_list))
        self._current_results = []

    def fetchall(self) -> list[tuple]:
        return list(self._current_results)

    def fetchone(self) -> tuple | None:
        if self._current_results:
            return self._current_results[0]
        return None

    def getbatcherrors(self) -> list[MockBatchError]:
        return list(self.batch_errors)

    def setinputsizes(self, **kwargs) -> None:
        self.input_sizes.update(kwargs)

    def close(self) -> None:
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def executed_sql(self) -> list[str]:
        return [sql for sql, _ in self.executed]

    @property
    def executemany_rows(self) -> list[dict] | None:
        """Return the row list from the most recent executemany call, or None."""
        for sql, params in reversed(self.executed):
            if "INSERT" in sql.upper() and isinstance(params, list):
                return params
        return None


# ---------------------------------------------------------------------------
# MockConnection
# ---------------------------------------------------------------------------

class MockConnection:
    """
    Fake Oracle connection for unit tests.

    Args:
        query_results: Passed through to the internal MockCursor.
        batch_errors:  Passed through to the internal MockCursor.
    """

    def __init__(
        self,
        query_results: list[list[tuple]] | None = None,
        batch_errors: list[MockBatchError] | None = None,
    ) -> None:
        self.cursor_obj = MockCursor(query_results, batch_errors)
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

def make_tab_columns_rows(columns: list[dict]) -> list[tuple]:
    """
    Build ALL_TAB_COLUMNS-shaped tuples for mock query results.

    Returns 9-tuples: (COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, DATA_LENGTH,
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
