"""
Reusable Oracle mock objects for Sprint 4+ unit tests.

Key design: ``MockConnection.cursor()`` returns a **fresh** ``MockCursor``
on every call.  All cursors created from the same connection share a single
query_results pool (consumed in order) and a single batch_errors list.
This mirrors real Oracle — you get a new cursor object each call, but the
underlying connection state is shared.

``cursor_obj`` is a property that returns the **most recently created** cursor,
preserving backward compatibility with tests that inspect ``conn.cursor_obj``.
"""

from __future__ import annotations

import sys
import types
from typing import Any


# ---------------------------------------------------------------------------
# Sentinel DB_TYPE objects
# ---------------------------------------------------------------------------

class _DBType:
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
    if "oracledb" in sys.modules:
        return

    mock_mod = types.ModuleType("oracledb")

    class _OracleError(Exception):
        pass

    mock_mod.Error          = _OracleError
    mock_mod.DatabaseError  = _OracleError
    mock_mod.InterfaceError = _OracleError

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
    Fake Oracle cursor.

    ``query_results`` is a **shared mutable list** passed in by
    ``MockConnection`` — each ``execute()`` call pops the next result set
    from that shared pool, so multiple cursors created from the same
    connection consume results in order.
    """

    def __init__(
        self,
        shared_results: list[list[tuple]],
        batch_errors: list[MockBatchError] | None = None,
    ) -> None:
        self._shared_results = shared_results          # shared reference
        self.batch_errors: list[MockBatchError] = list(batch_errors or [])
        self.executed: list[tuple[str, Any]] = []
        self._current_results: list[tuple] = []
        self.closed: bool = False
        self.bindarraysize: int = 1000
        self.input_sizes: dict = {}

    def execute(self, sql: str, params=None) -> None:
        if self.closed:
            raise RuntimeError("MockCursor: execute() called on closed cursor.")
        self.executed.append((sql.strip(), params))
        if self._shared_results:
            self._current_results = self._shared_results.pop(0)
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
        return self._current_results[0] if self._current_results else None

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
        for sql, params in reversed(self.executed):
            if "INSERT" in sql.upper() and isinstance(params, list):
                return params
        return None


# ---------------------------------------------------------------------------
# MockConnection
# ---------------------------------------------------------------------------

class MockConnection:
    """
    Fake Oracle connection.

    Every call to ``cursor()`` returns a **fresh** ``MockCursor``.
    All cursors share the same ``query_results`` pool and ``batch_errors``
    list — results are consumed in the order they were provided.

    ``cursor_obj`` is a property returning the most recently created cursor
    (backward-compatible with tests that inspect a single cursor).
    ``cursors`` holds all cursors ever created from this connection.
    """

    def __init__(
        self,
        query_results: list[list[tuple]] | None = None,
        batch_errors: list[MockBatchError] | None = None,
    ) -> None:
        self._shared_results: list[list[tuple]] = list(query_results or [])
        self._batch_errors: list[MockBatchError] = list(batch_errors or [])
        self.cursors: list[MockCursor] = []
        self.committed: int = 0
        self.closed: bool = False

    @property
    def cursor_obj(self) -> MockCursor | None:
        """Most recently created cursor — backward-compatible accessor."""
        return self.cursors[-1] if self.cursors else None

    def cursor(self) -> MockCursor:
        cur = MockCursor(self._shared_results, self._batch_errors)
        self.cursors.append(cur)
        return cur

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
