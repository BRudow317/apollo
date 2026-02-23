"""
Named bind type mapping for Oracle ``cursor.setinputsizes()``.

``setinputsizes`` must be called before ``executemany`` to pre-declare
Oracle types for each bind variable.  This prevents ``python-oracledb``
from inferring types from the first row, which can cause silent truncation
or type mismatch errors when early rows are null.

Mapping rules:
  - VARCHAR2  → ``oracledb.DB_TYPE_VARCHAR``
  - NUMBER    → ``oracledb.DB_TYPE_NUMBER``
  - DATE      → ``oracledb.DB_TYPE_DATE``
  - TIMESTAMP → ``oracledb.DB_TYPE_TIMESTAMP``
  - UNKNOWN   → ``oracledb.DB_TYPE_VARCHAR`` (safe fallback)

Usage::

    from src.loaders.binds import build_input_sizes
    sizes = build_input_sizes(meta)
    cursor.setinputsizes(**sizes)
    cursor.executemany(meta.insert_sql, rows, batcherrors=True)
"""

from __future__ import annotations

import oracledb

from src.models.models import OracleDataType, TableMeta

# ---------------------------------------------------------------------------
# Type map
# ---------------------------------------------------------------------------

_TYPE_MAP: dict[OracleDataType, object] = {
    "VARCHAR2":  oracledb.DB_TYPE_VARCHAR,
    "NUMBER":    oracledb.DB_TYPE_NUMBER,
    "DATE":      oracledb.DB_TYPE_DATE,
    "TIMESTAMP": oracledb.DB_TYPE_TIMESTAMP,
    "UNKNOWN":   oracledb.DB_TYPE_VARCHAR,  # safe fallback
}


def oracle_type_for(data_type: OracleDataType) -> object:
    """
    Return the ``oracledb`` DB type constant for a given Oracle data type string.

    Args:
        data_type: One of ``'VARCHAR2'``, ``'NUMBER'``, ``'DATE'``,
                   ``'TIMESTAMP'``, or ``'UNKNOWN'``.

    Returns:
        An ``oracledb.DB_TYPE_*`` constant.

    Raises:
        KeyError: If ``data_type`` is not in the type map.
    """
    if data_type not in _TYPE_MAP:
        raise KeyError(
            f"No Oracle bind type mapping for data_type '{data_type}'. "
            f"Valid types: {list(_TYPE_MAP)}"
        )
    return _TYPE_MAP[data_type]


def build_input_sizes(meta: TableMeta) -> dict[str, object]:
    """
    Build the ``**kwargs`` dict for ``cursor.setinputsizes()``.

    Keys are ``oracle_name`` strings (matching the named bind placeholders
    in ``meta.insert_sql``).  Values are ``oracledb`` DB type constants.

    Args:
        meta: Fully-populated ``TableMeta`` (``oracle_name`` and
              ``data_type`` must be set on every ``ColumnMap``).

    Returns:
        Dict suitable for ``cursor.setinputsizes(**sizes)``.

    Example::

        sizes = build_input_sizes(meta)
        # {"FIRST_NAME": DB_TYPE_VARCHAR, "AMOUNT": DB_TYPE_NUMBER, ...}
        cursor.setinputsizes(**sizes)
    """
    return {
        col.oracle_name: oracle_type_for(col.data_type)
        for col in meta.columns.values()
    }
