"""
Named bind type mapping for Oracle ``cursor.setinputsizes()``.

``oracledb`` is imported lazily inside each function — this allows
``validate`` and ``dry-run`` CLI commands to work without ``python-oracledb``
installed, since those paths never reach the loader.

Mapping rules:
  - VARCHAR2  → ``oracledb.DB_TYPE_VARCHAR``
  - NUMBER    → ``oracledb.DB_TYPE_NUMBER``
  - DATE      → ``oracledb.DB_TYPE_DATE``
  - TIMESTAMP → ``oracledb.DB_TYPE_TIMESTAMP``
  - UNKNOWN   → ``oracledb.DB_TYPE_VARCHAR`` (safe fallback)
"""

from __future__ import annotations

from src.models.models import OracleDataType, TableMeta


def oracle_type_for(data_type: OracleDataType) -> object:
    """
    Return the ``oracledb`` DB type constant for a given Oracle data type string.

    Raises:
        KeyError: If ``data_type`` is not in the type map.
    """
    import oracledb  # lazy — only needed when actually loading rows

    type_map: dict[OracleDataType, object] = {
        "VARCHAR2":  oracledb.DB_TYPE_VARCHAR,
        "NUMBER":    oracledb.DB_TYPE_NUMBER,
        "DATE":      oracledb.DB_TYPE_DATE,
        "TIMESTAMP": oracledb.DB_TYPE_TIMESTAMP,
        "UNKNOWN":   oracledb.DB_TYPE_VARCHAR,
    }

    if data_type not in type_map:
        raise KeyError(
            f"No Oracle bind type mapping for data_type '{data_type}'. "
            f"Valid types: {list(type_map)}"
        )
    return type_map[data_type]


def build_input_sizes(meta: TableMeta) -> dict[str, object]:
    """
    Build the ``**kwargs`` dict for ``cursor.setinputsizes()``.

    Keys are ``oracle_name`` strings; values are ``oracledb`` DB type constants.
    """
    return {
        col.oracle_name: oracle_type_for(col.data_type)
        for col in meta.columns.values()
    }
