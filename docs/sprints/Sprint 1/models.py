"""
Core data models for the ingestion pipeline.

ColumnMap   — metadata for a single column (source → Oracle mapping).
TableMeta   — metadata for the target Oracle table; owns the cached insert SQL.

Named bind strategy
-------------------
All DML uses Oracle named binds:

    INSERT INTO SCHEMA.TABLE (COL_A, COL_B) VALUES (:COL_A, :COL_B)

Bind names are derived from ``oracle_name`` on each ``ColumnMap``.
Column order is locked after Phase 4 metadata refresh and the
``_insert_sql`` cache is frozen at that point.  Do not mutate
``columns`` after calling ``insert_sql`` without calling
``invalidate_sql_cache()`` first.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


# Supported Oracle data type labels used across the pipeline.
OracleDataType = Literal["VARCHAR2", "NUMBER", "DATE", "TIMESTAMP", "UNKNOWN"]


@dataclass(slots=True)
class ColumnMap:
    """
    Metadata for a single column, tracking both CSV-side and Oracle-side attributes.

    Attributes:
        source_key:       Raw header string from the CSV (e.g. ``"Last Name"``).
        target_name:      Sanitized identifier candidate (e.g. ``"LAST_NAME"``).
        oracle_name:      Actual column name in ``ALL_TAB_COLUMNS`` once confirmed.
                          Equals ``target_name`` for new columns; may differ if the
                          DB column was created with a slightly different name.
        data_type:        Oracle data type string.
        length:           Max character length observed in the CSV (not bytes).
        max_byte_len:     Max UTF-8 byte length observed in the CSV.
        nullable:         Whether the column may contain NULLs.
        is_new:           True if the column must be ADDed or CREATEd in Oracle.
        precision:        For NUMBER columns: total significant digits.
        scale:            For NUMBER columns: digits after the decimal point.
        length_semantics: Always ``'CHAR'``; byte semantics are never used.
    """

    source_key: str
    target_name: str
    oracle_name: str = ""
    data_type: OracleDataType = "UNKNOWN"
    length: int = 0
    max_byte_len: int = 0
    nullable: bool = True
    is_new: bool = True
    precision: int | None = None
    scale: int | None = None
    length_semantics: str = "CHAR"

    def __post_init__(self) -> None:
        if not self.oracle_name:
            # Default oracle_name to target_name if not explicitly provided.
            object.__setattr__(self, "oracle_name", self.target_name)

    @property
    def bind_name(self) -> str:
        """The Oracle named bind placeholder, e.g. ``:LAST_NAME``."""
        return f":{self.oracle_name}"


@dataclass
class TableMeta:
    """
    Metadata for the target Oracle table.

    Attributes:
        table_name:  Sanitized Oracle table name (no schema prefix).
        schema_name: Sanitized Oracle schema/owner.
        columns:     Ordered dict of ``ColumnMap`` objects keyed by ``source_key``.
                     Order is insertion order (Python 3.7+) and is locked after
                     Phase 4 refresh.

    Notes:
        ``insert_sql`` is cached after first access.  If you modify ``columns``
        post-cache (e.g. after an ALTER), call ``invalidate_sql_cache()`` before
        reading ``insert_sql`` again so the new column list is reflected.
    """

    table_name: str
    schema_name: str
    columns: dict[str, ColumnMap] = field(default_factory=dict)

    # Internal cache — not part of the public interface.
    _insert_sql: str | None = field(default=None, init=False, repr=False, compare=False)

    @property
    def qualified_name(self) -> str:
        """``SCHEMA.TABLE`` string for use in DDL/DML."""
        return f"{self.schema_name}.{self.table_name}"

    @property
    def insert_sql(self) -> str:
        """
        Named-bind INSERT statement for this table.

        Generated once from the current state of ``columns`` and cached.
        Column order in the statement matches the iteration order of ``columns``.

        Returns:
            A SQL string of the form::

                INSERT INTO SCHEMA.TABLE (COL_A, COL_B)
                VALUES (:COL_A, :COL_B)

        Raises:
            ValueError: If ``columns`` is empty.
        """
        if self._insert_sql is not None:
            return self._insert_sql

        if not self.columns:
            raise ValueError(
                f"Cannot generate insert_sql for {self.qualified_name}: columns is empty."
            )

        col_maps = list(self.columns.values())
        col_list = ", ".join(c.oracle_name for c in col_maps)
        bind_list = ", ".join(c.bind_name for c in col_maps)

        sql = (
            f"INSERT INTO {self.qualified_name} ({col_list})\n"
            f"VALUES ({bind_list})"
        )
        # Store via object.__setattr__ to work with dataclass field tracking.
        object.__setattr__(self, "_insert_sql", sql)
        return self._insert_sql  # type: ignore[return-value]

    def invalidate_sql_cache(self) -> None:
        """
        Clear the cached ``insert_sql``.

        Call this after modifying ``columns`` (e.g. after Phase 4 schema refresh)
        so the next access to ``insert_sql`` regenerates from the updated column list.
        """
        object.__setattr__(self, "_insert_sql", None)

    def ordered_oracle_names(self) -> list[str]:
        """Return column names in insertion order — matches bind order in ``insert_sql``."""
        return [c.oracle_name for c in self.columns.values()]
