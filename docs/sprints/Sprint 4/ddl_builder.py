"""
DDL generation for the Apollo ingestion pipeline.

All functions are **pure** — they accept data structures and return SQL
strings.  No database connection is required.  This makes them trivially
testable and reusable from dry-run mode.

Responsibilities:
  - ``column_definition``    — single column DDL fragment
  - ``build_create_table``   — full CREATE TABLE statement (Scenario A)
  - ``build_alter_add``      — ALTER TABLE ... ADD for new columns (Scenario B)
  - ``build_alter_modify``   — ALTER TABLE ... MODIFY to resize a column (Phase 4)

Oracle rules enforced here:
  - All VARCHAR2 columns use CHAR length semantics: ``VARCHAR2(N CHAR)``
  - VARCHAR2 size = min(observed_max + growth_buffer, 4000)
  - NUMBER columns use ``NUMBER(precision, scale)`` if precision is known,
    else plain ``NUMBER``
  - DATE and TIMESTAMP columns have no size qualifier
  - Any column that would exceed 4000 CHAR raises ``SizeBreachError``
"""

from __future__ import annotations

from src.configs.config import PipelineConfig, ORACLE_MAX_VARCHAR2_CHAR
from src.configs.exceptions import SizeBreachError, DDLError
from src.models.models import ColumnMap, TableMeta


# ---------------------------------------------------------------------------
# Column definition fragment
# ---------------------------------------------------------------------------

def column_definition(col: ColumnMap, config: PipelineConfig) -> str:
    """
    Generate the DDL fragment for a single column.

    Examples::

        LAST_NAME VARCHAR2(150 CHAR) NULL
        AMOUNT NUMBER(12, 2) NULL
        CREATED_DATE DATE NULL
        LAST_MODIFIED TIMESTAMP NULL

    Args:
        col:    Fully-populated ``ColumnMap`` (data_type, length, precision,
                scale must be set).
        config: Pipeline configuration (supplies growth buffer).

    Returns:
        SQL fragment string — column name + type + nullability.

    Raises:
        SizeBreachError: If the column's observed length exceeds 4000 CHAR.
        DDLError:        If ``data_type`` is ``'UNKNOWN'``.
    """
    if col.data_type == "UNKNOWN":
        raise DDLError(
            f"Cannot generate DDL for column '{col.oracle_name}': data_type is UNKNOWN. "
            "Run type inference before building DDL.",
            ddl=col.oracle_name,
        )

    nullable_clause = "NULL" if col.nullable else "NOT NULL"
    type_clause = _type_clause(col, config)
    return f"{col.oracle_name} {type_clause} {nullable_clause}"


def _type_clause(col: ColumnMap, config: PipelineConfig) -> str:
    """Return the Oracle type string for a column (e.g. ``VARCHAR2(100 CHAR)``)."""
    if col.data_type == "VARCHAR2":
        if col.length > ORACLE_MAX_VARCHAR2_CHAR:
            raise SizeBreachError(
                f"Column '{col.oracle_name}' observed length {col.length} "
                f"exceeds VARCHAR2 limit of {ORACLE_MAX_VARCHAR2_CHAR} CHAR.",
                column_name=col.oracle_name,
                char_length=col.length,
                limit=ORACLE_MAX_VARCHAR2_CHAR,
            )
        sized = config.effective_max_varchar2(col.length) if col.length > 0 else config.varchar2_growth_buffer
        return f"VARCHAR2({sized} CHAR)"

    if col.data_type == "NUMBER":
        if col.precision is not None and col.scale is not None:
            return f"NUMBER({col.precision}, {col.scale})"
        if col.precision is not None:
            return f"NUMBER({col.precision})"
        return "NUMBER"

    if col.data_type == "DATE":
        return "DATE"

    if col.data_type == "TIMESTAMP":
        return "TIMESTAMP"

    raise DDLError(
        f"Unrecognised data_type '{col.data_type}' on column '{col.oracle_name}'.",
    )


# ---------------------------------------------------------------------------
# CREATE TABLE (Scenario A)
# ---------------------------------------------------------------------------

def build_create_table(meta: TableMeta, config: PipelineConfig) -> str:
    """
    Generate a ``CREATE TABLE`` statement for a new table.

    Args:
        meta:   ``TableMeta`` with all columns fully populated.
        config: Pipeline configuration.

    Returns:
        Complete ``CREATE TABLE`` SQL string.

    Raises:
        DDLError:        If ``meta.columns`` is empty.
        SizeBreachError: If any column exceeds 4000 CHAR.
    """
    if not meta.columns:
        raise DDLError(
            f"Cannot generate CREATE TABLE for {meta.qualified_name}: no columns defined."
        )

    col_defs = []
    for col in meta.columns.values():
        col_defs.append(f"    {column_definition(col, config)}")

    cols_sql = ",\n".join(col_defs)
    return (
        f"CREATE TABLE {meta.qualified_name} (\n"
        f"{cols_sql}\n"
        f")"
    )


# ---------------------------------------------------------------------------
# ALTER TABLE ADD (Scenario B — new columns in existing table)
# ---------------------------------------------------------------------------

def build_alter_add(
    meta: TableMeta,
    new_columns: list[ColumnMap],
    config: PipelineConfig,
) -> str:
    """
    Generate an ``ALTER TABLE ... ADD`` statement for new columns.

    Args:
        meta:        ``TableMeta`` identifying the target table.
        new_columns: List of ``ColumnMap`` objects that do not yet exist in
                     Oracle and must be added.
        config:      Pipeline configuration.

    Returns:
        ``ALTER TABLE ... ADD (col1 type1, col2 type2, ...)`` SQL string.

    Raises:
        DDLError:        If ``new_columns`` is empty.
        SizeBreachError: If any column exceeds 4000 CHAR.
    """
    if not new_columns:
        raise DDLError(
            f"build_alter_add called with empty new_columns for {meta.qualified_name}."
        )

    col_defs = ", ".join(column_definition(c, config) for c in new_columns)
    return f"ALTER TABLE {meta.qualified_name} ADD ({col_defs})"


# ---------------------------------------------------------------------------
# ALTER TABLE MODIFY (Phase 4 — resize existing VARCHAR2 column)
# ---------------------------------------------------------------------------

def build_alter_modify(
    meta: TableMeta,
    col: ColumnMap,
    config: PipelineConfig,
) -> str:
    """
    Generate an ``ALTER TABLE ... MODIFY`` statement to resize a VARCHAR2 column.

    Only VARCHAR2 columns can be resized this way.  Numeric and date columns
    are never modified by the pipeline — schema changes to those require manual
    intervention.

    Args:
        meta:   ``TableMeta`` identifying the target table.
        col:    ``ColumnMap`` with the updated ``length`` value from the sniff.
        config: Pipeline configuration (applies growth buffer).

    Returns:
        ``ALTER TABLE ... MODIFY (col_name VARCHAR2(N CHAR))`` SQL string.

    Raises:
        DDLError:        If the column is not VARCHAR2.
        SizeBreachError: If the new size exceeds 4000 CHAR.
    """
    if col.data_type != "VARCHAR2":
        raise DDLError(
            f"build_alter_modify only supports VARCHAR2 columns; "
            f"'{col.oracle_name}' is {col.data_type}.",
            ddl=col.oracle_name,
        )

    if col.length > ORACLE_MAX_VARCHAR2_CHAR:
        raise SizeBreachError(
            f"Column '{col.oracle_name}' observed length {col.length} "
            f"exceeds VARCHAR2 limit of {ORACLE_MAX_VARCHAR2_CHAR} CHAR.",
            column_name=col.oracle_name,
            char_length=col.length,
            limit=ORACLE_MAX_VARCHAR2_CHAR,
        )

    new_size = config.effective_max_varchar2(col.length)
    return f"ALTER TABLE {meta.qualified_name} MODIFY ({col.oracle_name} VARCHAR2({new_size} CHAR))"
