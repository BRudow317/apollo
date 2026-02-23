"""
Phase 3 & 4: Remote Discovery (The Oracle Handshake) + Alignment.

Connects inbound ``TableMeta`` (from the local sniff) to the live Oracle
schema, resolves Scenario A (new table) or Scenario B (existing table),
executes any required DDL, and returns a ``DiscoveryResult`` describing
what happened.

After this module runs:
  - All ``ColumnMap.oracle_name`` values are confirmed against the DB.
  - All ``ColumnMap.is_new`` flags reflect reality.
  - Any necessary ``CREATE TABLE`` / ``ALTER TABLE ADD`` / ``ALTER TABLE MODIFY``
    has been executed.
  - ``meta._insert_sql`` cache is invalidated and re-locked for Phase 5+.

``ALL_TAB_COLUMNS`` columns used:
    COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, DATA_LENGTH,
    DATA_PRECISION, DATA_SCALE, NULLABLE, CHAR_USED, COLUMN_ID
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from src.configs.config import PipelineConfig
from src.configs.exceptions import DDLError, SizeBreachError
from src.discovery.ddl_builder import (
    build_alter_add,
    build_alter_modify,
    build_create_table,
)
from src.models.models import ColumnMap, TableMeta

# ---------------------------------------------------------------------------
# ALL_TAB_COLUMNS query
# ---------------------------------------------------------------------------
_TABLE_EXISTS_SQL = """
SELECT COUNT(*)
FROM ALL_TABLES
WHERE OWNER = :owner
  AND TABLE_NAME = :table_name
"""

_COLUMNS_SQL = """
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    CHAR_LENGTH,
    DATA_LENGTH,
    DATA_PRECISION,
    DATA_SCALE,
    NULLABLE,
    CHAR_USED,
    COLUMN_ID
FROM ALL_TAB_COLUMNS
WHERE OWNER = :owner
  AND TABLE_NAME = :table_name
ORDER BY COLUMN_ID
"""


# ---------------------------------------------------------------------------
# Result object
# ---------------------------------------------------------------------------

@dataclass
class DiscoveryResult:
    """
    Summary of what remote_discovery did.

    Attributes:
        scenario:          ``'A'`` (new table created) or ``'B'`` (existing table synced).
        ddl_executed:      DDL statements that were executed (or would be in dry-run).
        new_columns:       ``oracle_name`` values for columns that were ADDed.
        modified_columns:  ``oracle_name`` values for columns that were MODIFYed.
    """
    scenario: Literal["A", "B"]
    ddl_executed: list[str] = field(default_factory=list)
    new_columns: list[str] = field(default_factory=list)
    modified_columns: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def discover_and_sync(
    meta: TableMeta,
    connection,
    config: PipelineConfig,
    dry_run: bool = False,
) -> DiscoveryResult:
    """
    Sync ``meta`` with the live Oracle schema.

    Args:
        meta:       ``TableMeta`` produced by the local sniff (Phases 2 + 3).
        connection: Open ``oracledb`` connection (or any compatible mock).
        config:     Pipeline configuration.
        dry_run:    If ``True``, generate DDL but do not execute it.

    Returns:
        ``DiscoveryResult`` describing what was done (or would be done).

    Raises:
        SizeBreachError: If a column exceeds 4000 CHAR.
        DDLError:        If DDL generation fails.
    """
    cursor = connection.cursor()
    try:
        exists = _table_exists(cursor, meta.schema_name, meta.table_name)

        if not exists:
            result = _scenario_a(cursor, meta, config, dry_run)
        else:
            result = _scenario_b(cursor, meta, config, dry_run)

        # Lock insert_sql cache after schema is confirmed.
        meta.invalidate_sql_cache()
        _ = meta.insert_sql  # warm the cache — column order now frozen

        return result
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _table_exists(cursor, schema: str, table_name: str) -> bool:
    cursor.execute(_TABLE_EXISTS_SQL, {"owner": schema, "table_name": table_name})
    row = cursor.fetchone()
    return row is not None and row[0] > 0


def _fetch_db_columns(cursor, schema: str, table_name: str) -> dict[str, dict]:
    """
    Return ALL_TAB_COLUMNS rows as a dict keyed by COLUMN_NAME.

    Each value is a dict with lowercase keys:
      column_name, data_type, char_length, data_length,
      data_precision, data_scale, nullable, char_used, column_id
    """
    cursor.execute(_COLUMNS_SQL, {"owner": schema, "table_name": table_name})
    rows = cursor.fetchall()
    result = {}
    for row in rows:
        (col_name, data_type, char_length, data_length,
         data_precision, data_scale, nullable, char_used, column_id) = row
        result[col_name] = {
            "column_name": col_name,
            "data_type": data_type,
            "char_length": char_length or 0,
            "data_length": data_length or 0,
            "data_precision": data_precision,
            "data_scale": data_scale,
            "nullable": nullable == "Y",
            "char_used": char_used,
            "column_id": column_id,
        }
    return result


def _execute_ddl(cursor, sql: str, dry_run: bool) -> None:
    """Execute a DDL statement, or skip if dry_run."""
    if not dry_run:
        cursor.execute(sql)


def _scenario_a(cursor, meta: TableMeta, config: PipelineConfig, dry_run: bool) -> DiscoveryResult:
    """
    Scenario A: Table does not exist — generate and execute CREATE TABLE.

    All columns are marked ``is_new = True`` and ``oracle_name`` is set
    to ``target_name`` (since the table is being created fresh).
    """
    for col in meta.columns.values():
        col.oracle_name = col.target_name
        col.is_new = True

    ddl = build_create_table(meta, config)
    _execute_ddl(cursor, ddl, dry_run)

    return DiscoveryResult(
        scenario="A",
        ddl_executed=[ddl],
        new_columns=[c.oracle_name for c in meta.columns.values()],
    )


def _scenario_b(cursor, meta: TableMeta, config: PipelineConfig, dry_run: bool) -> DiscoveryResult:
    """
    Scenario B: Table exists — reconcile columns and resize as needed.

    Steps:
      1. Fetch ALL_TAB_COLUMNS for the existing table.
      2. Match each ColumnMap.target_name against db columns.
      3. New columns (no match) → ALTER TABLE ADD.
      4. Existing VARCHAR2 columns that need resizing → ALTER TABLE MODIFY.
    """
    db_columns = _fetch_db_columns(cursor, meta.schema_name, meta.table_name)
    result = DiscoveryResult(scenario="B")

    new_cols: list[ColumnMap] = []

    for col in meta.columns.values():
        db_col = db_columns.get(col.target_name)

        if db_col is None:
            # Column not in Oracle yet — needs ADD
            col.oracle_name = col.target_name
            col.is_new = True
            new_cols.append(col)
        else:
            # Column exists — confirm oracle_name and check sizing
            col.oracle_name = db_col["column_name"]
            col.is_new = False
            col.nullable = db_col["nullable"]

            # Phase 4: check if VARCHAR2 column needs resizing
            if (
                db_col["data_type"] == "VARCHAR2"
                and db_col["char_used"] == "C"  # already CHAR semantics
                and col.length > db_col["char_length"]
            ):
                modify_ddl = build_alter_modify(meta, col, config)
                _execute_ddl(cursor, modify_ddl, dry_run)
                result.ddl_executed.append(modify_ddl)
                result.modified_columns.append(col.oracle_name)

    # Execute a single ALTER TABLE ADD for all new columns at once
    if new_cols:
        add_ddl = build_alter_add(meta, new_cols, config)
        _execute_ddl(cursor, add_ddl, dry_run)
        result.ddl_executed.append(add_ddl)
        result.new_columns.extend(c.oracle_name for c in new_cols)

    return result
