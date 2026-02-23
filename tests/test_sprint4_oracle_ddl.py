"""
Sprint 4 — Oracle Discovery & DDL: test_sprint4_oracle_ddl.py

Must inject mock oracledb BEFORE any src imports — done at the top of this file.

Cumulative — re-affirms Sprint 1–3 contracts via smoke tests, then covers:

DDL Builder (pure, no DB):
  - column_definition generates correct VARCHAR2(N CHAR) / NUMBER / DATE / TIMESTAMP
  - VARCHAR2_GROWTH_BUFFER from config applied to sizing
  - VARCHAR2 capped at 4000; exceeding raises SizeBreachError
  - UNKNOWN data_type raises DDLError
  - build_create_table produces valid CREATE TABLE DDL
  - build_alter_add produces ALTER TABLE ADD DDL
  - build_alter_modify produces ALTER TABLE MODIFY DDL
  - Non-VARCHAR2 column passed to build_alter_modify raises DDLError
  - Empty columns raises DDLError for CREATE TABLE and ALTER ADD

Remote Discovery (mocked DB):
  - Scenario A: table absent → CREATE TABLE executed, all cols is_new=True
  - Scenario B: table exists, all cols match → no DDL executed
  - Scenario B: table exists, new column → ALTER TABLE ADD executed
  - Scenario B: VARCHAR2 col too small → ALTER TABLE MODIFY executed
  - Scenario B: col already large enough → no MODIFY
  - dry_run=True → DDL returned but not executed
  - insert_sql cache locked after discover_and_sync

Oracle Client:
  - OracleSession calls connect and closes on exit
  - Failed connect raises IngestionError
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# CRITICAL: inject mock oracledb before any src.discovery imports
# ---------------------------------------------------------------------------
import sys
import pathlib

_root = str(pathlib.Path(__file__).parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

from tests.fixtures.oracle_mocks import (
    install_mock_oracledb,
    MockConnection,
    MockCursor,
    make_tab_columns_rows,
)
install_mock_oracledb()

# ---------------------------------------------------------------------------
# Now safe to import src modules
# ---------------------------------------------------------------------------
import pytest

from src.models.models import ColumnMap, TableMeta
from src.configs.config import PipelineConfig, ORACLE_MAX_VARCHAR2_CHAR
from src.configs.exceptions import (
    IngestionError,
    QuarantineError,
    SizeBreachError,
    DDLError,
)
from src.utils.sanitizer import sanitize_identifier
from src.discovery.oracle_client import OracleSession, connect
from src.discovery.ddl_builder import (
    column_definition,
    build_create_table,
    build_alter_add,
    build_alter_modify,
)
from src.discovery.remote_discovery import discover_and_sync, DiscoveryResult


# ============================================================================
# Helpers
# ============================================================================

def make_config(**kwargs) -> PipelineConfig:
    return PipelineConfig(**kwargs)


def make_col(
    source_key: str = "name",
    target_name: str = "NAME",
    data_type="VARCHAR2",
    length: int = 50,
    precision=None,
    scale=None,
    nullable: bool = True,
) -> ColumnMap:
    col = ColumnMap(source_key=source_key, target_name=target_name)
    col.data_type = data_type
    col.length = length
    col.precision = precision
    col.scale = scale
    col.nullable = nullable
    return col


def make_meta(cols: list[ColumnMap], table="CONTACTS", schema="SALES") -> TableMeta:
    return TableMeta(
        table_name=table,
        schema_name=schema,
        columns={c.source_key: c for c in cols},
    )


# ============================================================================
# Prior sprint smoke tests
# ============================================================================

class TestPriorSprintSmoke:
    def test_sprint1_column_map(self):
        col = ColumnMap(source_key="x", target_name="X")
        assert col.bind_name == ":X"

    def test_sprint1_sanitizer(self):
        assert sanitize_identifier("date") == "DATE_COL"

    def test_sprint2_alignment_error(self):
        from src.configs.exceptions import AlignmentError
        assert issubclass(AlignmentError, QuarantineError)

    def test_sprint3_type_infer(self):
        from src.transformers.typing_infer import infer_cell_type
        assert infer_cell_type("2024-01-01") == "DATE"
        assert infer_cell_type("N/A") == "VARCHAR2"


# ============================================================================
# column_definition
# ============================================================================

class TestColumnDefinition:
    def test_varchar2_uses_char_semantics(self):
        col = make_col(length=80)
        cfg = make_config(varchar2_growth_buffer=20)
        result = column_definition(col, cfg)
        assert "CHAR" in result
        assert "VARCHAR2(100 CHAR)" in result

    def test_varchar2_applies_growth_buffer(self):
        col = make_col(length=100)
        cfg = make_config(varchar2_growth_buffer=50)
        result = column_definition(col, cfg)
        assert "VARCHAR2(150 CHAR)" in result

    def test_varchar2_capped_at_4000(self):
        col = make_col(length=3980)
        cfg = make_config(varchar2_growth_buffer=100)
        result = column_definition(col, cfg)
        assert "VARCHAR2(4000 CHAR)" in result

    def test_varchar2_breach_raises(self):
        col = make_col(length=4001)
        with pytest.raises(SizeBreachError):
            column_definition(col, make_config())

    def test_number_with_precision_and_scale(self):
        col = make_col(data_type="NUMBER", length=0, precision=12, scale=2)
        result = column_definition(col, make_config())
        assert "NUMBER(12, 2)" in result

    def test_number_with_precision_only(self):
        col = make_col(data_type="NUMBER", length=0, precision=10, scale=None)
        result = column_definition(col, make_config())
        assert "NUMBER(10)" in result

    def test_number_bare(self):
        col = make_col(data_type="NUMBER", length=0, precision=None, scale=None)
        result = column_definition(col, make_config())
        assert "NUMBER" in result
        assert "(" not in result

    def test_date_type(self):
        col = make_col(data_type="DATE", length=0)
        result = column_definition(col, make_config())
        assert "DATE" in result
        assert "(" not in result

    def test_timestamp_type(self):
        col = make_col(data_type="TIMESTAMP", length=0)
        result = column_definition(col, make_config())
        assert "TIMESTAMP" in result

    def test_nullable_clause(self):
        col = make_col(nullable=True)
        result = column_definition(col, make_config())
        assert "NULL" in result

    def test_not_null_clause(self):
        col = make_col(nullable=False)
        result = column_definition(col, make_config())
        assert "NOT NULL" in result

    def test_unknown_type_raises_ddl_error(self):
        col = make_col()
        col.data_type = "UNKNOWN"
        with pytest.raises(DDLError):
            column_definition(col, make_config())

    def test_column_name_in_definition(self):
        col = make_col(target_name="LAST_NAME", length=100)
        result = column_definition(col, make_config())
        assert result.startswith("LAST_NAME")


# ============================================================================
# build_create_table
# ============================================================================

class TestBuildCreateTable:
    def test_basic_create_table(self):
        col = make_col(target_name="NAME", length=50)
        meta = make_meta([col])
        sql = build_create_table(meta, make_config())
        assert "CREATE TABLE SALES.CONTACTS" in sql
        assert "NAME" in sql
        assert "VARCHAR2" in sql

    def test_qualified_name_in_sql(self):
        col = make_col(target_name="ID", data_type="NUMBER", length=0)
        meta = make_meta([col], table="ACCOUNTS", schema="CRM")
        sql = build_create_table(meta, make_config())
        assert "CRM.ACCOUNTS" in sql

    def test_multi_column_all_present(self):
        cols = [
            make_col("name", "NAME", "VARCHAR2", 50),
            make_col("amount", "AMOUNT", "NUMBER", 0, precision=12, scale=2),
            make_col("created", "CREATED_DATE", "DATE", 0),
        ]
        meta = make_meta(cols)
        sql = build_create_table(meta, make_config())
        assert "NAME" in sql
        assert "AMOUNT" in sql
        assert "CREATED_DATE" in sql

    def test_empty_columns_raises_ddl_error(self):
        meta = TableMeta(table_name="T", schema_name="S")
        with pytest.raises(DDLError):
            build_create_table(meta, make_config())

    def test_size_breach_propagates(self):
        col = make_col(length=4001)
        meta = make_meta([col])
        with pytest.raises(SizeBreachError):
            build_create_table(meta, make_config())


# ============================================================================
# build_alter_add
# ============================================================================

class TestBuildAlterAdd:
    def test_basic_alter_add(self):
        col = make_col(target_name="EMAIL", length=100)
        meta = make_meta([col])
        sql = build_alter_add(meta, [col], make_config())
        assert "ALTER TABLE SALES.CONTACTS ADD" in sql
        assert "EMAIL" in sql
        assert "VARCHAR2" in sql

    def test_multiple_columns(self):
        col_a = make_col("a", "COL_A", "VARCHAR2", 50)
        col_b = make_col("b", "COL_B", "NUMBER", 0, precision=5)
        meta = make_meta([col_a, col_b])
        sql = build_alter_add(meta, [col_a, col_b], make_config())
        assert "COL_A" in sql
        assert "COL_B" in sql

    def test_empty_columns_raises_ddl_error(self):
        meta = make_meta([make_col()])
        with pytest.raises(DDLError):
            build_alter_add(meta, [], make_config())


# ============================================================================
# build_alter_modify
# ============================================================================

class TestBuildAlterModify:
    def test_basic_modify(self):
        col = make_col(target_name="NOTES", length=200)
        meta = make_meta([col])
        sql = build_alter_modify(meta, col, make_config(varchar2_growth_buffer=50))
        assert "ALTER TABLE SALES.CONTACTS MODIFY" in sql
        assert "NOTES" in sql
        assert "VARCHAR2(250 CHAR)" in sql

    def test_modify_caps_at_4000(self):
        col = make_col(target_name="BIG_COL", length=3990)
        meta = make_meta([col])
        sql = build_alter_modify(meta, col, make_config(varchar2_growth_buffer=100))
        assert "VARCHAR2(4000 CHAR)" in sql

    def test_non_varchar2_raises_ddl_error(self):
        col = make_col(data_type="NUMBER", length=0, precision=5)
        meta = make_meta([col])
        with pytest.raises(DDLError):
            build_alter_modify(meta, col, make_config())

    def test_breach_raises_size_breach_error(self):
        col = make_col(target_name="BAD", length=4001)
        meta = make_meta([col])
        with pytest.raises(SizeBreachError):
            build_alter_modify(meta, col, make_config())


# ============================================================================
# Remote Discovery — Scenario A (new table)
# ============================================================================

class TestScenarioA:
    def _make_conn_new_table(self) -> MockConnection:
        # First query: ALL_TABLES COUNT → 0 (table does not exist)
        return MockConnection(query_results=[
            [(0,)],   # _TABLE_EXISTS_SQL → count = 0
        ])

    def test_scenario_a_returns_correct_scenario(self):
        col = make_col(target_name="NAME", length=50)
        meta = make_meta([col])
        conn = self._make_conn_new_table()
        result = discover_and_sync(meta, conn, make_config())
        assert result.scenario == "A"

    def test_scenario_a_executes_create_table(self):
        col = make_col(target_name="NAME", length=50)
        meta = make_meta([col])
        conn = self._make_conn_new_table()
        result = discover_and_sync(meta, conn, make_config())
        assert len(result.ddl_executed) == 1
        assert "CREATE TABLE" in result.ddl_executed[0]

    def test_scenario_a_all_columns_marked_new(self):
        cols = [make_col("a", "A"), make_col("b", "B")]
        meta = make_meta(cols)
        conn = self._make_conn_new_table()
        discover_and_sync(meta, conn, make_config())
        for col in meta.columns.values():
            assert col.is_new is True

    def test_scenario_a_oracle_name_set_to_target_name(self):
        col = make_col(target_name="FIRST_NAME", length=50)
        meta = make_meta([col])
        conn = self._make_conn_new_table()
        discover_and_sync(meta, conn, make_config())
        assert meta.columns["name"].oracle_name == "FIRST_NAME"

    def test_scenario_a_new_columns_in_result(self):
        col = make_col(target_name="EMAIL", length=100)
        meta = make_meta([col])
        conn = self._make_conn_new_table()
        result = discover_and_sync(meta, conn, make_config())
        assert "EMAIL" in result.new_columns

    def test_scenario_a_dry_run_no_execute(self):
        col = make_col(target_name="NAME", length=50)
        meta = make_meta([col])
        conn = self._make_conn_new_table()
        result = discover_and_sync(meta, conn, make_config(), dry_run=True)
        # DDL is returned but cursor should not have received the CREATE TABLE
        executed_sql = conn.cursor_obj.executed_sql
        assert not any("CREATE TABLE" in s for s in executed_sql)
        assert any("CREATE TABLE" in s for s in result.ddl_executed)

    def test_scenario_a_insert_sql_locked_after_sync(self):
        col = make_col(target_name="NAME", length=50)
        meta = make_meta([col])
        conn = self._make_conn_new_table()
        discover_and_sync(meta, conn, make_config())
        # Cache should be populated (locked)
        assert meta._insert_sql is not None


# ============================================================================
# Remote Discovery — Scenario B (existing table)
# ============================================================================

class TestScenarioB:
    def _conn_existing(self, db_cols: list[dict]) -> MockConnection:
        """
        Build a MockConnection for an existing table.

        query_results order:
          1. ALL_TABLES count → 1 (table exists)
          2. ALL_TAB_COLUMNS rows
        """
        return MockConnection(query_results=[
            [(1,)],                          # table exists
            make_tab_columns_rows(db_cols),  # existing columns
        ])

    def test_scenario_b_returns_correct_scenario(self):
        col = make_col(target_name="NAME", length=50)
        meta = make_meta([col])
        conn = self._conn_existing([{"column_name": "NAME", "char_length": 100}])
        result = discover_and_sync(meta, conn, make_config())
        assert result.scenario == "B"

    def test_scenario_b_no_ddl_when_all_match(self):
        col = make_col(target_name="NAME", length=50)
        meta = make_meta([col])
        conn = self._conn_existing([{"column_name": "NAME", "char_length": 100}])
        result = discover_and_sync(meta, conn, make_config())
        assert result.ddl_executed == []

    def test_scenario_b_matched_col_is_not_new(self):
        col = make_col(target_name="EMAIL", length=50)
        meta = make_meta([col])
        conn = self._conn_existing([{"column_name": "EMAIL", "char_length": 200}])
        discover_and_sync(meta, conn, make_config())
        assert meta.columns["name"].is_new is False

    def test_scenario_b_oracle_name_confirmed(self):
        col = make_col(target_name="EMAIL", length=50)
        meta = make_meta([col])
        conn = self._conn_existing([{"column_name": "EMAIL", "char_length": 200}])
        discover_and_sync(meta, conn, make_config())
        assert meta.columns["name"].oracle_name == "EMAIL"

    def test_scenario_b_new_column_triggers_alter_add(self):
        existing_col = make_col("name", "NAME", length=50)
        new_col = make_col("email", "EMAIL", length=80)
        meta = make_meta([existing_col, new_col])
        # DB only has NAME — EMAIL is missing
        conn = self._conn_existing([{"column_name": "NAME", "char_length": 100}])
        result = discover_and_sync(meta, conn, make_config())
        assert any("ALTER TABLE" in s and "ADD" in s for s in result.ddl_executed)
        assert "EMAIL" in result.new_columns

    def test_scenario_b_col_too_small_triggers_modify(self):
        col = make_col(target_name="NOTES", length=300)
        meta = make_meta([col])
        # DB has NOTES as VARCHAR2(100 CHAR) — smaller than observed 300
        conn = self._conn_existing([{
            "column_name": "NOTES",
            "data_type": "VARCHAR2",
            "char_length": 100,
            "char_used": "C",
        }])
        result = discover_and_sync(meta, conn, make_config())
        assert any("MODIFY" in s for s in result.ddl_executed)
        assert "NOTES" in result.modified_columns

    def test_scenario_b_col_already_large_enough_no_modify(self):
        col = make_col(target_name="NOTES", length=50)
        meta = make_meta([col])
        conn = self._conn_existing([{
            "column_name": "NOTES",
            "data_type": "VARCHAR2",
            "char_length": 500,
            "char_used": "C",
        }])
        result = discover_and_sync(meta, conn, make_config())
        assert not any("MODIFY" in s for s in result.ddl_executed)

    def test_scenario_b_dry_run_no_ddl_executed(self):
        col = make_col(target_name="NOTES", length=300)
        meta = make_meta([col])
        conn = self._conn_existing([{
            "column_name": "NOTES",
            "data_type": "VARCHAR2",
            "char_length": 100,
            "char_used": "C",
        }])
        result = discover_and_sync(meta, conn, make_config(), dry_run=True)
        executed = conn.cursor_obj.executed_sql
        assert not any("MODIFY" in s for s in executed)
        assert any("MODIFY" in s for s in result.ddl_executed)

    def test_scenario_b_insert_sql_locked(self):
        col = make_col(target_name="NAME", length=50)
        meta = make_meta([col])
        conn = self._conn_existing([{"column_name": "NAME", "char_length": 100}])
        discover_and_sync(meta, conn, make_config())
        assert meta._insert_sql is not None


# ============================================================================
# Oracle Client
# ============================================================================

class TestOracleClient:
    def test_oracle_session_calls_close_on_exit(self):
        """OracleSession.__exit__ must close the connection."""
        import sys, types

        closed = []

        class FakeConn:
            def cursor(self):
                return MockCursor(query_results=[])
            def close(self):
                closed.append(True)

        # Patch oracledb.connect to return FakeConn
        sys.modules["oracledb"].connect = lambda **kw: FakeConn()

        with OracleSession(dsn="x", user="u", password="p",
                           apply_session_settings=False) as conn:
            pass

        assert closed, "close() was not called on exit"

    def test_connect_wraps_oracle_error_as_ingestion_error(self):
        import sys
        _OracleError = sys.modules["oracledb"].Error

        def _bad_connect(**kwargs):
            raise _OracleError("TNS timeout")

        sys.modules["oracledb"].connect = _bad_connect

        with pytest.raises(IngestionError, match="TNS timeout"):
            connect(dsn="bad", user="u", password="p",
                    apply_session_settings=False)


# ============================================================================
# MockCursor and MockConnection self-tests
# ============================================================================

class TestMockFixtures:
    def test_mock_cursor_tracks_executed_sql(self):
        cur = MockCursor()
        cur.execute("SELECT 1 FROM DUAL")
        assert "SELECT 1 FROM DUAL" in cur.executed_sql

    def test_mock_cursor_returns_queued_results(self):
        cur = MockCursor(query_results=[[(42,)]])
        cur.execute("SELECT COUNT(*) FROM T")
        assert cur.fetchone() == (42,)

    def test_mock_cursor_multiple_result_sets(self):
        cur = MockCursor(query_results=[[(1,)], [(2,), (3,)]])
        cur.execute("first")
        assert cur.fetchone() == (1,)
        cur.execute("second")
        assert cur.fetchall() == [(2,), (3,)]

    def test_mock_connection_cursor_returns_same_object(self):
        conn = MockConnection()
        assert conn.cursor() is conn.cursor_obj

    def test_mock_connection_tracks_commit(self):
        conn = MockConnection()
        conn.commit()
        conn.commit()
        assert conn.committed == 2

    def test_make_tab_columns_rows_shape(self):
        rows = make_tab_columns_rows([{"column_name": "MY_COL"}])
        assert len(rows) == 1
        assert rows[0][0] == "MY_COL"     # COLUMN_NAME
        assert rows[0][6] == "Y"           # NULLABLE default
        assert rows[0][7] == "C"           # CHAR_USED default
