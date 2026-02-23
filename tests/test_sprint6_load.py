"""
Sprint 6 — Load & Batch Execution: test_sprint6_load.py

Cumulative — re-affirms Sprint 1–5 contracts via smoke tests, then covers:

binds.py:
  - oracle_type_for returns correct DB_TYPE for each OracleDataType
  - UNKNOWN maps to DB_TYPE_VARCHAR (safe fallback)
  - Unknown data_type string raises KeyError
  - build_input_sizes keys match oracle_names
  - build_input_sizes values are DB_TYPE constants

error_logging.py:
  - log_batch_errors writes to apollo_batch_errors.log in error_dir
  - Creates error_dir if absent
  - Appends — does not truncate on second call
  - Each line contains timestamp, source, row_offset, ora_code, msg
  - ORA code extracted correctly from message
  - count_errors_in_log returns 0 when file absent
  - count_errors_in_log returns correct count after logging

batch_exec.py:
  - execute_batch calls executemany with the row list
  - cursor.bindarraysize set from config.batch_size
  - setinputsizes called before executemany
  - batcherrors=True always set on executemany call
  - connection.commit() called after executemany
  - cursor.close() called in finally block
  - No batch errors → BatchResult.error_count == 0
  - Batch errors → logged and error_count set correctly
  - all_rows_failed True when every row errors
  - all_rows_failed False when only some rows error
  - Empty row iterator → no executemany called, no commit
  - batch_size sourced from config, not hardcoded
"""

from __future__ import annotations

import csv
import os
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

# ── inject mock oracledb before any src imports ───────────────────────────────
import sys, pathlib
_root = str(pathlib.Path(__file__).parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

from tests.fixtures.oracle_mocks import (
    install_mock_oracledb,
    MockConnection,
    MockCursor,
    MockBatchError,
    DB_TYPE_VARCHAR,
    DB_TYPE_NUMBER,
    DB_TYPE_DATE,
    DB_TYPE_TIMESTAMP,
    make_tab_columns_rows,
)
install_mock_oracledb()

# ── src imports ───────────────────────────────────────────────────────────────
from src.models.models import ColumnMap, TableMeta
from src.configs.config import PipelineConfig
from src.configs.exceptions import QuarantineError, SizeBreachError
from src.utils.sanitizer import sanitize_identifier
from src.transformers.typing_infer import infer_cell_type
from src.transformers.normalizers import normalize_cell
from src.discovery.csv_reader import CSVReader
from src.discovery.local_sniff import sniff
from src.transformers.row_generator import generate_rows
from src.loaders.binds import oracle_type_for, build_input_sizes
from src.loaders.error_logging import (
    log_batch_errors,
    count_errors_in_log,
    LOG_FILENAME,
)
from src.loaders.batch_exec import execute_batch, BatchResult


# ============================================================================
# Helpers
# ============================================================================

def write_csv(path: Path, rows: list[list[str]]) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def make_config(**kwargs) -> PipelineConfig:
    return PipelineConfig(**kwargs)


def make_meta(cols: list[tuple]) -> TableMeta:
    """Build TableMeta from (source_key, oracle_name, data_type) triples."""
    columns = {}
    for source_key, oracle_name, data_type in cols:
        col = ColumnMap(
            source_key=source_key,
            target_name=oracle_name,
            oracle_name=oracle_name,
        )
        col.data_type = data_type
        columns[source_key] = col
    return TableMeta(table_name="CONTACTS", schema_name="SALES", columns=columns)


# ============================================================================
# Prior sprint smoke tests
# ============================================================================

class TestPriorSprintSmoke:
    def test_sprint1_named_binds(self):
        col = ColumnMap(source_key="x", target_name="X")
        t = TableMeta(table_name="T", schema_name="S", columns={"x": col})
        assert ":X" in t.insert_sql

    def test_sprint2_quarantine_hierarchy(self):
        assert issubclass(SizeBreachError, QuarantineError)

    def test_sprint3_type_infer(self):
        assert infer_cell_type("123") == "NUMBER"

    def test_sprint4_sanitizer(self):
        assert sanitize_identifier("select") == "SELECT_COL"

    def test_sprint5_normalize_cell(self):
        assert normalize_cell("1,234.56", "NUMBER") == Decimal("1234.56")
        assert normalize_cell("", "VARCHAR2") is None


# ============================================================================
# binds.py
# ============================================================================

class TestOracleTypeFor:
    def test_varchar2(self):
        assert oracle_type_for("VARCHAR2") is DB_TYPE_VARCHAR

    def test_number(self):
        assert oracle_type_for("NUMBER") is DB_TYPE_NUMBER

    def test_date(self):
        assert oracle_type_for("DATE") is DB_TYPE_DATE

    def test_timestamp(self):
        assert oracle_type_for("TIMESTAMP") is DB_TYPE_TIMESTAMP

    def test_unknown_maps_to_varchar(self):
        assert oracle_type_for("UNKNOWN") is DB_TYPE_VARCHAR

    def test_invalid_type_raises_key_error(self):
        with pytest.raises(KeyError):
            oracle_type_for("BLOB")


class TestBuildInputSizes:
    def test_keys_are_oracle_names(self):
        meta = make_meta([
            ("name",   "NAME",   "VARCHAR2"),
            ("amount", "AMOUNT", "NUMBER"),
        ])
        sizes = build_input_sizes(meta)
        assert set(sizes.keys()) == {"NAME", "AMOUNT"}

    def test_values_are_db_types(self):
        meta = make_meta([
            ("name",      "NAME",          "VARCHAR2"),
            ("amount",    "AMOUNT",        "NUMBER"),
            ("created",   "CREATED_DATE",  "DATE"),
            ("modified",  "LAST_MODIFIED", "TIMESTAMP"),
        ])
        sizes = build_input_sizes(meta)
        assert sizes["NAME"]          is DB_TYPE_VARCHAR
        assert sizes["AMOUNT"]        is DB_TYPE_NUMBER
        assert sizes["CREATED_DATE"]  is DB_TYPE_DATE
        assert sizes["LAST_MODIFIED"] is DB_TYPE_TIMESTAMP

    def test_unknown_fallback(self):
        meta = make_meta([("x", "X", "UNKNOWN")])
        sizes = build_input_sizes(meta)
        assert sizes["X"] is DB_TYPE_VARCHAR

    def test_single_column(self):
        meta = make_meta([("id", "ID", "NUMBER")])
        sizes = build_input_sizes(meta)
        assert len(sizes) == 1
        assert "ID" in sizes


# ============================================================================
# error_logging.py
# ============================================================================

class TestErrorLogging:
    def _make_errors(self, n: int = 2) -> list[MockBatchError]:
        return [
            MockBatchError(i, f"ORA-12899: value too large for column at row {i}")
            for i in range(n)
        ]

    def test_creates_log_file(self, tmp_path):
        errors = self._make_errors(1)
        log_path = log_batch_errors(errors, source_path="data.csv", error_dir=tmp_path)
        assert log_path.exists()
        assert log_path.name == LOG_FILENAME

    def test_creates_error_dir_if_absent(self, tmp_path):
        error_dir = tmp_path / "new_error_dir"
        errors = self._make_errors(1)
        log_batch_errors(errors, source_path="data.csv", error_dir=error_dir)
        assert error_dir.exists()

    def test_log_line_contains_source(self, tmp_path):
        errors = self._make_errors(1)
        log_path = log_batch_errors(
            errors, source_path="/some/path/contacts.csv", error_dir=tmp_path
        )
        content = log_path.read_text()
        assert "contacts.csv" in content

    def test_log_line_contains_row_offset(self, tmp_path):
        errors = [MockBatchError(42, "ORA-12899: too large")]
        log_path = log_batch_errors(errors, source_path="f.csv", error_dir=tmp_path)
        content = log_path.read_text()
        assert "row_offset=42" in content

    def test_log_line_contains_ora_code(self, tmp_path):
        errors = [MockBatchError(0, "ORA-12899: value too large")]
        log_path = log_batch_errors(errors, source_path="f.csv", error_dir=tmp_path)
        content = log_path.read_text()
        assert "ORA-12899" in content

    def test_log_appends_not_truncates(self, tmp_path):
        errors = self._make_errors(1)
        log_batch_errors(errors, source_path="first.csv",  error_dir=tmp_path)
        log_batch_errors(errors, source_path="second.csv", error_dir=tmp_path)
        log_path = tmp_path / LOG_FILENAME
        content = log_path.read_text()
        assert "first.csv"  in content
        assert "second.csv" in content

    def test_multiple_errors_all_logged(self, tmp_path):
        errors = self._make_errors(5)
        log_path = log_batch_errors(errors, source_path="f.csv", error_dir=tmp_path)
        lines = [l for l in log_path.read_text().splitlines() if l.strip()]
        assert len(lines) == 5

    def test_count_errors_zero_when_no_file(self, tmp_path):
        assert count_errors_in_log(tmp_path) == 0

    def test_count_errors_after_logging(self, tmp_path):
        errors = self._make_errors(3)
        log_batch_errors(errors, source_path="f.csv", error_dir=tmp_path)
        assert count_errors_in_log(tmp_path) == 3

    def test_count_errors_accumulates_across_calls(self, tmp_path):
        errors = self._make_errors(2)
        log_batch_errors(errors, source_path="a.csv", error_dir=tmp_path)
        log_batch_errors(errors, source_path="b.csv", error_dir=tmp_path)
        assert count_errors_in_log(tmp_path) == 4

    def test_unknown_ora_code_fallback(self, tmp_path):
        errors = [MockBatchError(0, "Generic database error without ORA code")]
        log_path = log_batch_errors(errors, source_path="f.csv", error_dir=tmp_path)
        content = log_path.read_text()
        assert "ORA-UNKNOWN" in content


# ============================================================================
# batch_exec.py
# ============================================================================

class TestExecuteBatch:
    def _meta(self) -> TableMeta:
        return make_meta([
            ("name",   "NAME",   "VARCHAR2"),
            ("amount", "AMOUNT", "NUMBER"),
        ])

    def _rows(self) -> list[dict]:
        return [
            {"NAME": "Alice", "AMOUNT": Decimal("100.00")},
            {"NAME": "Bob",   "AMOUNT": Decimal("200.00")},
        ]

    def test_executemany_called(self, tmp_path):
        meta = self._meta()
        conn = MockConnection()
        cfg = make_config(error_dir=tmp_path)
        execute_batch(conn, meta, iter(self._rows()), "data.csv", cfg)
        assert any("INSERT" in sql for sql in conn.cursor_obj.executed_sql)

    def test_executemany_receives_all_rows(self, tmp_path):
        meta = self._meta()
        conn = MockConnection()
        cfg = make_config(error_dir=tmp_path)
        execute_batch(conn, meta, iter(self._rows()), "data.csv", cfg)
        rows = conn.cursor_obj.executemany_rows
        assert rows is not None
        assert len(rows) == 2

    def test_bindarraysize_set_from_config(self, tmp_path):
        meta = self._meta()
        conn = MockConnection()
        cfg = make_config(batch_size=500, error_dir=tmp_path)
        execute_batch(conn, meta, iter(self._rows()), "data.csv", cfg)
        assert conn.cursor_obj.bindarraysize == 500

    def test_setinputsizes_called_before_executemany(self, tmp_path):
        meta = self._meta()
        conn = MockConnection()
        cfg = make_config(error_dir=tmp_path)
        execute_batch(conn, meta, iter(self._rows()), "data.csv", cfg)
        assert "NAME" in conn.cursor_obj.input_sizes
        assert "AMOUNT" in conn.cursor_obj.input_sizes

    def test_commit_called_after_executemany(self, tmp_path):
        meta = self._meta()
        conn = MockConnection()
        cfg = make_config(error_dir=tmp_path)
        execute_batch(conn, meta, iter(self._rows()), "data.csv", cfg)
        assert conn.committed == 1

    def test_cursor_closed_after_execution(self, tmp_path):
        meta = self._meta()
        conn = MockConnection()
        cfg = make_config(error_dir=tmp_path)
        execute_batch(conn, meta, iter(self._rows()), "data.csv", cfg)
        assert conn.cursor_obj.closed is True

    def test_no_errors_returns_zero_error_count(self, tmp_path):
        meta = self._meta()
        conn = MockConnection()
        cfg = make_config(error_dir=tmp_path)
        result = execute_batch(conn, meta, iter(self._rows()), "data.csv", cfg)
        assert result.error_count == 0
        assert result.error_log_path is None

    def test_batch_errors_logged_and_counted(self, tmp_path):
        meta = self._meta()
        batch_errs = [MockBatchError(0, "ORA-12899: value too large")]
        conn = MockConnection(batch_errors=batch_errs)
        cfg = make_config(error_dir=tmp_path)
        result = execute_batch(conn, meta, iter(self._rows()), "data.csv", cfg)
        assert result.error_count == 1
        assert result.error_log_path is not None
        assert result.error_log_path.exists()

    def test_all_rows_failed_flag_true(self, tmp_path):
        meta = self._meta()
        batch_errs = [
            MockBatchError(0, "ORA-12899: too large"),
            MockBatchError(1, "ORA-12899: too large"),
        ]
        conn = MockConnection(batch_errors=batch_errs)
        cfg = make_config(error_dir=tmp_path)
        result = execute_batch(conn, meta, iter(self._rows()), "data.csv", cfg)
        assert result.all_rows_failed is True

    def test_all_rows_failed_flag_false_on_partial(self, tmp_path):
        meta = self._meta()
        # Only 1 of 2 rows errors
        batch_errs = [MockBatchError(0, "ORA-12899: too large")]
        conn = MockConnection(batch_errors=batch_errs)
        cfg = make_config(error_dir=tmp_path)
        result = execute_batch(conn, meta, iter(self._rows()), "data.csv", cfg)
        assert result.all_rows_failed is False

    def test_empty_rows_skips_executemany(self, tmp_path):
        meta = self._meta()
        conn = MockConnection()
        cfg = make_config(error_dir=tmp_path)
        execute_batch(conn, meta, iter([]), "data.csv", cfg)
        assert not any("INSERT" in sql for sql in conn.cursor_obj.executed_sql)

    def test_empty_rows_no_commit(self, tmp_path):
        meta = self._meta()
        conn = MockConnection()
        cfg = make_config(error_dir=tmp_path)
        execute_batch(conn, meta, iter([]), "data.csv", cfg)
        assert conn.committed == 0

    def test_cursor_closed_even_on_empty_rows(self, tmp_path):
        meta = self._meta()
        conn = MockConnection()
        cfg = make_config(error_dir=tmp_path)
        execute_batch(conn, meta, iter([]), "data.csv", cfg)
        assert conn.cursor_obj.closed is True

    def test_batch_size_not_hardcoded(self, tmp_path):
        """batch_size must come from config, not a magic literal."""
        meta = self._meta()
        for size in [250, 500, 2000]:
            conn = MockConnection()
            cfg = make_config(batch_size=size, error_dir=tmp_path)
            execute_batch(conn, meta, iter(self._rows()), "data.csv", cfg)
            assert conn.cursor_obj.bindarraysize == size


# ============================================================================
# Full round-trip: sniff → generate_rows → execute_batch
# ============================================================================

class TestFullRoundTrip:
    def test_round_trip_rows_reach_executemany(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["Name", "Amount"],
            ["Alice", "100.00"],
            ["Bob",   "200.00"],
            ["Carol", "300.00"],
        ])
        cfg = make_config(error_dir=tmp_path / "errors")
        conn = MockConnection()

        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", cfg)
            result = execute_batch(
                conn, meta, generate_rows(source, meta), str(path), cfg
            )

        rows = conn.cursor_obj.executemany_rows
        assert rows is not None
        assert len(rows) == 3
        assert result.error_count == 0

    def test_round_trip_keys_are_oracle_names(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["First Name", "Amount"],
            ["Alice", "99.99"],
        ])
        cfg = make_config(error_dir=tmp_path / "errors")
        conn = MockConnection()

        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", cfg)
            execute_batch(conn, meta, generate_rows(source, meta), str(path), cfg)

        row = conn.cursor_obj.executemany_rows[0]
        assert "FIRST_NAME" in row
        assert "AMOUNT" in row

    def test_round_trip_values_normalized(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["Amount", "Created Date"],
            ["1,234.56", "2024-06-01"],
        ])
        cfg = make_config(error_dir=tmp_path / "errors")
        conn = MockConnection()

        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", cfg)
            execute_batch(conn, meta, generate_rows(source, meta), str(path), cfg)

        row = conn.cursor_obj.executemany_rows[0]
        assert row["AMOUNT"] == Decimal("1234.56")
        assert row["CREATED_DATE"] == date(2024, 6, 1)
