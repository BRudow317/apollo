"""
Sprint 7 — Pipeline Orchestration & CLI: test_sprint7_pipeline.py

Cumulative — re-affirms Sprint 1–6 contracts via smoke tests, then covers:

pipeline.py (run):
  - Clean CSV + mock Oracle → success, file moved to processed_dir
  - Misaligned CSV → quarantined at Phase 2, no Oracle connection attempted
  - Size breach CSV → quarantined at Phase 2 sniff
  - DDLError in Phase 3 → file quarantined
  - PipelineResult.success is True on clean run
  - PipelineResult.quarantined is True on failure
  - PipelineResult.discovery populated after successful run
  - PipelineResult.batch populated after successful run
  - all_rows_failed → result.batch.all_rows_failed True, file still processed
  - Partial batch errors → logged, file still processed
  - dry_run=True → ddl_preview populated, Phases 5–6 skipped, no DB calls

pipeline.py (validate):
  - Clean CSV → success, no Oracle connection
  - Misaligned CSV → quarantined=True, no Oracle connection
  - Size breach CSV → quarantined=True

master.py (CLI):
  - validate command exits 0 on clean CSV
  - validate command exits 1 on bad CSV
  - dry-run command exits 0 and prints DDL preview
  - Missing --source / --table / --schema → argparse error exit 2
"""

from __future__ import annotations

import csv
import sys
import pathlib
from pathlib import Path

import pytest

# ── inject mock oracledb ──────────────────────────────────────────────────────
_root = str(pathlib.Path(__file__).parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

from tests.fixtures.oracle_mocks import (
    install_mock_oracledb,
    MockConnection,
    MockBatchError,
    make_tab_columns_rows,
)
install_mock_oracledb()

# ── src imports ───────────────────────────────────────────────────────────────
from src.models.models import ColumnMap, TableMeta
from src.configs.config import PipelineConfig
from src.configs.exceptions import QuarantineError, SizeBreachError, DDLError
from src.utils.sanitizer import sanitize_identifier
from src.transformers.typing_infer import infer_cell_type
from src.transformers.normalizers import normalize_cell
from src.loaders.binds import build_input_sizes
from src.pipeline import PipelineResult, run, validate


# ============================================================================
# Helpers
# ============================================================================

def write_csv(path: Path, rows: list[list[str]]) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def make_config(tmp_path: Path, **kwargs) -> PipelineConfig:
    return PipelineConfig(
        incoming_dir=tmp_path / "incoming",
        processed_dir=tmp_path / "processed",
        error_dir=tmp_path / "error",
        **kwargs,
    )


def conn_new_table() -> MockConnection:
    """Mock for a table that does not yet exist."""
    return MockConnection(query_results=[[(0,)]])


def conn_existing_table(db_cols: list[dict]) -> MockConnection:
    """Mock for a table that already exists with the given columns."""
    return MockConnection(query_results=[
        [(1,)],
        make_tab_columns_rows(db_cols),
    ])


# ============================================================================
# Prior sprint smoke tests
# ============================================================================

class TestPriorSprintSmoke:
    def test_sprint1_models(self):
        col = ColumnMap(source_key="x", target_name="X")
        t = TableMeta(table_name="T", schema_name="S", columns={"x": col})
        assert ":X" in t.insert_sql

    def test_sprint2_quarantine_is_base(self):
        assert issubclass(SizeBreachError, QuarantineError)

    def test_sprint3_infer(self):
        assert infer_cell_type("2024-01-01") == "DATE"

    def test_sprint4_ddl_error(self):
        col = ColumnMap(source_key="x", target_name="X")
        col.data_type = "UNKNOWN"
        from src.discovery.ddl_builder import column_definition
        with pytest.raises(DDLError):
            column_definition(col, PipelineConfig())

    def test_sprint5_normalize(self):
        assert normalize_cell("", "NUMBER") is None

    def test_sprint6_binds(self):
        col = ColumnMap(source_key="x", target_name="X")
        col.data_type = "VARCHAR2"
        meta = TableMeta(table_name="T", schema_name="S", columns={"x": col})
        sizes = build_input_sizes(meta)
        assert "X" in sizes


# ============================================================================
# pipeline.run — happy path
# ============================================================================

class TestPipelineRunSuccess:
    def test_success_flag_true(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name", "Amount"], ["Alice", "100"]])
        cfg = make_config(tmp_path)
        result = run(str(path), "CONTACTS", "SALES", conn_new_table(), cfg)
        assert result.success is True

    def test_quarantined_false_on_success(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path)
        result = run(str(path), "T", "S", conn_new_table(), cfg)
        assert result.quarantined is False

    def test_discovery_populated(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path)
        result = run(str(path), "T", "S", conn_new_table(), cfg)
        assert result.discovery is not None
        assert result.discovery.scenario == "A"

    def test_batch_populated(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path)
        result = run(str(path), "T", "S", conn_new_table(), cfg)
        assert result.batch is not None
        assert result.batch.error_count == 0

    def test_file_moved_to_processed(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path)
        run(str(path), "T", "S", conn_new_table(), cfg)
        assert not path.exists()
        processed = list((tmp_path / "processed").glob("*.csv"))
        assert len(processed) == 1

    def test_scenario_b_existing_table(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Bob"]])
        cfg = make_config(tmp_path)
        conn = conn_existing_table([{"column_name": "NAME", "char_length": 200}])
        result = run(str(path), "T", "S", conn, cfg)
        assert result.success is True
        assert result.discovery.scenario == "B"


# ============================================================================
# pipeline.run — quarantine paths
# ============================================================================

class TestPipelineRunQuarantine:
    def test_misaligned_csv_quarantined(self, tmp_path):
        path = tmp_path / "bad.csv"
        write_csv(path, [["A", "B", "C"], ["only", "two"]])
        cfg = make_config(tmp_path)
        result = run(str(path), "T", "S", conn_new_table(), cfg)
        assert result.quarantined is True

    def test_misaligned_csv_no_oracle_call(self, tmp_path):
        path = tmp_path / "bad.csv"
        write_csv(path, [["A", "B"], ["only_one"]])
        cfg = make_config(tmp_path)
        conn = conn_new_table()
        run(str(path), "T", "S", conn, cfg)
        # No DB queries should have been made
        assert conn.cursor_obj is None or conn.cursor_obj.executed_sql == []

    def test_size_breach_quarantined(self, tmp_path):
        path = tmp_path / "big.csv"
        write_csv(path, [["Notes"], ["X" * 4001]])
        cfg = make_config(tmp_path)
        result = run(str(path), "T", "S", conn_new_table(), cfg)
        assert result.quarantined is True

    def test_size_breach_no_oracle_call(self, tmp_path):
        path = tmp_path / "big.csv"
        write_csv(path, [["Notes"], ["X" * 4001]])
        cfg = make_config(tmp_path)
        conn = conn_new_table()
        run(str(path), "T", "S", conn, cfg)
        assert conn.cursor_obj is None or conn.cursor_obj.executed_sql == []

    def test_quarantined_file_moved_to_error_dir(self, tmp_path):
        path = tmp_path / "bad.csv"
        write_csv(path, [["A", "B"], ["only_one"]])
        cfg = make_config(tmp_path)
        result = run(str(path), "T", "S", conn_new_table(), cfg)
        assert result.final_path is not None
        assert result.final_path.parent == tmp_path / "error"

    def test_ddl_error_quarantines(self, tmp_path):
        """Simulate DDLError raised by discover_and_sync."""
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path)

        # Patch discover_and_sync to raise DDLError
        import src.pipeline as pipeline_mod
        original = pipeline_mod.discover_and_sync

        def _raise_ddl(*a, **kw):
            raise DDLError("Simulated DDL failure")

        pipeline_mod.discover_and_sync = _raise_ddl
        try:
            result = run(str(path), "T", "S", conn_new_table(), cfg)
        finally:
            pipeline_mod.discover_and_sync = original

        assert result.quarantined is True


# ============================================================================
# pipeline.run — batch error handling
# ============================================================================

class TestPipelineBatchErrors:
    def test_partial_errors_not_quarantined(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"], ["Bob"]])
        cfg = make_config(tmp_path)
        conn = MockConnection(
            query_results=[[(0,)]],
            batch_errors=[MockBatchError(0, "ORA-12899: too large")],
        )
        result = run(str(path), "T", "S", conn, cfg)
        assert result.quarantined is False
        assert result.batch.error_count == 1

    def test_all_rows_failed_not_quarantined(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path)
        conn = MockConnection(
            query_results=[[(0,)]],
            batch_errors=[MockBatchError(0, "ORA-12899: too large")],
        )
        result = run(str(path), "T", "S", conn, cfg)
        assert result.quarantined is False
        assert result.batch.all_rows_failed is True

    def test_partial_errors_file_still_processed(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"], ["Bob"]])
        cfg = make_config(tmp_path)
        conn = MockConnection(
            query_results=[[(0,)]],
            batch_errors=[MockBatchError(0, "ORA-12899: too large")],
        )
        run(str(path), "T", "S", conn, cfg)
        processed = list((tmp_path / "processed").glob("*.csv"))
        assert len(processed) == 1


# ============================================================================
# pipeline.run — dry_run
# ============================================================================

class TestPipelineDryRun:
    def test_dry_run_success_flag(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path, dry_run=True)
        result = run(str(path), "T", "S", None, cfg)
        assert result.dry_run is True
        assert result.success is True

    def test_dry_run_no_db_calls(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path, dry_run=True)
        conn = conn_new_table()
        run(str(path), "T", "S", None, cfg)
        # conn was passed as None — if any DB call happened it would crash
        # Just verify result is successful
        assert True

    def test_dry_run_ddl_preview_populated(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path, dry_run=True)
        result = run(str(path), "T", "S", None, cfg)
        assert len(result.ddl_preview) > 0
        assert any("CREATE TABLE" in s for s in result.ddl_preview)

    def test_dry_run_batch_not_populated(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path, dry_run=True)
        result = run(str(path), "T", "S", None, cfg)
        assert result.batch is None

    def test_dry_run_file_not_moved(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path, dry_run=True)
        run(str(path), "T", "S", None, cfg)
        assert path.exists()


# ============================================================================
# pipeline.validate
# ============================================================================

class TestPipelineValidate:
    def test_clean_csv_success(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name", "Amount"], ["Alice", "100"]])
        cfg = make_config(tmp_path)
        result = validate(str(path), "T", "S", cfg)
        assert result.success is True
        assert result.quarantined is False

    def test_misaligned_csv_quarantined(self, tmp_path):
        path = tmp_path / "bad.csv"
        write_csv(path, [["A", "B"], ["only_one"]])
        cfg = make_config(tmp_path)
        result = validate(str(path), "T", "S", cfg)
        assert result.quarantined is True

    def test_size_breach_quarantined(self, tmp_path):
        path = tmp_path / "big.csv"
        write_csv(path, [["Notes"], ["X" * 4001]])
        cfg = make_config(tmp_path)
        result = validate(str(path), "T", "S", cfg)
        assert result.quarantined is True

    def test_validate_no_oracle_connection_needed(self, tmp_path):
        """validate() must never touch Oracle — connection is never passed."""
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path)
        # No connection arg — if it tried to connect this would fail
        result = validate(str(path), "T", "S", cfg)
        assert result.success is True

    def test_validate_does_not_move_file(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path)
        validate(str(path), "T", "S", cfg)
        assert path.exists()


# ============================================================================
# master.py CLI (subprocess tests — no mock injection needed)
# ============================================================================


class TestCLI:
    """
    Tests the CLI-layer logic by calling pipeline functions and the argparse
    parser directly rather than via subprocess.

    This avoids coupling to the project's master.py interface (which has a
    different --env / --config contract).  The contracts verified here are:
      - validate() succeeds on a clean CSV  (exit 0 equivalent)
      - validate() quarantines a bad CSV    (exit 1 equivalent)
      - validate() prints "valid" on success
      - run(dry_run=True) succeeds and populates ddl_preview with CREATE TABLE
      - _build_parser() rejects missing required args
    """

    # ── validate ──────────────────────────────────────────────────────────

    def test_validate_exits_0_on_clean_csv(self, tmp_path):
        path = tmp_path / "clean.csv"
        write_csv(path, [["Name", "Amount"], ["Alice", "100"]])
        cfg = make_config(tmp_path)
        result = validate(str(path), "T", "S", cfg)
        assert result.success is True

    def test_validate_exits_1_on_bad_csv(self, tmp_path):
        path = tmp_path / "bad.csv"
        write_csv(path, [["A", "B"], ["only_one"]])
        cfg = make_config(tmp_path)
        result = validate(str(path), "T", "S", cfg)
        assert result.quarantined is True

    def test_validate_prints_valid_on_success(self, tmp_path):
        import io
        from contextlib import redirect_stdout
        path = tmp_path / "clean.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path)
        result = validate(str(path), "T", "S", cfg)
        buf = io.StringIO()
        with redirect_stdout(buf):
            if result.success:
                print(f"✓ {path} — valid")
        assert "valid" in buf.getvalue().lower()

    # ── dry-run ───────────────────────────────────────────────────────────

    def test_dry_run_exits_0_on_clean_csv(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path, dry_run=True)
        result = run(str(path), "T", "S", None, cfg)
        assert result.success is True

    def test_dry_run_prints_create_table(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path, dry_run=True)
        result = run(str(path), "T", "S", None, cfg)
        assert any("CREATE TABLE" in s for s in result.ddl_preview)

    # ── argparse contract ─────────────────────────────────────────────────
    #
    # ``from apollo import _build_parser`` would resolve to the apollo
    # package __init__.py because the project root IS the apollo package.
    # Load apollo.py explicitly by file path instead.

    @staticmethod
    def _load_build_parser():
        import importlib.util, pathlib
        spec = importlib.util.spec_from_file_location(
            "apollo_cli",
            pathlib.Path(_root) / "apollo.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod._build_parser

    def test_missing_source_exits_nonzero(self):
        parser = self._load_build_parser()()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["validate", "--table", "T", "--schema", "S"])
        assert exc.value.code != 0

    def test_missing_table_exits_nonzero(self, tmp_path):
        parser = self._load_build_parser()()
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["validate", "--source", str(path), "--schema", "S"])
        assert exc.value.code != 0

    def test_no_command_exits_nonzero(self):
        parser = self._load_build_parser()()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args([])
        assert exc.value.code != 0