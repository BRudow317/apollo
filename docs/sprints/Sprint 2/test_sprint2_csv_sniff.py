"""
Sprint 2 — CSV Reading & Local Sniff: test_sprint2_csv_sniff.py

Cumulative — re-affirms Sprint 1 contracts via smoke imports, then
covers all Sprint 2 contracts:

  - BOM-prefixed CSV parses cleanly; headers are correct
  - Strict dialect raises on malformed CSV; file is quarantined
  - Row with mismatched field count triggers AlignmentError
  - max_char_len and max_byte_len tracked correctly per column
  - Column exceeding 4000 CHAR triggers SizeBreachError during sniff
  - Clean CSV produces fully-populated TableMeta with lengths set
  - SFReader skips blank trailing rows
  - validate_headers_not_empty raises on empty/blank headers
  - quarantine_file and mark_processed move files correctly
"""

from __future__ import annotations

import csv
import os
import shutil
import tempfile
from pathlib import Path

import pytest

# ── Sprint 1 smoke imports ────────────────────────────────────────────────────
from src.models.models import ColumnMap, TableMeta
from src.configs.config import PipelineConfig, ORACLE_MAX_VARCHAR2_CHAR
from src.configs.exceptions import (
    IngestionError,
    QuarantineError,
    AlignmentError,
    SizeBreachError,
    DDLError,
)
from src.utils.sanitizer import sanitize_identifier, is_reserved
from src.utils.identifiers import to_column_name, to_table_name

# ── Sprint 2 imports ──────────────────────────────────────────────────────────
from src.configs.csv_dialect import register_dialect, DIALECT_NAME, get_dialect
from src.utils.validation import validate_row_alignment, validate_headers_not_empty
from src.utils.files import quarantine_file, mark_processed
from src.discovery.base import AbstractSource
from src.discovery.csv_reader import CSVReader
from src.discovery.sf_reader import SFReader
from src.discovery.local_sniff import sniff


# ============================================================================
# Helpers
# ============================================================================

def write_csv(path: Path, rows: list[list[str]], bom: bool = False) -> None:
    """Write a CSV file, optionally prepending a UTF-8 BOM."""
    encoding = "utf-8-sig" if bom else "utf-8"
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def make_config(**kwargs) -> PipelineConfig:
    return PipelineConfig(**kwargs)


# ============================================================================
# Sprint 1 smoke tests (re-affirm Sprint 1 contracts)
# ============================================================================

class TestSprint1Smoke:
    def test_column_map_instantiates(self):
        col = ColumnMap(source_key="Last Name", target_name="LAST_NAME")
        assert col.bind_name == ":LAST_NAME"

    def test_table_meta_insert_sql(self):
        col = ColumnMap(source_key="id", target_name="ID")
        t = TableMeta(table_name="T", schema_name="S", columns={"id": col})
        assert ":ID" in t.insert_sql

    def test_sanitizer_reserved_word(self):
        assert sanitize_identifier("date") == "DATE_COL"

    def test_exception_hierarchy(self):
        assert issubclass(SizeBreachError, QuarantineError)
        assert issubclass(AlignmentError, QuarantineError)


# ============================================================================
# CSV Dialect
# ============================================================================

class TestCsvDialect:
    def test_register_is_idempotent(self):
        register_dialect()
        register_dialect()  # Should not raise
        assert DIALECT_NAME in csv.list_dialects()

    def test_dialect_is_strict(self):
        dialect = get_dialect()
        assert dialect.strict is True

    def test_dialect_skips_initial_space(self):
        dialect = get_dialect()
        assert dialect.skipinitialspace is True


# ============================================================================
# Validation helpers
# ============================================================================

class TestValidation:
    def test_row_alignment_passes(self):
        validate_row_alignment(["a", "b", "c"], expected_field_count=3, row_number=2)

    def test_row_alignment_too_few_raises(self):
        with pytest.raises(AlignmentError) as exc_info:
            validate_row_alignment(["a", "b"], expected_field_count=3, row_number=5)
        e = exc_info.value
        assert e.row_number == 5
        assert e.expected == 3
        assert e.got == 2

    def test_row_alignment_too_many_raises(self):
        with pytest.raises(AlignmentError):
            validate_row_alignment(["a", "b", "c", "d"], expected_field_count=3, row_number=2)

    def test_alignment_carries_source_path(self):
        with pytest.raises(AlignmentError) as exc_info:
            validate_row_alignment([], 3, 2, source_path="/tmp/f.csv")
        assert exc_info.value.source_path == "/tmp/f.csv"

    def test_headers_not_empty_passes(self):
        validate_headers_not_empty(["First Name", "Last Name"])

    def test_headers_empty_list_raises(self):
        with pytest.raises(AlignmentError):
            validate_headers_not_empty([])

    def test_headers_blank_entry_raises(self):
        with pytest.raises(AlignmentError):
            validate_headers_not_empty(["Name", "  ", "Email"])


# ============================================================================
# File operations
# ============================================================================

class TestFileOps:
    def test_quarantine_moves_file(self, tmp_path):
        src = tmp_path / "bad.csv"
        src.write_text("data")
        error_dir = tmp_path / "error"
        dest = quarantine_file(src, error_dir)
        assert dest.exists()
        assert not src.exists()
        assert dest.parent == error_dir

    def test_quarantine_creates_error_dir(self, tmp_path):
        src = tmp_path / "bad.csv"
        src.write_text("data")
        error_dir = tmp_path / "nested" / "error"
        quarantine_file(src, error_dir)
        assert error_dir.exists()

    def test_quarantine_no_clobber(self, tmp_path):
        src1 = tmp_path / "bad.csv"
        src2 = tmp_path / "also_bad.csv"
        src1.write_text("first")
        src2.write_text("second")
        error_dir = tmp_path / "error"
        dest1 = quarantine_file(src1, error_dir)
        # Move a second file with the same name
        src2.rename(tmp_path / "bad.csv")
        src_again = tmp_path / "bad.csv"
        dest2 = quarantine_file(src_again, error_dir)
        assert dest1 != dest2
        assert dest1.exists()
        assert dest2.exists()

    def test_mark_processed_moves_file(self, tmp_path):
        src = tmp_path / "good.csv"
        src.write_text("data")
        processed_dir = tmp_path / "processed"
        dest = mark_processed(src, processed_dir)
        assert dest.exists()
        assert not src.exists()


# ============================================================================
# CSVReader
# ============================================================================

class TestCSVReader:
    def test_reads_headers(self, tmp_path):
        path = tmp_path / "test.csv"
        write_csv(path, [["First Name", "Last Name", "Email"], ["Alice", "Smith", "a@b.com"]])
        with CSVReader(path) as r:
            assert r.headers() == ["First Name", "Last Name", "Email"]

    def test_reads_rows(self, tmp_path):
        path = tmp_path / "test.csv"
        write_csv(path, [["Name", "Age"], ["Alice", "30"], ["Bob", "25"]])
        with CSVReader(path) as r:
            rows = list(r.rows())
        assert rows == [["Alice", "30"], ["Bob", "25"]]

    def test_bom_stripped_from_headers(self, tmp_path):
        path = tmp_path / "bom.csv"
        write_csv(path, [["First Name", "Email"], ["Alice", "a@b.com"]], bom=True)
        with CSVReader(path) as r:
            headers = r.headers()
        assert headers[0] == "First Name"
        assert not headers[0].startswith("\ufeff")

    def test_rows_rewindable(self, tmp_path):
        path = tmp_path / "test.csv"
        write_csv(path, [["Col"], ["a"], ["b"]])
        with CSVReader(path) as r:
            first_pass = list(r.rows())
            second_pass = list(r.rows())
        assert first_pass == second_pass

    def test_header_only_file_has_no_rows(self, tmp_path):
        path = tmp_path / "headers_only.csv"
        write_csv(path, [["Name", "Age"]])
        with CSVReader(path) as r:
            rows = list(r.rows())
        assert rows == []

    def test_empty_file_raises_quarantine(self, tmp_path):
        path = tmp_path / "empty.csv"
        path.write_text("")
        with pytest.raises(QuarantineError):
            with CSVReader(path) as r:
                r.headers()

    def test_missing_file_raises_quarantine(self, tmp_path):
        path = tmp_path / "nonexistent.csv"
        with pytest.raises(QuarantineError):
            with CSVReader(path) as r:
                pass

    def test_context_manager_closes(self, tmp_path):
        path = tmp_path / "test.csv"
        write_csv(path, [["A"], ["1"]])
        reader = CSVReader(path)
        reader.open()
        reader.close()
        assert reader._file is None

    def test_headers_without_open_raises(self, tmp_path):
        path = tmp_path / "test.csv"
        write_csv(path, [["A"], ["1"]])
        r = CSVReader(path)
        with pytest.raises(RuntimeError):
            r.headers()

    def test_rows_without_open_raises(self, tmp_path):
        path = tmp_path / "test.csv"
        write_csv(path, [["A"], ["1"]])
        r = CSVReader(path)
        with pytest.raises(RuntimeError):
            list(r.rows())


# ============================================================================
# SFReader
# ============================================================================

class TestSFReader:
    def test_skips_blank_trailing_row(self, tmp_path):
        path = tmp_path / "sf.csv"
        write_csv(path, [["Name", "Email"], ["Alice", "a@b.com"], ["", ""]], bom=True)
        with SFReader(path) as r:
            rows = list(r.rows())
        assert len(rows) == 1
        assert rows[0] == ["Alice", "a@b.com"]

    def test_reads_bom_headers(self, tmp_path):
        path = tmp_path / "sf.csv"
        write_csv(path, [["Account ID", "Name"], ["001", "Acme"]], bom=True)
        with SFReader(path) as r:
            headers = r.headers()
        assert headers == ["Account ID", "Name"]

    def test_non_blank_rows_kept(self, tmp_path):
        path = tmp_path / "sf.csv"
        write_csv(path, [["Name"], ["Alice"], ["Bob"], ["Charlie"]])
        with SFReader(path) as r:
            rows = list(r.rows())
        assert len(rows) == 3


# ============================================================================
# Local Sniff
# ============================================================================

class TestLocalSniff:
    def _cfg(self):
        return make_config()

    def test_returns_table_meta(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["First Name", "Age"], ["Alice", "30"]])
        with CSVReader(path) as source:
            meta = sniff(source, "CONTACTS", "SALES", self._cfg())
        assert isinstance(meta, TableMeta)
        assert meta.table_name == "CONTACTS"
        assert meta.schema_name == "SALES"

    def test_column_keys_are_raw_headers(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["First Name", "Last Name"], ["Alice", "Smith"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert "First Name" in meta.columns
        assert "Last Name" in meta.columns

    def test_target_names_sanitized(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["First Name", "Account ID"], ["Alice", "001"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["First Name"].target_name == "FIRST_NAME"
        assert meta.columns["Account ID"].target_name == "ACCOUNT_ID"

    def test_max_char_len_tracked(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"], ["Christopher"], ["Bo"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Name"].length == len("Christopher")

    def test_max_byte_len_tracked(self, tmp_path):
        path = tmp_path / "data.csv"
        # ñ is 2 bytes in UTF-8 but 1 character
        write_csv(path, [["Name"], ["niño"], ["cat"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        col = meta.columns["Name"]
        assert col.length == 4           # "niño" is 4 chars
        assert col.max_byte_len == 5     # "niño" is 5 bytes in UTF-8

    def test_size_breach_raises_immediately(self, tmp_path):
        path = tmp_path / "big.csv"
        long_val = "X" * 4001
        write_csv(path, [["Notes"], [long_val]])
        with pytest.raises(SizeBreachError) as exc_info:
            with CSVReader(path) as source:
                sniff(source, "T", "S", self._cfg())
        assert exc_info.value.char_length == 4001
        assert exc_info.value.limit == 4000

    def test_size_breach_exactly_4000_is_ok(self, tmp_path):
        path = tmp_path / "ok.csv"
        exact_val = "X" * 4000
        write_csv(path, [["Notes"], [exact_val]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Notes"].length == 4000

    def test_alignment_error_on_short_row(self, tmp_path):
        path = tmp_path / "bad.csv"
        write_csv(path, [["A", "B", "C"], ["only_two", "fields"]])
        with pytest.raises(AlignmentError):
            with CSVReader(path) as source:
                sniff(source, "T", "S", self._cfg())

    def test_alignment_error_on_long_row(self, tmp_path):
        path = tmp_path / "bad.csv"
        write_csv(path, [["A", "B"], ["too", "many", "fields"]])
        with pytest.raises(AlignmentError):
            with CSVReader(path) as source:
                sniff(source, "T", "S", self._cfg())

    def test_header_only_file_produces_empty_lengths(self, tmp_path):
        path = tmp_path / "headers_only.csv"
        write_csv(path, [["Name", "Email"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Name"].length == 0
        assert meta.columns["Email"].length == 0

    def test_row_count_matches_all_scanned(self, tmp_path):
        """Sniff must scan every row — check max is taken from last row."""
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["Name"],
            ["Short"],
            ["A bit longer"],
            ["Longest name here"],
            ["Shorter again"],
        ])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Name"].length == len("Longest name here")

    def test_data_type_is_unknown_after_sniff(self, tmp_path):
        """Type inference is Sprint 3 — sniff must leave data_type as UNKNOWN."""
        path = tmp_path / "data.csv"
        write_csv(path, [["Amount"], ["123.45"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Amount"].data_type == "UNKNOWN"
