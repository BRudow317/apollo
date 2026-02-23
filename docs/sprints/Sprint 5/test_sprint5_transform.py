"""
Sprint 5 — Transform & Row Generator: test_sprint5_transform.py

Cumulative — re-affirms Sprint 1–4 contracts via smoke tests, then covers:

Normalizers (normalize_cell):
  - \\x00 stripped from all types
  - Empty string → None (all types)
  - Whitespace-only → None (all types)
  - NUMBER: plain int string → Decimal
  - NUMBER: decimal string → Decimal with correct precision
  - NUMBER: comma-formatted → Decimal after strip
  - NUMBER: negative → Decimal
  - NUMBER: "N/A" in NUMBER column → None (no raise)
  - DATE: YYYY-MM-DD → datetime.date
  - TIMESTAMP: YYYY-MM-DDTHH:MM:SS.000Z → datetime.datetime (tz-naive)
  - TIMESTAMP: space separator → datetime.datetime
  - TIMESTAMP: with offset → datetime.datetime (tz stripped)
  - VARCHAR2: returned as stripped string
  - VARCHAR2: null bytes stripped
  - UNKNOWN: pass-through stripped string

Row Generator (generate_rows):
  - Yields dicts keyed by oracle_name
  - Key set matches TableMeta.columns oracle_names exactly
  - Correct values after normalization
  - Generator is lazy — yields one row at a time
  - Rewindable — second call produces fresh iterator from row 1
  - Full round-trip row count matches CSV data row count
  - Empty string cells → None in output dict
  - \\x00 in cell → stripped in output dict
"""

from __future__ import annotations

import csv
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

# ── Sprint 1–3 smoke ─────────────────────────────────────────────────────────
from src.models.models import ColumnMap, TableMeta
from src.configs.config import PipelineConfig
from src.configs.exceptions import QuarantineError, SizeBreachError
from src.utils.sanitizer import sanitize_identifier
from src.transformers.typing_infer import infer_cell_type

# ── Sprint 4 smoke ────────────────────────────────────────────────────────────
from tests.fixtures.oracle_mocks import install_mock_oracledb
install_mock_oracledb()
from src.discovery.ddl_builder import build_create_table

# ── Sprint 5 ──────────────────────────────────────────────────────────────────
from src.transformers.normalizers import normalize_cell, strip_null_bytes, is_empty
from src.transformers.row_generator import generate_rows
from src.discovery.csv_reader import CSVReader
from src.discovery.local_sniff import sniff


# ============================================================================
# Helpers
# ============================================================================

def write_csv(path: Path, rows: list[list[str]], bom: bool = True) -> None:
    enc = "utf-8-sig" if bom else "utf-8"
    with open(path, "w", encoding=enc, newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def make_config() -> PipelineConfig:
    return PipelineConfig()


def make_meta(cols: list[tuple[str, str, str]]) -> TableMeta:
    """
    Build a TableMeta from (source_key, oracle_name, data_type) triples.
    """
    columns = {}
    for source_key, oracle_name, data_type in cols:
        col = ColumnMap(source_key=source_key, target_name=oracle_name, oracle_name=oracle_name)
        col.data_type = data_type
        columns[source_key] = col
    return TableMeta(table_name="T", schema_name="S", columns=columns)


# ============================================================================
# Prior sprint smoke tests
# ============================================================================

class TestPriorSprintSmoke:
    def test_sprint1_insert_sql_named_binds(self):
        col = ColumnMap(source_key="x", target_name="X")
        t = TableMeta(table_name="T", schema_name="S", columns={"x": col})
        assert ":X" in t.insert_sql

    def test_sprint2_size_breach_error(self):
        assert issubclass(SizeBreachError, QuarantineError)

    def test_sprint3_infer_cell_type(self):
        assert infer_cell_type("2024-01-01T10:00:00Z") == "TIMESTAMP"

    def test_sprint4_ddl_builder_pure(self):
        col = ColumnMap(source_key="n", target_name="NAME")
        col.data_type = "VARCHAR2"
        col.length = 50
        meta = TableMeta(table_name="T", schema_name="S", columns={"n": col})
        sql = build_create_table(meta, make_config())
        assert "CREATE TABLE" in sql


# ============================================================================
# normalize_cell — null byte stripping
# ============================================================================

class TestNullByteStripping:
    def test_null_byte_stripped_varchar2(self):
        result = normalize_cell("hel\x00lo", "VARCHAR2")
        assert result == "hello"

    def test_null_byte_stripped_number(self):
        result = normalize_cell("4\x002", "NUMBER")
        assert result == Decimal("42")

    def test_null_bytes_only_becomes_none(self):
        result = normalize_cell("\x00\x00", "VARCHAR2")
        assert result is None

    def test_multiple_null_bytes_stripped(self):
        result = normalize_cell("\x00a\x00b\x00", "VARCHAR2")
        assert result == "ab"


# ============================================================================
# normalize_cell — empty → None
# ============================================================================

class TestEmptyToNone:
    def test_empty_string_varchar2(self):
        assert normalize_cell("", "VARCHAR2") is None

    def test_empty_string_number(self):
        assert normalize_cell("", "NUMBER") is None

    def test_empty_string_date(self):
        assert normalize_cell("", "DATE") is None

    def test_empty_string_timestamp(self):
        assert normalize_cell("", "TIMESTAMP") is None

    def test_whitespace_only_varchar2(self):
        assert normalize_cell("   ", "VARCHAR2") is None

    def test_whitespace_only_number(self):
        assert normalize_cell("   ", "NUMBER") is None


# ============================================================================
# normalize_cell — NUMBER
# ============================================================================

class TestNormalizeNumber:
    def test_integer_string(self):
        assert normalize_cell("42", "NUMBER") == Decimal("42")

    def test_decimal_string(self):
        assert normalize_cell("3.14", "NUMBER") == Decimal("3.14")

    def test_negative_integer(self):
        assert normalize_cell("-7", "NUMBER") == Decimal("-7")

    def test_negative_decimal(self):
        assert normalize_cell("-99.99", "NUMBER") == Decimal("-99.99")

    def test_comma_formatted(self):
        assert normalize_cell("1,234.56", "NUMBER") == Decimal("1234.56")

    def test_large_comma_formatted(self):
        assert normalize_cell("1,234,567.89", "NUMBER") == Decimal("1234567.89")

    def test_non_numeric_returns_none(self):
        assert normalize_cell("N/A", "NUMBER") is None

    def test_text_in_number_column_returns_none(self):
        assert normalize_cell("hello", "NUMBER") is None

    def test_decimal_precision_preserved(self):
        result = normalize_cell("100.00", "NUMBER")
        assert result == Decimal("100.00")
        assert result.as_tuple().exponent == -2

    def test_zero(self):
        assert normalize_cell("0", "NUMBER") == Decimal("0")


# ============================================================================
# normalize_cell — DATE
# ============================================================================

class TestNormalizeDate:
    def test_iso_date(self):
        result = normalize_cell("2024-01-15", "DATE")
        assert isinstance(result, date)
        assert result == date(2024, 1, 15)

    def test_date_year_month_day(self):
        result = normalize_cell("2024-12-31", "DATE")
        assert result == date(2024, 12, 31)

    def test_unparseable_date_returns_string(self):
        result = normalize_cell("15/01/2024", "DATE")
        assert isinstance(result, str)
        assert result == "15/01/2024"


# ============================================================================
# normalize_cell — TIMESTAMP
# ============================================================================

class TestNormalizeTimestamp:
    def test_salesforce_format(self):
        result = normalize_cell("2024-01-15T09:30:00.000Z", "TIMESTAMP")
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 9
        assert result.minute == 30

    def test_no_millis_with_z(self):
        result = normalize_cell("2024-01-15T09:30:00Z", "TIMESTAMP")
        assert isinstance(result, datetime)

    def test_space_separator(self):
        result = normalize_cell("2024-01-15 09:30:00", "TIMESTAMP")
        assert isinstance(result, datetime)

    def test_with_offset_stripped_to_naive(self):
        result = normalize_cell("2024-01-15T09:30:00+05:30", "TIMESTAMP")
        assert isinstance(result, datetime)
        assert result.tzinfo is None

    def test_with_negative_offset(self):
        result = normalize_cell("2024-01-15T09:30:00-05:00", "TIMESTAMP")
        assert isinstance(result, datetime)

    def test_unparseable_timestamp_returns_string(self):
        result = normalize_cell("not-a-timestamp", "TIMESTAMP")
        assert isinstance(result, str)

    def test_timestamp_is_timezone_naive(self):
        result = normalize_cell("2024-06-01T12:00:00.000Z", "TIMESTAMP")
        assert isinstance(result, datetime)
        assert result.tzinfo is None


# ============================================================================
# normalize_cell — VARCHAR2 / UNKNOWN
# ============================================================================

class TestNormalizeVarchar2:
    def test_plain_string(self):
        assert normalize_cell("Alice", "VARCHAR2") == "Alice"

    def test_string_stripped(self):
        assert normalize_cell("  Alice  ", "VARCHAR2") == "Alice"

    def test_null_bytes_removed(self):
        assert normalize_cell("A\x00B", "VARCHAR2") == "AB"

    def test_unknown_passthrough(self):
        assert normalize_cell("anything", "UNKNOWN") == "anything"

    def test_unknown_stripped(self):
        assert normalize_cell("  spaces  ", "UNKNOWN") == "spaces"


# ============================================================================
# strip_null_bytes / is_empty helpers
# ============================================================================

class TestHelpers:
    def test_strip_null_bytes(self):
        assert strip_null_bytes("a\x00b") == "ab"

    def test_strip_null_bytes_clean(self):
        assert strip_null_bytes("clean") == "clean"

    def test_is_empty_true(self):
        assert is_empty("") is True
        assert is_empty("   ") is True
        assert is_empty("\x00") is True

    def test_is_empty_false(self):
        assert is_empty("a") is False
        assert is_empty("0") is False


# ============================================================================
# generate_rows
# ============================================================================

class TestGenerateRows:
    def _meta(self) -> TableMeta:
        return make_meta([
            ("First Name", "FIRST_NAME", "VARCHAR2"),
            ("Amount",     "AMOUNT",     "NUMBER"),
            ("Created",    "CREATED_DATE", "DATE"),
        ])

    def test_yields_dicts(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["First Name", "Amount", "Created"],
            ["Alice", "100.00", "2024-01-01"],
        ])
        meta = self._meta()
        with CSVReader(path) as source:
            rows = list(generate_rows(source, meta))
        assert len(rows) == 1
        assert isinstance(rows[0], dict)

    def test_keys_are_oracle_names(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["First Name", "Amount", "Created"],
            ["Alice", "100.00", "2024-01-01"],
        ])
        meta = self._meta()
        with CSVReader(path) as source:
            rows = list(generate_rows(source, meta))
        assert set(rows[0].keys()) == {"FIRST_NAME", "AMOUNT", "CREATED_DATE"}

    def test_values_normalized(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["First Name", "Amount", "Created"],
            ["Alice", "1,234.56", "2024-06-15"],
        ])
        meta = self._meta()
        with CSVReader(path) as source:
            rows = list(generate_rows(source, meta))
        assert rows[0]["FIRST_NAME"] == "Alice"
        assert rows[0]["AMOUNT"] == Decimal("1234.56")
        assert rows[0]["CREATED_DATE"] == date(2024, 6, 15)

    def test_empty_cell_becomes_none(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["First Name", "Amount", "Created"],
            ["", "100.00", "2024-01-01"],
        ])
        meta = self._meta()
        with CSVReader(path) as source:
            rows = list(generate_rows(source, meta))
        assert rows[0]["FIRST_NAME"] is None

    def test_null_byte_in_cell_stripped(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["First Name", "Amount", "Created"],
            ["Ali\x00ce", "100.00", "2024-01-01"],
        ])
        meta = self._meta()
        with CSVReader(path) as source:
            rows = list(generate_rows(source, meta))
        assert rows[0]["FIRST_NAME"] == "Alice"

    def test_row_count_matches_csv(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["First Name", "Amount", "Created"],
            ["Alice", "100.00", "2024-01-01"],
            ["Bob",   "200.00", "2024-02-01"],
            ["Carol", "300.00", "2024-03-01"],
        ])
        meta = self._meta()
        with CSVReader(path) as source:
            rows = list(generate_rows(source, meta))
        assert len(rows) == 3

    def test_generator_is_lazy(self, tmp_path):
        """Consuming one row should not pre-load all rows."""
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["First Name", "Amount", "Created"],
            ["Alice", "100.00", "2024-01-01"],
            ["Bob",   "200.00", "2024-02-01"],
        ])
        meta = self._meta()
        with CSVReader(path) as source:
            gen = generate_rows(source, meta)
            first = next(gen)
            assert first["FIRST_NAME"] == "Alice"
            second = next(gen)
            assert second["FIRST_NAME"] == "Bob"

    def test_generator_rewindable(self, tmp_path):
        """Calling generate_rows twice should yield the same rows."""
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["First Name", "Amount", "Created"],
            ["Alice", "100.00", "2024-01-01"],
        ])
        meta = self._meta()
        with CSVReader(path) as source:
            first_pass  = list(generate_rows(source, meta))
            second_pass = list(generate_rows(source, meta))
        assert first_pass[0]["FIRST_NAME"] == second_pass[0]["FIRST_NAME"]
        assert first_pass[0]["AMOUNT"] == second_pass[0]["AMOUNT"]

    def test_header_only_file_yields_nothing(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["First Name", "Amount", "Created"]])
        meta = self._meta()
        with CSVReader(path) as source:
            rows = list(generate_rows(source, meta))
        assert rows == []

    def test_na_in_number_column_yields_none(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["First Name", "Amount", "Created"],
            ["Alice", "N/A", "2024-01-01"],
        ])
        meta = self._meta()
        with CSVReader(path) as source:
            rows = list(generate_rows(source, meta))
        assert rows[0]["AMOUNT"] is None


# ============================================================================
# Full round-trip: sniff → generate_rows
# ============================================================================

class TestSniffToGeneratorRoundTrip:
    def test_round_trip_row_count(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["Name", "Score"],
            ["Alice", "95"],
            ["Bob",   "87"],
            ["Carol", "91"],
        ])
        cfg = make_config()
        with CSVReader(path) as source:
            meta = sniff(source, "SCORES", "TEST", cfg)
            rows = list(generate_rows(source, meta))
        assert len(rows) == 3

    def test_round_trip_oracle_names_present(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["First Name", "Last Name"],
            ["Alice", "Smith"],
        ])
        cfg = make_config()
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", cfg)
            rows = list(generate_rows(source, meta))
        assert "FIRST_NAME" in rows[0]
        assert "LAST_NAME" in rows[0]

    def test_round_trip_number_type(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["Amount"],
            ["123.45"],
        ])
        cfg = make_config()
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", cfg)
            rows = list(generate_rows(source, meta))
        assert isinstance(rows[0]["AMOUNT"], Decimal)

    def test_round_trip_date_type(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["Created Date"],
            ["2024-03-15"],
        ])
        cfg = make_config()
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", cfg)
            rows = list(generate_rows(source, meta))
        assert isinstance(rows[0]["CREATED_DATE"], date)

    def test_round_trip_timestamp_type(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["Last Modified"],
            ["2024-03-15T08:00:00.000Z"],
        ])
        cfg = make_config()
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", cfg)
            rows = list(generate_rows(source, meta))
        assert isinstance(rows[0]["LAST_MODIFIED"], datetime)

    def test_round_trip_all_types(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["Name", "Amount", "Date Col", "Timestamp Col"],
            ["Alice", "99.99", "2024-01-01", "2024-01-01T12:00:00Z"],
        ])
        cfg = make_config()
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", cfg)
            rows = list(generate_rows(source, meta))
        row = rows[0]
        assert isinstance(row["NAME"],          str)
        assert isinstance(row["AMOUNT"],         Decimal)
        assert isinstance(row["DATE_COL"],       date)
        assert isinstance(row["TIMESTAMP_COL"],  datetime)