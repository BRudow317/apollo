"""
Sprint 3 — Type Inference: test_sprint3_type_infer.py

Cumulative — re-affirms Sprint 1 and 2 contracts via smoke tests,
then covers all Sprint 3 contracts:

Cell-level (infer_cell_type):
  - Integer strings → NUMBER
  - Decimal strings → NUMBER
  - Comma-formatted numbers → NUMBER
  - YYYY-MM-DD → DATE
  - YYYY-MM-DDTHH:MM:SS.000Z → TIMESTAMP
  - Ambiguous date formats → VARCHAR2 (no guess)
  - "N/A", "null", random strings → VARCHAR2
  - Empty / whitespace → UNKNOWN (null sentinel)

Column-level (infer_column_type):
  - All integers → NUMBER with precision/scale
  - All decimals → NUMBER with precision/scale
  - Mixed int + decimal → NUMBER (broadest numeric type)
  - Comma-formatted numbers → NUMBER after strip
  - All dates → DATE
  - All timestamps → TIMESTAMP
  - Mixed DATE + TIMESTAMP → TIMESTAMP (promoted)
  - Mixed numeric + "N/A" → VARCHAR2
  - All null/empty → VARCHAR2
  - Negative numbers → NUMBER

Integration (sniff with inference):
  - data_type set correctly after sniff (not UNKNOWN)
  - NUMBER column has precision and scale
  - VARCHAR2 column has no precision/scale
  - Mixed-type column falls back to VARCHAR2
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

# ── Sprint 1 smoke ────────────────────────────────────────────────────────────
from src.models.models import ColumnMap, TableMeta
from src.configs.config import PipelineConfig
from src.configs.exceptions import QuarantineError, AlignmentError, SizeBreachError
from src.utils.sanitizer import sanitize_identifier

# ── Sprint 2 smoke ────────────────────────────────────────────────────────────
from src.discovery.csv_reader import CSVReader
from src.discovery.local_sniff import sniff

# ── Sprint 3 ──────────────────────────────────────────────────────────────────
from src.transformers.typing_infer import (
    infer_cell_type,
    infer_column_type,
    apply_type_inference,
)


# ============================================================================
# Helpers
# ============================================================================

def write_csv(path: Path, rows: list[list[str]]) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def make_config() -> PipelineConfig:
    return PipelineConfig()


# ============================================================================
# Sprint 1 + 2 smoke (re-affirm prior contracts)
# ============================================================================

class TestPriorSprintSmoke:
    def test_sprint1_column_map(self):
        col = ColumnMap(source_key="x", target_name="X")
        assert col.bind_name == ":X"

    def test_sprint1_reserved_word(self):
        assert sanitize_identifier("select") == "SELECT_COL"

    def test_sprint1_exception_hierarchy(self):
        assert issubclass(SizeBreachError, QuarantineError)

    def test_sprint2_csv_reader_context_manager(self, tmp_path):
        path = tmp_path / "t.csv"
        write_csv(path, [["A"], ["1"]])
        with CSVReader(path) as r:
            assert r.headers() == ["A"]

    def test_sprint2_sniff_returns_table_meta(self, tmp_path):
        path = tmp_path / "t.csv"
        write_csv(path, [["Name"], ["Alice"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", make_config())
        assert isinstance(meta, TableMeta)


# ============================================================================
# Cell-level inference
# ============================================================================

class TestInferCellType:
    # NUMBER — integer
    def test_integer(self):
        assert infer_cell_type("42") == "NUMBER"

    def test_integer_negative(self):
        assert infer_cell_type("-7") == "NUMBER"

    def test_integer_zero(self):
        assert infer_cell_type("0") == "NUMBER"

    def test_integer_large(self):
        assert infer_cell_type("1000000") == "NUMBER"

    # NUMBER — decimal
    def test_decimal_plain(self):
        assert infer_cell_type("3.14") == "NUMBER"

    def test_decimal_negative(self):
        assert infer_cell_type("-99.99") == "NUMBER"

    def test_decimal_comma_formatted(self):
        assert infer_cell_type("1,234.56") == "NUMBER"

    def test_decimal_comma_large(self):
        assert infer_cell_type("1,234,567.89") == "NUMBER"

    def test_decimal_no_fractional(self):
        # "1,000" — comma-grouped integer
        assert infer_cell_type("1,000") == "NUMBER"

    # DATE
    def test_date_iso(self):
        assert infer_cell_type("2024-01-15") == "DATE"

    def test_date_iso_end_of_month(self):
        assert infer_cell_type("2024-12-31") == "DATE"

    # TIMESTAMP
    def test_timestamp_salesforce(self):
        assert infer_cell_type("2024-01-15T09:30:00.000Z") == "TIMESTAMP"

    def test_timestamp_no_millis(self):
        assert infer_cell_type("2024-01-15T09:30:00Z") == "TIMESTAMP"

    def test_timestamp_space_separator(self):
        assert infer_cell_type("2024-01-15 09:30:00") == "TIMESTAMP"

    def test_timestamp_with_offset(self):
        assert infer_cell_type("2024-01-15T09:30:00+05:30") == "TIMESTAMP"

    # Ambiguous / VARCHAR2 fallback
    def test_ambiguous_us_date(self):
        assert infer_cell_type("01/15/2024") == "VARCHAR2"

    def test_ambiguous_dmy(self):
        assert infer_cell_type("15-01-2024") == "VARCHAR2"

    def test_ambiguous_mon_date(self):
        assert infer_cell_type("15-JAN-2024") == "VARCHAR2"

    def test_na_string(self):
        assert infer_cell_type("N/A") == "VARCHAR2"

    def test_null_string(self):
        assert infer_cell_type("null") == "VARCHAR2"

    def test_none_string(self):
        assert infer_cell_type("None") == "VARCHAR2"

    def test_random_string(self):
        assert infer_cell_type("hello world") == "VARCHAR2"

    def test_currency_symbol(self):
        # "$1,234" — has non-numeric prefix → VARCHAR2
        assert infer_cell_type("$1,234.56") == "VARCHAR2"

    # Null sentinel
    def test_empty_string(self):
        assert infer_cell_type("") == "UNKNOWN"

    def test_whitespace_only(self):
        assert infer_cell_type("   ") == "UNKNOWN"


# ============================================================================
# Column-level inference
# ============================================================================

class TestInferColumnType:
    # NUMBER
    def test_all_integers(self):
        dtype, prec, scale = infer_column_type(["1", "2", "3"])
        assert dtype == "NUMBER"
        assert scale == 0 or scale is None

    def test_all_decimals(self):
        dtype, prec, scale = infer_column_type(["1.5", "2.75", "3.0"])
        assert dtype == "NUMBER"
        assert scale is not None and scale > 0

    def test_mixed_int_and_decimal(self):
        dtype, prec, scale = infer_column_type(["1", "2.5", "3"])
        assert dtype == "NUMBER"

    def test_comma_formatted_numbers(self):
        dtype, prec, scale = infer_column_type(["1,234.56", "9,999.00"])
        assert dtype == "NUMBER"

    def test_negative_numbers(self):
        dtype, prec, scale = infer_column_type(["-100", "-200.5", "0"])
        assert dtype == "NUMBER"

    def test_precision_is_max_significant_digits(self):
        dtype, prec, scale = infer_column_type(["1.5", "123.456"])
        assert dtype == "NUMBER"
        assert prec is not None and prec >= 6  # 123.456 → 6 significant digits

    def test_scale_is_max_decimal_places(self):
        dtype, prec, scale = infer_column_type(["1.5", "1.999"])
        assert dtype == "NUMBER"
        assert scale == 3  # "1.999" has 3 decimal places

    # DATE
    def test_all_dates(self):
        dtype, prec, scale = infer_column_type(["2024-01-01", "2024-06-15"])
        assert dtype == "DATE"
        assert prec is None and scale is None

    # TIMESTAMP
    def test_all_timestamps(self):
        dtype, prec, scale = infer_column_type(
            ["2024-01-01T00:00:00Z", "2024-06-15T12:30:00.000Z"]
        )
        assert dtype == "TIMESTAMP"

    def test_mixed_date_and_timestamp_promotes_to_timestamp(self):
        dtype, _, __ = infer_column_type(["2024-01-01", "2024-01-02T10:00:00Z"])
        assert dtype == "TIMESTAMP"

    # VARCHAR2 fallback
    def test_mixed_number_and_na(self):
        dtype, _, __ = infer_column_type(["1", "2", "N/A", "4"])
        assert dtype == "VARCHAR2"

    def test_mixed_number_and_string(self):
        dtype, _, __ = infer_column_type(["1", "hello"])
        assert dtype == "VARCHAR2"

    def test_mixed_date_and_string(self):
        dtype, _, __ = infer_column_type(["2024-01-01", "not-a-date"])
        assert dtype == "VARCHAR2"

    def test_all_empty_cells(self):
        dtype, _, __ = infer_column_type(["", "", ""])
        assert dtype == "VARCHAR2"

    def test_empty_list(self):
        dtype, _, __ = infer_column_type([])
        assert dtype == "VARCHAR2"

    def test_nulls_ignored_in_consensus(self):
        # Nulls mixed with valid NUMBERs → still NUMBER
        dtype, _, __ = infer_column_type(["", "1", "", "2", ""])
        assert dtype == "NUMBER"


# ============================================================================
# apply_type_inference
# ============================================================================

class TestApplyTypeInference:
    def _make_meta(self, keys: list[str]) -> TableMeta:
        cols = {k: ColumnMap(source_key=k, target_name=k.upper()) for k in keys}
        return TableMeta(table_name="T", schema_name="S", columns=cols)

    def test_sets_data_type(self):
        meta = self._make_meta(["amount"])
        apply_type_inference(meta, {"amount": ["1.5", "2.0"]})
        assert meta.columns["amount"].data_type == "NUMBER"

    def test_sets_precision_and_scale(self):
        meta = self._make_meta(["amount"])
        apply_type_inference(meta, {"amount": ["123.45", "9.9"]})
        col = meta.columns["amount"]
        assert col.precision is not None
        assert col.scale == 2

    def test_varchar2_has_no_precision(self):
        meta = self._make_meta(["name"])
        apply_type_inference(meta, {"name": ["Alice", "Bob"]})
        col = meta.columns["name"]
        assert col.data_type == "VARCHAR2"
        assert col.precision is None
        assert col.scale is None

    def test_missing_column_values_defaults_to_varchar2(self):
        meta = self._make_meta(["name", "amount"])
        # Only provide values for one column
        apply_type_inference(meta, {"name": ["Alice"]})
        assert meta.columns["amount"].data_type == "VARCHAR2"

    def test_mutates_in_place(self):
        meta = self._make_meta(["col"])
        original_col = meta.columns["col"]
        apply_type_inference(meta, {"col": ["42"]})
        assert meta.columns["col"] is original_col  # same object, mutated


# ============================================================================
# Integration — sniff with type inference
# ============================================================================

class TestSniffWithTypeInference:
    def _cfg(self):
        return make_config()

    def test_number_column_detected(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Amount"], ["100"], ["200.50"], ["99"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Amount"].data_type == "NUMBER"

    def test_date_column_detected(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Created Date"], ["2024-01-01"], ["2024-06-15"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Created Date"].data_type == "DATE"

    def test_timestamp_column_detected(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Last Modified"], ["2024-01-01T10:00:00.000Z"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Last Modified"].data_type == "TIMESTAMP"

    def test_varchar2_column_detected(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"], ["Bob"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Name"].data_type == "VARCHAR2"

    def test_mixed_column_falls_back_to_varchar2(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Value"], ["1"], ["N/A"], ["3"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Value"].data_type == "VARCHAR2"

    def test_number_column_has_scale(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Price"], ["9.99"], ["19.99"], ["4.50"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        col = meta.columns["Price"]
        assert col.data_type == "NUMBER"
        assert col.scale == 2

    def test_varchar2_has_no_precision(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Name"].precision is None

    def test_all_types_in_one_file(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [
            ["Name", "Amount", "Created Date", "Last Modified"],
            ["Alice", "100.00", "2024-01-01", "2024-01-01T10:00:00Z"],
            ["Bob", "200.50", "2024-02-14", "2024-02-14T08:00:00Z"],
        ])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Name"].data_type == "VARCHAR2"
        assert meta.columns["Amount"].data_type == "NUMBER"
        assert meta.columns["Created Date"].data_type == "DATE"
        assert meta.columns["Last Modified"].data_type == "TIMESTAMP"

    def test_header_only_file_all_varchar2(self, tmp_path):
        path = tmp_path / "headers_only.csv"
        write_csv(path, [["Name", "Amount"]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Name"].data_type == "VARCHAR2"
        assert meta.columns["Amount"].data_type == "VARCHAR2"

    def test_data_type_never_unknown_after_sniff(self, tmp_path):
        """UNKNOWN is a cell-level sentinel only — no column should retain it."""
        path = tmp_path / "data.csv"
        write_csv(path, [["Col"], [""], [""], [""]])
        with CSVReader(path) as source:
            meta = sniff(source, "T", "S", self._cfg())
        assert meta.columns["Col"].data_type != "UNKNOWN"