"""
Sprint 1 — Foundation: test_sprint1_foundation.py

Covers every contract listed in the Sprint 1 plan:

  - ColumnMap and TableMeta instantiation (valid and invalid inputs)
  - insert_sql generates correct named bind syntax (:oracle_name)
  - insert_sql is cached — same string object returned on second access
  - Sanitizer: uppercase, strip invalid chars, truncate at Oracle limit
  - Reserved words receive _COL suffix
  - All custom exceptions are raisable with expected messages and attributes
"""

from __future__ import annotations

import pytest

# ── models ──────────────────────────────────────────────────────────────────
from ingestor.core.models import ColumnMap, TableMeta

# ── config ──────────────────────────────────────────────────────────────────
from ingestor.core.config import (
    PipelineConfig,
    ORACLE_MAX_VARCHAR2_CHAR,
    ORACLE_MAX_IDENTIFIER_LEN_LEGACY,
    ORACLE_MAX_IDENTIFIER_LEN_EXTENDED,
)

# ── exceptions ───────────────────────────────────────────────────────────────
from ingestor.core.exceptions import (
    IngestionError,
    QuarantineError,
    AlignmentError,
    SizeBreachError,
    DDLError,
)

# ── sanitizer ────────────────────────────────────────────────────────────────
from ingestor.core.sanitizer import sanitize_identifier, is_reserved

# ── identifier helpers ───────────────────────────────────────────────────────
from ingestor.utils.identifiers import to_column_name, to_table_name, to_schema_name


# ============================================================================
# ColumnMap
# ============================================================================


class TestColumnMap:
    def test_basic_instantiation(self):
        col = ColumnMap(source_key="Last Name", target_name="LAST_NAME")
        assert col.source_key == "Last Name"
        assert col.target_name == "LAST_NAME"

    def test_oracle_name_defaults_to_target_name(self):
        col = ColumnMap(source_key="Account ID", target_name="ACCOUNT_ID")
        assert col.oracle_name == "ACCOUNT_ID"

    def test_oracle_name_explicit_override(self):
        col = ColumnMap(
            source_key="Account ID",
            target_name="ACCOUNT_ID",
            oracle_name="ACCT_ID",
        )
        assert col.oracle_name == "ACCT_ID"

    def test_bind_name_format(self):
        col = ColumnMap(source_key="Last Name", target_name="LAST_NAME")
        assert col.bind_name == ":LAST_NAME"

    def test_bind_name_uses_oracle_name(self):
        col = ColumnMap(
            source_key="Date",
            target_name="DATE_COL",
            oracle_name="DATE_COL",
        )
        assert col.bind_name == ":DATE_COL"

    def test_default_values(self):
        col = ColumnMap(source_key="x", target_name="X")
        assert col.data_type == "UNKNOWN"
        assert col.length == 0
        assert col.max_byte_len == 0
        assert col.nullable is True
        assert col.is_new is True
        assert col.precision is None
        assert col.scale is None
        assert col.length_semantics == "CHAR"


# ============================================================================
# TableMeta — instantiation
# ============================================================================


class TestTableMetaInstantiation:
    def _make_table(self) -> TableMeta:
        col_a = ColumnMap(source_key="First Name", target_name="FIRST_NAME")
        col_b = ColumnMap(source_key="Account ID", target_name="ACCOUNT_ID")
        return TableMeta(
            table_name="CONTACTS",
            schema_name="SALES",
            columns={"First Name": col_a, "Account ID": col_b},
        )

    def test_qualified_name(self):
        t = self._make_table()
        assert t.qualified_name == "SALES.CONTACTS"

    def test_ordered_oracle_names(self):
        t = self._make_table()
        assert t.ordered_oracle_names() == ["FIRST_NAME", "ACCOUNT_ID"]

    def test_empty_columns_raises_on_insert_sql(self):
        t = TableMeta(table_name="EMPTY", schema_name="S")
        with pytest.raises(ValueError, match="columns is empty"):
            _ = t.insert_sql


# ============================================================================
# TableMeta — insert_sql named bind generation
# ============================================================================


class TestInsertSql:
    def _make_table(self, col_names: list[str]) -> TableMeta:
        cols = {
            name: ColumnMap(source_key=name, target_name=name.upper())
            for name in col_names
        }
        return TableMeta(table_name="MY_TABLE", schema_name="MY_SCHEMA", columns=cols)

    def test_single_column(self):
        t = self._make_table(["email"])
        sql = t.insert_sql
        assert "INSERT INTO MY_SCHEMA.MY_TABLE (EMAIL)" in sql
        assert "VALUES (:EMAIL)" in sql

    def test_multi_column_named_binds(self):
        t = self._make_table(["first_name", "last_name", "account_id"])
        sql = t.insert_sql
        assert "(FIRST_NAME, LAST_NAME, ACCOUNT_ID)" in sql
        assert "(:FIRST_NAME, :LAST_NAME, :ACCOUNT_ID)" in sql

    def test_column_order_matches_columns_dict(self):
        """Column order in SQL must match columns dict insertion order."""
        col_a = ColumnMap(source_key="Z Col", target_name="Z_COL")
        col_b = ColumnMap(source_key="A Col", target_name="A_COL")
        t = TableMeta(
            table_name="T",
            schema_name="S",
            columns={"Z Col": col_a, "A Col": col_b},
        )
        sql = t.insert_sql
        z_pos = sql.index("Z_COL")
        a_pos = sql.index("A_COL")
        assert z_pos < a_pos, "Z_COL was inserted first and must appear first in SQL"

    def test_oracle_name_used_not_target_name(self):
        col = ColumnMap(
            source_key="Date",
            target_name="DATE_COL",
            oracle_name="CREATED_DATE",
        )
        t = TableMeta(table_name="T", schema_name="S", columns={"Date": col})
        sql = t.insert_sql
        assert "CREATED_DATE" in sql
        assert ":CREATED_DATE" in sql
        assert "DATE_COL" not in sql

    def test_qualified_name_in_sql(self):
        t = self._make_table(["col"])
        assert "MY_SCHEMA.MY_TABLE" in t.insert_sql


# ============================================================================
# TableMeta — insert_sql caching
# ============================================================================


class TestInsertSqlCaching:
    def _make_table(self) -> TableMeta:
        col = ColumnMap(source_key="id", target_name="ID")
        return TableMeta(table_name="T", schema_name="S", columns={"id": col})

    def test_same_string_on_second_access(self):
        t = self._make_table()
        first = t.insert_sql
        second = t.insert_sql
        assert first == second

    def test_same_object_on_second_access(self):
        """Cache must return the identical object, not a recomputed string."""
        t = self._make_table()
        first = t.insert_sql
        second = t.insert_sql
        assert first is second

    def test_invalidate_clears_cache(self):
        t = self._make_table()
        first = t.insert_sql
        t.invalidate_sql_cache()
        # Add a new column to produce a different SQL
        t.columns["name"] = ColumnMap(source_key="name", target_name="NAME")
        second = t.insert_sql
        assert first is not second
        assert "NAME" in second

    def test_cache_not_populated_before_first_access(self):
        t = self._make_table()
        # Access private field directly to verify cache starts empty
        assert t._insert_sql is None

    def test_cache_populated_after_first_access(self):
        t = self._make_table()
        _ = t.insert_sql
        assert t._insert_sql is not None


# ============================================================================
# PipelineConfig
# ============================================================================


class TestPipelineConfig:
    def test_defaults(self):
        cfg = PipelineConfig()
        assert cfg.varchar2_growth_buffer == 50
        assert cfg.batch_size == 1000
        assert cfg.oracle_max_identifier_len == ORACLE_MAX_IDENTIFIER_LEN_LEGACY
        assert cfg.dry_run is False

    def test_custom_values(self):
        cfg = PipelineConfig(varchar2_growth_buffer=100, batch_size=500)
        assert cfg.varchar2_growth_buffer == 100
        assert cfg.batch_size == 500

    def test_oracle_constants(self):
        assert ORACLE_MAX_VARCHAR2_CHAR == 4000
        assert ORACLE_MAX_IDENTIFIER_LEN_LEGACY == 30
        assert ORACLE_MAX_IDENTIFIER_LEN_EXTENDED == 128

    def test_effective_max_varchar2_applies_buffer(self):
        cfg = PipelineConfig(varchar2_growth_buffer=50)
        assert cfg.effective_max_varchar2(100) == 150

    def test_effective_max_varchar2_caps_at_4000(self):
        cfg = PipelineConfig(varchar2_growth_buffer=100)
        assert cfg.effective_max_varchar2(3980) == 4000

    def test_effective_max_varchar2_exactly_4000_input(self):
        cfg = PipelineConfig(varchar2_growth_buffer=0)
        assert cfg.effective_max_varchar2(4000) == 4000

    def test_effective_max_varchar2_raises_on_breach(self):
        cfg = PipelineConfig()
        with pytest.raises(ValueError, match="already exceeds"):
            cfg.effective_max_varchar2(4001)


# ============================================================================
# Sanitizer
# ============================================================================


class TestSanitizer:
    def test_basic_lowercase_to_upper(self):
        assert sanitize_identifier("last name") == "LAST_NAME"

    def test_spaces_become_underscores(self):
        assert sanitize_identifier("First Name") == "FIRST_NAME"

    def test_special_chars_become_underscores(self):
        result = sanitize_identifier("My-Column (Q3)!")
        assert result == "MY_COLUMN_Q3"

    def test_leading_digit_prefixed(self):
        result = sanitize_identifier("123abc")
        assert result == "_123ABC", f"Expected '_123ABC', got {result!r}"

    def test_consecutive_underscores_collapsed(self):
        result = sanitize_identifier("a  b  c")
        assert "__" not in result

    def test_reserved_word_gets_suffix(self):
        result = sanitize_identifier("date")
        assert result == "DATE_COL"

    def test_reserved_word_table(self):
        assert sanitize_identifier("table") == "TABLE_COL"

    def test_reserved_word_select(self):
        assert sanitize_identifier("select") == "SELECT_COL"

    def test_already_clean_identifier(self):
        assert sanitize_identifier("LAST_NAME") == "LAST_NAME"

    def test_truncation_at_30(self):
        long_name = "A" * 50
        result = sanitize_identifier(long_name, max_len=30)
        assert len(result) <= 30

    def test_truncation_at_128(self):
        long_name = "B" * 200
        result = sanitize_identifier(long_name, max_len=128)
        assert len(result) <= 128

    def test_reserved_word_truncation_preserves_suffix(self):
        """If a reserved word near the length limit gets _COL, the suffix must survive."""
        # "DATE" is 4 chars; with max_len=7 → "DATE_CO" without logic, but we want "DATE_COL"
        # max_len=8 → "DATE_COL" fits exactly
        result = sanitize_identifier("date", max_len=8)
        assert result.endswith("_COL")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            sanitize_identifier("")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValueError):
            sanitize_identifier("   ")

    def test_invalid_max_len_raises(self):
        with pytest.raises(ValueError):
            sanitize_identifier("col", max_len=0)

    def test_is_reserved_true(self):
        assert is_reserved("date") is True
        assert is_reserved("DATE") is True
        assert is_reserved("Select") is True

    def test_is_reserved_false(self):
        assert is_reserved("LAST_NAME") is False
        assert is_reserved("my_column") is False


# ============================================================================
# Identifier helpers
# ============================================================================


class TestIdentifierHelpers:
    def setup_method(self):
        self.cfg = PipelineConfig()

    def test_to_column_name(self):
        assert to_column_name("Last Name", self.cfg) == "LAST_NAME"

    def test_to_table_name(self):
        assert to_table_name("My Contacts", self.cfg) == "MY_CONTACTS"

    def test_to_schema_name(self):
        assert to_schema_name("sales_db", self.cfg) == "SALES_DB"

    def test_respects_config_max_len(self):
        cfg = PipelineConfig(oracle_max_identifier_len=10)
        result = to_column_name("A" * 50, cfg)
        assert len(result) <= 10

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            to_column_name("", self.cfg)


# ============================================================================
# Exceptions — raisable with expected attributes
# ============================================================================


class TestExceptions:
    # Hierarchy
    def test_quarantine_is_ingestion_error(self):
        assert issubclass(QuarantineError, IngestionError)

    def test_alignment_is_quarantine(self):
        assert issubclass(AlignmentError, QuarantineError)

    def test_size_breach_is_quarantine(self):
        assert issubclass(SizeBreachError, QuarantineError)

    def test_ddl_error_is_ingestion_error(self):
        assert issubclass(DDLError, IngestionError)

    # QuarantineError
    def test_quarantine_message(self):
        e = QuarantineError("bad file", source_path="/tmp/bad.csv")
        assert "bad file" in str(e)
        assert e.source_path == "/tmp/bad.csv"

    def test_quarantine_no_path(self):
        e = QuarantineError("oops")
        assert e.source_path is None
        assert "oops" in str(e)

    # AlignmentError
    def test_alignment_attributes(self):
        e = AlignmentError(
            "row misaligned",
            source_path="/tmp/f.csv",
            row_number=42,
            expected=5,
            got=3,
        )
        assert e.row_number == 42
        assert e.expected == 5
        assert e.got == 3
        s = str(e)
        assert "row=42" in s
        assert "expected=5" in s
        assert "got=3" in s

    def test_alignment_raisable(self):
        with pytest.raises(AlignmentError):
            raise AlignmentError("misalign")

    # SizeBreachError
    def test_size_breach_attributes(self):
        e = SizeBreachError(
            "too long",
            source_path="/tmp/big.csv",
            column_name="NOTES",
            char_length=5000,
        )
        assert e.column_name == "NOTES"
        assert e.char_length == 5000
        assert e.limit == 4000
        s = str(e)
        assert "column=NOTES" in s
        assert "char_length=5000" in s
        assert "limit=4000" in s

    def test_size_breach_raisable(self):
        with pytest.raises(SizeBreachError):
            raise SizeBreachError("breach", column_name="X", char_length=9999)

    def test_size_breach_caught_as_quarantine(self):
        with pytest.raises(QuarantineError):
            raise SizeBreachError("breach")

    # DDLError
    def test_ddl_error_attributes(self):
        e = DDLError("bad ddl", ddl="CREATE TABLE ...")
        assert e.ddl == "CREATE TABLE ..."
        assert "CREATE TABLE" in str(e)

    def test_ddl_error_raisable(self):
        with pytest.raises(DDLError):
            raise DDLError("ddl fail")

    def test_ddl_error_no_ddl(self):
        e = DDLError("something went wrong")
        assert e.ddl is None
