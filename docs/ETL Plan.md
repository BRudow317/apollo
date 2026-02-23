# CSV to Oracle ETL Plan (V6)

---

## Phase 1: DataStructure (Metadata & Mapping)
---

* **ColumnMap Object (Dataclass, slots=True):**
    - `source_key`: Raw string from SF CSV header (e.g., `"Last Name"`).
    - `target_name`: Sanitized identifier (e.g., `"LAST_NAME"`).
    - `oracle_name`: Actual name found in `ALL_TAB_COLUMNS` (matches `target_name` in happy path).
    - `data_type`: Oracle type (`VARCHAR2`, `NUMBER`, `DATE`, `TIMESTAMP`, etc.).
    - `length`: Current max **char length** found in CSV (not bytes).
    - `max_byte_len`: Max byte length for UTF-8 safety.
    - `nullable`: Boolean for null constraints.
    - `is_new`: Boolean flag if column requires `ADD` or `CREATE`.
    - `precision`, `scale`: For numeric columns.
    - `length_semantics`: Always `'CHAR'`.

* **TableMeta Object (Dataclass, slots=True):**
    - `table_name`: Sanitized Oracle table name.
    - `schema_name`: Sanitized Oracle schema/owner.
    - `columns`: `dict[str, ColumnMap]` (Keyed by `source_key`).
    - `insert_sql`: Cached property (computed once, stored) generating named bind SQL:
        ```sql
        INSERT INTO {schema}.{table} (COL_A, COL_B, COL_C)
        VALUES (:COL_A, :COL_B, :COL_C)
        ```
        - Bind names derive from `oracle_name` on each `ColumnMap`.
        - Column order is locked at **Phase 4 refresh** and never recomputed mid-pipeline.
        - Cached as `_insert_sql` after first generation; invalidated explicitly if schema changes.

* **Bind Strategy: Named Binds (Resolved)**
    - All bind parameters use `:oracle_name` syntax.
    - Row generator yields `dict` keyed by `oracle_name`.
    - `setinputsizes` called with `**{col.oracle_name: bind_type}` for all columns.
    - Rationale: decouples column insertion order from `ALL_TAB_COLUMNS` return order; safe across `ALTER TABLE ADD` operations; eliminates positional drift bugs.

---

## Phase 2: Local Discovery & Integrity (The Sniff)
---

* **Action:** Open CSV with:
    - `encoding='utf-8-sig'` (handles BOM)
    - `newline=''` for Windows safety
    - Strict dialect validation (`dialect.strict = True`)

* **Action:** Apply Regex Sanitizer to `table_name` and `schema_name`:
    - Uppercase, replace invalid chars with `_`, truncate to Oracle max identifier length (30 or 128 depending on version).
    - Avoid reserved words by suffixing `_COL`.

* **Action:** Loop through CSV headers; generate `target_name` for each and initialize `ColumnMap`.

* **Integrity Pass:** Scan 100% of the file:
    1. **Field Alignment:** Verify `len(row_fields) == len(TableMeta.columns)` for every parsed row.
    2. **Sizing Check:** Track:
        - `max_char_len = len(cell)`
        - `max_byte_len = len(cell.encode('utf-8'))`
    3. **Type Guess:** Infer `NUMBER`, `DATE`, `TIMESTAMP`, else fallback to `VARCHAR2`.
    4. **Early Size Breach:** If any column's `max_char_len > 4000` during sniff → quarantine immediately; do not proceed to Phase 3.

* **Quarantine:** If row alignment fails or size breach detected, move file to `ERROR_FOLDER` and exit.

---

## Phase 3: Remote Discovery (The Oracle Handshake)
---

* **Action:** Query `ALL_TAB_COLUMNS` with `OWNER = :schema` to verify if `table_name` exists.

* **Scenario A (New Table):**
    - Generate `CREATE TABLE` using:
        - `VARCHAR2({min(max_char_len + VARCHAR2_GROWTH_BUFFER, 4000)} CHAR)`
        - `VARCHAR2_GROWTH_BUFFER` sourced from `config.py` (see Additional Rules).
        - Use inferred types for numeric/date columns.

* **Scenario B (Existing Table):**
    - Compare `ALL_TAB_COLUMNS` results against `ColumnMap.target_name`.
    - Identify gaps → `ALTER TABLE ADD (...)`.
    - Ensure CHAR semantics for new columns.

---

## Phase 4: Alignment & Dynamic Sizing
---

* **Action:** Compare Phase 2 `ColumnMap.length` vs Oracle `CHAR_LENGTH`.
* **Evolve Schema:** If `ColumnMap.length > data_length`:
    - Execute:
      ```sql
      ALTER TABLE {table_name} MODIFY ({oracle_name} VARCHAR2({min(new_length + VARCHAR2_GROWTH_BUFFER, 4000)} CHAR))
      ```
    - **Hard Fail:** If `ColumnMap.length > 4000` → quarantine batch/file for manual review. (Should already be caught in Phase 2; this is a second safety net.)
* **Refresh & Lock Metadata:** Re-query `ALL_TAB_COLUMNS` and re-sync `TableMeta.columns`. Lock column order and regenerate `_insert_sql` cache once. This is the final bind order — no further changes mid-pipeline.

---

## Phase 5: The Clean Stream (The Generator)
---

* **Action:** Re-initialize CSV reader (`seek(0)`).
* **Generator Function:**
    - **Input:** Raw `csv.DictReader` row.
    - **Transformation:**
        - Strip `\x00`.
        - Normalize empty strings `""` → `None`.
        - Strip commas from numeric strings (e.g., `"1,234.56"` → `Decimal("1234.56")`).
        - Parse dates: handle `YYYY-MM-DD` and `YYYY-MM-DDTHH:MM:SS.000Z`; ambiguous formats (e.g., `01/02/2024`) fall back to `VARCHAR2` — do not guess.
        - Convert numeric strings → `Decimal` or `int`.
        - Mixed-type columns (e.g., 99% numeric + one `"N/A"`) → fallback to `VARCHAR2`; never raise mid-stream.
    - **Output:** `dict` keyed by `oracle_name` — one dict per row.
* **Memory Efficiency:** Only current row in RAM.

---

## Phase 6: Batch Execution (The Heavy Lift)
---

* **Action:** Set:
    ```python
    cursor.bindarraysize = config.batch_size  # configurable, default 1000
    cursor.setinputsizes(**{col.oracle_name: bind_type for col in columns})
    ```
* **Action:** Call:
    ```python
    cursor.executemany(TableMeta.insert_sql, generator, batcherrors=True)
    ```
* **Error Handling:**
    - Capture `cursor.getbatcherrors()` → anomaly log.
    - If any error indicates size breach (>4000 CHAR), quarantine batch.
* **Commit:** `connection.commit()` and `cursor.close()`.

---

## Additional Rules
---

- **Max VARCHAR2 size:** 4000 CHAR (no CLOB fallback).
- **Length semantics:** Always CHAR.
- **Fail fast:** Size breach caught at Phase 2 sniff; Phase 4 is a secondary safety net.
- **Type inference:** NUMBER, DATE, TIMESTAMP where possible; fallback to VARCHAR2 on ambiguity — never raise mid-stream.
- **`VARCHAR2_GROWTH_BUFFER`:** Named constant in `config.py`. Default TBD pending ALTER MODIFY testing. Applied consistently to both CREATE TABLE and ALTER TABLE MODIFY.
- **`batch_size`:** Named constant in `config.py`. Default 1000. Tune based on row width.
- **Bind strategy:** Named binds throughout. No positional binds anywhere in the codebase.
- **`insert_sql` caching:** Computed once after Phase 4 refresh; stored as `_insert_sql`; never recomputed mid-pipeline.
- **Sanitization:** Strict regex, reserved word handling, identifier truncation. Sanitizer output is the only source of column/table names in SQL — raw CSV headers never reach a query string.
- **Security:** Sanitizer is the SQL injection boundary for identifiers. Bind parameters handle value injection. Both must be tested explicitly.
- **DDL failure/re-run:** On re-run after partial failure, Phase 3 detects existing table (Scenario B) and proceeds normally. No deduplication of already-loaded rows — this is append-only; deduplication is the caller's responsibility or handled via a staging table pattern.

---

## Project Structure

```shell

```

---

## Sprint Plan

Each sprint delivers a vertical slice of functionality and closes with a single sprint test file. The sprint test is **cumulative** — it re-affirms all prior sprint contracts plus the new one, so a passing sprint suite means the chain is unbroken end-to-end up to that point. Unit tests in `tests/unit/` provide granular per-module coverage alongside the sprint tests.

---

### Sprint 1 — Foundation
**Goal:** Core data structures, configuration, exceptions, and identifier sanitization are solid and independently testable before any I/O is introduced.

**Files delivered:**
- `src/ingestor/core/models.py` — `ColumnMap`, `TableMeta`, named bind SQL generation (cached `_insert_sql`)
- `src/ingestor/core/config.py` — `VARCHAR2_GROWTH_BUFFER`, `batch_size`, paths, Oracle limits
- `src/ingestor/core/exceptions.py` — `QuarantineError`, `SizeBreachError`, `AlignmentError`, `DDLError`
- `src/ingestor/core/sanitizer.py` — identifier regex, reserved word handling, truncation
- `src/ingestor/utils/identifiers.py` — helper wrappers around sanitizer

**Sprint test:** `tests/sprint/test_sprint1_foundation.py`

Covers:
- `ColumnMap` and `TableMeta` instantiation with valid and invalid inputs
- `insert_sql` generates correct named bind syntax (`:oracle_name`)
- `insert_sql` is cached — same object returned on second access
- Sanitizer uppercases, strips invalid chars, truncates at Oracle limit
- Reserved words receive `_COL` suffix
- All custom exceptions are raisable with expected messages

---

### Sprint 2 — CSV Reading & Local Sniff
**Goal:** Open a CSV safely, validate headers and every row, track column sizes and field alignment, quarantine on failure.

**Files delivered:**
- `src/ingestor/core/csv_dialect.py` — strict dialect, BOM stripping
- `src/ingestor/core/validation.py` — header alignment, row field count checks
- `src/ingestor/sources/base.py` — `AbstractSource` interface
- `src/ingestor/sources/csv_sf/reader.py` — SF CSV reader
- `src/ingestor/sources/csv_generic/reader.py` — generic CSV reader
- `src/ingestor/discovery/local_sniff.py` — full file scan: sizes, alignment, early breach detection
- `src/ingestor/utils/files.py` — quarantine mover

**Sprint test:** `tests/sprint/test_sprint2_csv_sniff.py`

Covers:
- Sprint 1 contracts (re-affirmed via import smoke tests)
- BOM-prefixed CSV parses cleanly; headers are correct
- Strict dialect raises on malformed CSV; file is quarantined
- Row with mismatched field count triggers `AlignmentError` and quarantine
- `max_char_len` and `max_byte_len` are tracked correctly per column
- Column exceeding 4000 CHAR triggers `SizeBreachError` during sniff (before Phase 3)
- Clean CSV produces a fully-populated `TableMeta` with all `ColumnMap` lengths set

---

### Sprint 3 — Type Inference
**Goal:** Reliably infer Oracle types from CSV cell values with documented, tested rules for every edge case.

**Files delivered:**
- `src/ingestor/core/typing_infer.py` — inference logic for NUMBER, DATE, TIMESTAMP, VARCHAR2 fallback

**Sprint test:** `tests/sprint/test_sprint3_type_infer.py`

Covers:
- Sprint 1–2 contracts (re-affirmed)
- Integer strings → `NUMBER` (no scale)
- Decimal strings → `NUMBER` with correct precision/scale
- Comma-formatted numbers (`"1,234.56"`) → `NUMBER` after strip
- `YYYY-MM-DD` → `DATE`
- `YYYY-MM-DDTHH:MM:SS.000Z` → `TIMESTAMP`
- Ambiguous date formats (e.g., `"01/02/2024"`) → `VARCHAR2` (no guess)
- `"N/A"`, `"null"`, mixed-type columns → `VARCHAR2` (no raise)
- Empty cell / `None` → does not affect type inference for the column

---

### Sprint 4 — Oracle Discovery & DDL
**Goal:** Connect to Oracle, read `ALL_TAB_COLUMNS`, generate correct CREATE/ALTER DDL, enforce CHAR semantics.

**Files delivered:**
- `src/ingestor/discovery/oracle/client.py` — connection management, session settings
- `src/ingestor/discovery/oracle/remote_discovery.py` — `ALL_TAB_COLUMNS` queries, Scenario A/B detection
- `src/ingestor/discovery/oracle/ddl_builder.py` — CREATE TABLE, ALTER TABLE ADD, ALTER TABLE MODIFY generators

**Sprint test:** `tests/sprint/test_sprint4_oracle_ddl.py`

Covers:
- Sprint 1–3 contracts (re-affirmed)
- `ddl_builder` produces `VARCHAR2(N CHAR)` — never byte semantics
- `VARCHAR2_GROWTH_BUFFER` from config is applied to sizing
- Column size capped at 4000; anything over raises `SizeBreachError`
- New table scenario produces valid `CREATE TABLE` DDL (validated via string parse, not live DB)
- Existing table scenario produces `ALTER TABLE ADD` for missing columns only
- Existing column needing resize produces `ALTER TABLE MODIFY` with correct size
- Column already large enough produces no DDL
- Integration test (requires Oracle): `tests/integration/test_oracle_discovery.py`

---

### Sprint 5 — Transform & Row Generator
**Goal:** Stream rows from CSV through normalizers and yield named-bind dicts ready for `executemany`.

**Files delivered:**
- `src/ingestor/transform/normalizers.py` — `\x00` strip, empty→None, date parse, number parse
- `src/ingestor/transform/row_generator.py` — generator yielding `dict[oracle_name, value]`

**Sprint test:** `tests/sprint/test_sprint5_transform.py`

Covers:
- Sprint 1–4 contracts (re-affirmed)
- `\x00` characters stripped from all string fields
- Empty string `""` normalized to `None`
- `"1,234.56"` → `Decimal("1234.56")`
- `"N/A"` in a VARCHAR2 column passes through as string; does not raise
- `"N/A"` in a NUMBER column → `None` (not a crash)
- Dates parsed to `datetime` for DATE/TIMESTAMP columns
- Generator yields `dict` keyed by `oracle_name`; key set matches `TableMeta.columns`
- Generator is lazy — second `next()` does not re-read first row
- Full CSV round-trip: `local_sniff` → `row_generator` produces same row count as input

---

### Sprint 6 — Load & Batch Execution
**Goal:** Execute named-bind batches against Oracle with `executemany`, capture batch errors, commit cleanly.

**Files delivered:**
- `src/ingestor/load/binds.py` — named bind type mapping, `setinputsizes` dict builder
- `src/ingestor/load/batch_exec.py` — `executemany` wrapper, `batcherrors=True`, error capture
- `src/ingestor/load/error_logging.py` — anomaly log sink

**Sprint test:** `tests/sprint/test_sprint6_load.py`

Covers:
- Sprint 1–5 contracts (re-affirmed)
- `binds.py` produces correct `oracle.db` type mappings for VARCHAR2, NUMBER, DATE, TIMESTAMP
- `setinputsizes` dict keys match `oracle_name` for all columns
- `batch_exec` calls `executemany` once with the full generator
- `batcherrors=True` is always set; errors are captured, not raised
- Batch errors containing size breach are quarantined
- Clean batch commits; cursor is closed after commit
- `batch_size` is sourced from config, not hardcoded
- Integration test (requires Oracle): `tests/integration/test_batch_exec.py`

---

### Sprint 7 — Pipeline Orchestration & CLI
**Goal:** Wire all phases into a single callable pipeline, enforce quarantine policy end-to-end, expose a CLI.

**Files delivered:**
- `src/ingestor/pipeline/phases.py` — discrete phase functions (callable independently)
- `src/ingestor/pipeline/orchestrator.py` — wires Phases 1–6; applies quarantine on any breach
- `src/ingestor/utils/logging.py` — logger factory
- `src/ingestor/utils/checkpoints.py` — optional resume markers
- `src/cli/main.py` — Typer CLI: `ingest`, `dry-run`, `validate` commands

**Sprint test:** `tests/sprint/test_sprint7_pipeline.py`

Covers:
- Sprint 1–6 contracts (re-affirmed)
- Clean CSV + mock Oracle → full pipeline runs without error; file moves to `processed/`
- Misaligned CSV → quarantined at Phase 2; no DDL executed
- Size breach CSV → quarantined at Phase 2 sniff; no Oracle connection attempted
- Phase 4 MODIFY failure → file quarantined; partial DDL state documented in error log
- Dry-run mode → DDL printed, no DB calls made, no files moved
- CLI `ingest` command accepts `--source`, `--schema`, `--table`, `--config` flags
- CLI `validate` command runs Phase 1–2 only and exits with code 0 (clean) or 1 (error)
- End-to-end integration test with real Oracle: `tests/integration/test_batch_exec.py` (extended)

