"""
Pipeline orchestrator for the Apollo ingestion pipeline.

Wires Phases 1-6 in order and enforces quarantine policy at every
boundary.  This is the single callable that ``master.py`` invokes.

Phase order:
  1. Open source → read headers
  2. Local sniff  → populate TableMeta (sizes, types)
  3. Oracle handshake → CREATE / ALTER TABLE as needed (skipped in dry_run)
  4. Alignment / resize → MODIFY if column too small (part of Phase 3 in impl)
  5. Generate rows → lazy named-bind dict stream
  6. Batch execute → executemany, commit, log errors

Quarantine policy:
  - ``QuarantineError`` (alignment, size breach) at Phase 2 → move file to
    ``error_dir``, return ``PipelineResult`` with ``quarantined=True``.
  - ``DDLError`` at Phase 3 → move file to ``error_dir``, ``quarantined=True``.
  - ``IngestionError`` at Phase 6 → log, continue; do NOT quarantine.
  - ``all_rows_failed`` from ``BatchResult`` → logged as a warning in the
    result, but file still moves to ``processed_dir`` (rows were attempted).
  - ``dry_run=True`` → Phases 1-4 run (DDL printed, not executed),
    Phases 5-6 skipped entirely.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from src.configs.config import PipelineConfig
from src.configs.exceptions import DDLError, IngestionError, QuarantineError
from src.discovery.csv_reader import CSVReader
from src.discovery.local_sniff import sniff
from src.discovery.remote_discovery import DiscoveryResult, discover_and_sync
from src.loaders.batch_exec import BatchResult, execute_batch
from src.transformers.row_generator import generate_rows
from src.utils.files import mark_processed, quarantine_file
from src.utils.identifiers import to_schema_name, to_table_name

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result object
# ---------------------------------------------------------------------------

@dataclass
class PipelineResult:
    """
    Summary of a single file's pipeline run.

    Attributes:
        source_path:      Path of the CSV that was processed.
        quarantined:      True if the file was moved to ``error_dir``.
        dry_run:          True if this was a dry-run (no DB writes).
        discovery:        ``DiscoveryResult`` from Phase 3, or ``None`` if
                          not reached (quarantine or dry-run skipped load).
        batch:            ``BatchResult`` from Phase 6, or ``None`` if skipped.
        ddl_preview:      DDL statements that would be executed (dry-run only).
        error:            Exception that caused quarantine, if any.
        final_path:       Where the file ended up (processed or error dir).
    """
    source_path: Path
    quarantined: bool = False
    dry_run: bool = False
    discovery: DiscoveryResult | None = None
    batch: BatchResult | None = None
    ddl_preview: list[str] = field(default_factory=list)
    error: Exception | None = None
    final_path: Path | None = None

    @property
    def success(self) -> bool:
        return not self.quarantined and self.error is None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(
    source_path: Path | str,
    table_name: str,
    schema_name: str,
    connection,
    config: PipelineConfig,
) -> PipelineResult:
    """
    Execute the full ingestion pipeline for a single CSV file.

    Args:
        source_path:  Path to the CSV file.
        table_name:   Raw table name (will be sanitized).
        schema_name:  Raw schema name (will be sanitized).
        connection:   Open Oracle connection (or mock).  Pass ``None``
                      when ``config.dry_run`` is True to skip DB phases.
        config:       Pipeline configuration.

    Returns:
        ``PipelineResult`` describing the outcome.
    """
    source_path = Path(source_path)
    result = PipelineResult(source_path=source_path, dry_run=config.dry_run)

    safe_table  = to_table_name(table_name, config)
    safe_schema = to_schema_name(schema_name, config)

    # ── Phase 2: Local sniff ───────────────────────────────────────────────
    try:
        with CSVReader(source_path) as source:
            meta = sniff(source, safe_table, safe_schema, config)
    except QuarantineError as e:
        return _quarantine(result, e, config)
    except Exception as e:
        return _quarantine(result, IngestionError(str(e)), config)

    logger.info(
        "Sniff complete: %s columns, table=%s.%s",
        len(meta.columns), safe_schema, safe_table,
    )

    # ── Phase 3+4: Oracle discovery & DDL ─────────────────────────────────
    if config.dry_run:
        try:
            # Build DDL for preview without executing
            from src.discovery.ddl_builder import build_create_table
            result.ddl_preview = [build_create_table(meta, config)]
        except (DDLError, Exception):
            pass  # best-effort preview
        logger.info("Dry-run: skipping DB phases.")
        return result

    try:
        discovery = discover_and_sync(meta, connection, config, dry_run=False)
        result.discovery = discovery
        logger.info(
            "Discovery complete: scenario=%s, new=%s, modified=%s",
            discovery.scenario,
            discovery.new_columns,
            discovery.modified_columns,
        )
    except (DDLError, QuarantineError) as e:
        return _quarantine(result, e, config)
    except Exception as e:
        return _quarantine(result, IngestionError(str(e)), config)

    # ── Phase 5+6: Stream & load ───────────────────────────────────────────
    try:
        with CSVReader(source_path) as source:
            batch = execute_batch(
                connection=connection,
                meta=meta,
                rows=generate_rows(source, meta),
                source_path=source_path,
                config=config,
            )
        result.batch = batch

        if batch.all_rows_failed:
            logger.warning(
                "All rows failed for %s — %d batch errors logged to %s",
                source_path.name,
                batch.error_count,
                batch.error_log_path,
            )
        elif batch.error_count:
            logger.warning(
                "%d batch error(s) for %s — see %s",
                batch.error_count,
                source_path.name,
                batch.error_log_path,
            )

    except IngestionError as e:
        logger.error("Load error for %s: %s", source_path.name, e)
        result.error = e
        # Do not quarantine — partial commit may have occurred
        return result

    # ── Move to processed ─────────────────────────────────────────────────
    try:
        result.final_path = mark_processed(source_path, config.processed_dir)
    except IngestionError as e:
        logger.warning("Could not move %s to processed: %s", source_path.name, e)

    return result


# ---------------------------------------------------------------------------
# Validate-only mode (Phases 1-2, no DB)
# ---------------------------------------------------------------------------

def validate(
    source_path: Path | str,
    table_name: str,
    schema_name: str,
    config: PipelineConfig,
) -> PipelineResult:
    """
    Run Phases 1-2 only: open CSV, sniff, return result without touching Oracle.

    Used by the ``validate`` CLI command.

    Returns:
        ``PipelineResult`` with ``quarantined=True`` if sniff fails,
        or ``success=True`` if the file is clean.
    """
    source_path = Path(source_path)
    result = PipelineResult(source_path=source_path, dry_run=True)

    safe_table  = to_table_name(table_name, config)
    safe_schema = to_schema_name(schema_name, config)

    try:
        with CSVReader(source_path) as source:
            sniff(source, safe_table, safe_schema, config)
    except QuarantineError as e:
        result.quarantined = True
        result.error = e
        return result
    except Exception as e:
        result.quarantined = True
        result.error = IngestionError(str(e))
        return result

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _quarantine(
    result: PipelineResult,
    error: Exception,
    config: PipelineConfig,
) -> PipelineResult:
    """Move source file to error_dir and mark result as quarantined."""
    result.quarantined = True
    result.error = error
    logger.error("Quarantining %s: %s", result.source_path.name, error)
    try:
        result.final_path = quarantine_file(result.source_path, config.error_dir)
    except IngestionError as move_err:
        logger.error("Failed to quarantine file: %s", move_err)
    return result
