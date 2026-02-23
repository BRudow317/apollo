"""
Phase 6: Batch Execution (The Heavy Lift).

Streams the row generator into Oracle via ``cursor.executemany()`` with
``batcherrors=True``.  All rows that succeed are committed; rows that fail
are logged to the error log and the pipeline continues.

Key behaviours:
  - ``cursor.bindarraysize`` is set from ``config.batch_size`` before execution.
  - ``cursor.setinputsizes()`` is called with the full type map before execution.
  - ``batcherrors=True`` is always set — errors never abort the batch.
  - After execution, ``cursor.getbatcherrors()`` is inspected:
      - Errors are logged via ``error_logging.log_batch_errors``.
      - If every row in the batch errored, a warning is logged but execution
        continues (no quarantine — that decision belongs to the orchestrator).
  - ``connection.commit()`` is called once after executemany.
  - ``cursor.close()`` is called in a ``finally`` block.

Usage::

    from src.loaders.batch_exec import execute_batch

    result = execute_batch(
        connection=conn,
        meta=meta,
        rows=generate_rows(source, meta),
        source_path=csv_path,
        config=config,
    )
    print(f"Inserted rows, {result.error_count} batch errors logged.")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

from src.configs.config import PipelineConfig
from src.loaders.binds import build_input_sizes
from src.loaders.error_logging import log_batch_errors
from src.models.models import TableMeta


# ---------------------------------------------------------------------------
# Result object
# ---------------------------------------------------------------------------

@dataclass
class BatchResult:
    """
    Summary of a ``execute_batch`` call.

    Attributes:
        error_count:    Total number of row-level errors from ``getbatcherrors()``.
        error_log_path: Path to the error log file, or ``None`` if no errors occurred.
        all_rows_failed: True if every row in the batch errored.
    """
    error_count: int = 0
    error_log_path: Path | None = None
    all_rows_failed: bool = False


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def execute_batch(
    connection,
    meta: TableMeta,
    rows: Iterator[dict],
    source_path: Path | str,
    config: PipelineConfig,
) -> BatchResult:
    """
    Execute ``cursor.executemany()`` for all rows and commit.

    Args:
        connection:  Open Oracle connection (real or mock).
        meta:        Fully-populated ``TableMeta`` with locked ``insert_sql``.
        rows:        Iterator of named-bind dicts from ``generate_rows()``.
        source_path: Path of the source CSV (used for error logging).
        config:      Pipeline configuration (``batch_size``, ``error_dir``).

    Returns:
        ``BatchResult`` with error counts and log path.

    Notes:
        - ``cursor.close()`` is always called, even on exception.
        - ``connection.commit()`` is called after ``executemany`` regardless
          of batch errors — partial success is committed.
    """
    cursor = connection.cursor()
    result = BatchResult()

    try:
        # Pre-declare bind sizes and types for performance + safety.
        cursor.bindarraysize = config.batch_size
        input_sizes = build_input_sizes(meta)
        cursor.setinputsizes(**input_sizes)

        # Materialise rows into a list so we know the total count for
        # all-failed detection. For very large files this is acceptable
        # because batch_size controls memory — the orchestrator (Sprint 7)
        # will chunk the generator into batches of batch_size before calling
        # this function.
        row_list = list(rows)

        if not row_list:
            return result

        cursor.executemany(meta.insert_sql, row_list, batcherrors=True)
        connection.commit()

        # Inspect batch errors.
        batch_errors = cursor.getbatcherrors()
        if batch_errors:
            result.error_count = len(batch_errors)
            result.error_log_path = log_batch_errors(
                batch_errors,
                source_path=source_path,
                error_dir=config.error_dir,
            )
            result.all_rows_failed = len(batch_errors) == len(row_list)

    finally:
        cursor.close()

    return result
