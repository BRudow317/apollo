"""
Batch error logging for the Apollo ingestion pipeline.

Appends Oracle batch errors to a single ``.log`` file in ``error_dir``.
Each entry includes the timestamp, source file name, row offset, Oracle
error code, and error message.

Log format (one line per error)::

    2024-01-15T09:30:00 | source=contacts.csv | row_offset=42 | ora_code=ORA-12899 | msg=value too large ...

The log file is named ``apollo_batch_errors.log`` and is appended to on
every run — never truncated.  This means errors from multiple files and
runs are all in one place for easy ``grep``.

Usage::

    from src.loaders.error_logging import log_batch_errors

    errors = cursor.getbatcherrors()
    if errors:
        log_batch_errors(errors, source_path=path, error_dir=config.error_dir)
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

LOG_FILENAME = "apollo_batch_errors.log"


def log_batch_errors(
    batch_errors: list,
    source_path: Path | str,
    error_dir: Path | str,
) -> Path:
    """
    Append ``batch_errors`` from ``cursor.getbatcherrors()`` to the log file.

    Args:
        batch_errors: List of error objects returned by ``cursor.getbatcherrors()``.
                      Each object must have ``.offset`` (int) and ``.message`` (str)
                      attributes — this is the standard ``python-oracledb`` contract.
        source_path:  Path of the CSV file being processed (used in log entries).
        error_dir:    Directory where the log file lives.  Created if absent.

    Returns:
        Path to the log file that was written.
    """
    error_dir = Path(error_dir)
    error_dir.mkdir(parents=True, exist_ok=True)

    log_path = error_dir / LOG_FILENAME
    source_name = Path(source_path).name
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    with open(log_path, "a", encoding="utf-8") as f:
        for err in batch_errors:
            ora_code = _extract_ora_code(str(err.message))
            line = (
                f"{timestamp} | "
                f"source={source_name} | "
                f"row_offset={err.offset} | "
                f"ora_code={ora_code} | "
                f"msg={err.message.strip()}\n"
            )
            f.write(line)

    return log_path


def _extract_ora_code(message: str) -> str:
    """
    Extract the ORA-XXXXX code from an Oracle error message string.

    Returns ``'ORA-UNKNOWN'`` if no code is found.
    """
    import re
    match = re.search(r"ORA-\d+", message)
    return match.group(0) if match else "ORA-UNKNOWN"


def count_errors_in_log(error_dir: Path | str) -> int:
    """
    Count the number of error lines in the log file.

    Returns 0 if the log file does not exist.
    """
    log_path = Path(error_dir) / LOG_FILENAME
    if not log_path.exists():
        return 0
    with open(log_path, encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())
