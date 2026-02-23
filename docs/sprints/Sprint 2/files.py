"""
Safe file operations for the Apollo ingestion pipeline.

Handles moving files to the quarantine (error) folder on failure and to
the processed folder on success.  All operations are atomic where the OS
permits (same filesystem moves) and fail loudly on error rather than
silently swallowing exceptions.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from src.configs.exceptions import IngestionError


def quarantine_file(
    source_path: Path | str,
    error_dir: Path | str,
    reason: str = "",
) -> Path:
    """
    Move ``source_path`` to ``error_dir``.

    Creates ``error_dir`` if it does not exist.  If a file with the same
    name already exists in ``error_dir``, appends a numeric suffix to
    avoid clobbering it.

    Args:
        source_path: Path of the file to quarantine.
        error_dir:   Destination quarantine folder.
        reason:      Optional human-readable reason (logged, not written to disk here).

    Returns:
        The final destination path the file was moved to.

    Raises:
        IngestionError: If the move operation fails.
    """
    source = Path(source_path)
    dest_dir = Path(error_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = dest_dir / source.name
    # Avoid clobbering existing quarantined files.
    if dest.exists():
        stem = source.stem
        suffix = source.suffix
        counter = 1
        while dest.exists():
            dest = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    try:
        shutil.move(str(source), str(dest))
    except OSError as e:
        raise IngestionError(
            f"Failed to quarantine {source} → {dest}: {e}"
        ) from e

    return dest


def mark_processed(
    source_path: Path | str,
    processed_dir: Path | str,
) -> Path:
    """
    Move ``source_path`` to ``processed_dir`` after successful ingestion.

    Args:
        source_path:   Path of the successfully processed file.
        processed_dir: Destination folder for processed files.

    Returns:
        The final destination path.

    Raises:
        IngestionError: If the move operation fails.
    """
    source = Path(source_path)
    dest_dir = Path(processed_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = dest_dir / source.name
    if dest.exists():
        stem = source.stem
        suffix = source.suffix
        counter = 1
        while dest.exists():
            dest = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    try:
        shutil.move(str(source), str(dest))
    except OSError as e:
        raise IngestionError(
            f"Failed to mark {source} as processed → {dest}: {e}"
        ) from e

    return dest
