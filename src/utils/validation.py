"""
Validation helpers for CSV structure integrity.

These functions are called during Phase 2 (local sniff) to catch structural
problems before any Oracle connection is made.

All functions raise the appropriate exception on failure rather than returning
a boolean — callers are expected to let exceptions propagate to the quarantine
handler in the orchestrator.
"""

from __future__ import annotations

from src.configs.exceptions import AlignmentError


def validate_row_alignment(
    row: list[str],
    expected_field_count: int,
    row_number: int,
    source_path: str | None = None,
) -> None:
    """
    Assert that a CSV row has exactly the expected number of fields.

    Args:
        row:                  The parsed row as a list of strings.
        expected_field_count: Number of fields in the header row.
        row_number:           1-based row number for error reporting.
        source_path:          Path of the CSV file being processed.

    Raises:
        AlignmentError: If ``len(row) != expected_field_count``.
    """
    actual = len(row)
    if actual != expected_field_count:
        raise AlignmentError(
            f"Row {row_number} has {actual} fields, expected {expected_field_count}.",
            source_path=source_path,
            row_number=row_number,
            expected=expected_field_count,
            got=actual,
        )


def validate_headers_not_empty(
    headers: list[str],
    source_path: str | None = None,
) -> None:
    """
    Assert that the header row is non-empty and contains no blank headers.

    Args:
        headers:     List of header strings from the CSV.
        source_path: Path of the CSV file being processed.

    Raises:
        AlignmentError: If the header list is empty or any header is blank.
    """
    if not headers:
        raise AlignmentError(
            "CSV file has no headers.",
            source_path=source_path,
            row_number=1,
            expected=1,
            got=0,
        )

    for i, h in enumerate(headers):
        if not h.strip():
            raise AlignmentError(
                f"Header at position {i} is blank.",
                source_path=source_path,
                row_number=1,
                expected=len(headers),
                got=len(headers),
            )
