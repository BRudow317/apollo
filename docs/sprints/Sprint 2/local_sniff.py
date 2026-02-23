"""
Phase 2: Local Discovery & Integrity (The Sniff).

Scans 100% of the CSV file before any Oracle connection is made:
1. Reads headers and builds sanitized ``ColumnMap`` objects.
2. Validates row alignment on every row.
3. Tracks ``max_char_len`` and ``max_byte_len`` per column.
4. Detects size breaches (> 4000 CHAR) immediately and raises.

Returns a fully populated ``TableMeta`` ready for Phase 3.

Type inference (NUMBER / DATE / TIMESTAMP) is handled in Phase 3
(``src/transformers/typing_infer.py``) — data_type is left as
``'UNKNOWN'`` after the sniff.
"""

from __future__ import annotations

from pathlib import Path

from src.configs.config import PipelineConfig, ORACLE_MAX_VARCHAR2_CHAR
from src.configs.exceptions import SizeBreachError, QuarantineError
from src.discovery.base import AbstractSource
from src.models.models import ColumnMap, TableMeta
from src.utils.identifiers import to_column_name
from src.utils.validation import validate_headers_not_empty, validate_row_alignment


def sniff(
    source: AbstractSource,
    table_name: str,
    schema_name: str,
    config: PipelineConfig,
) -> TableMeta:
    """
    Execute the full Phase 2 sniff against an open ``AbstractSource``.

    Args:
        source:      An already-opened ``AbstractSource`` instance.
        table_name:  Sanitized Oracle table name.
        schema_name: Sanitized Oracle schema name.
        config:      Pipeline configuration.

    Returns:
        ``TableMeta`` with all ``ColumnMap.length`` and
        ``ColumnMap.max_byte_len`` values populated.
        ``data_type`` is ``'UNKNOWN'`` — set in Sprint 3.

    Raises:
        QuarantineError:  On alignment failure or unreadable file.
        SizeBreachError:  If any column value exceeds 4000 CHAR.
    """
    source_path = str(source.path)
    raw_headers = source.headers()

    validate_headers_not_empty(raw_headers, source_path=source_path)

    # Build sanitized ColumnMap for each header.
    columns: dict[str, ColumnMap] = {}
    for raw_header in raw_headers:
        target_name = to_column_name(raw_header, config)
        col = ColumnMap(
            source_key=raw_header,
            target_name=target_name,
        )
        columns[raw_header] = col

    expected_field_count = len(raw_headers)

    # Full-file scan.
    for row_number, row in enumerate(source.rows(), start=2):  # row 1 is header
        validate_row_alignment(
            row,
            expected_field_count=expected_field_count,
            row_number=row_number,
            source_path=source_path,
        )

        for raw_header, cell in zip(raw_headers, row):
            col = columns[raw_header]
            char_len = len(cell)
            byte_len = len(cell.encode("utf-8"))

            if char_len > ORACLE_MAX_VARCHAR2_CHAR:
                raise SizeBreachError(
                    f"Column '{raw_header}' contains a value of {char_len} characters "
                    f"which exceeds the Oracle VARCHAR2 limit of {ORACLE_MAX_VARCHAR2_CHAR}.",
                    source_path=source_path,
                    column_name=col.target_name,
                    char_length=char_len,
                    limit=ORACLE_MAX_VARCHAR2_CHAR,
                )

            if char_len > col.length:
                col.length = char_len
            if byte_len > col.max_byte_len:
                col.max_byte_len = byte_len

    return TableMeta(
        table_name=table_name,
        schema_name=schema_name,
        columns=columns,
    )
