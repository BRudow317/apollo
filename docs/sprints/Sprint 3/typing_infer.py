"""
Phase 2 (extended): Type inference for Oracle column types.

Inference rules — applied in this order to every non-null cell value:

  1. NUMBER (integer)  — matches r'^-?\\d+$'
  2. NUMBER (decimal)  — matches r'^-?\\d{1,3}(,\\d{3})*(\\.\\d+)?$'
                         or r'^-?\\d+\\.\\d+$' (plain decimal, no commas)
  3. TIMESTAMP         — matches ISO-8601 with time component:
                         YYYY-MM-DDTHH:MM:SS[.sss][Z]
  4. DATE              — matches YYYY-MM-DD only
  5. VARCHAR2          — fallback; also used when a column has mixed types
                         (e.g. 99% numeric + one "N/A")

Ambiguous formats (MM/DD/YYYY, DD-MON-YYYY, etc.) intentionally fall
back to VARCHAR2 — the pipeline never guesses locale-dependent date
formats.

Column-level resolution
-----------------------
Each cell is inferred independently.  The column type is the *consensus*
of all non-null cells:
  - If every non-null cell agrees on one type → that type wins.
  - Any disagreement → VARCHAR2.
  - All cells null/empty → VARCHAR2 (safest fallback).

Precision/scale for NUMBER columns
-----------------------------------
``precision`` = max total significant digits seen across all rows.
``scale``      = max digits after the decimal point seen.
Both are stored on ``ColumnMap`` for use in Phase 3 DDL generation.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

from src.models.models import ColumnMap, TableMeta, OracleDataType


# Compiled patterns

_INT_RE = re.compile(r"^-?\d+$")
_DECIMAL_COMMA_RE = re.compile(r"^-?\d{1,3}(,\d{3})*(\.\d+)?$")
_DECIMAL_PLAIN_RE = re.compile(r"^-?\d+\.\d+$")
_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$"
)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")



# Cell-level inference


def infer_cell_type(value: str) -> OracleDataType:
    """
    Infer the Oracle type for a single cell value.

    Args:
        value: Raw string cell value.  Empty string and whitespace-only
               strings return ``'UNKNOWN'`` (treated as null for type
               consensus — does not influence column type).

    Returns:
        One of ``'NUMBER'``, ``'DATE'``, ``'TIMESTAMP'``, ``'VARCHAR2'``,
        or ``'UNKNOWN'`` for null/empty cells.
    """
    stripped = value.strip()

    if not stripped:
        return "UNKNOWN"  # null/empty — excluded from consensus

    if _INT_RE.match(stripped) or _DECIMAL_COMMA_RE.match(stripped) or _DECIMAL_PLAIN_RE.match(stripped):
        return "NUMBER"

    if _TIMESTAMP_RE.match(stripped):
        return "TIMESTAMP"

    if _DATE_RE.match(stripped):
        return "DATE"

    return "VARCHAR2"



# Precision / scale extraction


def _extract_precision_scale(value: str) -> tuple[int, int]:
    """
    Return ``(precision, scale)`` for a numeric string.

    Strips commas before parsing.  Returns ``(0, 0)`` if parsing fails.
    """
    cleaned = value.strip().replace(",", "")
    try:
        d = Decimal(cleaned)
    except InvalidOperation:
        return 0, 0

    sign, digits, exponent = d.as_tuple()
    num_digits = len(digits)

    if isinstance(exponent, int) and exponent < 0:
        scale = -exponent
        precision = max(num_digits, scale)
    else:
        scale = 0
        precision = num_digits

    return precision, scale



# Column-level inference


def infer_column_type(values: list[str]) -> tuple[OracleDataType, int | None, int | None]:
    """
    Determine the Oracle type for a column from all its observed values.

    Args:
        values: All raw cell strings for this column across every row.

    Returns:
        Tuple of ``(data_type, precision, scale)`` where ``precision``
        and ``scale`` are ``None`` for non-NUMBER types.

    Rules:
        - Empty / null cells (``UNKNOWN``) are excluded from consensus.
        - Any disagreement between non-null cells → ``VARCHAR2``.
        - All cells null → ``VARCHAR2``.
        - Mixed NUMBER cells (some int, some decimal) → ``NUMBER``
          (they agree on the broader type; precision/scale resolved).
    """
    cell_types: list[OracleDataType] = []
    max_precision = 0
    max_scale = 0

    for v in values:
        t = infer_cell_type(v)
        if t == "UNKNOWN":
            continue  # skip nulls
        cell_types.append(t)

        if t == "NUMBER":
            p, s = _extract_precision_scale(v)
            if p > max_precision:
                max_precision = p
            if s > max_scale:
                max_scale = s

    if not cell_types:
        return "VARCHAR2", None, None

    unique_types = set(cell_types)

    if unique_types == {"NUMBER"}:
        return "NUMBER", max_precision or None, max_scale or None

    if unique_types in ({"DATE"}, {"TIMESTAMP"}, {"DATE", "TIMESTAMP"}):
        # If any cell has a time component, promote entire column to TIMESTAMP.
        resolved: OracleDataType = "TIMESTAMP" if "TIMESTAMP" in unique_types else "DATE"
        return resolved, None, None

    # Any other mixture → VARCHAR2
    return "VARCHAR2", None, None



# TableMeta update


def apply_type_inference(
    meta: TableMeta,
    column_values: dict[str, list[str]],
) -> None:
    """
    Infer and set ``data_type``, ``precision``, and ``scale`` on every
    ``ColumnMap`` in ``meta`` in-place.

    Args:
        meta:           The ``TableMeta`` whose columns will be updated.
        column_values:  Dict mapping ``source_key`` → list of all raw cell
                        strings for that column (one entry per data row).

    Notes:
        - Mutates ``meta.columns`` in-place.
        - Does NOT invalidate ``meta._insert_sql`` — type changes don't
          affect the SQL template (bind names are unchanged).
        - Columns present in ``meta`` but absent from ``column_values``
          (e.g. header-only file) are set to ``VARCHAR2``.
    """
    for source_key, col in meta.columns.items():
        values = column_values.get(source_key, [])
        data_type, precision, scale = infer_column_type(values)
        col.data_type = data_type
        col.precision = precision
        col.scale = scale