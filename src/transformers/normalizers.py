"""
Cell-level value normalizers for the Apollo ingestion pipeline.

Each normalizer is a **pure function** — it takes a raw string cell value
and an Oracle data type, and returns the appropriate Python object for
binding into an Oracle cursor.

Normalizer chain (applied in this order by ``normalize_cell``):
  1. Strip null bytes (``\\x00``)
  2. Normalize empty string / whitespace-only → ``None``
  3. Type-specific conversion:
       - NUMBER  → ``Decimal`` (preserves precision; handles comma-formatting)
       - DATE    → ``datetime.date``
       - TIMESTAMP → ``datetime.datetime``
       - VARCHAR2 → stripped ``str``

Fallback contract:
  - If a NUMBER cell cannot be parsed (e.g. ``"N/A"`` in a mixed column
    that somehow slipped through as NUMBER), return ``None`` rather than raise.
  - If a DATE/TIMESTAMP cell cannot be parsed, return the raw string rather
    than raise — the DB will surface the error at bind time, which is the
    correct place to catch it.

DATE / TIMESTAMP formats handled:
  - ``YYYY-MM-DD``                        → ``datetime.date``
  - ``YYYY-MM-DDTHH:MM:SS[.sss][Z]``     → ``datetime.datetime``
  - ``YYYY-MM-DD HH:MM:SS[.sss]``        → ``datetime.datetime``
  - ``YYYY-MM-DDTHH:MM:SS+HH:MM``        → ``datetime.datetime`` (tz stripped to naive)
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from src.models.models import OracleDataType

# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------
_NULL_BYTE_RE = re.compile(r"\x00")
_COMMA_RE = re.compile(r",")

_DATE_FMT = "%Y-%m-%d"
_TIMESTAMP_FMTS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
]
# Timezone offset suffix pattern — strip before parsing
_TZ_OFFSET_RE = re.compile(r"[+-]\d{2}:?\d{2}$")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize_cell(raw: str, data_type: OracleDataType) -> Any:
    """
    Normalize a single CSV cell value for Oracle binding.

    Args:
        raw:       Raw string from the CSV row.
        data_type: Oracle data type of the target column.

    Returns:
        - ``None``            for empty / null cells (all types)
        - ``Decimal``         for NUMBER
        - ``datetime.date``   for DATE
        - ``datetime.datetime`` for TIMESTAMP
        - ``str``             for VARCHAR2 (stripped, null-bytes removed)
        - ``str``             for UNKNOWN (pass-through stripped string)
    """
    # Step 1: strip null bytes
    value = _NULL_BYTE_RE.sub("", raw)

    # Step 2: empty / whitespace → None
    if not value.strip():
        return None

    # Step 3: type-specific conversion
    if data_type == "NUMBER":
        return _to_decimal(value)

    if data_type == "DATE":
        return _to_date(value)

    if data_type == "TIMESTAMP":
        return _to_datetime(value)

    # VARCHAR2 / UNKNOWN — return stripped string
    return value.strip()


def strip_null_bytes(value: str) -> str:
    """Remove all null bytes from a string."""
    return _NULL_BYTE_RE.sub("", value)


def is_empty(value: str) -> bool:
    """Return True if the cell should be treated as NULL."""
    return not _NULL_BYTE_RE.sub("", value).strip()


# ---------------------------------------------------------------------------
# Private converters
# ---------------------------------------------------------------------------

def _to_decimal(value: str) -> Decimal | None:
    """Convert a numeric string to ``Decimal``. Returns ``None`` on failure."""
    cleaned = _COMMA_RE.sub("", value.strip())
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _to_date(value: str) -> date | str:
    """
    Parse a date string to ``datetime.date``.
    Returns the original string if parsing fails (let Oracle surface the error).
    """
    stripped = value.strip()
    try:
        return datetime.strptime(stripped[:10], _DATE_FMT).date()
    except ValueError:
        return stripped


def _to_datetime(value: str) -> datetime | str:
    """
    Parse a timestamp string to ``datetime.datetime`` (timezone-naive).
    Returns the original string if parsing fails.
    """
    stripped = value.strip()

    # Strip timezone offset suffix before parsing
    cleaned = _TZ_OFFSET_RE.sub("", stripped)

    for fmt in _TIMESTAMP_FMTS:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue

    return stripped