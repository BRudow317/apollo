"""
Oracle identifier sanitizer.

This module is the **sole SQL injection boundary** for column, table, and schema
names derived from CSV headers or user input.  Raw strings from those sources
must never appear in a SQL statement without passing through `sanitize_identifier`
first.

Rules applied (in order):
1. Strip leading/trailing whitespace.
2. Uppercase.
3. Replace any character that is not A-Z, 0-9, or _ with an underscore.
4. If the result starts with a digit, prefix with an underscore.
5. Collapse consecutive underscores to a single underscore.
6. Strip leading/trailing underscores produced by the above steps.
7. If the cleaned result is an Oracle reserved word, append ``_COL``.
8. Truncate to ``max_len`` characters (default: ``ORACLE_MAX_IDENTIFIER_LEN_LEGACY``).
9. If the result is empty after all steps, raise ``ValueError``.
"""

from __future__ import annotations

import re

from ingestor.core.config import ORACLE_MAX_IDENTIFIER_LEN_LEGACY

# ---------------------------------------------------------------------------
# Oracle reserved words (subset that most commonly appear as CSV headers)
# Extend as needed; this is not exhaustive but covers common collisions.
# ---------------------------------------------------------------------------
_RESERVED_WORDS: frozenset[str] = frozenset(
    {
        "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC",
        "AUDIT", "BETWEEN", "BY", "CHAR", "CHECK", "CLUSTER", "COLUMN",
        "COMMENT", "COMPRESS", "CONNECT", "CREATE", "CURRENT", "DATE",
        "DECIMAL", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP",
        "ELSE", "EXCLUSIVE", "EXISTS", "FILE", "FLOAT", "FOR", "FROM",
        "GRANT", "GROUP", "HAVING", "IDENTIFIED", "IMMEDIATE", "IN",
        "INCREMENT", "INDEX", "INITIAL", "INSERT", "INTEGER", "INTERSECT",
        "INTO", "IS", "LEVEL", "LIKE", "LOCK", "LONG", "MAXEXTENTS",
        "MINUS", "MLSLABEL", "MODE", "MODIFY", "NOAUDIT", "NOCOMPRESS",
        "NOT", "NOWAIT", "NULL", "NUMBER", "OF", "OFFLINE", "ON",
        "ONLINE", "OPTION", "OR", "ORDER", "PCTFREE", "PRIOR",
        "PRIVILEGES", "PUBLIC", "RAW", "RENAME", "RESOURCE", "REVOKE",
        "ROW", "ROWID", "ROWNUM", "ROWS", "SELECT", "SESSION", "SET",
        "SHARE", "SIZE", "SMALLINT", "START", "SUCCESSFUL", "SYNONYM",
        "SYSDATE", "TABLE", "THEN", "TO", "TRIGGER", "UID", "UNION",
        "UNIQUE", "UPDATE", "USER", "VALIDATE", "VALUES", "VARCHAR",
        "VARCHAR2", "VIEW", "WHENEVER", "WHERE", "WITH",
    }
)

_INVALID_CHARS_RE = re.compile(r"[^A-Z0-9_]")
_LEADING_DIGIT_RE = re.compile(r"^[0-9]")
_MULTI_UNDERSCORE_RE = re.compile(r"_{2,}")


def sanitize_identifier(
    raw: str,
    max_len: int = ORACLE_MAX_IDENTIFIER_LEN_LEGACY,
) -> str:
    """
    Convert an arbitrary string into a safe Oracle identifier.

    Args:
        raw: The raw string (e.g. a CSV header like ``"Last Name"``).
        max_len: Maximum identifier length.  Defaults to the legacy 30-char limit.
                 Pass ``ORACLE_MAX_IDENTIFIER_LEN_EXTENDED`` (128) for 12.2+ databases.

    Returns:
        A sanitized, uppercase Oracle-safe identifier string.

    Raises:
        ValueError: If ``raw`` is empty or reduces to an empty string after sanitization.
        ValueError: If ``max_len`` is less than 1.
    """
    if max_len < 1:
        raise ValueError(f"max_len must be >= 1, got {max_len}")

    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"Cannot sanitize empty or non-string identifier: {raw!r}")

    # Step 1: Strip + uppercase
    result = raw.strip().upper()

    # Step 2: Replace invalid chars with _
    result = _INVALID_CHARS_RE.sub("_", result)

    # Step 3: Collapse multiple underscores
    result = _MULTI_UNDERSCORE_RE.sub("_", result)

    # Step 4: Strip leading/trailing underscores (from substitution, not digit prefix)
    result = result.strip("_")

    # Step 5: Prefix leading digit — done AFTER stripping so the _ we add is intentional
    if _LEADING_DIGIT_RE.match(result):
        result = "_" + result

    if not result:
        raise ValueError(
            f"Identifier {raw!r} reduced to empty string after sanitization."
        )

    # Step 6: Reserved word handling
    if result in _RESERVED_WORDS:
        result = result + "_COL"

    # Step 7: Truncate — after reserved-word suffix to avoid cutting off the suffix
    # If adding _COL pushed us over, truncate the base and re-append _COL.
    if len(result) > max_len:
        if result.endswith("_COL"):
            base = result[: max_len - 4].rstrip("_")
            result = base + "_COL"
        else:
            result = result[:max_len]

    if not result:
        raise ValueError(
            f"Identifier {raw!r} is empty after truncation to max_len={max_len}."
        )

    return result


def is_reserved(name: str) -> bool:
    """Return True if ``name`` (uppercased) is an Oracle reserved word."""
    return name.upper() in _RESERVED_WORDS
