"""
CSV dialect configuration for the Apollo ingestion pipeline.

Registers a strict dialect (``apollo_strict``) that:
- Raises on malformed rows rather than silently skipping them.
- Uses standard comma delimiter and double-quote quoting.
- Handles Windows-style CRLF line endings.

Usage:
    import csv
    from src.configs.csv_dialect import register_dialect, DIALECT_NAME

    register_dialect()
    reader = csv.reader(f, dialect=DIALECT_NAME)

BOM Handling:
    Open files with ``encoding='utf-8-sig'`` to strip the UTF-8 BOM
    (\\xef\\xbb\\xbf) that Salesforce sometimes prepends.  The dialect
    itself does not handle this — it is an encoding concern.

    with open(path, encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f, dialect=DIALECT_NAME)
"""

from __future__ import annotations

import csv

DIALECT_NAME: str = "apollo_strict"


class ApolloStrictDialect(csv.excel):
    """
    Strict CSV dialect for Apollo ingestion.

    Inherits from ``csv.excel`` (comma-delimited, double-quote) and
    enables strict mode so malformed rows raise ``csv.Error`` immediately
    rather than being silently accepted.
    """

    strict: bool = True
    skipinitialspace: bool = True


def register_dialect() -> None:
    """
    Register the ``apollo_strict`` dialect with the ``csv`` module.

    Safe to call multiple times — re-registration is a no-op if the
    dialect is already registered.
    """
    existing = csv.list_dialects()
    if DIALECT_NAME not in existing:
        csv.register_dialect(DIALECT_NAME, ApolloStrictDialect)


def get_dialect() -> type[csv.Dialect]:
    """Return the dialect class without registering it."""
    return ApolloStrictDialect
