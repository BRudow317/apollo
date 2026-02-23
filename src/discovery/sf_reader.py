"""
Salesforce CSV reader implementing ``AbstractSource``.

Salesforce CSV exports have a few quirks on top of standard CSV:
- Always UTF-8 with BOM.
- Empty fields are exported as ``""`` (quoted empty string) rather than
  a bare empty field — the strict dialect handles this correctly.
- Boolean fields are exported as ``"true"``/``"false"`` (lowercase).
- The final row is sometimes a blank row; this reader skips it.
- Date format: ``YYYY-MM-DD``.
- Datetime format: ``YYYY-MM-DDTHH:MM:SS.000Z``.

Everything else delegates to ``CSVReader``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

from src.discovery.csv_reader import CSVReader


class SFReader(CSVReader):
    """
    Salesforce-flavoured CSV reader.

    Extends ``CSVReader`` with Salesforce-specific row post-processing:
    - Skips blank trailing rows (Salesforce sometimes appends one).

    Args:
        path: Path to the Salesforce CSV export file.
    """

    def __init__(self, path: Path | str) -> None:
        super().__init__(path)

    def rows(self) -> Iterator[list[str]]:
        """
        Yield each data row, skipping blank trailing rows.

        Inherits malformed-row quarantine behaviour from ``CSVReader``.
        """
        for row in super().rows():
            # Skip rows that are entirely empty (Salesforce trailing blank).
            if any(cell.strip() for cell in row):
                yield row
