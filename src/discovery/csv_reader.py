"""
Generic CSV reader implementing ``AbstractSource``.

Handles:
- UTF-8 with or without BOM (``utf-8-sig``).
- Windows CRLF and Unix LF line endings (``newline=''``).
- Strict dialect — raises ``csv.Error`` on malformed rows.
- Seek-back so the source can be iterated more than once.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator

from src.configs.csv_dialect import DIALECT_NAME, register_dialect
from src.configs.exceptions import QuarantineError
from src.discovery.base import AbstractSource


class CSVReader(AbstractSource):
    """
    Generic CSV file reader.

    Args:
        path: Path to the CSV file.
    """

    def __init__(self, path: Path | str) -> None:
        super().__init__(path)
        self._file = None
        self._headers: list[str] | None = None

    # ── AbstractSource interface ─────────────────────────────────────────

    def open(self) -> None:
        """
        Open the file and read the header row.

        Raises:
            QuarantineError: If the file cannot be opened or has no headers.
        """
        register_dialect()
        try:
            self._file = open(  # noqa: WPS515
                self.path,
                encoding="utf-8-sig",
                newline="",
            )
        except OSError as e:
            raise QuarantineError(
                f"Cannot open {self.path}: {e}",
                source_path=str(self.path),
            ) from e

        reader = csv.reader(self._file, dialect=DIALECT_NAME)
        try:
            raw_headers = next(reader)
        except StopIteration:
            raise QuarantineError(
                f"CSV file is empty: {self.path}",
                source_path=str(self.path),
            )
        except csv.Error as e:
            raise QuarantineError(
                f"Malformed CSV header in {self.path}: {e}",
                source_path=str(self.path),
            ) from e

        self._headers = [h.strip() for h in raw_headers]

    def headers(self) -> list[str]:
        """Return the cached header list.  ``open()`` must be called first."""
        if self._headers is None:
            raise RuntimeError("CSVReader.open() must be called before headers().")
        return self._headers

    def rows(self) -> Iterator[list[str]]:
        """
        Yield each data row.  Rewinds to the first data row on each call.

        Raises:
            QuarantineError: If a malformed row is encountered (strict dialect).
        """
        if self._file is None:
            raise RuntimeError("CSVReader.open() must be called before rows().")

        # Rewind past the header row.
        self._file.seek(0)
        reader = csv.reader(self._file, dialect=DIALECT_NAME)
        next(reader)  # skip header

        try:
            for row in reader:
                yield row
        except csv.Error as e:
            raise QuarantineError(
                f"Malformed row in {self.path}: {e}",
                source_path=str(self.path),
            ) from e

    def close(self) -> None:
        """Close the underlying file handle."""
        if self._file is not None:
            self._file.close()
            self._file = None
