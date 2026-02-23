"""
Abstract base class for all CSV/file source readers.

Every concrete reader (SF CSV, generic CSV, future Parquet) must implement
this interface.  The orchestrator and local sniff work exclusively against
``AbstractSource`` so the pipeline is source-agnostic.

Usage:
    with SFReader(path, config) as source:
        headers = source.headers()
        for row in source.rows():
            process(row)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator


class AbstractSource(ABC):
    """
    Interface for all ingestion source readers.

    Subclasses must implement ``open``, ``headers``, ``rows``, and ``close``.
    Context manager support (``__enter__`` / ``__exit__``) is provided by
    this base class and delegates to ``open`` / ``close``.
    """

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    @abstractmethod
    def open(self) -> None:
        """Open the source for reading.  Must be called before ``headers`` or ``rows``."""

    @abstractmethod
    def headers(self) -> list[str]:
        """
        Return the raw header strings from the source.

        Must be called after ``open``.  Returns the same list on every call
        (headers are read once and cached internally).
        """

    @abstractmethod
    def rows(self) -> Iterator[list[str]]:
        """
        Yield each data row as a list of raw strings.

        Must be called after ``open``.  The header row is NOT included.
        Each call rewinds to the first data row so the source can be
        iterated multiple times (e.g. sniff pass + generator pass).
        """

    @abstractmethod
    def close(self) -> None:
        """Release any open file handles or resources."""

    # ── context manager ──────────────────────────────────────────────────

    def __enter__(self) -> "AbstractSource":
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
        return None
