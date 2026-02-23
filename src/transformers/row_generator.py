"""
Phase 5: The Clean Stream (The Generator).

Streams rows from an ``AbstractSource`` through the normalizer chain and
yields one ``dict[oracle_name, value]`` per row, ready for
``cursor.executemany()``.

Key properties:
  - **Lazy** — only one row is in memory at a time.
  - **Named-bind output** — dict keys are ``oracle_name`` values from
    ``TableMeta.columns``, matching the ``insert_sql`` bind placeholders.
  - **Rewindable** — calling ``generate_rows`` a second time produces a
    fresh iterator from the beginning (relies on ``AbstractSource.rows()``
    rewinding on each call).
  - **Non-raising** — normalizer failures fall back to ``None`` rather than
    stopping the stream; true errors surface at executemany bind time.

Usage::

    for row_dict in generate_rows(source, meta):
        # row_dict == {"FIRST_NAME": "Alice", "AMOUNT": Decimal("100.00"), ...}
        pass

    # Or pass directly to executemany:
    cursor.executemany(meta.insert_sql, generate_rows(source, meta), batcherrors=True)
"""

from __future__ import annotations

from typing import Iterator

from src.discovery.base import AbstractSource
from src.models.models import TableMeta
from src.transformers.normalizers import normalize_cell


def generate_rows(
    source: AbstractSource,
    meta: TableMeta,
) -> Iterator[dict]:
    """
    Stream rows from ``source`` as named-bind dicts.

    Args:
        source: An already-opened ``AbstractSource``.  ``rows()`` is called
                once per invocation of this generator — the source rewinds
                itself on each call to ``rows()``.
        meta:   ``TableMeta`` with fully-populated ``ColumnMap`` objects
                (``oracle_name`` and ``data_type`` must be set — i.e.
                after Phase 3 / 4 have run).

    Yields:
        One ``dict`` per data row.  Keys are ``oracle_name`` strings;
        values are normalized Python objects ready for Oracle binding.

    Notes:
        - Columns present in ``meta`` but absent from the CSV row (should
          not happen after Phase 2 alignment) are bound as ``None``.
        - Row alignment is assumed to be valid (Phase 2 already checked).
          This generator does not re-validate alignment.
    """
    raw_headers = source.headers()

    # Build a stable list of (source_key, oracle_name, data_type) triples
    # in the same order as meta.columns (which matches insert_sql column order).
    col_info = [
        (col.source_key, col.oracle_name, col.data_type)
        for col in meta.columns.values()
    ]

    for raw_row in source.rows():
        # Map raw headers → cell values for this row
        row_by_source_key: dict[str, str] = dict(zip(raw_headers, raw_row))

        yield {
            oracle_name: normalize_cell(
                row_by_source_key.get(source_key, ""),
                data_type,
            )
            for source_key, oracle_name, data_type in col_info
        }