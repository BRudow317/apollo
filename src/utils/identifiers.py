"""
Identifier helpers for the ingestion pipeline.

Thin wrappers around ``sanitizer.sanitize_identifier`` that apply context-specific
defaults (e.g. the correct max_len for the current Oracle version) and make
call sites more readable.

Usage:
    from ingestor.utils.identifiers import to_column_name, to_table_name

    col   = to_column_name("Last Name", cfg)          # → "LAST_NAME"
    table = to_table_name("My Report (Q3)", cfg)      # → "MY_REPORT_Q3"
"""

from __future__ import annotations

from src.configs.config import PipelineConfig
from src.utils.sanitizer import sanitize_identifier


def to_column_name(raw: str, config: PipelineConfig) -> str:
    """
    Sanitize a raw CSV header into an Oracle column name.

    Args:
        raw:    Raw CSV header string.
        config: Pipeline configuration (supplies ``oracle_max_identifier_len``).

    Returns:
        Sanitized, uppercase, Oracle-safe column name.

    Raises:
        ValueError: If ``raw`` is empty or unsanitizable.
    """
    return sanitize_identifier(raw, max_len=config.oracle_max_identifier_len)


def to_table_name(raw: str, config: PipelineConfig) -> str:
    """
    Sanitize a raw string into an Oracle table name.

    Args:
        raw:    Raw table name (e.g. from CLI argument or filename stem).
        config: Pipeline configuration (supplies ``oracle_max_identifier_len``).

    Returns:
        Sanitized, uppercase, Oracle-safe table name.

    Raises:
        ValueError: If ``raw`` is empty or unsanitizable.
    """
    return sanitize_identifier(raw, max_len=config.oracle_max_identifier_len)


def to_schema_name(raw: str, config: PipelineConfig) -> str:
    """
    Sanitize a raw string into an Oracle schema/owner name.

    Args:
        raw:    Raw schema name.
        config: Pipeline configuration.

    Returns:
        Sanitized, uppercase, Oracle-safe schema name.

    Raises:
        ValueError: If ``raw`` is empty or unsanitizable.
    """
    return sanitize_identifier(raw, max_len=config.oracle_max_identifier_len)
