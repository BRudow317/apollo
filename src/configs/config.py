"""
Pipeline configuration.

All tuneable constants live here. Import from this module everywhere —
never hardcode buffer sizes, batch sizes, or Oracle limits inline.

Usage:
    from ingestor.core.config import PipelineConfig
    cfg = PipelineConfig()          # defaults
    cfg = PipelineConfig(batch_size=500)

Environment overrides (optional) can be loaded via .env / os.environ before
constructing the config object; this module does not load .env itself.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path



# Oracle hard limits (do not change unless Oracle version changes)
ORACLE_MAX_VARCHAR2_CHAR: int = 4000
"""Hard ceiling for VARCHAR2 with CHAR length semantics."""

ORACLE_MAX_IDENTIFIER_LEN_LEGACY: int = 30
"""Max identifier length for Oracle < 12.2 (pre-long-identifiers)."""

ORACLE_MAX_IDENTIFIER_LEN_EXTENDED: int = 128
"""Max identifier length for Oracle >= 12.2 with COMPATIBLE >= 12.2."""


@dataclass(slots=True)
class PipelineConfig:
    """
    Runtime configuration for the ingestion pipeline.

    Attributes:
        varchar2_growth_buffer: Characters added on top of observed max_char_len
            when sizing a VARCHAR2 column in CREATE TABLE or ALTER TABLE MODIFY.
            Value TBD pending ALTER MODIFY testing; default is conservative.
        batch_size: Number of rows per executemany call. Tune based on row width.
            Wide rows (many large VARCHAR2 cols) → lower this. Narrow rows → raise it.
        oracle_max_identifier_len: Set to 30 for legacy Oracle, 128 for extended.
        incoming_dir: Where raw CSVs are picked up.
        processed_dir: Where successfully imported CSVs are moved.
        error_dir: Quarantine folder for failed files/batches.
        log_config: Path to logging.yaml.
        dry_run: If True, generate DDL and SQL but make no DB calls and move no files.
    """

    varchar2_growth_buffer: int = field(
        default_factory=lambda: int(os.environ.get("VARCHAR2_GROWTH_BUFFER", "50"))
    )
    batch_size: int = field(
        default_factory=lambda: int(os.environ.get("BATCH_SIZE", "1000"))
    )
    oracle_max_identifier_len: int = field(
        default_factory=lambda: int(
            os.environ.get("ORACLE_MAX_IDENTIFIER_LEN", str(ORACLE_MAX_IDENTIFIER_LEN_LEGACY))
        )
    )
    incoming_dir: Path = field(
        default_factory=lambda: Path(os.environ.get("INCOMING_DIR", "data/incoming"))
    )
    processed_dir: Path = field(
        default_factory=lambda: Path(os.environ.get("PROCESSED_DIR", "data/processed"))
    )
    error_dir: Path = field(
        default_factory=lambda: Path(os.environ.get("ERROR_DIR", "data/error"))
    )
    log_config: Path = field(
        default_factory=lambda: Path(os.environ.get("LOG_CONFIG", "configs/logging.yaml"))
    )
    dry_run: bool = field(
        default_factory=lambda: os.environ.get("DRY_RUN", "false").lower() == "true"
    )

    def effective_max_varchar2(self, observed_char_len: int) -> int:
        """
        Return the VARCHAR2 size to use for a column with the given observed max
        char length, after applying the growth buffer and capping at the Oracle limit.

        Raises:
            ValueError: If observed_char_len already exceeds ORACLE_MAX_VARCHAR2_CHAR.
                        (Caller should have caught this earlier via SizeBreachError.)
        """
        if observed_char_len > ORACLE_MAX_VARCHAR2_CHAR:
            raise ValueError(
                f"observed_char_len {observed_char_len} already exceeds "
                f"ORACLE_MAX_VARCHAR2_CHAR {ORACLE_MAX_VARCHAR2_CHAR}"
            )
        return min(observed_char_len + self.varchar2_growth_buffer, ORACLE_MAX_VARCHAR2_CHAR)
