"""
Apollo — Salesforce to Oracle Ingestion CLI

Intended to be launched by master.py, which injects environment variables
from config.dat before this process starts:

    python master.py --env dev --exec python apollo.py ingest \\
        --source incoming/contacts.csv --table CONTACTS --schema SALES

Environment variables read (all injected by master.py from config.dat):
    DB_DSN        Oracle DSN string  (e.g. localhost:1521/XEPDB1)
    DB_USER       Oracle username
    DB_PASSWORD   Oracle password
    INCOMING_DIR  Optional: override default incoming directory
    PROCESSED_DIR Optional: override default processed directory
    ERROR_DIR     Optional: override default error directory
    BATCH_SIZE    Optional: override default batch size
    VARCHAR2_GROWTH_BUFFER  Optional: override default VARCHAR2 buffer

Commands:
    ingest    Full pipeline — sniff, DDL, load, commit.
    dry-run   Sniff + DDL preview only. No DB writes, no file moves.
    validate  Sniff only. No Oracle connection required.

Usage examples:
    python master.py --env dev --exec python apollo.py ingest \\
        --source data/contacts.csv --table CONTACTS --schema SALES

    python apollo.py dry-run  --source data/contacts.csv --table CONTACTS --schema SALES
    python apollo.py validate --source data/contacts.csv --table CONTACTS --schema SALES

Exit codes:
    0  Success (or dry-run / validate passed)
    1  Quarantine — file failed validation or sniff
    2  Configuration / argument error
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import os
import sys
from pathlib import Path

# ── optional dotenv (dev convenience; not needed when master.py injects env) ──
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from src.configs.config import PipelineConfig
from src.pipeline import PipelineResult, run, validate


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        level=level,
        stream=sys.stdout,
    )


# ---------------------------------------------------------------------------
# Config — env vars (from master.py) + optional CLI overrides
# ---------------------------------------------------------------------------

def _build_config(args: argparse.Namespace) -> PipelineConfig:
    """
    Priority order for each setting:
      1. CLI flag (--batch-size, --error-dir, etc.)
      2. Environment variable injected by master.py
      3. PipelineConfig default
    """
    kwargs: dict = {}

    def _int(flag_val, env_key: str) -> int | None:
        if flag_val is not None:
            return flag_val
        raw = os.environ.get(env_key)
        return int(raw) if raw else None

    def _path(flag_val, env_key: str) -> Path | None:
        val = flag_val or os.environ.get(env_key)
        return Path(val) if val else None

    batch_size = _int(getattr(args, "batch_size", None), "BATCH_SIZE")
    buffer     = _int(getattr(args, "buffer",     None), "VARCHAR2_GROWTH_BUFFER")
    incoming   = _path(getattr(args, "incoming_dir",  None), "INCOMING_DIR")
    processed  = _path(getattr(args, "processed_dir", None), "PROCESSED_DIR")
    error_dir  = _path(getattr(args, "error_dir",     None), "ERROR_DIR")

    if batch_size: kwargs["batch_size"]             = batch_size
    if buffer:     kwargs["varchar2_growth_buffer"] = buffer
    if incoming:   kwargs["incoming_dir"]            = incoming
    if processed:  kwargs["processed_dir"]           = processed
    if error_dir:  kwargs["error_dir"]               = error_dir

    return PipelineConfig(**kwargs)


# ---------------------------------------------------------------------------
# Oracle connection — credentials come from env vars set by master.py
# ---------------------------------------------------------------------------

def _build_connection():
    """
    Open an Oracle connection from DB_DSN / DB_USER / DB_PASSWORD env vars.
    These are injected by master.py from config.dat.
    Exits 2 if any are missing.
    """
    from src.discovery.oracle_client import connect

    dsn      = os.environ.get("DB_DSN")
    user     = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")

    missing = [k for k, v in [("DB_DSN", dsn), ("DB_USER", user), ("DB_PASSWORD", password)] if not v]
    if missing:
        print(
            f"ERROR: Missing required environment variable(s): {', '.join(missing)}\n"
            f"These should be injected by master.py from config.dat.\n"
            f"Add to config.dat:\n"
            f"  DB_DSN='localhost:1521/XEPDB1'\n"
            f"  DB_USER='myuser'\n"
            f"  DB_PASSWORD='mypassword'",
            file=sys.stderr,
        )
        sys.exit(2)

    return connect(dsn=dsn, user=user, password=password)


# ---------------------------------------------------------------------------
# Result printer
# ---------------------------------------------------------------------------

def _print_result(result: PipelineResult) -> None:
    if result.dry_run:
        print("\n── Dry-run complete ──────────────────────────────────")
        if result.ddl_preview:
            print("DDL that would be executed:")
            for stmt in result.ddl_preview:
                print(stmt)
                print()
        else:
            print("(No DDL required — table already up to date)")
        if result.error:
            print(f"Warning: {result.error}")
        return

    if result.quarantined:
        print(f"\n✗ QUARANTINED: {result.source_path.name}")
        print(f"  Reason : {result.error}")
        print(f"  Moved  : {result.final_path}")
        return

    print(f"\n✓ SUCCESS: {result.source_path.name}")
    if result.discovery:
        d = result.discovery
        print(f"  Scenario : {d.scenario}")
        if d.new_columns:
            print(f"  Added    : {', '.join(d.new_columns)}")
        if d.modified_columns:
            print(f"  Resized  : {', '.join(d.modified_columns)}")
    if result.batch:
        b = result.batch
        if b.error_count:
            print(f"  Errors   : {b.error_count} row(s) failed — see {b.error_log_path}")
        else:
            print("  Rows     : all loaded successfully")
    if result.final_path:
        print(f"  Moved    : {result.final_path}")


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def _cmd_ingest(args: argparse.Namespace) -> int:
    config = _build_config(args)
    conn = _build_connection()
    try:
        result = run(
            source_path=args.source,
            table_name=args.table,
            schema_name=args.schema,
            connection=conn,
            config=config,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass
    _print_result(result)
    return 1 if result.quarantined else 0


def _cmd_dry_run(args: argparse.Namespace) -> int:
    config = _build_config(args)
    config = dataclasses.replace(config, dry_run=True)
    result = run(
        source_path=args.source,
        table_name=args.table,
        schema_name=args.schema,
        connection=None,
        config=config,
    )
    _print_result(result)
    return 1 if result.quarantined else 0


def _cmd_validate(args: argparse.Namespace) -> int:
    config = _build_config(args)
    result = validate(
        source_path=args.source,
        table_name=args.table,
        schema_name=args.schema,
        config=config,
    )
    if result.success:
        print(f"✓ {args.source} — valid")
    else:
        print(f"✗ {args.source} — {result.error}", file=sys.stderr)
    return 1 if result.quarantined else 0


# ---------------------------------------------------------------------------
# Argument parser (importable for tests)
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="apollo",
        description="Salesforce CSV → Oracle ingestion pipeline",
        epilog=(
            "Credentials (DB_DSN, DB_USER, DB_PASSWORD) are read from environment\n"
            "variables — inject them via master.py or a .env file."
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true")

    sub = parser.add_subparsers(dest="command", required=True)

    def _source_args(p):
        p.add_argument("--source",  required=True)
        p.add_argument("--table",   required=True)
        p.add_argument("--schema",  required=True)

    def _config_args(p):
        p.add_argument("--batch-size",    type=int, default=None, dest="batch_size")
        p.add_argument("--buffer",        type=int, default=None)
        p.add_argument("--error-dir",     default=None, dest="error_dir")
        p.add_argument("--processed-dir", default=None, dest="processed_dir")

    p_ingest = sub.add_parser("ingest",   help="Run full pipeline")
    _source_args(p_ingest); _config_args(p_ingest)

    p_dry = sub.add_parser("dry-run",  help="Preview DDL without executing")
    _source_args(p_dry); _config_args(p_dry)

    p_val = sub.add_parser("validate", help="Validate CSV only (no Oracle)")
    _source_args(p_val); _config_args(p_val)

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    _setup_logging(args.verbose)
    handlers = {"ingest": _cmd_ingest, "dry-run": _cmd_dry_run, "validate": _cmd_validate}
    sys.exit(handlers[args.command](args))


if __name__ == "__main__":
    main()