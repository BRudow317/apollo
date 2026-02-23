"""
Apollo — Salesforce to Oracle Ingestion CLI

Commands:
  ingest    Run the full pipeline for a CSV file.
  dry-run   Run Phases 1-4 (sniff + DDL preview) without executing DB writes.
  validate  Run Phases 1-2 only (CSV sniff, no Oracle connection needed).

Usage examples:
  python master.py ingest   --source data/incoming/contacts.csv --table CONTACTS --schema SALES
  python master.py dry-run  --source data/incoming/contacts.csv --table CONTACTS --schema SALES
  python master.py validate --source data/incoming/contacts.csv --table CONTACTS --schema SALES

Environment variables (loaded from .env if python-dotenv is available):
  DB_DSN       Oracle DSN string  (e.g. localhost:1521/XEPDB1)
  DB_USER      Oracle username
  DB_PASSWORD  Oracle password
  INCOMING_DIR, PROCESSED_DIR, ERROR_DIR, BATCH_SIZE, VARCHAR2_GROWTH_BUFFER
  DRY_RUN      Set to 'true' to force dry-run mode

Exit codes:
  0  Success (or dry-run / validate passed)
  1  Quarantine — file failed validation or sniff
  2  Configuration / argument error
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# ── optional dotenv support ───────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not required

from src.configs.config import PipelineConfig
from src.pipeline import PipelineResult, run, validate


# ---------------------------------------------------------------------------
# Logging setup
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
# Config builder from env + CLI overrides
# ---------------------------------------------------------------------------

def _build_config(args: argparse.Namespace) -> PipelineConfig:
    kwargs = {}
    if hasattr(args, "batch_size") and args.batch_size:
        kwargs["batch_size"] = args.batch_size
    if hasattr(args, "buffer") and args.buffer:
        kwargs["varchar2_growth_buffer"] = args.buffer
    if hasattr(args, "incoming_dir") and args.incoming_dir:
        kwargs["incoming_dir"] = Path(args.incoming_dir)
    if hasattr(args, "processed_dir") and args.processed_dir:
        kwargs["processed_dir"] = Path(args.processed_dir)
    if hasattr(args, "error_dir") and args.error_dir:
        kwargs["error_dir"] = Path(args.error_dir)
    return PipelineConfig(**kwargs)


# ---------------------------------------------------------------------------
# Oracle connection builder
# ---------------------------------------------------------------------------

def _build_connection(args: argparse.Namespace):
    """
    Build an Oracle connection from env vars or CLI args.

    Raises SystemExit(2) if required credentials are missing.
    """
    import oracledb  # hard import — user must have python-oracledb installed
    from src.discovery.oracle_client import connect

    dsn      = getattr(args, "dsn",      None) or os.environ.get("DB_DSN")
    user     = getattr(args, "user",     None) or os.environ.get("DB_USER")
    password = getattr(args, "password", None) or os.environ.get("DB_PASSWORD")

    missing = [name for name, val in [("DB_DSN", dsn), ("DB_USER", user), ("DB_PASSWORD", password)] if not val]
    if missing:
        print(f"ERROR: Missing required credentials: {', '.join(missing)}", file=sys.stderr)
        print("Set them via environment variables or --dsn / --user / --password flags.", file=sys.stderr)
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
    conn = _build_connection(args)
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
    config = PipelineConfig(
        **{k: getattr(config, k) for k in config.__dataclass_fields__
           if k != "dry_run"},
        dry_run=True,
    )
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
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="apollo",
        description="Salesforce CSV → Oracle ingestion pipeline",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    sub = parser.add_subparsers(dest="command", required=True)

    # Shared source/table/schema args
    def _add_source_args(p):
        p.add_argument("--source",  required=True, help="Path to the CSV file")
        p.add_argument("--table",   required=True, help="Target Oracle table name")
        p.add_argument("--schema",  required=True, help="Target Oracle schema/owner")

    def _add_db_args(p):
        p.add_argument("--dsn",      default=None, help="Oracle DSN (overrides DB_DSN env var)")
        p.add_argument("--user",     default=None, help="Oracle user (overrides DB_USER)")
        p.add_argument("--password", default=None, help="Oracle password (overrides DB_PASSWORD)")

    def _add_config_args(p):
        p.add_argument("--batch-size", type=int, default=None, dest="batch_size")
        p.add_argument("--buffer",     type=int, default=None, help="VARCHAR2 growth buffer")
        p.add_argument("--error-dir",  default=None, dest="error_dir")
        p.add_argument("--processed-dir", default=None, dest="processed_dir")

    # ingest
    p_ingest = sub.add_parser("ingest", help="Run full pipeline")
    _add_source_args(p_ingest)
    _add_db_args(p_ingest)
    _add_config_args(p_ingest)

    # dry-run
    p_dry = sub.add_parser("dry-run", help="Preview DDL without executing")
    _add_source_args(p_dry)
    _add_config_args(p_dry)

    # validate
    p_val = sub.add_parser("validate", help="Validate CSV structure only (no Oracle)")
    _add_source_args(p_val)
    _add_config_args(p_val)

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    _setup_logging(args.verbose)

    handlers = {
        "ingest":   _cmd_ingest,
        "dry-run":  _cmd_dry_run,
        "validate": _cmd_validate,
    }
    exit_code = handlers[args.command](args)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
