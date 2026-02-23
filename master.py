#!/usr/bin/env python3
"""
master.py - Universal pipeline orchestrator
Python 3.6+, stdlib only, no external dependencies

Usage:
    python3 master.py --env dev --config /stage/scripts/config.dat --exec python3 myscript.py [-- extra args]

Arguments:
    --env       Environment to run (dev, sit, uat, etc.)
    --config    Path to .env file
    --exec      Program and script to run (optional label, everything unrecognized is the command)
    --          Separator: everything after this is passed through to the child as args

Example:
    python3 master.py --env dev --config Q:/.secrets/.env --exec python3 apollo.py -- \
    --source Q:/apollo/incoming/contacts.csv \
    --table CONTACTS \
    --schema SALES
    
"""

import sys
import os
import subprocess
import argparse
import threading



# Config parser
# Handles the legacy bash-style format:
#   user_dev_host='/path/to/db'
#   user_dev_username='myuser'
#   user_app_host=user_dev_host   <- indirect reference, resolved using env
def strip_quotes(val):
    """
    Strip matching outer quote pairs only.
    'myvalue'  -> myvalue
    "myvalue"  -> myvalue
    myvalue    -> myvalue
    'myvalue"  -> 'myvalue"  (mismatched, left alone)
    """
    if len(val) >= 2:
        if (val[0] == "'" and val[-1] == "'") or \
           (val[0] == '"' and val[-1] == '"'):
            return val[1:-1]
    return val


def parse_config(config_path, env):
    """
    Parse a bash-style key=value config file and resolve indirect references.

    Given env=dev, a variable like:
        user_app_host=user_dev_host
    is resolved to the actual value of user_dev_host.

    Also handles {env} placeholder style:
        user_app_host=user_{env}_host  -> looks up user_dev_host

    Returns a dict of all variables with indirect references resolved.
    """
    if not os.path.isfile(config_path):
        fatal("Config file not found: {}".format(config_path))

    raw = {}

    with open(config_path, "r") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()

            # skip blanks, comments, and shebangs
            if not line or line.startswith("#") or line.startswith("!"):
                continue

            # skip lines that don't look like assignments
            if "=" not in line:
                continue

            key, _, val = line.partition("=")
            key = key.strip()
            val = strip_quotes(val.strip())

            raw[key] = val

    # resolve indirect references using env
    # handles both:
    #   user_app_host=user_dev_host          (direct key reference)
    #   user_app_host=user_{env}_host        ({env} placeholder)
    resolved = {}
    for key, val in raw.items():
        candidate = val.replace("{env}", env)
        if candidate in raw:
            resolved[key] = raw[candidate]
        elif val in raw:
            resolved[key] = raw[val]
        else:
            resolved[key] = val

    return resolved


def build_child_env(config_vars):
    """
    Merge config variables into a copy of the current OS environment.
    Child processes inherit this enriched environment.
    """
    child_env = os.environ.copy()
    child_env.update(config_vars)
    return child_env



# Streaming subprocess runner
# Streams stdout and stderr live to console as the child produces output.
# Exits with the child's exit code on failure.
def run(cmd, child_env):
    """
    Run a command with live streaming stdout/stderr.
    Returns the exit code.
    """
    log("Running: {}".format(" ".join(cmd)))
    log("-" * 60)

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=child_env
        )

        def stream_pipe(pipe, out_stream):
            for line in iter(pipe.readline, b""):
                out_stream.write(line.decode("utf-8", errors="replace"))
                out_stream.flush()
            pipe.close()

        stdout_thread = threading.Thread(
            target=stream_pipe, args=(process.stdout, sys.stdout)
        )
        stderr_thread = threading.Thread(
            target=stream_pipe, args=(process.stderr, sys.stderr)
        )

        stdout_thread.start()
        stderr_thread.start()

        stdout_thread.join()
        stderr_thread.join()

        process.wait()
        return process.returncode

    except FileNotFoundError:
        fatal("Command not found: {}".format(cmd[0]))
    except PermissionError:
        fatal("Permission denied running: {}".format(cmd[0]))
    except Exception as e:
        fatal("Unexpected error running child process: {}".format(str(e)))



# Argument parsing
# Uses parse_known_args so master.py only claims --env and --config.
# Everything else lands in child_args automatically — no manual slicing,
# no maintained flags list, no fragile token scanning.
def parse_args(argv):
    """
    --env and --config are parsed by argparse.
    Everything unrecognized becomes the child command + passthrough args.
    -- is still accepted as an explicit separator if desired.
    --exec is optional: accepted and stripped if present, ignored otherwise.
    """
    parser = argparse.ArgumentParser(
        description="master.py - universal pipeline orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        "--env",
        required=False,
        help="Environment to run against (dev, sit, uat, prod, etc.)"
    )

    parser.add_argument(
        "--config",
        default="Q:/.secrets/.env",
        required=False,
        help="Path to env file"
    )

    # parse_known_args returns (known_args, list_of_unrecognized_tokens)
    # unrecognized tokens become the child command + any passthrough args
    args, child_args = parser.parse_known_args(argv)

    # strip leading -- separator if present
    if child_args and child_args[0] == "--":
        child_args = child_args[1:]

    # --exec is optional but accepted for readability, strip it if present
    if child_args and child_args[0] == "--exec":
        child_args = child_args[1:]

    if not child_args:
        parser.error(
            "No command provided. Example: master.py --env dev python3 app.py"
        )

    return args, child_args



# Logging helpers
def log(msg):
    print("[master] {}".format(msg), flush=True)

def fatal(msg):
    print("[master] FATAL: {}".format(msg), file=sys.stderr, flush=True)
    sys.exit(1)



# Entry point
def main():
    args, child_args = parse_args(sys.argv[1:])

    log("Environment : {}".format(args.env))
    log("Config      : {}".format(args.config))
    log("Exec        : {}".format(" ".join(child_args)))

    config_vars = parse_config(args.config, args.env)
    child_env = build_child_env(config_vars)

    log("Loaded {} config variables".format(len(config_vars)))

    exit_code = run(child_args, child_env)

    log("-" * 60)
    if exit_code == 0:
        log("Process completed successfully (exit 0)")
    else:
        log("Process exited with code {}".format(exit_code))

    sys.exit(exit_code)


if __name__ == "__main__":
    main()