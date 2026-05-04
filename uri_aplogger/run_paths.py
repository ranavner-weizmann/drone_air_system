"""
Helpers for resolving per-run output directories.

Output layout:
    output/<run_timestamp>/
        csv/   - sensor data files and spectro summary/full
        log/   - process logs and per-sensor logs

Coordination across processes uses the RUN_DIR environment variable, which
runall.py sets to the absolute run folder before spawning subprocesses.
When RUN_DIR is not set (e.g. running a sensor script standalone), a fresh
run folder is created based on the current timestamp.
"""

import os
from datetime import datetime
from pathlib import Path


def get_run_dir() -> Path:
    """Return the active run directory, creating one if needed.

    Honors the RUN_DIR env var. If unset, creates output/<timestamp>/ relative
    to the current working directory and exports RUN_DIR for any children.
    """
    env_dir = os.environ.get("RUN_DIR")
    if env_dir:
        run_dir = Path(env_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = Path("output") / timestamp
        os.environ["RUN_DIR"] = str(run_dir.resolve())

    run_dir = Path(run_dir)
    (run_dir / "csv").mkdir(parents=True, exist_ok=True)
    (run_dir / "log").mkdir(parents=True, exist_ok=True)
    return run_dir


def get_csv_dir() -> Path:
    return get_run_dir() / "csv"


def get_log_dir() -> Path:
    return get_run_dir() / "log"
