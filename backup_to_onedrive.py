#!/usr/bin/env python3
"""
Incremental backup of uri_aplogger/output/ to OneDrive via rclone.

Usage:
    python3 backup_to_onedrive.py            # back up the entire output/ tree
    python3 backup_to_onedrive.py latest     # back up only the most recent run folder

The run folders are named YYYYMMDD_HHMMSS (created by uri_aplogger/runall.py).
'latest' picks the lexicographically-greatest such folder, which is also the
chronologically newest given the fixed-width timestamp format.

Setup (one-time):
    1. rclone is already installed.
    2. Run `rclone config` and create a remote of type 'onedrive' named exactly
       what RCLONE_REMOTE is set to below. Follow the prompts to authenticate.
    3. The destination path on OneDrive is ONEDRIVE_DEST; rclone creates it
       on first upload.
"""

import re
import shutil
import subprocess
import sys
from pathlib import Path

RCLONE_REMOTE = "onedrive"
ONEDRIVE_DEST = "drone_air_system_backups/uri_aplogger_output"

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR / "uri_aplogger" / "output"
RUN_DIR_PATTERN = re.compile(r"^\d{8}_\d{6}$")


def find_latest_run_dir(output_dir: Path) -> Path:
    candidates = [
        p for p in output_dir.iterdir()
        if p.is_dir() and RUN_DIR_PATTERN.match(p.name)
    ]
    if not candidates:
        raise SystemExit(
            f"No run directories matching YYYYMMDD_HHMMSS found in {output_dir}"
        )
    return max(candidates, key=lambda p: p.name)


def rclone_copy(src: Path, dest_subpath: str) -> int:
    dest = f"{RCLONE_REMOTE}:{ONEDRIVE_DEST}"
    if dest_subpath:
        dest = f"{dest}/{dest_subpath}"
    cmd = [
        "rclone", "copy",
        str(src),
        dest,
        "--progress",
        "--transfers", "4",
        "--checkers", "8",
    ]
    print("$ " + " ".join(cmd), flush=True)
    return subprocess.call(cmd)


def main() -> int:
    if shutil.which("rclone") is None:
        print("error: rclone is not installed or not on PATH.", file=sys.stderr)
        return 2

    if not OUTPUT_DIR.is_dir():
        print(f"error: output directory not found: {OUTPUT_DIR}", file=sys.stderr)
        return 2

    args = sys.argv[1:]
    if not args:
        return rclone_copy(OUTPUT_DIR, "")
    if args == ["latest"]:
        run_dir = find_latest_run_dir(OUTPUT_DIR)
        print(f"Latest run: {run_dir.name}")
        return rclone_copy(run_dir, run_dir.name)

    print(f"usage: {sys.argv[0]} [latest]", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
