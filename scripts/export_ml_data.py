#!/usr/bin/env python3
"""
export_ml_data.py — Export IPL ML training data to CSV files and push to GitHub.

Exports:
  ball_by_ball/real_over_snapshots.csv   — rich per-over ML features (44k+ rows)
  ball_by_ball/over_snapshots.csv        — legacy per-over snapshots
  ball_by_ball/match_outcomes.csv        — match outcome labels
  ball_by_ball/ball_log.csv              — ball-by-ball replay log
  series/series_matches.csv              — 2026 match results + phase runs
  series/series_batting.csv             — 2026 batting scorecards
  series/series_bowling.csv             — 2026 bowling scorecards
  series/player_innings.csv             — historical player innings
  series/bowler_innings.csv             — historical bowler innings

Run daily via systemd timer after IPL matches end (~11:30 PM IST / 18:00 UTC).
"""

from __future__ import annotations

import csv
import logging
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ml_exporter")

BOT_DIR = Path(__file__).resolve().parent.parent
DATA_REPO = Path("/home/support/workspace/ipl_ml_data")

# Static exports — ML training DBs and replay logs
STATIC_EXPORTS = [
    ("ml_training_v2.db",  "real_over_snapshots", "ball_by_ball/real_over_snapshots.csv"),
    ("ml_training.db",     "over_snapshots",       "ball_by_ball/over_snapshots.csv"),
    ("ml_training.db",     "match_outcomes",       "ball_by_ball/match_outcomes.csv"),
    ("match_replay.db",    "ball_log",             "ball_by_ball/ball_log.csv"),
    ("match_replay.db",    "scan_snapshots",       "ball_by_ball/scan_snapshots.csv"),
    ("match_replay.db",    "signal_log",           "ball_by_ball/signal_log.csv"),
]

# Historical stats DBs — auto-discovered by naming convention *_stats.db
# e.g. ipl_stats.db, psl_stats.db, bbl_stats.db
STATS_TABLES = ["matches", "player_innings", "bowler_innings"]

# Series DBs auto-discovered: series_<comp>_<year>.db
# Tables: series_matches, series_batting, series_bowling
SERIES_TABLES = ["series_matches", "series_batting", "series_bowling"]


def build_exports() -> list[tuple[str, str, str]]:
    """Build full export list dynamically, discovering all series and stats DBs."""
    exports = list(STATIC_EXPORTS)
    data_dir = BOT_DIR / "data"

    # Auto-discover series_<comp>_<year>.db files
    for db_file in sorted(data_dir.glob("series_*.db")):
        db_name = db_file.name
        # Parse competition and year from filename: series_ipl_2026.db
        parts = db_file.stem.split("_")  # ["series", "ipl", "2026"]
        if len(parts) < 3:
            continue
        comp = parts[1]   # ipl, psl, bbl, etc.
        year = parts[2]
        for table in SERIES_TABLES:
            csv_name = f"{table}.csv"
            out_path = f"series/{comp}/{year}/{csv_name}"
            exports.append((db_name, table, out_path))

    # Auto-discover *_stats.db files (ipl_stats.db, psl_stats.db, etc.)
    for db_file in sorted(data_dir.glob("*_stats.db")):
        db_name = db_file.name
        comp = db_file.stem.replace("_stats", "")  # ipl, psl
        for table in STATS_TABLES:
            out_path = f"series/{comp}/historical/{table}.csv"
            exports.append((db_name, table, out_path))

    return exports


def export_table(db_path: Path, table: str, out_path: Path) -> int:
    """Export a SQLite table to CSV. Returns number of rows written."""
    if not db_path.exists():
        logger.warning("DB not found, skipping: %s", db_path)
        return 0

    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            logger.info("Table %s is empty, writing header only", table)
            # Write header-only CSV so the file always exists
            schema = conn.execute(f"PRAGMA table_info({table})").fetchall()
            cols = [c[1] for c in schema]
            with open(out_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
            return 0

        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows([dict(r) for r in rows])

        logger.info("Exported %s.%s → %s (%d rows)", db_path.name, table, out_path.name, len(rows))
        return len(rows)
    except sqlite3.OperationalError as e:
        logger.error("Failed to export %s.%s: %s", db_path.name, table, e)
        return 0
    finally:
        conn.close()


def write_manifest(total_rows: dict[str, int]) -> None:
    """Write a manifest file with export metadata."""
    manifest_path = DATA_REPO / "MANIFEST.md"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# IPL ML Data — Export Manifest",
        "",
        f"Last updated: **{now}**",
        "",
        "| File | Rows |",
        "|------|------|",
    ]
    for csv_path, count in sorted(total_rows.items()):
        lines.append(f"| `{csv_path}` | {count:,} |")
    lines.append("")
    manifest_path.write_text("\n".join(lines))
    logger.info("Manifest written: %s", manifest_path)


def git_push(message: str) -> bool:
    """Stage all changes, commit, and push to origin/main."""
    env = os.environ.copy()
    env["GIT_SSH_COMMAND"] = (
        "ssh -i /home/support/.ssh/github_ml_data "
        "-o StrictHostKeyChecking=no "
        "-o BatchMode=yes"
    )

    def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
        return subprocess.run(cmd, cwd=DATA_REPO, env=env, capture_output=True, text=True, **kwargs)

    # Check if there are any changes
    status = run(["git", "status", "--porcelain"])
    if not status.stdout.strip():
        logger.info("No changes to commit.")
        return True

    run(["git", "config", "user.email", "ipl-bot@glitch-executor.dev"])
    run(["git", "config", "user.name", "IPL ML Bot"])
    run(["git", "add", "-A"])

    result = run(["git", "commit", "-m", message])
    if result.returncode != 0:
        logger.error("git commit failed: %s", result.stderr)
        return False
    logger.info("Committed: %s", message)

    result = run(["git", "push", "origin", "main"])
    if result.returncode != 0:
        logger.error("git push failed: %s", result.stderr)
        return False
    logger.info("Pushed to origin/main")
    return True


def main() -> int:
    logger.info("=== Cricket ML Data Export started ===")

    exports = build_exports()
    logger.info("Exporting %d tables across all competitions", len(exports))

    total_rows: dict[str, int] = {}
    for db_name, table, csv_rel in exports:
        db_path = BOT_DIR / "data" / db_name
        out_path = DATA_REPO / csv_rel
        count = export_table(db_path, table, out_path)
        total_rows[csv_rel] = count

    write_manifest(total_rows)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total = sum(total_rows.values())
    message = f"data: daily export {today} ({total:,} total rows)"

    success = git_push(message)
    if not success:
        logger.error("Push failed — data exported locally but not pushed")
        return 1

    logger.info("=== Export complete: %d total rows pushed ===", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
