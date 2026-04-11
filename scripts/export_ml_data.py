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

# Map: (source_db, table_name) -> output csv path (relative to DATA_REPO)
EXPORTS = [
    ("ml_training_v2.db",     "real_over_snapshots",  "ball_by_ball/real_over_snapshots.csv"),
    ("ml_training.db",        "over_snapshots",        "ball_by_ball/over_snapshots.csv"),
    ("ml_training.db",        "match_outcomes",        "ball_by_ball/match_outcomes.csv"),
    ("match_replay.db",       "ball_log",              "ball_by_ball/ball_log.csv"),
    ("match_replay.db",       "scan_snapshots",        "ball_by_ball/scan_snapshots.csv"),
    ("match_replay.db",       "signal_log",            "ball_by_ball/signal_log.csv"),
    ("series_ipl_2026.db",    "series_matches",        "series/series_matches.csv"),
    ("series_ipl_2026.db",    "series_batting",        "series/series_batting.csv"),
    ("series_ipl_2026.db",    "series_bowling",        "series/series_bowling.csv"),
    ("ipl_stats.db",          "player_innings",        "series/player_innings.csv"),
    ("ipl_stats.db",          "bowler_innings",        "series/bowler_innings.csv"),
    ("ipl_stats.db",          "matches",               "series/historical_matches.csv"),
]


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
        "ssh -i /home/support/.ssh/github_ipl_bot "
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
    logger.info("=== IPL ML Data Export started ===")

    total_rows: dict[str, int] = {}
    for db_name, table, csv_rel in EXPORTS:
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
