"""
ML Training Data Collector.

During every live match, saves a snapshot at the end of each over:
  features  = match state at that moment (score, wickets, RR, venue, phase, ...)
  labels    = what actually happened after (filled in at match end)

This builds the training dataset for replacing the hardcoded base rates
with a real XGBoost model that learns from actual IPL/PSL data.

Schema: ml_training.db
  table: over_snapshots  — one row per over per innings
  table: match_outcomes  — final result of each match (labels)
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger("ipl_spotter.ml_collector")

_SNAPSHOTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS over_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at    TEXT    NOT NULL,

    -- Match context
    match_id        INTEGER NOT NULL,
    competition     TEXT,
    venue           TEXT,
    home_team       TEXT,
    away_team       TEXT,
    innings         INTEGER,
    toss_winner     TEXT,
    toss_decision   TEXT,       -- 'bat' or 'field'

    -- State at END of this over
    over_num        INTEGER NOT NULL,   -- 1-indexed
    score           INTEGER,
    wickets         INTEGER,
    run_rate        REAL,               -- runs / overs so far
    pp_runs         INTEGER,            -- powerplay total (if complete)
    last_over_runs  INTEGER,            -- runs in just this over

    -- Phase indicators
    phase           TEXT,               -- 'powerplay' | 'middle' | 'death'

    -- Live player context (from Sportmonks batting/bowling cards)
    striker_sr      REAL,               -- current striker's strike rate this innings
    striker_runs    INTEGER,
    bowler_econ     REAL,               -- current bowler's economy this innings
    bowler_wickets  INTEGER,

    -- Labels — filled in at match end by _finalise_match()
    actual_innings_total    INTEGER,    -- final score this innings
    actual_runs_from_here   INTEGER,    -- runs scored from this over to end
    actual_pp_total         INTEGER,    -- powerplay total (1-6 overs)
    actual_7_15_total       INTEGER,    -- overs 7-15 runs
    actual_death_total      INTEGER,    -- overs 16-20 runs
    label_filled            INTEGER DEFAULT 0   -- 0=pending, 1=done
)
"""

_OUTCOMES_SCHEMA = """
CREATE TABLE IF NOT EXISTS match_outcomes (
    match_id            INTEGER PRIMARY KEY,
    competition         TEXT,
    venue               TEXT,
    home_team           TEXT,
    away_team           TEXT,
    innings             INTEGER,
    toss_winner         TEXT,
    toss_decision       TEXT,
    innings_total       INTEGER,
    pp_runs             INTEGER,
    middle_runs         INTEGER,        -- overs 7-15
    death_runs          INTEGER,        -- overs 16-20
    winner              TEXT,
    collected_at        TEXT
)
"""


class MLCollector:
    """Collects live match data for ML model training."""

    def __init__(self, db_path: str = "data/ml_training.db"):
        self.db_path = db_path
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        self._create_tables()

        # Track last over saved per (match_id, innings) to avoid duplicates
        self._last_saved: Dict[tuple, int] = {}

        logger.info("MLCollector ready: %s", db_path)

    def _create_tables(self) -> None:
        with self._lock:
            self._conn.execute(_SNAPSHOTS_SCHEMA)
            self._conn.execute(_OUTCOMES_SCHEMA)
            self._conn.commit()

    # ── Per-over snapshot ─────────────────────────────────────────────

    def record_over(
        self,
        match_id: int,
        competition: str,
        venue: str,
        home: str,
        away: str,
        innings: int,
        over_num: int,          # 1-indexed, completed over
        score: int,
        wickets: int,
        last_over_runs: int,
        pp_runs: int,
        phase: str,
        toss_winner: str = "",
        toss_decision: str = "",
        striker_sr: float = 0.0,
        striker_runs: int = 0,
        bowler_econ: float = 0.0,
        bowler_wickets: int = 0,
    ) -> None:
        """Save a snapshot at the end of a completed over.

        Labels are left NULL and filled in when the match ends.
        """
        key = (match_id, innings)
        if self._last_saved.get(key) == over_num:
            return  # already saved this over

        run_rate = round(score / over_num, 2) if over_num > 0 else 0.0
        now = datetime.now(timezone.utc).isoformat()

        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO over_snapshots
                   (collected_at, match_id, competition, venue, home_team, away_team,
                    innings, toss_winner, toss_decision,
                    over_num, score, wickets, run_rate, pp_runs, last_over_runs, phase,
                    striker_sr, striker_runs, bowler_econ, bowler_wickets)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    now, match_id, competition, venue, home, away,
                    innings, toss_winner, toss_decision,
                    over_num, score, wickets, run_rate, pp_runs, last_over_runs, phase,
                    striker_sr, striker_runs, bowler_econ, bowler_wickets,
                ),
            )
            self._conn.commit()

        self._last_saved[key] = over_num
        logger.debug(
            "ML snapshot: match=%d inn=%d over=%d score=%d/%d RR=%.1f",
            match_id, innings, over_num, score, wickets, run_rate,
        )

    # ── Match end — fill in labels ────────────────────────────────────

    def finalise_match(
        self,
        match_id: int,
        innings: int,
        innings_total: int,
        pp_runs: int,
        middle_runs: int,       # overs 7-15
        death_runs: int,        # overs 16-20
        competition: str = "",
        venue: str = "",
        home: str = "",
        away: str = "",
        toss_winner: str = "",
        toss_decision: str = "",
        winner: str = "",
    ) -> int:
        """Called when an innings ends. Fills in all label columns for every
        snapshot row belonging to this match + innings.

        Returns number of rows labelled.
        """
        now = datetime.now(timezone.utc).isoformat()

        with self._lock:
            # For each snapshot at over N: runs_from_here = total - score_at_N
            cur = self._conn.execute(
                "SELECT id, over_num, score FROM over_snapshots "
                "WHERE match_id=? AND innings=? AND label_filled=0",
                (match_id, innings),
            )
            rows = cur.fetchall()

            for row_id, over_num, score_at_n in rows:
                runs_from_here = max(0, innings_total - score_at_n)
                self._conn.execute(
                    """UPDATE over_snapshots SET
                       actual_innings_total=?,
                       actual_runs_from_here=?,
                       actual_pp_total=?,
                       actual_7_15_total=?,
                       actual_death_total=?,
                       label_filled=1
                       WHERE id=?""",
                    (innings_total, runs_from_here, pp_runs, middle_runs, death_runs, row_id),
                )

            # Save match outcome summary
            self._conn.execute(
                """INSERT OR REPLACE INTO match_outcomes
                   (match_id, competition, venue, home_team, away_team, innings,
                    toss_winner, toss_decision,
                    innings_total, pp_runs, middle_runs, death_runs, winner, collected_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    match_id, competition, venue, home, away, innings,
                    toss_winner, toss_decision,
                    innings_total, pp_runs, middle_runs, death_runs, winner, now,
                ),
            )
            self._conn.commit()

        logger.info(
            "ML labels filled: match=%d inn=%d total=%d (%d snapshots)",
            match_id, innings, innings_total, len(rows),
        )
        return len(rows)

    # ── Stats ─────────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        cur = self._conn.execute(
            "SELECT COUNT(*), SUM(label_filled) FROM over_snapshots"
        )
        total, labelled = cur.fetchone()
        matches = self._conn.execute(
            "SELECT COUNT(DISTINCT match_id) FROM over_snapshots"
        ).fetchone()[0]
        return {
            "total_snapshots": total or 0,
            "labelled": labelled or 0,
            "pending_labels": (total or 0) - (labelled or 0),
            "matches": matches,
        }

    def close(self) -> None:
        self._conn.close()
