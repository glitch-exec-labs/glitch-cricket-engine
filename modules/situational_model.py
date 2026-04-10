"""
Situational Prediction Model for IPL Edge Spotter.

Builds a lookup table from historical match data, bucketing by
(wickets, overs, run_rate) to produce context-aware innings projections.

"82/1 after 10 overs" and "82/5 after 10 overs" now produce very different
projections instead of near-identical flat-average estimates.
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

from modules.match_state import MatchState
from modules.stats_db import StatsDB

logger = logging.getLogger("ipl_spotter.situational_model")

# ── Bucket definitions ──────────────────────────────────────────────────────

WICKET_BUCKETS: List[Tuple[int, int, str]] = [
    (0, 1, "0-1"),
    (2, 3, "2-3"),
    (4, 5, "4-5"),
    (6, 7, "6-7"),
    (8, 10, "8+"),
]

OVER_BUCKETS: List[Tuple[float, float, str]] = [
    (1.0, 6.0, "1-6"),
    (7.0, 10.0, "7-10"),
    (11.0, 15.0, "11-15"),
    (16.0, 20.0, "16-20"),
]

RUN_RATE_BUCKETS: List[Tuple[float, float, str]] = [
    (0.0, 6.0, "<6"),
    (6.0, 7.5, "6-7.5"),
    (7.5, 9.0, "7.5-9"),
    (9.0, 10.5, "9-10.5"),
    (10.5, 999.0, ">10.5"),
]


def _wicket_bucket(wickets: int) -> str:
    """Assign a wicket count to its bucket label."""
    for lo, hi, label in WICKET_BUCKETS:
        if lo <= wickets <= hi:
            return label
    return "8+"


def _over_bucket(overs: float) -> str:
    """Assign an overs value to its bucket label."""
    for lo, hi, label in OVER_BUCKETS:
        if lo <= overs <= hi:
            return label
    # Edge: before over 1 or exactly 0
    if overs < 1.0:
        return "1-6"
    return "16-20"


def _run_rate_bucket(run_rate: float) -> str:
    """Assign a run rate to its bucket label."""
    for lo, hi, label in RUN_RATE_BUCKETS:
        if lo <= run_rate < hi:
            return label
    # Catch exact upper boundary
    return ">10.5"


def _std_dev(values: List[float], mean: float) -> float:
    """Population standard deviation."""
    if len(values) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


# ── Lookup table entry ──────────────────────────────────────────────────────

class BucketStats:
    """Aggregated statistics for a single (wickets, overs, run_rate) bucket."""

    __slots__ = ("expected", "std_dev", "sample_count", "totals")

    def __init__(self) -> None:
        self.expected: float = 0.0
        self.std_dev: float = 0.0
        self.sample_count: int = 0
        self.totals: List[float] = []

    def add(self, final_total: float) -> None:
        self.totals.append(final_total)

    def finalise(self) -> None:
        n = len(self.totals)
        self.sample_count = n
        if n == 0:
            return
        self.expected = sum(self.totals) / n
        self.std_dev = _std_dev(self.totals, self.expected)


# ── Main class ──────────────────────────────────────────────────────────────

class SituationalPredictor:
    """
    Context-aware innings total predictor.

    Builds a 3-D lookup table from historical match data:
        (wicket_bucket, over_bucket, run_rate_bucket) -> expected final total

    Falls back to venue average when sample count is too low.
    """

    MIN_SAMPLE_COUNT = 5

    def __init__(self, stats_db: StatsDB) -> None:
        self.stats_db = stats_db
        self._table: Dict[Tuple[str, str, str], BucketStats] = {}
        self._build_table()

    # ── Public API ──────────────────────────────────────────────────────────

    def predict_innings_total(
        self,
        match_state: MatchState,
        venue_avg: float = 172.0,
    ) -> Dict[str, Any]:
        """
        Predict the final innings total given the current match situation.

        Returns a dict with: expected, std_dev, confidence, sample_count, bucket.
        """
        overs = match_state.overs_completed
        wickets = match_state.wickets
        run_rate = match_state.current_run_rate

        wb = _wicket_bucket(wickets)
        ob = _over_bucket(overs) if overs >= 1.0 else "1-6"
        rb = _run_rate_bucket(run_rate) if run_rate > 0 else "<6"
        bucket_key = (wb, ob, rb)
        bucket_label = f"W{wb}_O{ob}_RR{rb}"

        stats = self._table.get(bucket_key)

        if stats is not None and stats.sample_count >= self.MIN_SAMPLE_COUNT:
            # Blend with venue average based on sample confidence
            confidence_weight = min(1.0, stats.sample_count / 30.0)
            expected = (
                confidence_weight * stats.expected
                + (1.0 - confidence_weight) * venue_avg
            )
            std_dev = stats.std_dev
            sample_count = stats.sample_count
        else:
            # Fallback: use venue average
            expected = venue_avg
            std_dev = 25.0
            sample_count = stats.sample_count if stats is not None else 0

        confidence = self._confidence_label(std_dev, expected)

        return {
            "expected": round(expected, 1),
            "std_dev": round(std_dev, 1),
            "confidence": confidence,
            "sample_count": sample_count,
            "bucket": bucket_label,
        }

    # ── Bucket helpers (exposed for testing) ────────────────────────────────

    @staticmethod
    def wicket_bucket(wickets: int) -> str:
        return _wicket_bucket(wickets)

    @staticmethod
    def over_bucket(overs: float) -> str:
        return _over_bucket(overs)

    @staticmethod
    def run_rate_bucket(run_rate: float) -> str:
        return _run_rate_bucket(run_rate)

    # ── Internals ───────────────────────────────────────────────────────────

    def _build_table(self) -> None:
        """
        Query the matches table and build the lookup matrix.

        For each innings (1st and 2nd), we derive the score and wickets at
        checkpoint overs (6, 10, 15) from phase-level data stored in the DB,
        then record what the final innings total was.
        """
        conn = self.stats_db.conn
        rows = conn.execute(
            """
            SELECT
                match_id,
                first_innings_total,
                second_innings_total,
                powerplay_runs_1st,
                powerplay_runs_2nd,
                middle_runs_1st,
                middle_runs_2nd,
                death_runs_1st,
                death_runs_2nd
            FROM matches
            WHERE first_innings_total IS NOT NULL
            """
        ).fetchall()

        if not rows:
            logger.info("No historical match data found; situational table empty.")
            return

        for row in rows:
            # Process both innings
            for innings_suffix, total_key in [("1st", "first_innings_total"), ("2nd", "second_innings_total")]:
                final_total = row[total_key]
                if final_total is None or final_total <= 0:
                    continue

                pp_runs = row[f"powerplay_runs_{innings_suffix}"] or 0
                mid_runs = row[f"middle_runs_{innings_suffix}"] or 0
                death_runs = row[f"death_runs_{innings_suffix}"] or 0

                # Checkpoint: after 6 overs (end of powerplay)
                if pp_runs > 0:
                    score_at_6 = pp_runs
                    rr_at_6 = score_at_6 / 6.0
                    # We don't have per-checkpoint wicket data from the matches
                    # table, so we estimate wickets from scoring rate patterns.
                    # Use a heuristic: low run rate suggests more wickets lost.
                    est_wickets_6 = self._estimate_wickets_from_rate(rr_at_6, 6)
                    self._add_entry(est_wickets_6, 6.0, rr_at_6, float(final_total))

                # Checkpoint: after 10 overs
                if pp_runs > 0 and mid_runs > 0:
                    # Middle overs span 7-15 (9 overs). Score at 10 is
                    # powerplay + proportional share of middle overs (4/9).
                    mid_fraction = 4.0 / 9.0
                    score_at_10 = pp_runs + mid_runs * mid_fraction
                    rr_at_10 = score_at_10 / 10.0
                    est_wickets_10 = self._estimate_wickets_from_rate(rr_at_10, 10)
                    self._add_entry(est_wickets_10, 10.0, rr_at_10, float(final_total))

                # Checkpoint: after 15 overs
                if pp_runs > 0 and mid_runs > 0:
                    score_at_15 = pp_runs + mid_runs
                    rr_at_15 = score_at_15 / 15.0
                    est_wickets_15 = self._estimate_wickets_from_rate(rr_at_15, 15)
                    self._add_entry(est_wickets_15, 15.0, rr_at_15, float(final_total))

        # Finalise all bucket stats
        for stats in self._table.values():
            stats.finalise()

        total_entries = sum(s.sample_count for s in self._table.values())
        logger.info(
            "Situational table built: %d buckets, %d total data points",
            len(self._table),
            total_entries,
        )

    def _add_entry(
        self,
        wickets: int,
        overs: float,
        run_rate: float,
        final_total: float,
    ) -> None:
        wb = _wicket_bucket(wickets)
        ob = _over_bucket(overs)
        rb = _run_rate_bucket(run_rate)
        key = (wb, ob, rb)
        if key not in self._table:
            self._table[key] = BucketStats()
        self._table[key].add(final_total)

    @staticmethod
    def _estimate_wickets_from_rate(run_rate: float, overs: int) -> int:
        """
        Rough heuristic to estimate wickets from run rate and overs.

        Lower run rates at later stages suggest more wickets have fallen.
        This is an approximation used only for building the historical table.
        """
        if overs <= 6:
            if run_rate >= 9.0:
                return 0
            elif run_rate >= 7.5:
                return 1
            elif run_rate >= 6.0:
                return 2
            else:
                return 3
        elif overs <= 10:
            if run_rate >= 9.0:
                return 1
            elif run_rate >= 7.5:
                return 2
            elif run_rate >= 6.0:
                return 3
            else:
                return 5
        else:  # overs <= 15
            if run_rate >= 9.0:
                return 1
            elif run_rate >= 7.5:
                return 3
            elif run_rate >= 6.0:
                return 4
            else:
                return 6

    @staticmethod
    def _confidence_label(std_dev: float, expected: float) -> str:
        """Return HIGH / MEDIUM / LOW based on coefficient of variation."""
        if expected <= 0:
            return "LOW"
        cv = std_dev / expected
        if cv < 0.10:
            return "HIGH"
        if cv < 0.20:
            return "MEDIUM"
        return "LOW"
