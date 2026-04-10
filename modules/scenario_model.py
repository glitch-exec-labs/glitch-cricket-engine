"""Scenario Model — over-by-over probability tree for innings projection.

Instead of extrapolating a single number, this model branches the next 1-5
overs on wicket probability, computing a distribution of outcomes.  Each
branch has different expected runs (wicket overs yield fewer runs).  The tree
collapses into (expected, std_dev) for the EdgeDetector.
"""
from __future__ import annotations

import json
import logging
import math
import os
from typing import Any

logger = logging.getLogger("ipl_spotter.scenario_model")

# Bucket functions (same as situational_model.py / wicket_hazard.py)
_WICKET_BUCKETS = [(0, 1, "0-1"), (2, 3, "2-3"), (4, 5, "4-5"), (6, 7, "6-7"), (8, 10, "8+")]
_RR_BUCKETS = [(0.0, 6.0, "<6"), (6.0, 7.5, "6-7.5"), (7.5, 9.0, "7.5-9"), (9.0, 10.5, "9-10.5"), (10.5, 999.0, ">10.5")]

_PHASE_RUN_DEFAULTS = {"powerplay": 8.1, "middle": 8.1, "death": 10.0}
_WICKET_OVER_DISCOUNT = 0.70  # wicket overs yield ~30% fewer runs


def _wicket_bucket(wickets: int) -> str:
    for lo, hi, label in _WICKET_BUCKETS:
        if lo <= wickets <= hi:
            return label
    return "8+"


def _rr_bucket(run_rate: float) -> str:
    for lo, hi, label in _RR_BUCKETS:
        if lo <= run_rate < hi:
            return label
    return ">10.5"


def _phase_for_over(over_num: int) -> str:
    if over_num < 6:
        return "powerplay"
    if over_num < 15:
        return "middle"
    return "death"


class ScenarioModel:
    """Over-by-over probability tree for innings score projection."""

    def __init__(
        self,
        table_path: str = "data/scenario_tables.json",
        wicket_model: Any = None,
        max_depth: int = 5,
    ) -> None:
        self._runs: dict[str, tuple[float, float]] = {}     # key -> (mean, std)
        self._runs_wkt: dict[str, tuple[float, float]] = {}  # key -> (mean, std) for wicket overs
        self._max_depth = max_depth

        # Load scenario tables
        if os.path.exists(table_path):
            with open(table_path) as f:
                raw = json.load(f)
            for key, val in raw.items():
                if key.startswith("runs:") and not key.startswith("runs_wkt:"):
                    self._runs[key] = (val["mean_runs"], val["std_runs"])
                elif key.startswith("runs_wkt:"):
                    self._runs_wkt[key] = (val["mean_runs"], val["std_runs"])

        # Wicket hazard model
        self._wicket_model = wicket_model
        if wicket_model is None:
            try:
                from modules.wicket_hazard import WicketHazardModel
                self._wicket_model = WicketHazardModel(table_path=table_path)
            except Exception:
                pass

        logger.info(
            "ScenarioModel ready: %d run buckets, %d wicket-run buckets, wicket_model=%s",
            len(self._runs), len(self._runs_wkt), "YES" if self._wicket_model else "NO",
        )

    @property
    def available(self) -> bool:
        return bool(self._runs)

    def project_to_over(
        self,
        match_state: Any,
        target_over: float,
        innings_state: Any = None,
    ) -> dict[str, Any]:
        """Project score distribution at target_over via probability tree.

        Returns {expected, std_dev, confidence, tree_depth}.
        """
        current_over_raw = float(match_state.overs_completed)
        current_over = int(current_over_raw)  # completed whole overs
        partial = current_over_raw - current_over  # fractional part (0.0-0.5)
        target = int(min(target_over, 20))
        score = float(match_state.total_runs)
        wickets = int(match_state.wickets)
        run_rate = float(match_state.current_run_rate or 0)
        innings = int(getattr(match_state, "current_innings", 1))

        if target <= current_over:
            return {
                "expected": score,
                "std_dev": 2.0,
                "confidence": "HIGH",
                "tree_depth": 0,
            }

        # Account for partial overs: if 10.3 overs done, the current over
        # is partially bowled. Whole overs to project = target - current - 1
        # (the current partial over) + fraction of current over remaining.
        whole_overs_ahead = target - current_over
        if partial > 0:
            # We're mid-over; reduce projected overs by the partial fraction
            # The tree projects whole overs, so we subtract partial from the first over's runs
            pass  # handled by fractional_discount below
        overs_ahead = min(whole_overs_ahead, self._max_depth)
        extra_overs = max(0, whole_overs_ahead - self._max_depth)
        fractional_discount = 1.0 - (partial / 1.0) if partial > 0 else 1.0  # e.g. 0.3 partial → first over gets 0.7x weight

        # Build tree: list of (probability, score, variance) leaves
        leaves = self._build_tree(
            current_over=current_over,
            target_over=current_over + overs_ahead,
            score=score,
            wickets=wickets,
            run_rate=run_rate,
            innings_state=innings_state,
            innings=innings,
            prob=1.0,
            var=0.0,
            first_over_discount=fractional_discount,
        )

        if not leaves:
            return {
                "expected": float(score),
                "std_dev": 20.0,
                "confidence": "LOW",
                "tree_depth": 0,
            }

        # Collapse tree
        expected, combined_std = self._collapse_tree(leaves, float(score))

        # If there are extra overs beyond max_depth, extrapolate linearly
        if extra_overs > 0 and expected > score:
            avg_per_over = (expected - score) / overs_ahead
            expected += avg_per_over * extra_overs
            combined_std += 2.5 * extra_overs  # add uncertainty for extrapolated overs

        # Confidence from std relative to expected
        if expected > 0:
            cv = combined_std / expected
            if cv < 0.10:
                confidence = "HIGH"
            elif cv < 0.20:
                confidence = "MEDIUM"
            else:
                confidence = "LOW"
        else:
            confidence = "LOW"

        return {
            "expected": round(expected, 1),
            "std_dev": round(combined_std, 1),
            "confidence": confidence,
            "tree_depth": overs_ahead,
        }

    def project_innings_total(
        self,
        match_state: Any,
        innings_state: Any = None,
    ) -> dict[str, Any]:
        """Convenience: project to over 20."""
        return self.project_to_over(match_state, 20.0, innings_state)

    def _build_tree(
        self,
        current_over: int,
        target_over: int,
        score: float,
        wickets: int,
        run_rate: float,
        innings_state: Any,
        innings: int,
        prob: float,
        var: float,
        first_over_discount: float = 1.0,
    ) -> list[tuple[float, float, float]]:
        """Recursive tree. Returns [(probability, score, cumulative_variance)].

        score is kept as float throughout to avoid truncation bias.
        first_over_discount accounts for partial overs (e.g. 0.7 if 0.3 of
        current over already bowled).
        """
        # Base cases
        if current_over >= target_over or wickets >= 10 or prob < 0.001:
            return [(prob, score, var)]

        phase = _phase_for_over(current_over)

        # Get wicket probability
        wkt_prob = 0.25
        if self._wicket_model:
            try:
                bat_depth = getattr(innings_state, "batting_depth", 5) if innings_state else 5
                dbq = getattr(innings_state, "death_bowling_quality", 0.5) if innings_state else 0.5
                wkt_prob = self._wicket_model.predict(
                    over_num=current_over, score=int(score), wickets=wickets,
                    run_rate=run_rate, phase=phase,
                    batting_depth=bat_depth, death_bowling_quality=dbq,
                    innings=innings,
                )
            except Exception:
                pass

        # Apply partial-over discount to first over only
        discount = first_over_discount if first_over_discount < 1.0 else 1.0

        # Get expected runs for each branch
        no_wkt_mean, no_wkt_std = self._runs_for_over(phase, wickets, run_rate, wicket_fell=False)
        wkt_mean, wkt_std = self._runs_for_over(phase, wickets, run_rate, wicket_fell=True)

        # Scale first over by discount (partial over → fewer runs expected)
        no_wkt_mean *= discount
        wkt_mean *= discount
        no_wkt_std *= discount
        wkt_std *= discount

        leaves: list[tuple[float, float, float]] = []

        # NO WICKET branch
        no_wkt_prob = 1.0 - wkt_prob
        if no_wkt_prob > 0.001:
            new_score = score + no_wkt_mean  # float, no truncation
            new_rr = new_score / (current_over + 1) if current_over > 0 else run_rate
            new_var = var + no_wkt_std ** 2
            leaves.extend(self._build_tree(
                current_over + 1, target_over,
                new_score, wickets, new_rr,
                innings_state, innings,
                prob * no_wkt_prob, new_var,
                first_over_discount=1.0,  # only first over is discounted
            ))

        # WICKET branch
        if wkt_prob > 0.001:
            new_score = score + wkt_mean  # float, no truncation
            new_wickets = wickets + 1
            new_rr = new_score / (current_over + 1) if current_over > 0 else run_rate * 0.85
            new_var = var + wkt_std ** 2
            leaves.extend(self._build_tree(
                current_over + 1, target_over,
                new_score, new_wickets, new_rr,
                innings_state, innings,
                prob * wkt_prob, new_var,
                first_over_discount=1.0,
            ))

        return leaves

    def _runs_for_over(
        self,
        phase: str,
        wickets: int,
        run_rate: float,
        wicket_fell: bool,
    ) -> tuple[float, float]:
        """Lookup (mean_runs, std_runs) for one over in given state."""
        wb = _wicket_bucket(wickets)
        rb = _rr_bucket(run_rate)

        if wicket_fell:
            key = f"runs_wkt:{phase}:{wb}:{rb}"
            entry = self._runs_wkt.get(key)
            if entry:
                return entry
            # Fallback: use no-wicket mean × discount
            no_wkt_key = f"runs:{phase}:{wb}:{rb}"
            no_wkt = self._runs.get(no_wkt_key)
            if no_wkt:
                return (no_wkt[0] * _WICKET_OVER_DISCOUNT, no_wkt[1])
        else:
            key = f"runs:{phase}:{wb}:{rb}"
            entry = self._runs.get(key)
            if entry:
                return entry

        # Phase default
        default_mean = _PHASE_RUN_DEFAULTS.get(phase, 8.0)
        if wicket_fell:
            default_mean *= _WICKET_OVER_DISCOUNT
        return (default_mean, 3.5)

    def _collapse_tree(
        self,
        leaves: list[tuple[float, float, float]],
        current_score: float,
    ) -> tuple[float, float]:
        """Collapse probability-weighted leaves into (expected, std_dev)."""
        total_prob = sum(p for p, _, _ in leaves)
        if total_prob <= 0:
            return current_score, 20.0

        # Normalize probabilities
        norm = [(p / total_prob, s, v) for p, s, v in leaves]

        expected = sum(p * s for p, s, _ in norm)
        # Combined variance = within-node variance + between-node variance
        variance = sum(p * (v + (s - expected) ** 2) for p, s, v in norm)
        std_dev = math.sqrt(max(0.0, variance))

        return expected, std_dev
