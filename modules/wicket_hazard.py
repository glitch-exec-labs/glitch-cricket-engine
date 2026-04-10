"""Wicket Hazard Model — P(wicket falls in next over) from current match state.

Blends a precomputed lookup table with an XGBoost binary classifier for
robust wicket probability estimation. Used by ScenarioModel to branch the
over-by-over probability tree.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger("ipl_spotter.wicket_hazard")

# Phase-based defaults when bucket is missing or data is sparse
_PHASE_DEFAULTS = {"powerplay": 0.22, "middle": 0.25, "death": 0.42}

# Bucket functions (must match situational_model.py)
_WICKET_BUCKETS = [(0, 1, "0-1"), (2, 3, "2-3"), (4, 5, "4-5"), (6, 7, "6-7"), (8, 10, "8+")]
_RR_BUCKETS = [(0.0, 6.0, "<6"), (6.0, 7.5, "6-7.5"), (7.5, 9.0, "7.5-9"), (9.0, 10.5, "9-10.5"), (10.5, 999.0, ">10.5")]


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


class WicketHazardModel:
    """Predict P(wicket in next over) from match state."""

    def __init__(
        self,
        table_path: str = "data/scenario_tables.json",
        xgb_model_path: str = "data/models/wicket_fell_next_over.json",
        xgb_encoder_path: str = "data/models/wicket_fell_next_over_encoders.json",
    ) -> None:
        # Load lookup table
        self._lookup: dict[str, float] = {}
        if os.path.exists(table_path):
            with open(table_path) as f:
                raw = json.load(f)
            for key, val in raw.items():
                if key.startswith("wicket:"):
                    self._lookup[key] = val.get("prob", 0.25)

        # Load XGBoost classifier (optional)
        self._xgb = None
        self._xgb_encoders: dict[str, list] = {}
        try:
            import xgboost as xgb
            if os.path.exists(xgb_model_path):
                self._xgb = xgb.XGBClassifier()
                self._xgb.load_model(xgb_model_path)
                if os.path.exists(xgb_encoder_path):
                    with open(xgb_encoder_path) as f:
                        self._xgb_encoders = json.load(f)
                logger.info("WicketHazard: XGB classifier loaded")
        except Exception:
            self._xgb = None

        logger.info(
            "WicketHazard ready: %d lookup entries, xgb=%s",
            len(self._lookup), "YES" if self._xgb else "NO",
        )

    @property
    def available(self) -> bool:
        return bool(self._lookup)

    def predict(
        self,
        over_num: int,
        score: int,
        wickets: int,
        run_rate: float,
        phase: str = "",
        striker_career_sr: float = 130.0,
        bowler_career_econ: float = 8.5,
        batting_depth: int = 5,
        death_bowling_quality: float = 0.5,
        innings: int = 1,
        last_over_runs: int = 0,
    ) -> float:
        """Return P(wicket in next over) in [0.01, 0.95]."""
        if not phase:
            phase = _phase_for_over(over_num)

        # Lookup estimate
        lookup_prob = self._lookup_prob(phase, wickets, run_rate)

        # XGB estimate (if available)
        if self._xgb is not None:
            try:
                xgb_prob = self._xgb_prob(
                    over_num=over_num, score=score, wickets=wickets,
                    run_rate=run_rate, phase=phase,
                    striker_career_sr=striker_career_sr,
                    bowler_career_econ=bowler_career_econ,
                    batting_depth=batting_depth,
                    death_bowling_quality=death_bowling_quality,
                    innings=innings, last_over_runs=last_over_runs,
                )
                # Blend: 60% XGB, 40% lookup
                prob = 0.6 * xgb_prob + 0.4 * lookup_prob
            except Exception:
                prob = lookup_prob
        else:
            prob = lookup_prob

        return max(0.01, min(0.95, prob))

    def _lookup_prob(self, phase: str, wickets: int, run_rate: float) -> float:
        key = f"wicket:{phase}:{_wicket_bucket(wickets)}:{_rr_bucket(run_rate)}"
        prob = self._lookup.get(key)
        if prob is not None:
            return prob
        return _PHASE_DEFAULTS.get(phase, 0.25)

    def _xgb_prob(self, **kwargs: Any) -> float:
        """Run XGBoost classifier and return P(wicket)."""
        import numpy as np

        # Build feature vector matching FEATURE_COLS_V2
        features = {
            "competition": "ipl",
            "venue_group": "medium",
            "toss_decision": "bat",
            "innings": kwargs.get("innings", 1),
            "over_num": kwargs.get("over_num", 10),
            "score": kwargs.get("score", 80),
            "wickets": kwargs.get("wickets", 2),
            "run_rate": kwargs.get("run_rate", 8.0),
            "pp_runs_so_far": 0,
            "last_over_runs": kwargs.get("last_over_runs", 0),
            "phase": kwargs.get("phase", "middle"),
            "striker_innings_sr": kwargs.get("striker_career_sr", 130.0),
            "striker_innings_runs": 0,
            "striker_career_sr": kwargs.get("striker_career_sr", 130.0),
            "bowler_innings_econ": kwargs.get("bowler_career_econ", 8.5),
            "bowler_innings_wickets": 0,
            "bowler_career_econ": kwargs.get("bowler_career_econ", 8.5),
            "batting_team_form": 167.0,
            "bowling_team_form": 167.0,
            "venue_avg_1st": 167.0,
        }

        # Encode categoricals
        cats = ["competition", "venue_group", "toss_decision", "phase"]
        for cat in cats:
            classes = self._xgb_encoders.get(cat, [])
            val = str(features[cat])
            if val in classes:
                features[cat] = classes.index(val)
            else:
                features[cat] = 0

        # Build array in FEATURE_COLS_V2 order
        col_order = [
            "competition", "venue_group", "toss_decision", "innings",
            "over_num", "score", "wickets", "run_rate", "pp_runs_so_far",
            "last_over_runs", "phase",
            "striker_innings_sr", "striker_innings_runs", "striker_career_sr",
            "bowler_innings_econ", "bowler_innings_wickets", "bowler_career_econ",
            "batting_team_form", "bowling_team_form", "venue_avg_1st",
        ]
        X = np.array([[float(features.get(c, 0)) for c in col_order]])
        proba = self._xgb.predict_proba(X)
        return float(proba[0][1])
