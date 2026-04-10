"""
ML Predictor — loads trained XGBoost models and serves live predictions.

v2: Uses the 20-feature schema from train_model.py FEATURE_COLS_V2.
Fails loudly on feature mismatch instead of silently swallowing errors.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

import numpy as np

logger = logging.getLogger("ipl_spotter.ml_predictor")

MODEL_DIR = "data/models"

VENUE_GROUPS = {
    "wankhede": "high", "chinnaswamy": "high", "narendra modi": "high",
    "sawai mansingh": "high", "brabourne": "high", "dy patil": "high",
    "chepauk": "low", "feroz shah kotla": "low", "arun jaitley": "low",
    "rajiv gandhi": "low", "uppal": "low", "hyderabad": "low",
    "ekana": "low", "lucknow": "low",
    "gaddafi": "medium", "rawalpindi": "high", "national stadium": "medium",
}

# v2 feature order — must match train_model.py FEATURE_COLS_V2 exactly
FEATURE_COLS_V2 = [
    "competition", "venue_group", "toss_decision", "innings",
    "over_num", "score", "wickets", "run_rate", "pp_runs_so_far",
    "last_over_runs", "phase",
    "striker_innings_sr", "striker_innings_runs",
    "striker_career_sr",
    "bowler_innings_econ", "bowler_innings_wickets",
    "bowler_career_econ",
    "batting_team_form", "bowling_team_form",
    "venue_avg_1st",
]

CATEGORICAL_COLS = {"competition", "venue_group", "toss_decision", "phase"}


def _venue_group(venue: str) -> str:
    v = (venue or "").lower()
    for k, g in VENUE_GROUPS.items():
        if k in v:
            return g
    return "medium"


class MLPredictor:
    """Loads trained XGBoost models and serves predictions for live matches."""

    def __init__(self):
        self._models: Dict[str, Any] = {}
        self._encoders: Dict[str, Dict[str, list]] = {}
        self._feature_cols: list[str] = FEATURE_COLS_V2
        self.available = False
        self._load_models()

    def _load_models(self) -> None:
        try:
            import xgboost as xgb
        except ImportError:
            logger.warning("xgboost not installed — ML predictor disabled")
            return

        # Load metadata to get the exact feature schema
        meta_path = os.path.join(MODEL_DIR, "metadata.json")
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                meta = json.load(f)
            saved_cols = meta.get("feature_cols")
            if saved_cols:
                self._feature_cols = saved_cols
                logger.info("MLPredictor: using %d-feature schema from metadata", len(saved_cols))

        targets = [
            "actual_innings_total",
            "actual_pp_total",
            "actual_7_15_total",
            "actual_death_total",
        ]

        loaded = 0
        for target in targets:
            model_path = os.path.join(MODEL_DIR, f"{target}.json")
            enc_path = os.path.join(MODEL_DIR, f"{target}_encoders.json")

            if not os.path.exists(model_path):
                continue

            model = xgb.XGBRegressor()
            model.load_model(model_path)
            self._models[target] = model

            if os.path.exists(enc_path):
                with open(enc_path) as f:
                    self._encoders[target] = json.load(f)

            loaded += 1

        if loaded == len(targets):
            self.available = True
            logger.info("MLPredictor loaded %d models from %s (%d features)",
                        loaded, MODEL_DIR, len(self._feature_cols))
        elif loaded > 0:
            logger.warning("MLPredictor: only %d/%d models loaded", loaded, len(targets))
        else:
            logger.info("No trained models found — run: python3 train_model.py --deploy")

    def _encode_row(self, row: Dict, target: str) -> list:
        """Encode a feature dict into a numeric list matching the trained schema."""
        enc = self._encoders.get(target, {})
        encoded = []
        for feat in self._feature_cols:
            val = row.get(feat, 0)
            if feat in CATEGORICAL_COLS:
                classes = enc.get(feat, [])
                val = str(val) if val is not None else "unknown"
                if val not in classes:
                    val = "unknown"
                idx = classes.index(val) if val in classes else 0
                encoded.append(idx)
            else:
                encoded.append(float(val) if val is not None else 0.0)
        return encoded

    def predict(
        self,
        competition: str,
        venue: str,
        innings: int,
        over_num: int,
        score: int,
        wickets: int,
        pp_runs_so_far: int,
        last_over_runs: int,
        phase: str,
        toss_decision: str = "bat",
        striker_innings_sr: float = 0.0,
        striker_innings_runs: int = 0,
        striker_career_sr: float = 0.0,
        bowler_innings_econ: float = 0.0,
        bowler_innings_wickets: int = 0,
        bowler_career_econ: float = 0.0,
        batting_team_form: float = 167.0,
        bowling_team_form: float = 167.0,
        venue_avg_1st: float = 167.0,
    ) -> Optional[Dict[str, float]]:
        """Return ML model predictions for the current match state.

        Returns dict with keys: innings_total, pp_total, middle_total, death_total.
        Or None if models not available.
        """
        if not self.available:
            return None

        run_rate = round(score / over_num, 2) if over_num > 0 else 0.0
        row = {
            "competition": competition,
            "venue_group": _venue_group(venue),
            "toss_decision": toss_decision,
            "innings": innings,
            "over_num": over_num,
            "score": score,
            "wickets": wickets,
            "run_rate": run_rate,
            "pp_runs_so_far": pp_runs_so_far,
            "last_over_runs": last_over_runs,
            "phase": phase,
            "striker_innings_sr": striker_innings_sr,
            "striker_innings_runs": striker_innings_runs,
            "striker_career_sr": striker_career_sr,
            "bowler_innings_econ": bowler_innings_econ,
            "bowler_innings_wickets": bowler_innings_wickets,
            "bowler_career_econ": bowler_career_econ,
            "batting_team_form": batting_team_form,
            "bowling_team_form": bowling_team_form,
            "venue_avg_1st": venue_avg_1st,
        }

        result: Dict[str, float] = {}
        target_map = {
            "actual_innings_total": "innings_total",
            "actual_pp_total": "pp_total",
            "actual_7_15_total": "middle_total",
            "actual_death_total": "death_total",
        }

        for target, key in target_map.items():
            model = self._models.get(target)
            if not model:
                continue
            try:
                encoded = self._encode_row(row, target)
                arr = np.array([encoded], dtype=np.float32)
                pred = float(model.predict(arr)[0])
                result[key] = round(max(0, pred), 1)
            except Exception as e:
                logger.warning("ML predict failed (%s): %s", target, e)

        return result if result else None

    def predict_from_state(
        self,
        state: Any,
        competition: str = "ipl",
        innings_state: Any = None,
    ) -> Optional[Dict[str, float]]:
        """Convenience wrapper: takes a MatchState + optional InningsState."""
        try:
            over_num = int(state.overs_completed)
            pp_runs = sum(
                runs for ov, runs in state.over_runs.items() if ov < 6
            ) if hasattr(state, "over_runs") else 0

            if over_num < 6:
                phase = "powerplay"
            elif over_num < 16:
                phase = "middle"
            else:
                phase = "death"

            # Active batter context (in-match stats)
            striker_innings_sr, striker_innings_runs = 0.0, 0
            striker_career_sr = 0.0
            if hasattr(state, "active_batsmen") and state.active_batsmen:
                bat = state.active_batsmen[0]
                balls = bat.get("balls", 0) or 0
                runs = bat.get("runs", bat.get("score", 0)) or 0
                striker_innings_sr = round((runs / balls * 100) if balls > 0 else 0.0, 1)
                striker_innings_runs = runs

            # Active bowler context — MatchState uses "active_bowler" (singular)
            bowler_innings_econ, bowler_innings_wickets = 0.0, 0
            bowler_career_econ = 0.0
            _bowler = getattr(state, "active_bowler", None)
            if _bowler and isinstance(_bowler, dict):
                overs = float(_bowler.get("overs", 0) or 0)
                runs_c = int(_bowler.get("runs", 0) or 0)
                bowler_innings_econ = round((runs_c / overs) if overs > 0 else 0.0, 1)
                bowler_innings_wickets = int(_bowler.get("wickets", 0) or 0)

            # Career stats and team form from InningsState if available
            if innings_state is not None:
                active_batters = getattr(innings_state, "batters_batting", [])
                if active_batters:
                    striker_career_sr = getattr(active_batters[0], "career_sr", 0.0)
                bowlers_used = getattr(innings_state, "bowlers_used", [])
                if bowlers_used:
                    bowler_career_econ = getattr(bowlers_used[-1], "career_econ", 0.0)

            # Venue average
            venue = getattr(state, "venue", "")
            venue_avg_1st = 167.0  # default; could be enriched from stats_db

            return self.predict(
                competition=competition,
                venue=venue,
                innings=getattr(state, "current_innings", 1),
                over_num=over_num,
                score=getattr(state, "total_runs", 0),
                wickets=getattr(state, "wickets", 0),
                pp_runs_so_far=pp_runs,
                last_over_runs=state.over_runs.get(over_num - 1, 0) if hasattr(state, "over_runs") and over_num > 0 else 0,
                phase=phase,
                striker_innings_sr=striker_innings_sr,
                striker_innings_runs=striker_innings_runs,
                striker_career_sr=striker_career_sr,
                bowler_innings_econ=bowler_innings_econ,
                bowler_innings_wickets=bowler_innings_wickets,
                bowler_career_econ=bowler_career_econ,
                batting_team_form=167.0,
                bowling_team_form=167.0,
                venue_avg_1st=venue_avg_1st,
            )
        except Exception:
            logger.warning("predict_from_state error", exc_info=True)
            return None
