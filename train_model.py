"""
Train XGBoost prediction models from historical + live-collected data.

Models trained:
  1. innings_total   — predict final innings score given mid-match state
  2. pp_total        — predict powerplay total (over 1-6) at toss time
  3. session_7_15    — predict overs 7-15 runs
  4. death_total     — predict overs 16-20 runs

Usage:
    python3 train_model.py              # train + evaluate
    python3 train_model.py --deploy     # train + save to data/models/

Models are saved as XGBoost JSON files, loaded by ml_predictor.py at runtime.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("train_model")

DB_STATS  = "data/ipl_stats.db"
DB_LIVE   = "data/ml_training.db"
DB_REAL   = "data/ml_training_v2.db"
MODEL_DIR = "data/models"


# ── Feature engineering ───────────────────────────────────────────────────────

VENUE_GROUPS = {
    # High-scoring
    "wankhede": "high", "chinnaswamy": "high", "narendra modi": "high",
    "sawai mansingh": "high", "brabourne": "high", "dy patil": "high",
    # Low-scoring
    "chepauk": "low", "feroz shah kotla": "low", "arun jaitley": "low",
    "rajiv gandhi": "low", "uppal": "low", "hyderabad": "low",
    "ekana": "low", "lucknow": "low",
    # PSL
    "gaddafi": "medium", "rawalpindi": "high", "national stadium": "medium",
}

def venue_group(venue: str) -> str:
    v = (venue or "").lower()
    for k, g in VENUE_GROUPS.items():
        if k in v:
            return g
    return "medium"


def encode_features(df: pd.DataFrame, le_cache: Dict[str, LabelEncoder], fit: bool) -> pd.DataFrame:
    """Encode categorical columns. fit=True during training, False at inference."""
    cats = ["competition", "venue_group", "toss_decision", "phase"]
    for col in cats:
        if col not in df.columns:
            df[col] = "unknown"
        if fit:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].fillna("unknown").astype(str))
            le_cache[col] = le
        else:
            le = le_cache.get(col)
            if le:
                known = set(le.classes_)
                df[col] = df[col].apply(lambda x: x if x in known else "unknown")
                df[col] = le.transform(df[col].fillna("unknown").astype(str))
            else:
                df[col] = 0
    return df


# ── Load data ─────────────────────────────────────────────────────────────────

def load_historical() -> pd.DataFrame:
    """Load the 1454 historical matches from ipl_stats.db.

    For ML training we create multiple rows per match:
      - row at 'toss' (over 0): predict full innings
      - row at 'after_pp' (over 6): predict innings total from pp_runs
      - row at 'after_middle' (over 15): predict death runs
    """
    conn = sqlite3.connect(DB_STATS)
    df = pd.read_sql_query("""
        SELECT match_id, venue,
               first_innings_total   AS innings_total,
               powerplay_runs_1st    AS pp_runs,
               middle_runs_1st       AS middle_runs,
               death_runs_1st        AS death_runs,
               toss_winner, toss_decision
        FROM matches
        WHERE first_innings_total > 50
          AND powerplay_runs_1st  > 0
          AND middle_runs_1st     > 0
          AND death_runs_1st      > 0
    """, conn)
    conn.close()

    df["competition"] = df["match_id"].apply(
        lambda x: "psl" if x > 1200000 else "ipl"
    )
    df["venue_group"] = df["venue"].apply(venue_group)

    rows = []

    for _, r in df.iterrows():
        base = {
            "competition": r["competition"],
            "venue_group": r["venue_group"],
            "toss_decision": r.get("toss_decision", "bat"),
            "innings": 1,
            # labels
            "actual_innings_total": r["innings_total"],
            "actual_pp_total":      r["pp_runs"],
            "actual_7_15_total":    r["middle_runs"],
            "actual_death_total":   r["death_runs"],
        }

        # Snapshot: at toss (over 0) — no live state yet
        rows.append({**base,
            "over_num": 0, "score": 0, "wickets": 0,
            "run_rate": 0.0, "pp_runs_so_far": 0,
            "last_over_runs": 0, "phase": "powerplay",
            "striker_sr": 0.0, "striker_runs": 0,
            "bowler_econ": 0.0, "bowler_wickets": 0,
        })

        # Snapshot: after powerplay (over 6)
        rows.append({**base,
            "over_num": 6, "score": r["pp_runs"], "wickets": 1,  # rough avg
            "run_rate": round(r["pp_runs"] / 6, 2), "pp_runs_so_far": r["pp_runs"],
            "last_over_runs": round(r["pp_runs"] / 6),
            "phase": "middle",
            "striker_sr": 130.0, "striker_runs": 30,
            "bowler_econ": 7.5, "bowler_wickets": 1,
        })

        # Snapshot: after over 10 (half-way)
        half = r["pp_runs"] + round(r["middle_runs"] * 4 / 9)
        rows.append({**base,
            "over_num": 10, "score": half, "wickets": 2,
            "run_rate": round(half / 10, 2), "pp_runs_so_far": r["pp_runs"],
            "last_over_runs": round(r["middle_runs"] / 9),
            "phase": "middle",
            "striker_sr": 125.0, "striker_runs": 35,
            "bowler_econ": 7.8, "bowler_wickets": 1,
        })

        # Snapshot: after over 15
        score_15 = r["pp_runs"] + r["middle_runs"]
        rows.append({**base,
            "over_num": 15, "score": score_15, "wickets": 3,
            "run_rate": round(score_15 / 15, 2), "pp_runs_so_far": r["pp_runs"],
            "last_over_runs": round(r["middle_runs"] / 9),
            "phase": "death",
            "striker_sr": 140.0, "striker_runs": 40,
            "bowler_econ": 8.5, "bowler_wickets": 2,
        })

    return pd.DataFrame(rows)


def load_real_snapshots() -> pd.DataFrame:
    """Load real per-over snapshots from ml_training_v2.db (built from ball-by-ball CSV).

    These are 8x more data than load_historical() with real wicket counts, real
    player career stats, and no hardcoded synthetic values.
    """
    if not os.path.exists(DB_REAL):
        return pd.DataFrame()

    conn = sqlite3.connect(DB_REAL)
    df = pd.read_sql_query("""
        SELECT match_id, competition, venue, venue_avg_1st,
               toss_decision, innings, over_num, score, wickets, run_rate,
               pp_runs_so_far, last_over_runs, phase,
               striker_innings_sr, striker_innings_runs,
               striker_career_sr,
               bowler_innings_econ, bowler_innings_wickets,
               bowler_career_econ,
               batting_team_form, bowling_team_form,
               actual_innings_total, actual_pp_total,
               actual_7_15_total, actual_death_total
        FROM real_over_snapshots
        WHERE actual_innings_total > 50
    """, conn)
    conn.close()

    df["venue_group"] = df["venue"].apply(venue_group)
    # Map to v1-compatible columns for backward compatibility
    df["striker_sr"] = df["striker_innings_sr"]
    df["striker_runs"] = df["striker_innings_runs"]
    df["bowler_econ"] = df["bowler_innings_econ"]
    df["bowler_wickets"] = df["bowler_innings_wickets"]

    # Derive wicket_fell_next_over for hazard model (from consecutive overs)
    df = df.sort_values(["match_id", "innings", "over_num"]).copy()
    next_wk = df.groupby(["match_id", "innings"])["wickets"].shift(-1)
    df["wicket_fell_next_over"] = (next_wk > df["wickets"]).astype(float)
    # Last over of each innings has no "next" — drop those NaN rows for wicket model only
    # (keep them for run models by leaving NaN; train_one handles it per-target)

    return df


def load_live() -> pd.DataFrame:
    """Load labelled snapshots from live match collection."""
    if not os.path.exists(DB_LIVE):
        return pd.DataFrame()

    conn = sqlite3.connect(DB_LIVE)
    df = pd.read_sql_query("""
        SELECT competition, venue, innings, toss_decision,
               over_num, score, wickets, run_rate, pp_runs AS pp_runs_so_far,
               last_over_runs, phase,
               striker_sr, striker_runs, bowler_econ, bowler_wickets,
               actual_innings_total, actual_pp_total,
               actual_7_15_total, actual_death_total
        FROM over_snapshots
        WHERE label_filled = 1
    """, conn)
    conn.close()

    df["venue_group"] = df["venue"].apply(venue_group)
    return df


# ── Train models ──────────────────────────────────────────────────────────────

FEATURE_COLS = [
    "competition", "venue_group", "toss_decision", "innings",
    "over_num", "score", "wickets", "run_rate", "pp_runs_so_far",
    "last_over_runs", "phase",
    "striker_sr", "striker_runs", "bowler_econ", "bowler_wickets",
]

# v2 expanded features (used when training from real ball-by-ball data)
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

XGB_PARAMS = {
    "n_estimators": 500,       # v2: more trees
    "max_depth": 5,
    "learning_rate": 0.05,     # v2: smaller steps
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 3,
    "objective": "reg:squarederror",
    "random_state": 42,
    "verbosity": 0,
    "early_stopping_rounds": 20,  # v2: prevent overfitting
}


def train_one(
    df: pd.DataFrame,
    target: str,
    le_cache: Dict[str, LabelEncoder],
    label: str,
    feature_cols: List[str] | None = None,
    is_classifier: bool = False,
) -> Tuple[Any, float]:
    """Train one XGBoost model. Returns (model, metric).

    For regressors: metric = cross-validated MAE.
    For classifiers: metric = cross-validated ROC-AUC.
    """
    features = feature_cols or FEATURE_COLS
    df = df[df[target].notna()].copy()
    df = encode_features(df, le_cache, fit=True)

    for col in features:
        if col not in df.columns:
            df[col] = 0
    X = df[features].fillna(0)
    y = df[target].astype(float)

    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.15, random_state=42)
    params = dict(XGB_PARAMS)
    es_rounds = params.pop("early_stopping_rounds", None)

    if is_classifier:
        params["objective"] = "binary:logistic"
        params["eval_metric"] = "logloss"
        model_cv = xgb.XGBClassifier(**params)
        scores = cross_val_score(model_cv, X, y, cv=5, scoring="roc_auc")
        cv_metric = scores.mean()

        model = xgb.XGBClassifier(**params)
        if es_rounds:
            model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)
        else:
            model.fit(X_tr, y_tr)
        from sklearn.metrics import roc_auc_score
        holdout_auc = roc_auc_score(y_te, model.predict_proba(X_te)[:, 1])

        model_full = xgb.XGBClassifier(**params)
        model_full.fit(X, y)

        logger.info(
            "%-25s  rows=%-5d  CV_AUC=%.3f  holdout_AUC=%.3f",
            label, len(df), cv_metric, holdout_auc,
        )
        return model_full, cv_metric
    else:
        model_cv = xgb.XGBRegressor(**params)
        scores = cross_val_score(model_cv, X, y, cv=5, scoring="neg_mean_absolute_error")
        cv_mae = -scores.mean()

        model = xgb.XGBRegressor(**params)
        if es_rounds:
            model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)
        else:
            model.fit(X_tr, y_tr)
        holdout_mae = mean_absolute_error(y_te, model.predict(X_te))

        model_full = xgb.XGBRegressor(**params)
        model_full.fit(X, y)

        logger.info(
            "%-25s  rows=%-5d  CV_MAE=%.1f  holdout_MAE=%.1f",
            label, len(df), cv_mae, holdout_mae,
        )
        return model_full, cv_mae


def train_all(deploy: bool = False) -> Dict[str, Any]:
    """Train all models. Optionally save to MODEL_DIR."""
    logger.info("Loading data...")

    # v2: prefer real ball-by-ball snapshots over synthetic ones
    real = load_real_snapshots()
    live = load_live()

    if len(real) > 0:
        use_v2_features = True
        if len(live) > 0:
            logger.info("Combining %d real + %d live snapshots (v2 mode)", len(real), len(live))
            df = pd.concat([real, live], ignore_index=True)
        else:
            logger.info("Using %d real ball-by-ball snapshots (v2 mode)", len(real))
            df = real
    else:
        use_v2_features = False
        hist = load_historical()
        if len(live) > 0:
            logger.info("Combining %d historical + %d live snapshots (v1 fallback)", len(hist), len(live))
            df = pd.concat([hist, live], ignore_index=True)
        else:
            logger.info("Using %d historical snapshots (v1 fallback)", len(hist))
            df = hist

    active_features = FEATURE_COLS_V2 if use_v2_features else FEATURE_COLS
    logger.info("Feature set: %s (%d features)", "v2" if use_v2_features else "v1", len(active_features))

    logger.info("Training 5 models (4 run + 1 wicket)...")
    results = {}
    le_caches = {}

    targets = [
        ("actual_innings_total", "innings_total   (final score)", False),
        ("actual_pp_total",      "powerplay_total (1-6 overs)  ", False),
        ("actual_7_15_total",    "middle_total    (7-15 overs) ", False),
        ("actual_death_total",   "death_total     (16-20 overs)", False),
        ("wicket_fell_next_over", "wicket_next_over (classifier)", True),
    ]

    for target, label, is_classifier in targets:
        le_cache: Dict[str, LabelEncoder] = {}
        model, metric = train_one(
            df.copy(), target, le_cache, label,
            feature_cols=active_features, is_classifier=is_classifier,
        )
        results[target] = {"model": model, "mae": round(metric, 2), "le": le_cache, "features": active_features}
        le_caches[target] = le_cache

    if deploy:
        os.makedirs(MODEL_DIR, exist_ok=True)
        for target, data in results.items():
            model_path = os.path.join(MODEL_DIR, f"{target}.json")
            data["model"].save_model(model_path)

            # Save label encoders as JSON
            le_path = os.path.join(MODEL_DIR, f"{target}_encoders.json")
            le_data = {
                col: le.classes_.tolist()
                for col, le in data["le"].items()
            }
            with open(le_path, "w") as f:
                json.dump(le_data, f)

            if target == "wicket_fell_next_over":
                logger.info("Saved %s → %s  (AUC %.3f)", target, model_path, data["mae"])
            else:
                logger.info("Saved %s → %s  (MAE ±%.1f runs)", target, model_path, data["mae"])

        # Save training metadata
        meta = {
            "trained_at": __import__("datetime").datetime.utcnow().isoformat(),
            "total_rows": len(df),
            "live_rows": len(live),
            "feature_set": "v2" if use_v2_features else "v1",
            "feature_cols": active_features,
            "models": {
                t: ({"auc": d["mae"]} if t == "wicket_fell_next_over" else {"mae": d["mae"]})
                for t, d in results.items()
            },
        }
        with open(os.path.join(MODEL_DIR, "metadata.json"), "w") as f:
            json.dump(meta, f, indent=2)
        logger.info("Metadata saved → %s/metadata.json", MODEL_DIR)

    return results


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--deploy", action="store_true",
                        help="Save trained models to data/models/ for live use")
    args = parser.parse_args()

    results = train_all(deploy=args.deploy)

    print("\n" + "=" * 55)
    print("  Model Training Summary")
    print("=" * 55)
    for target, data in results.items():
        print(f"  {target:<30}  MAE ±{data['mae']:.1f} runs")
    print("=" * 55)
    if not args.deploy:
        print("\n  Run with --deploy to save models for live use")
        print("  python3 train_model.py --deploy")
