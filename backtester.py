#!/usr/bin/env python3
"""
Backtester for the Cricket Edge Spotter.

Loads historical match data from the SQLite DB, constructs MatchState objects
at key overs (after 6, 10, 15), runs IPLPredictor predictions, compares to
actual final scores, and simulates edge detection against proxy bookmaker lines.

Usage:
    python3 backtester.py
    python3 backtester.py --seasons 2024,2025
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import sys
from typing import Any

# Ensure project root is on the path
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from modules.match_state import MatchState
from modules.predictor import IPLPredictor
from modules.edge_detector import EdgeDetector

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(_PROJECT_ROOT, "data", "ipl_stats.db")
RESULTS_PATH = os.path.join(_PROJECT_ROOT, "data", "backtest_results.json")

# IPL teams (used to filter out PSL matches when no season filter)
IPL_TEAMS = {
    "Chennai Super Kings", "Mumbai Indians", "Kolkata Knight Riders",
    "Royal Challengers Bangalore", "Royal Challengers Bengaluru",
    "Rajasthan Royals", "Sunrisers Hyderabad", "Delhi Capitals",
    "Delhi Daredevils", "Punjab Kings", "Kings XI Punjab",
    "Gujarat Titans", "Lucknow Super Giants", "Gujarat Lions",
    "Rising Pune Supergiant", "Rising Pune Supergiants",
    "Deccan Chargers", "Pune Warriors", "Kochi Tuskers Kerala",
}

# Match-ID ranges for IPL seasons (approximate, based on Sportmonks IDs).
# These cover the 2024 and 2025 seasons.  When --seasons is not given we
# fall back to filtering by team names so all IPL matches are included.
SEASON_ID_RANGES: dict[int, tuple[int, int]] = {
    2024: (1422119, 1426312),
    2025: (1473438, 1485779),
}

# Checkpoints: (label, overs_completed, phase for MatchState)
CHECKPOINTS = [
    ("after_6", 6),
    ("after_10", 10),
    ("after_15", 15),
]

PROXY_ODDS = 1.90  # typical -110 / 1.90 line odds
EDGE_SPREAD_MIN = 3
EDGE_SPREAD_MAX = 12

random.seed(42)  # reproducible proxy lines

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_match_state_at_over(
    match: dict[str, Any],
    target_over: int,
    innings: int = 1,
) -> MatchState | None:
    """Construct a minimal MatchState at a given over checkpoint.

    We only have phase-level granularity from the DB (powerplay / middle / death),
    so we distribute runs proportionally across overs within each phase to
    approximate the score at a given over.
    """
    suffix = "_1st" if innings == 1 else "_2nd"
    pp_runs = match.get(f"powerplay_runs{suffix}") or 0
    mid_runs = match.get(f"middle_runs{suffix}") or 0
    death_runs = match.get(f"death_runs{suffix}") or 0
    total = match.get(f"first_innings_total" if innings == 1 else "second_innings_total") or 0

    if total <= 0:
        return None

    team1 = match["team1"]
    team2 = match["team2"]
    venue = match["venue"]

    # Batting / bowling assignment: team1 batted first by convention in the DB
    if innings == 1:
        batting_team, bowling_team = team1, team2
    else:
        batting_team, bowling_team = team2, team1

    state = MatchState(batting_team=batting_team, bowling_team=bowling_team, venue=venue)
    state.current_innings = innings

    if innings == 2:
        first_total = match.get("first_innings_total") or 0
        state.target_runs = first_total + 1

    # Distribute runs to get score at target_over using phase data.
    # Powerplay: overs 0-6 (6 overs), Middle: overs 6-15 (9 overs), Death: overs 15-20 (5 overs)
    runs_at_over = 0
    phase_runs_assigned: dict[str, int] = {"powerplay": 0, "middle": 0, "death": 0}

    if target_over <= 6:
        # Partial powerplay: pro-rate
        runs_at_over = int(pp_runs * (target_over / 6.0))
        phase_runs_assigned["powerplay"] = runs_at_over
    elif target_over <= 15:
        runs_at_over = pp_runs + int(mid_runs * ((target_over - 6) / 9.0))
        phase_runs_assigned["powerplay"] = pp_runs
        phase_runs_assigned["middle"] = runs_at_over - pp_runs
    else:
        runs_at_over = pp_runs + mid_runs + int(death_runs * ((target_over - 15) / 5.0))
        phase_runs_assigned["powerplay"] = pp_runs
        phase_runs_assigned["middle"] = mid_runs
        phase_runs_assigned["death"] = runs_at_over - pp_runs - mid_runs

    state.total_runs = runs_at_over
    state.overs_completed = float(target_over)
    state.balls_faced = target_over * 6
    state.current_over = target_over
    state.current_ball = 0
    state.phase_runs = phase_runs_assigned
    # Estimate wickets proportional to progress (no per-ball data available)
    progress = target_over / 20.0
    state.wickets = min(9, int(progress * 3.5))  # ~3.5 wickets in 20 overs avg

    return state


def _is_ipl_match(match: dict[str, Any]) -> bool:
    return match["team1"] in IPL_TEAMS and match["team2"] in IPL_TEAMS


def _load_matches(db_path: str, seasons: list[int] | None = None) -> list[dict[str, Any]]:
    """Load match rows from the SQLite DB, optionally filtered by season."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    if seasons:
        # Use ID ranges for requested seasons; fall back to all IPL if range unknown
        conditions = []
        for s in seasons:
            if s in SEASON_ID_RANGES:
                lo, hi = SEASON_ID_RANGES[s]
                conditions.append(f"(match_id BETWEEN {lo} AND {hi})")
        if conditions:
            where = " OR ".join(conditions)
            sql = f"SELECT * FROM matches WHERE {where} ORDER BY match_id"
        else:
            sql = "SELECT * FROM matches ORDER BY match_id"
    else:
        sql = "SELECT * FROM matches ORDER BY match_id"

    rows = conn.execute(sql).fetchall()
    conn.close()

    matches = [dict(r) for r in rows]

    # Filter to IPL teams only (removes PSL etc.)
    matches = [m for m in matches if _is_ipl_match(m)]

    # Require non-zero totals and complete phase data
    matches = [
        m for m in matches
        if (m.get("first_innings_total") or 0) > 0
        and (m.get("powerplay_runs_1st") or 0) > 0
    ]
    return matches


# ---------------------------------------------------------------------------
# Backtest Engine
# ---------------------------------------------------------------------------


class BacktestResult:
    """Accumulates per-checkpoint prediction vs. actual comparison records."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def add(self, rec: dict[str, Any]) -> None:
        self.records.append(rec)

    # -- aggregate helpers --------------------------------------------------

    def _group_by(self, key: str) -> dict[str, list[dict[str, Any]]]:
        groups: dict[str, list[dict[str, Any]]] = {}
        for r in self.records:
            k = str(r.get(key, "unknown"))
            groups.setdefault(k, []).append(r)
        return groups

    def mae(self, records: list[dict[str, Any]] | None = None) -> float:
        recs = records or self.records
        if not recs:
            return 0.0
        return sum(abs(r["error"]) for r in recs) / len(recs)

    def bias(self, records: list[dict[str, Any]] | None = None) -> float:
        recs = records or self.records
        if not recs:
            return 0.0
        return sum(r["error"] for r in recs) / len(recs)

    def edge_hit_rate(self, records: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        recs = records or self.records
        over_signals = [r for r in recs if r.get("edge_direction") == "OVER"]
        under_signals = [r for r in recs if r.get("edge_direction") == "UNDER"]
        over_hits = sum(1 for r in over_signals if r["actual"] > r["bookmaker_line"])
        under_hits = sum(1 for r in under_signals if r["actual"] < r["bookmaker_line"])
        return {
            "over_signals": len(over_signals),
            "over_hits": over_hits,
            "over_hit_pct": round(100.0 * over_hits / len(over_signals), 1) if over_signals else 0.0,
            "under_signals": len(under_signals),
            "under_hits": under_hits,
            "under_hit_pct": round(100.0 * under_hits / len(under_signals), 1) if under_signals else 0.0,
        }

    def simulated_ev(self, records: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """Simulate flat-stake betting at PROXY_ODDS on every edge signal."""
        recs = records or self.records
        edge_recs = [r for r in recs if r.get("edge_direction") is not None]
        if not edge_recs:
            return {"bets": 0, "wins": 0, "pnl": 0.0, "roi_pct": 0.0}

        wins = 0
        for r in edge_recs:
            if r["edge_direction"] == "OVER" and r["actual"] > r["bookmaker_line"]:
                wins += 1
            elif r["edge_direction"] == "UNDER" and r["actual"] < r["bookmaker_line"]:
                wins += 1

        stake_per_bet = 1.0
        total_staked = stake_per_bet * len(edge_recs)
        total_return = wins * stake_per_bet * PROXY_ODDS
        pnl = total_return - total_staked
        roi = (pnl / total_staked) * 100.0 if total_staked > 0 else 0.0
        return {
            "bets": len(edge_recs),
            "wins": wins,
            "pnl": round(pnl, 2),
            "roi_pct": round(roi, 2),
        }


def run_backtest(matches: list[dict[str, Any]]) -> BacktestResult:
    """Run the full backtest across all matches and checkpoints."""
    predictor = IPLPredictor()
    edge_detector = EdgeDetector({"min_ev_pct": 3.0, "min_edge_runs": 2.0})
    result = BacktestResult()

    for match in matches:
        match_id = match["match_id"]
        venue = match["venue"]
        actual_1st_total = match["first_innings_total"]
        actual_2nd_total = match.get("second_innings_total") or 0
        winner = match.get("winner", "")

        venue_mod = predictor.get_venue_modifier(venue)
        venue_avg = 172.0 + venue_mod

        for label, target_over in CHECKPOINTS:
            # --- First innings predictions ---
            state = _build_match_state_at_over(match, target_over, innings=1)
            if state is None:
                continue

            pred = predictor.predict_innings_total(state, venue_avg=venue_avg)
            predicted = pred["expected"]
            error = predicted - actual_1st_total  # positive = over-predicted
            std_dev = pred["std_dev"]

            # Generate proxy bookmaker line: actual +/- random spread
            spread = random.uniform(EDGE_SPREAD_MIN, EDGE_SPREAD_MAX)
            sign = random.choice([-1, 1])
            bookmaker_line = actual_1st_total + sign * spread

            edge = edge_detector.evaluate_line(
                market=f"innings_total_{label}",
                model_expected=predicted,
                model_std_dev=std_dev,
                bookmaker_line=bookmaker_line,
                over_odds=PROXY_ODDS,
                under_odds=PROXY_ODDS,
            )

            rec = {
                "match_id": match_id,
                "venue": venue,
                "innings": 1,
                "checkpoint": label,
                "target_over": target_over,
                "predicted": round(predicted, 1),
                "actual": actual_1st_total,
                "error": round(error, 1),
                "abs_error": round(abs(error), 1),
                "bookmaker_line": round(bookmaker_line, 1),
                "edge_direction": edge["direction"] if edge else None,
                "edge_ev_pct": edge["ev_pct"] if edge else None,
                "edge_confidence": edge["confidence"] if edge else None,
            }
            result.add(rec)

            # --- Second innings predictions (only after_6 checkpoint for brevity) ---
            if label == "after_6" and actual_2nd_total > 0:
                state2 = _build_match_state_at_over(match, target_over, innings=2)
                if state2 is not None:
                    pred2 = predictor.predict_innings_total(state2, venue_avg=venue_avg)
                    predicted2 = pred2["expected"]
                    error2 = predicted2 - actual_2nd_total

                    spread2 = random.uniform(EDGE_SPREAD_MIN, EDGE_SPREAD_MAX)
                    sign2 = random.choice([-1, 1])
                    bk_line2 = actual_2nd_total + sign2 * spread2

                    edge2 = edge_detector.evaluate_line(
                        market=f"innings_total_2nd_{label}",
                        model_expected=predicted2,
                        model_std_dev=pred2["std_dev"],
                        bookmaker_line=bk_line2,
                        over_odds=PROXY_ODDS,
                        under_odds=PROXY_ODDS,
                    )

                    rec2 = {
                        "match_id": match_id,
                        "venue": venue,
                        "innings": 2,
                        "checkpoint": label,
                        "target_over": target_over,
                        "predicted": round(predicted2, 1),
                        "actual": actual_2nd_total,
                        "error": round(error2, 1),
                        "abs_error": round(abs(error2), 1),
                        "bookmaker_line": round(bk_line2, 1),
                        "edge_direction": edge2["direction"] if edge2 else None,
                        "edge_ev_pct": edge2["ev_pct"] if edge2 else None,
                        "edge_confidence": edge2["confidence"] if edge2 else None,
                    }
                    result.add(rec2)

        # --- Match winner prediction at each checkpoint ---
        for label, target_over in CHECKPOINTS:
            state_mw = _build_match_state_at_over(match, target_over, innings=1)
            if state_mw is None:
                continue
            mw_pred = predictor.predict_match_winner(
                state_mw,
                home=match["team1"],
                away=match["team2"],
                venue_avg=venue_avg,
            )
            home_prob = mw_pred["home_prob"]
            predicted_winner = match["team1"] if home_prob >= 0.5 else match["team2"]
            correct = 1 if predicted_winner == winner else 0

            result.add({
                "match_id": match_id,
                "venue": venue,
                "innings": 0,  # 0 signals match-winner record
                "checkpoint": f"winner_{label}",
                "target_over": target_over,
                "predicted": home_prob,
                "actual": 1 if winner == match["team1"] else 0,
                "error": home_prob - (1 if winner == match["team1"] else 0),
                "abs_error": abs(home_prob - (1 if winner == match["team1"] else 0)),
                "bookmaker_line": 0,
                "edge_direction": None,
                "edge_ev_pct": None,
                "edge_confidence": None,
                "winner_correct": correct,
            })

    return result


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _fmt_pct(val: float) -> str:
    return f"{val:+.1f}%" if val != 0 else "0.0%"


def print_report(result: BacktestResult, matches_count: int) -> dict[str, Any]:
    """Print a human-readable summary and return the full results dict."""

    innings_recs = [r for r in result.records if r["innings"] in (1, 2)]
    winner_recs = [r for r in result.records if r["innings"] == 0]

    print("=" * 72)
    print("  CRICKET EDGE SPOTTER -- BACKTEST REPORT")
    print("=" * 72)
    print(f"  Matches analysed : {matches_count}")
    print(f"  Total records    : {len(result.records)}")
    print(f"    Innings recs   : {len(innings_recs)}")
    print(f"    Winner recs    : {len(winner_recs)}")
    print()

    # --- Per-checkpoint MAE and bias ---
    report: dict[str, Any] = {"matches": matches_count, "checkpoints": {}, "venues": {}, "edge": {}, "ev": {}}

    print("-" * 72)
    print("  INNINGS TOTAL PREDICTION ACCURACY (by checkpoint)")
    print("-" * 72)
    print(f"  {'Checkpoint':<16} {'N':>6} {'MAE':>8} {'Bias':>8}")
    print(f"  {'-'*16} {'-'*6} {'-'*8} {'-'*8}")

    for label, _ in CHECKPOINTS:
        recs = [r for r in innings_recs if r["checkpoint"] == label]
        if not recs:
            continue
        mae = result.mae(recs)
        bias = result.bias(recs)
        print(f"  {label:<16} {len(recs):>6} {mae:>8.1f} {bias:>+8.1f}")
        report["checkpoints"][label] = {
            "n": len(recs),
            "mae": round(mae, 2),
            "bias": round(bias, 2),
        }

    # 2nd innings after_6 separately
    recs_2nd = [r for r in innings_recs if r["innings"] == 2 and r["checkpoint"] == "after_6"]
    if recs_2nd:
        mae2 = result.mae(recs_2nd)
        bias2 = result.bias(recs_2nd)
        print(f"  {'2nd_inn_after_6':<16} {len(recs_2nd):>6} {mae2:>8.1f} {bias2:>+8.1f}")
        report["checkpoints"]["2nd_inn_after_6"] = {
            "n": len(recs_2nd),
            "mae": round(mae2, 2),
            "bias": round(bias2, 2),
        }

    print()

    # --- Edge signal hit rates ---
    print("-" * 72)
    print("  EDGE SIGNAL HIT RATES")
    print("-" * 72)

    for label, _ in CHECKPOINTS:
        recs = [r for r in innings_recs if r["checkpoint"] == label]
        hr = result.edge_hit_rate(recs)
        ev = result.simulated_ev(recs)
        print(f"  {label}:")
        print(f"    OVER  signals: {hr['over_signals']:>5}  hits: {hr['over_hits']:>5}  hit%: {hr['over_hit_pct']:>6.1f}%")
        print(f"    UNDER signals: {hr['under_signals']:>5}  hits: {hr['under_hits']:>5}  hit%: {hr['under_hit_pct']:>6.1f}%")
        print(f"    Sim EV: {ev['bets']} bets, {ev['wins']} wins, PnL={ev['pnl']:+.2f}, ROI={ev['roi_pct']:+.1f}%")
        print()
        report["edge"][label] = hr
        report["ev"][label] = ev

    # Overall edge stats
    all_hr = result.edge_hit_rate(innings_recs)
    all_ev = result.simulated_ev(innings_recs)
    print(f"  OVERALL:")
    print(f"    OVER  signals: {all_hr['over_signals']:>5}  hits: {all_hr['over_hits']:>5}  hit%: {all_hr['over_hit_pct']:>6.1f}%")
    print(f"    UNDER signals: {all_hr['under_signals']:>5}  hits: {all_hr['under_hits']:>5}  hit%: {all_hr['under_hit_pct']:>6.1f}%")
    print(f"    Sim EV: {all_ev['bets']} bets, {all_ev['wins']} wins, PnL={all_ev['pnl']:+.2f}, ROI={all_ev['roi_pct']:+.1f}%")
    print()
    report["edge"]["overall"] = all_hr
    report["ev"]["overall"] = all_ev

    # --- Match winner accuracy ---
    if winner_recs:
        print("-" * 72)
        print("  MATCH WINNER PREDICTION ACCURACY")
        print("-" * 72)
        for label, _ in CHECKPOINTS:
            cp_label = f"winner_{label}"
            recs = [r for r in winner_recs if r["checkpoint"] == cp_label]
            if not recs:
                continue
            correct = sum(r.get("winner_correct", 0) for r in recs)
            pct = 100.0 * correct / len(recs) if recs else 0.0
            print(f"  {cp_label:<22} {len(recs):>5} predictions, {correct:>5} correct ({pct:.1f}%)")
            report["checkpoints"][cp_label] = {
                "n": len(recs),
                "correct": correct,
                "accuracy_pct": round(pct, 1),
            }
        print()

    # --- Venue breakdown (top 10 by match count) ---
    print("-" * 72)
    print("  VENUE BREAKDOWN (1st innings, after_6 checkpoint)")
    print("-" * 72)

    venue_recs = [r for r in innings_recs if r["checkpoint"] == "after_6" and r["innings"] == 1]
    venue_groups: dict[str, list[dict[str, Any]]] = {}
    for r in venue_recs:
        venue_groups.setdefault(r["venue"], []).append(r)

    sorted_venues = sorted(venue_groups.items(), key=lambda x: -len(x[1]))[:15]
    print(f"  {'Venue':<52} {'N':>4} {'MAE':>7} {'Bias':>7}")
    print(f"  {'-'*52} {'-'*4} {'-'*7} {'-'*7}")
    for venue_name, recs in sorted_venues:
        v_mae = result.mae(recs)
        v_bias = result.bias(recs)
        short_name = venue_name[:50]
        print(f"  {short_name:<52} {len(recs):>4} {v_mae:>7.1f} {v_bias:>+7.1f}")
        report["venues"][venue_name] = {
            "n": len(recs),
            "mae": round(v_mae, 2),
            "bias": round(v_bias, 2),
        }

    print()
    print("=" * 72)
    print("  Backtest complete.")
    print(f"  Results saved to: {RESULTS_PATH}")
    print("=" * 72)

    report["all_records"] = result.records
    return report


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Cricket Edge Spotter Backtester")
    parser.add_argument(
        "--seasons",
        type=str,
        default=None,
        help="Comma-separated list of seasons to filter (e.g. 2024,2025). "
             "If omitted, all IPL matches in the DB are used.",
    )
    args = parser.parse_args()

    seasons: list[int] | None = None
    if args.seasons:
        seasons = [int(s.strip()) for s in args.seasons.split(",")]

    print(f"Loading matches from {DB_PATH} ...")
    matches = _load_matches(DB_PATH, seasons=seasons)
    if not matches:
        print("No matches found. Check DB path and season filters.")
        sys.exit(1)

    print(f"Loaded {len(matches)} IPL matches.")
    if seasons:
        print(f"Season filter: {seasons}")
    print("Running backtest ...\n")

    result = run_backtest(matches)
    report = print_report(result, len(matches))

    # Save results (excluding the large all_records for the JSON summary)
    save_data = {k: v for k, v in report.items() if k != "all_records"}
    save_data["sample_records"] = report["all_records"][:50]  # first 50 for inspection

    os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(save_data, f, indent=2, default=str)


if __name__ == "__main__":
    main()
