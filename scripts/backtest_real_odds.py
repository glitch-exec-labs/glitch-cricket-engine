#!/usr/bin/env python3
"""Backtest the bot's predictions against real Cloudbet odds from odds_history.db.

For each odds snapshot where an edge was detected, checks whether the signal
(OVER/UNDER) would have won or lost against the actual innings total.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

ODDS_DB = ROOT / "data" / "odds_history.db"
ML_DB = ROOT / "data" / "ml_training.db"


def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))


def main() -> None:
    if not ODDS_DB.exists() or not ML_DB.exists():
        print("ERROR: Required databases not found")
        sys.exit(1)

    # Load actual innings totals
    ml_conn = sqlite3.connect(str(ML_DB))
    ml_conn.row_factory = sqlite3.Row
    outcomes = ml_conn.execute(
        "SELECT match_id, innings, innings_total, pp_runs, middle_runs, death_runs "
        "FROM match_outcomes"
    ).fetchall()
    ml_conn.close()

    # Build actuals lookup: (match_id, innings) → totals
    actuals: dict[tuple[int, int], dict] = {}
    for r in outcomes:
        key = (r["match_id"], r["innings"])
        actuals[key] = {
            "innings_total": r["innings_total"],
            "6_over": r["pp_runs"],
            "powerplay_runs": r["pp_runs"],
            "10_over": r["pp_runs"] + int(r["middle_runs"] * 4 / 9),  # approximate
            "15_over": r["pp_runs"] + r["middle_runs"],
        }

    # Load odds snapshots
    odds_conn = sqlite3.connect(str(ODDS_DB))
    odds_conn.row_factory = sqlite3.Row
    snapshots = odds_conn.execute(
        "SELECT match_id, home, away, innings, overs, score, wickets, "
        "market, line, over_odds, under_odds, model_expected, model_std_dev, edge_runs "
        "FROM odds_snapshots "
        "WHERE market IN ('6_over', '10_over', '15_over', 'innings_total', 'powerplay_runs') "
        "AND line > 0 AND model_expected > 0 "
        "ORDER BY match_id, innings, overs"
    ).fetchall()
    odds_conn.close()

    print(f"Loaded {len(snapshots)} odds snapshots, {len(actuals)} innings outcomes\n")

    # Filter to snapshots where we can settle (have actuals)
    signals: list[dict] = []
    for snap in snapshots:
        key = (snap["match_id"], snap["innings"])
        actual = actuals.get(key)
        if not actual:
            continue

        market = snap["market"]
        actual_val = actual.get(market)
        if actual_val is None:
            continue

        line = float(snap["line"])
        model_expected = float(snap["model_expected"])
        std_dev = float(snap["model_std_dev"]) if snap["model_std_dev"] else 20.0
        over_odds = float(snap["over_odds"]) if snap["over_odds"] else 0
        under_odds = float(snap["under_odds"]) if snap["under_odds"] else 0
        edge_runs = float(snap["edge_runs"]) if snap["edge_runs"] else 0

        # Compute model probability
        if std_dev > 0:
            z = (line - model_expected) / std_dev
            prob_over = 1.0 - normal_cdf(z)
            prob_under = normal_cdf(z)
        else:
            continue

        # Determine direction and EV
        if abs(edge_runs) < 2.0:
            continue  # below signal threshold

        if edge_runs > 0:  # model expects OVER the line
            direction = "OVER"
            odds = over_odds if over_odds > 1.0 else 1.90
            fair_odds = 1.0 / prob_over if prob_over > 0.01 else 99.0
            ev_pct = ((odds / fair_odds) - 1) * 100
        else:
            direction = "UNDER"
            odds = under_odds if under_odds > 1.0 else 1.90
            fair_odds = 1.0 / prob_under if prob_under > 0.01 else 99.0
            ev_pct = ((odds / fair_odds) - 1) * 100

        if ev_pct < 3.0:
            continue  # below min EV threshold

        # Settle
        if direction == "OVER":
            won = actual_val > line
        else:
            won = actual_val < line

        pnl = (odds - 1) if won else -1.0

        signals.append({
            "match_id": snap["match_id"],
            "home": snap["home"],
            "away": snap["away"],
            "innings": snap["innings"],
            "overs": float(snap["overs"]),
            "market": market,
            "direction": direction,
            "line": line,
            "model_expected": model_expected,
            "actual": actual_val,
            "edge_runs": edge_runs,
            "ev_pct": round(ev_pct, 1),
            "odds": round(odds, 2),
            "won": won,
            "pnl": round(pnl, 2),
        })

    # Deduplicate: keep one signal per (match, innings, market, direction) — first one
    seen = set()
    deduped = []
    for s in signals:
        key = (s["match_id"], s["innings"], s["market"], s["direction"])
        if key not in seen:
            seen.add(key)
            deduped.append(s)

    signals = deduped
    print(f"Deduped to {len(signals)} unique signal-level entries\n")

    # ── Report ──────────────────────────────────────────────────────

    print("=" * 70)
    print("  REAL ODDS BACKTEST REPORT")
    print("=" * 70)

    # Overall
    total = len(signals)
    wins = sum(1 for s in signals if s["won"])
    total_pnl = sum(s["pnl"] for s in signals)
    print(f"\n  Total signals: {total}")
    print(f"  Wins: {wins}  ({wins/total*100:.1f}%)" if total else "  No signals")
    print(f"  PnL: {total_pnl:+.2f} units  (ROI: {total_pnl/total*100:.1f}%)" if total else "")

    # By market
    print(f"\n{'Market':<20} {'N':>5} {'Wins':>5} {'Win%':>6} {'PnL':>8} {'ROI%':>7}")
    print("-" * 55)
    by_market = defaultdict(list)
    for s in signals:
        by_market[s["market"]].append(s)
    for mkt in sorted(by_market.keys()):
        ss = by_market[mkt]
        n = len(ss)
        w = sum(1 for s in ss if s["won"])
        pnl = sum(s["pnl"] for s in ss)
        print(f"  {mkt:<18} {n:>5} {w:>5} {w/n*100:>5.1f}% {pnl:>+7.2f} {pnl/n*100:>+6.1f}%")

    # By direction
    print(f"\n{'Direction':<20} {'N':>5} {'Wins':>5} {'Win%':>6} {'PnL':>8} {'ROI%':>7}")
    print("-" * 55)
    for d in ["OVER", "UNDER"]:
        ss = [s for s in signals if s["direction"] == d]
        if not ss:
            continue
        n = len(ss)
        w = sum(1 for s in ss if s["won"])
        pnl = sum(s["pnl"] for s in ss)
        print(f"  {d:<18} {n:>5} {w:>5} {w/n*100:>5.1f}% {pnl:>+7.2f} {pnl/n*100:>+6.1f}%")

    # By EV tier
    print(f"\n{'EV Tier':<20} {'N':>5} {'Wins':>5} {'Win%':>6} {'PnL':>8}")
    print("-" * 48)
    tiers = [(3, 10, "3-10%"), (10, 20, "10-20%"), (20, 50, "20-50%"), (50, 999, "50%+")]
    for lo, hi, label in tiers:
        ss = [s for s in signals if lo <= s["ev_pct"] < hi]
        if not ss:
            continue
        n = len(ss)
        w = sum(1 for s in ss if s["won"])
        pnl = sum(s["pnl"] for s in ss)
        print(f"  {label:<18} {n:>5} {w:>5} {w/n*100:>5.1f}% {pnl:>+7.2f}")

    # Model accuracy: model_expected vs actual
    errors = [abs(s["model_expected"] - s["actual"]) for s in signals if s["actual"]]
    if errors:
        avg_error = sum(errors) / len(errors)
        print(f"\n  Model accuracy: avg |expected - actual| = {avg_error:.1f} runs")

    print("\n" + "=" * 70)

    # Save results
    out_path = ROOT / "data" / "backtest_real_odds_results.json"
    with open(out_path, "w") as f:
        json.dump({
            "total_signals": total,
            "wins": wins,
            "win_pct": round(wins / total * 100, 1) if total else 0,
            "total_pnl": round(total_pnl, 2),
            "roi_pct": round(total_pnl / total * 100, 1) if total else 0,
            "signals": signals[:100],  # sample
        }, f, indent=2)
    print(f"\n  Results saved → {out_path}")


if __name__ == "__main__":
    main()
