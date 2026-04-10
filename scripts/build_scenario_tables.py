#!/usr/bin/env python3
"""Build scenario lookup tables from real over-transition data.

Computes per-bucket statistics for:
1. Expected runs per over (with and without wicket falling)
2. Wicket probability per over
3. Grouped by (phase, wicket_bucket, run_rate_bucket)

Output: data/scenario_tables.json
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

DB_PATH = ROOT / "data" / "ml_training_v2.db"
OUT_PATH = ROOT / "data" / "scenario_tables.json"

# ── Bucket definitions (must match situational_model.py) ─────────────

WICKET_BUCKETS = [(0, 1, "0-1"), (2, 3, "2-3"), (4, 5, "4-5"), (6, 7, "6-7"), (8, 10, "8+")]
RR_BUCKETS = [(0.0, 6.0, "<6"), (6.0, 7.5, "6-7.5"), (7.5, 9.0, "7.5-9"), (9.0, 10.5, "9-10.5"), (10.5, 999.0, ">10.5")]


def wicket_bucket(wickets: int) -> str:
    for lo, hi, label in WICKET_BUCKETS:
        if lo <= wickets <= hi:
            return label
    return "8+"


def rr_bucket(run_rate: float) -> str:
    for lo, hi, label in RR_BUCKETS:
        if lo <= run_rate < hi:
            return label
    return ">10.5"


def phase_for_over(over_num: int) -> str:
    if over_num < 6:
        return "powerplay"
    if over_num < 15:
        return "middle"
    return "death"


def std_dev(values: list[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    return math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))


def main() -> None:
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found. Run scripts/build_training_data.py first.")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Load all snapshots sorted by match/innings/over
    rows = conn.execute("""
        SELECT match_id, innings, over_num, score, wickets, run_rate, phase
        FROM real_over_snapshots
        ORDER BY match_id, innings, over_num
    """).fetchall()
    conn.close()

    print(f"Loaded {len(rows)} snapshots")

    # Build consecutive over-pairs
    transitions: list[dict] = []
    prev = None
    for row in rows:
        if prev is not None:
            # Same match and innings, consecutive overs
            if (row["match_id"] == prev["match_id"]
                    and row["innings"] == prev["innings"]
                    and row["over_num"] == prev["over_num"] + 1):
                runs_next = row["score"] - prev["score"]
                wkt_fell = row["wickets"] > prev["wickets"]
                transitions.append({
                    "phase": phase_for_over(prev["over_num"]),
                    "wickets": prev["wickets"],
                    "run_rate": prev["run_rate"] or 0.0,
                    "runs_next": runs_next,
                    "wicket_fell": wkt_fell,
                    "over_num": prev["over_num"],
                })
        prev = row

    print(f"Built {len(transitions)} over-transitions")

    # Group by (phase, wicket_bucket, rr_bucket)
    # Accumulate: runs (all), runs (no wicket), runs (wicket), wicket count
    groups: dict[str, dict] = defaultdict(lambda: {
        "all_runs": [], "no_wkt_runs": [], "wkt_runs": [],
        "wicket_count": 0, "total_count": 0,
    })

    for t in transitions:
        key = f"{t['phase']}:{wicket_bucket(t['wickets'])}:{rr_bucket(t['run_rate'])}"
        g = groups[key]
        g["all_runs"].append(t["runs_next"])
        g["total_count"] += 1
        if t["wicket_fell"]:
            g["wkt_runs"].append(t["runs_next"])
            g["wicket_count"] += 1
        else:
            g["no_wkt_runs"].append(t["runs_next"])

    # Build output JSON
    tables: dict[str, dict] = {}

    for key, g in groups.items():
        # Runs table (no wicket overs)
        if g["no_wkt_runs"]:
            mean = sum(g["no_wkt_runs"]) / len(g["no_wkt_runs"])
            sd = std_dev(g["no_wkt_runs"], mean)
            tables[f"runs:{key}"] = {
                "mean_runs": round(mean, 2),
                "std_runs": round(sd, 2),
                "n": len(g["no_wkt_runs"]),
            }

        # Runs table (wicket overs)
        if g["wkt_runs"]:
            mean = sum(g["wkt_runs"]) / len(g["wkt_runs"])
            sd = std_dev(g["wkt_runs"], mean)
            tables[f"runs_wkt:{key}"] = {
                "mean_runs": round(mean, 2),
                "std_runs": round(sd, 2),
                "n": len(g["wkt_runs"]),
            }

        # Wicket probability
        if g["total_count"] > 0:
            tables[f"wicket:{key}"] = {
                "prob": round(g["wicket_count"] / g["total_count"], 4),
                "n": g["total_count"],
            }

    # Save
    with open(OUT_PATH, "w") as f:
        json.dump(tables, f, indent=2, sort_keys=True)

    # Report
    print(f"\nWritten {len(tables)} entries to {OUT_PATH}")

    # Coverage report
    phases = ["powerplay", "middle", "death"]
    wkt_labels = [b[2] for b in WICKET_BUCKETS]
    rr_labels = [b[2] for b in RR_BUCKETS]
    total_combos = len(phases) * len(wkt_labels) * len(rr_labels)
    covered = 0
    sparse = []
    for p in phases:
        for w in wkt_labels:
            for r in rr_labels:
                k = f"wicket:{p}:{w}:{r}"
                entry = tables.get(k)
                if entry and entry["n"] >= 10:
                    covered += 1
                elif entry:
                    sparse.append(f"  {k}: n={entry['n']} (sparse)")

    print(f"\nCoverage: {covered}/{total_combos} buckets have ≥10 samples ({covered/total_combos*100:.0f}%)")
    if sparse:
        print(f"Sparse buckets ({len(sparse)}):")
        for s in sparse[:10]:
            print(s)

    # Phase-level summary
    print("\nPhase-level wicket rates:")
    for p in phases:
        wkt_total = sum(g["wicket_count"] for k, g in groups.items() if k.startswith(p))
        all_total = sum(g["total_count"] for k, g in groups.items() if k.startswith(p))
        if all_total:
            print(f"  {p:12s}: {wkt_total}/{all_total} = {wkt_total/all_total*100:.1f}%")

    print("\nPhase-level avg runs per over:")
    for p in phases:
        all_runs = []
        for k, g in groups.items():
            if k.startswith(p):
                all_runs.extend(g["all_runs"])
        if all_runs:
            print(f"  {p:12s}: {sum(all_runs)/len(all_runs):.1f} runs/over (n={len(all_runs)})")


if __name__ == "__main__":
    main()
