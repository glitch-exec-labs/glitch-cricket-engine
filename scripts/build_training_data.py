#!/usr/bin/env python3
"""Build real per-over ML training snapshots from Ball_By_Ball_Match_Data.csv.

Replaces the synthetic training data (hardcoded wickets at over 6/10/15) with
real match states at every over boundary, including actual player career stats
computed without data leakage.

Output: data/ml_training_v2.db  table real_over_snapshots  (~20K rows)
"""
from __future__ import annotations

import csv
import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

BBB_CSV = ROOT / "data" / "raw" / "Ball_By_Ball_Match_Data.csv"
MATCH_CSV = ROOT / "data" / "raw" / "Match_Info.csv"
OUT_DB = ROOT / "data" / "ml_training_v2.db"

# ── Schema ──────────────────────────────────────────────────────────

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS real_over_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,
    competition TEXT DEFAULT 'ipl',
    venue TEXT,
    venue_avg_1st REAL,
    home TEXT,
    away TEXT,
    toss_winner TEXT,
    toss_decision TEXT,
    winner TEXT,
    innings INTEGER NOT NULL,
    over_num INTEGER NOT NULL,
    score INTEGER NOT NULL,
    wickets INTEGER NOT NULL,
    run_rate REAL,
    pp_runs_so_far INTEGER,
    last_over_runs INTEGER,
    phase TEXT,
    striker TEXT,
    striker_innings_runs INTEGER,
    striker_innings_balls INTEGER,
    striker_innings_sr REAL,
    striker_career_runs INTEGER,
    striker_career_balls INTEGER,
    striker_career_sr REAL,
    bowler TEXT,
    bowler_innings_runs INTEGER,
    bowler_innings_balls INTEGER,
    bowler_innings_wickets INTEGER,
    bowler_innings_econ REAL,
    bowler_career_runs INTEGER,
    bowler_career_balls INTEGER,
    bowler_career_wickets INTEGER,
    bowler_career_econ REAL,
    batting_team_form REAL,
    bowling_team_form REAL,
    actual_innings_total INTEGER,
    actual_pp_total INTEGER,
    actual_7_15_total INTEGER,
    actual_death_total INTEGER,
    actual_runs_from_here INTEGER
);
"""


def phase_for_over(over_num: int) -> str:
    if over_num < 6:
        return "powerplay"
    if over_num < 15:
        return "middle"
    return "death"


def main() -> None:
    if not BBB_CSV.exists():
        print(f"ERROR: {BBB_CSV} not found")
        sys.exit(1)
    if not MATCH_CSV.exists():
        print(f"ERROR: {MATCH_CSV} not found")
        sys.exit(1)

    # ── Load match metadata ──────────────────────────────────────────
    print("Loading match metadata...")
    match_meta: dict[int, dict] = {}
    with open(MATCH_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mid = int(row["match_number"])
            match_meta[mid] = {
                "home": row["team1"],
                "away": row["team2"],
                "venue": row.get("venue", ""),
                "toss_winner": row.get("toss_winner", ""),
                "toss_decision": row.get("toss_decision", ""),
                "winner": row.get("winner", ""),
                "date": row.get("match_date", ""),
            }
    print(f"  {len(match_meta)} matches in metadata")

    # Sort matches chronologically for career stat accumulation
    sorted_match_ids = sorted(match_meta.keys(), key=lambda m: match_meta[m]["date"])

    # ── Parse ball-by-ball data grouped by match ─────────────────────
    print("Loading ball-by-ball data...")
    balls_by_match: dict[int, list[dict]] = defaultdict(list)
    with open(BBB_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mid = int(row["ID"])
            balls_by_match[mid].append(row)
    print(f"  {sum(len(v) for v in balls_by_match.values())} balls across {len(balls_by_match)} matches")

    # ── Career stats accumulators (no data leakage) ──────────────────
    # Updated BEFORE processing each match, from all prior matches only.
    bat_career: dict[str, dict] = defaultdict(lambda: {"runs": 0, "balls": 0})
    bowl_career: dict[str, dict] = defaultdict(lambda: {"runs": 0, "balls": 0, "wickets": 0})
    team_totals: dict[str, list[int]] = defaultdict(list)

    # ── Venue averages from all data ─────────────────────────────────
    venue_totals: dict[str, list[int]] = defaultdict(list)
    for mid in sorted_match_ids:
        balls = balls_by_match.get(mid, [])
        meta = match_meta.get(mid, {})
        venue = meta.get("venue", "")
        # Quick pass to get innings totals for venue averages
        inn_score: dict[int, int] = defaultdict(int)
        for b in balls:
            inn = int(b["Innings"])
            inn_score[inn] += int(b["TotalRun"])
        if 1 in inn_score and inn_score[1] > 50:
            venue_totals[venue].append(inn_score[1])

    venue_avg_map: dict[str, float] = {}
    for v, totals in venue_totals.items():
        if len(totals) >= 3:
            venue_avg_map[v] = sum(totals) / len(totals)

    # ── Process matches in chronological order ───────────────────────
    print("Building per-over snapshots...")
    all_snapshots: list[dict] = []
    processed = 0

    for mid in sorted_match_ids:
        balls = balls_by_match.get(mid, [])
        meta = match_meta.get(mid, {})
        if not balls or not meta:
            continue

        venue = meta.get("venue", "")
        venue_avg = venue_avg_map.get(venue, 167.0)

        # Snapshot career stats BEFORE this match
        bat_career_snap = {k: dict(v) for k, v in bat_career.items()}
        bowl_career_snap = {k: dict(v) for k, v in bowl_career.items()}
        team_form_snap = {k: list(v) for k, v in team_totals.items()}

        # Sort balls by innings, over, ball number
        balls.sort(key=lambda b: (int(b["Innings"]), int(b["Overs"]), int(b["BallNumber"])))

        # Group by innings
        innings_balls: dict[int, list[dict]] = defaultdict(list)
        for b in balls:
            innings_balls[int(b["Innings"])].append(b)

        # Compute innings totals for labels
        innings_totals: dict[int, int] = {}
        innings_phase_runs: dict[int, dict[str, int]] = {}
        for inn_num, inn_balls in innings_balls.items():
            if inn_num > 2:
                continue
            total = sum(int(b["TotalRun"]) for b in inn_balls)
            innings_totals[inn_num] = total
            pp = mid_r = death = 0
            for b in inn_balls:
                ov = int(b["Overs"])
                runs = int(b["TotalRun"])
                if ov < 6:
                    pp += runs
                elif ov < 15:
                    mid_r += runs
                else:
                    death += runs
            innings_phase_runs[inn_num] = {"pp": pp, "middle": mid_r, "death": death}

        # Process each innings
        for inn_num in [1, 2]:
            inn_balls = innings_balls.get(inn_num, [])
            if not inn_balls:
                continue

            actual_total = innings_totals.get(inn_num)
            if actual_total is None or actual_total < 20:
                continue
            phases = innings_phase_runs.get(inn_num, {})
            actual_pp = phases.get("pp", 0)
            actual_mid = phases.get("middle", 0)
            actual_death = phases.get("death", 0)

            batting_team = inn_balls[0].get("BattingTeam", "")
            bowling_team = meta["away"] if batting_team == meta["home"] else meta["home"]

            # Team form: average of last 10 innings totals
            bat_form_list = team_form_snap.get(batting_team, [])
            bowl_form_list = team_form_snap.get(bowling_team, [])
            batting_team_form = sum(bat_form_list[-10:]) / len(bat_form_list[-10:]) if bat_form_list else 167.0
            bowling_team_form = sum(bowl_form_list[-10:]) / len(bowl_form_list[-10:]) if bowl_form_list else 167.0

            # Walk through balls, emit snapshot at end of each over
            score = 0
            wickets = 0
            over_runs: dict[int, int] = defaultdict(int)
            # Track batsman/bowler innings stats
            bat_inn: dict[str, dict] = defaultdict(lambda: {"runs": 0, "balls": 0})
            bowl_inn: dict[str, dict] = defaultdict(lambda: {"runs": 0, "balls": 0, "wk": 0})
            current_striker = ""
            current_bowler = ""

            prev_over = -1
            for b in inn_balls:
                ov = int(b["Overs"])
                bat_run = int(b["BatsmanRun"])
                total_run = int(b["TotalRun"])
                is_wicket = int(b["IsWicketDelivery"])
                batter = b["Batter"]
                bowler = b["Bowler"]
                extra_type = b.get("ExtraType", "")

                score += total_run
                over_runs[ov] += total_run

                # Track batting (only count legal deliveries for balls faced)
                bat_inn[batter]["runs"] += bat_run
                is_legal = extra_type not in ("wides",)
                if is_legal:
                    bat_inn[batter]["balls"] += 1

                # Track bowling
                if is_legal:
                    bowl_inn[bowler]["balls"] += 1
                bowl_inn[bowler]["runs"] += total_run
                if is_wicket:
                    wickets += 1
                    bowl_inn[bowler]["wk"] += 1

                current_striker = batter
                current_bowler = bowler

                # Emit snapshot at end of each over (when over number changes or last ball)
                if ov != prev_over and prev_over >= 0:
                    # Snapshot for the COMPLETED over (prev_over)
                    completed_over = prev_over
                    snap_score = score - total_run  # score before this ball
                    # Recalculate: use cumulative up to end of prev_over
                    snap_score = sum(over_runs[o] for o in range(completed_over + 1))
                    snap_wickets = wickets - (1 if is_wicket else 0)
                    overs_done = completed_over + 1
                    snap_rr = snap_score / overs_done if overs_done > 0 else 0.0
                    pp_so_far = sum(over_runs[o] for o in range(min(6, completed_over + 1)))
                    last_ov_runs = over_runs[completed_over]

                    # Get striker/bowler from previous over's last ball
                    s_inn = bat_inn.get(current_striker, {"runs": 0, "balls": 0})
                    s_career = bat_career_snap.get(current_striker, {"runs": 0, "balls": 0})
                    b_inn = bowl_inn.get(current_bowler, {"runs": 0, "balls": 0, "wk": 0})
                    b_career = bowl_career_snap.get(current_bowler, {"runs": 0, "balls": 0, "wickets": 0})

                    all_snapshots.append({
                        "match_id": mid,
                        "venue": venue,
                        "venue_avg_1st": venue_avg,
                        "home": meta["home"],
                        "away": meta["away"],
                        "toss_winner": meta["toss_winner"],
                        "toss_decision": meta["toss_decision"],
                        "winner": meta["winner"],
                        "innings": inn_num,
                        "over_num": completed_over,
                        "score": snap_score,
                        "wickets": snap_wickets,
                        "run_rate": round(snap_rr, 2),
                        "pp_runs_so_far": pp_so_far,
                        "last_over_runs": last_ov_runs,
                        "phase": phase_for_over(completed_over),
                        "striker": current_striker,
                        "striker_innings_runs": s_inn["runs"],
                        "striker_innings_balls": s_inn["balls"],
                        "striker_innings_sr": round(s_inn["runs"] / s_inn["balls"] * 100, 1) if s_inn["balls"] > 0 else 0.0,
                        "striker_career_runs": s_career["runs"],
                        "striker_career_balls": s_career["balls"],
                        "striker_career_sr": round(s_career["runs"] / s_career["balls"] * 100, 1) if s_career["balls"] > 0 else 0.0,
                        "bowler": current_bowler,
                        "bowler_innings_runs": b_inn["runs"],
                        "bowler_innings_balls": b_inn["balls"],
                        "bowler_innings_wickets": b_inn["wk"],
                        "bowler_innings_econ": round(b_inn["runs"] / (b_inn["balls"] / 6), 2) if b_inn["balls"] > 0 else 0.0,
                        "bowler_career_runs": b_career["runs"],
                        "bowler_career_balls": b_career["balls"],
                        "bowler_career_wickets": b_career["wickets"],
                        "bowler_career_econ": round(b_career["runs"] / (b_career["balls"] / 6), 2) if b_career["balls"] > 0 else 0.0,
                        "batting_team_form": round(batting_team_form, 1),
                        "bowling_team_form": round(bowling_team_form, 1),
                        "actual_innings_total": actual_total,
                        "actual_pp_total": actual_pp,
                        "actual_7_15_total": actual_mid,
                        "actual_death_total": actual_death,
                        "actual_runs_from_here": actual_total - snap_score,
                    })

                prev_over = ov

            # Emit final over snapshot
            if prev_over >= 0:
                completed_over = prev_over
                snap_score = sum(over_runs[o] for o in range(completed_over + 1))
                overs_done = completed_over + 1
                snap_rr = snap_score / overs_done if overs_done > 0 else 0.0
                pp_so_far = sum(over_runs[o] for o in range(min(6, completed_over + 1)))
                last_ov_runs = over_runs[completed_over]

                s_inn = bat_inn.get(current_striker, {"runs": 0, "balls": 0})
                s_career = bat_career_snap.get(current_striker, {"runs": 0, "balls": 0})
                b_inn = bowl_inn.get(current_bowler, {"runs": 0, "balls": 0, "wk": 0})
                b_career = bowl_career_snap.get(current_bowler, {"runs": 0, "balls": 0, "wickets": 0})

                all_snapshots.append({
                    "match_id": mid,
                    "venue": venue,
                    "venue_avg_1st": venue_avg,
                    "home": meta["home"],
                    "away": meta["away"],
                    "toss_winner": meta["toss_winner"],
                    "toss_decision": meta["toss_decision"],
                    "winner": meta["winner"],
                    "innings": inn_num,
                    "over_num": completed_over,
                    "score": snap_score,
                    "wickets": wickets,
                    "run_rate": round(snap_rr, 2),
                    "pp_runs_so_far": pp_so_far,
                    "last_over_runs": last_ov_runs,
                    "phase": phase_for_over(completed_over),
                    "striker": current_striker,
                    "striker_innings_runs": s_inn["runs"],
                    "striker_innings_balls": s_inn["balls"],
                    "striker_innings_sr": round(s_inn["runs"] / s_inn["balls"] * 100, 1) if s_inn["balls"] > 0 else 0.0,
                    "striker_career_runs": s_career["runs"],
                    "striker_career_balls": s_career["balls"],
                    "striker_career_sr": round(s_career["runs"] / s_career["balls"] * 100, 1) if s_career["balls"] > 0 else 0.0,
                    "bowler": current_bowler,
                    "bowler_innings_runs": b_inn["runs"],
                    "bowler_innings_balls": b_inn["balls"],
                    "bowler_innings_wickets": b_inn["wk"],
                    "bowler_innings_econ": round(b_inn["runs"] / (b_inn["balls"] / 6), 2) if b_inn["balls"] > 0 else 0.0,
                    "bowler_career_runs": b_career["runs"],
                    "bowler_career_balls": b_career["balls"],
                    "bowler_career_wickets": b_career["wickets"],
                    "bowler_career_econ": round(b_career["runs"] / (b_career["balls"] / 6), 2) if b_career["balls"] > 0 else 0.0,
                    "batting_team_form": round(batting_team_form, 1),
                    "bowling_team_form": round(bowling_team_form, 1),
                    "actual_innings_total": actual_total,
                    "actual_pp_total": actual_pp,
                    "actual_7_15_total": actual_mid,
                    "actual_death_total": actual_death,
                    "actual_runs_from_here": actual_total - snap_score,
                })

        # Update career accumulators AFTER processing this match
        for b in balls:
            batter = b["Batter"]
            bat_run = int(b["BatsmanRun"])
            extra_type = b.get("ExtraType", "")
            bat_career[batter]["runs"] += bat_run
            if extra_type not in ("wides",):
                bat_career[batter]["balls"] += 1

            bowler = b["Bowler"]
            total_run = int(b["TotalRun"])
            is_wicket = int(b["IsWicketDelivery"])
            bowl_career[bowler]["runs"] += total_run
            if extra_type not in ("wides",):
                bowl_career[bowler]["balls"] += 1
            if is_wicket:
                bowl_career[bowler]["wickets"] += 1

        # Update team totals for form tracking
        for inn_num, total in innings_totals.items():
            inn_b = innings_balls.get(inn_num, [])
            if inn_b:
                bt = inn_b[0].get("BattingTeam", "")
                if bt:
                    team_totals[bt].append(total)

        processed += 1
        if processed % 200 == 0:
            print(f"  Processed {processed}/{len(sorted_match_ids)} matches, {len(all_snapshots)} snapshots so far")

    print(f"\nTotal snapshots: {len(all_snapshots)} from {processed} matches")

    # ── Write to SQLite ──────────────────────────────────────────────
    print(f"Writing to {OUT_DB}...")
    if OUT_DB.exists():
        OUT_DB.unlink()
    conn = sqlite3.connect(str(OUT_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(CREATE_TABLE)

    cols = [
        "match_id", "competition", "venue", "venue_avg_1st", "home", "away",
        "toss_winner", "toss_decision", "winner", "innings", "over_num",
        "score", "wickets", "run_rate", "pp_runs_so_far", "last_over_runs", "phase",
        "striker", "striker_innings_runs", "striker_innings_balls", "striker_innings_sr",
        "striker_career_runs", "striker_career_balls", "striker_career_sr",
        "bowler", "bowler_innings_runs", "bowler_innings_balls", "bowler_innings_wickets",
        "bowler_innings_econ", "bowler_career_runs", "bowler_career_balls",
        "bowler_career_wickets", "bowler_career_econ",
        "batting_team_form", "bowling_team_form",
        "actual_innings_total", "actual_pp_total", "actual_7_15_total",
        "actual_death_total", "actual_runs_from_here",
    ]
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO real_over_snapshots ({', '.join(cols)}) VALUES ({placeholders})"

    batch = []
    for snap in all_snapshots:
        values = tuple(snap.get(c, None) for c in cols)
        batch.append(values)
        if len(batch) >= 1000:
            conn.executemany(insert_sql, batch)
            batch.clear()
    if batch:
        conn.executemany(insert_sql, batch)
    conn.commit()

    # Verify
    count = conn.execute("SELECT COUNT(*) FROM real_over_snapshots").fetchone()[0]
    matches = conn.execute("SELECT COUNT(DISTINCT match_id) FROM real_over_snapshots").fetchone()[0]
    sample = conn.execute(
        "SELECT match_id, innings, over_num, score, wickets, actual_innings_total, striker_career_sr "
        "FROM real_over_snapshots LIMIT 5"
    ).fetchall()
    conn.close()

    print(f"\nDone! {count} snapshots from {matches} matches written to {OUT_DB}")
    print("\nSample rows:")
    for row in sample:
        print(f"  match={row[0]} inn={row[1]} over={row[2]} score={row[3]}/{row[4]} actual={row[5]} career_sr={row[6]}")

    # Leakage check: first match should have zero career stats
    conn = sqlite3.connect(str(OUT_DB))
    first = conn.execute(
        "SELECT striker_career_sr, bowler_career_econ FROM real_over_snapshots ORDER BY match_id, innings, over_num LIMIT 1"
    ).fetchone()
    conn.close()
    print(f"\nData leakage check (first match): striker_career_sr={first[0]}, bowler_career_econ={first[1]}")
    if first[0] == 0.0 and first[1] == 0.0:
        print("  PASS — no data leakage")
    else:
        print("  WARNING — career stats non-zero for first match (possible leakage)")


if __name__ == "__main__":
    main()
