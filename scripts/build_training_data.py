#!/usr/bin/env python3
"""Build real per-over ML training snapshots from ball-by-ball CSV data.

Supports two input formats:
  - IPL custom format  (data/raw/Ball_By_Ball_Match_Data.csv + Match_Info.csv)
  - Cricsheet format   (data/raw/<competition>/*.csv)  — PSL, BBL, international, etc.

Output: data/ml_training_v2.db  table real_over_snapshots

Usage:
  python scripts/build_training_data.py           # rebuild IPL only
  python scripts/build_training_data.py --all     # rebuild IPL + all Cricsheet competitions
  python scripts/build_training_data.py --comp psl  # append/rebuild PSL only
  python scripts/build_training_data.py --comp bbl  # once you add data/raw/bbl/
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

# ── IPL source files ─────────────────────────────────────────────────
IPL_BBB_CSV   = ROOT / "data" / "raw" / "Ball_By_Ball_Match_Data.csv"
IPL_MATCH_CSV = ROOT / "data" / "raw" / "Match_Info.csv"

# ── Cricsheet source dirs (one dir per competition) ──────────────────
CRICSHEET_DIR = ROOT / "data" / "raw"

OUT_DB = ROOT / "data" / "ml_training_v2.db"

# ── Schema ───────────────────────────────────────────────────────────

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS real_over_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id    INTEGER NOT NULL,
    competition TEXT    NOT NULL DEFAULT 'ipl',
    venue       TEXT,
    venue_avg_1st REAL,
    home        TEXT,
    away        TEXT,
    toss_winner TEXT,
    toss_decision TEXT,
    winner      TEXT,
    innings     INTEGER NOT NULL,
    over_num    INTEGER NOT NULL,
    score       INTEGER NOT NULL,
    wickets     INTEGER NOT NULL,
    run_rate    REAL,
    pp_runs_so_far INTEGER,
    last_over_runs INTEGER,
    phase       TEXT,
    striker     TEXT,
    striker_innings_runs    INTEGER,
    striker_innings_balls   INTEGER,
    striker_innings_sr      REAL,
    striker_career_runs     INTEGER,
    striker_career_balls    INTEGER,
    striker_career_sr       REAL,
    bowler      TEXT,
    bowler_innings_runs     INTEGER,
    bowler_innings_balls    INTEGER,
    bowler_innings_wickets  INTEGER,
    bowler_innings_econ     REAL,
    bowler_career_runs      INTEGER,
    bowler_career_balls     INTEGER,
    bowler_career_wickets   INTEGER,
    bowler_career_econ      REAL,
    batting_team_form  REAL,
    bowling_team_form  REAL,
    actual_innings_total INTEGER,
    actual_pp_total      INTEGER,
    actual_7_15_total    INTEGER,
    actual_death_total   INTEGER,
    actual_runs_from_here INTEGER,
    UNIQUE (competition, match_id, innings, over_num)
);
"""

INSERT_COLS = [
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


# ── Helpers ──────────────────────────────────────────────────────────

def phase_for_over(over_num: int) -> str:
    if over_num < 6:
        return "powerplay"
    if over_num < 15:
        return "middle"
    return "death"


def _build_snapshots(
    competition: str,
    sorted_match_ids: list,
    match_meta: dict,       # mid -> {home, away, venue, toss_winner, toss_decision, winner, date}
    balls_by_match: dict,   # mid -> list of normalised ball dicts
    bat_career: dict,       # shared across competitions — cross-comp career stats
    bowl_career: dict,
) -> list[dict]:
    """Core snapshot builder — shared by all competition parsers.

    Each ball dict must have these normalised keys:
      innings (int), over (int), bat_run (int), total_run (int),
      is_wicket (int 0/1), is_wide (bool), batter (str), bowler (str)
    """
    # Venue averages for this competition's data only
    venue_totals: dict[str, list[int]] = defaultdict(list)
    for mid in sorted_match_ids:
        balls = balls_by_match.get(mid, [])
        meta = match_meta.get(mid, {})
        venue = meta.get("venue", "")
        inn_score: dict[int, int] = defaultdict(int)
        for b in balls:
            inn_score[b["innings"]] += b["total_run"]
        if 1 in inn_score and inn_score[1] > 50:
            venue_totals[venue].append(inn_score[1])

    venue_avg_map: dict[str, float] = {}
    for v, totals in venue_totals.items():
        if len(totals) >= 3:
            venue_avg_map[v] = sum(totals) / len(totals)

    # Team form per competition (T20 form is comp-specific)
    team_totals: dict[str, list[int]] = defaultdict(list)

    all_snapshots: list[dict] = []
    processed = 0

    for mid in sorted_match_ids:
        balls = balls_by_match.get(mid, [])
        meta = match_meta.get(mid, {})
        if not balls or not meta:
            continue

        venue = meta.get("venue", "")
        venue_avg = venue_avg_map.get(venue, 167.0)

        # Snapshot career stats BEFORE this match (no leakage)
        bat_career_snap  = {k: dict(v) for k, v in bat_career.items()}
        bowl_career_snap = {k: dict(v) for k, v in bowl_career.items()}
        team_form_snap   = {k: list(v) for k, v in team_totals.items()}

        # Sort balls
        balls.sort(key=lambda b: (b["innings"], b["over"], b.get("ball_num", 0)))

        # Group by innings
        innings_balls: dict[int, list[dict]] = defaultdict(list)
        for b in balls:
            if b["innings"] <= 2:
                innings_balls[b["innings"]].append(b)

        # Compute innings totals for labels
        innings_totals: dict[int, int] = {}
        innings_phase_runs: dict[int, dict[str, int]] = {}
        for inn_num, inn_balls in innings_balls.items():
            total = sum(b["total_run"] for b in inn_balls)
            innings_totals[inn_num] = total
            pp = mid_r = death = 0
            for b in inn_balls:
                ov = b["over"]
                r  = b["total_run"]
                if ov < 6:       pp    += r
                elif ov < 15:    mid_r += r
                else:            death += r
            innings_phase_runs[inn_num] = {"pp": pp, "middle": mid_r, "death": death}

        for inn_num in [1, 2]:
            inn_balls = innings_balls.get(inn_num, [])
            if not inn_balls:
                continue
            actual_total = innings_totals.get(inn_num)
            if actual_total is None or actual_total < 20:
                continue
            phases = innings_phase_runs.get(inn_num, {})

            batting_team  = inn_balls[0].get("batting_team", "")
            bowling_team  = meta["away"] if batting_team == meta["home"] else meta["home"]
            bat_form_list = team_form_snap.get(batting_team, [])
            bowl_form_list = team_form_snap.get(bowling_team, [])
            batting_team_form  = sum(bat_form_list[-10:])  / len(bat_form_list[-10:])  if bat_form_list  else 167.0
            bowling_team_form  = sum(bowl_form_list[-10:]) / len(bowl_form_list[-10:]) if bowl_form_list else 167.0

            score    = 0
            wickets  = 0
            over_runs: dict[int, int] = defaultdict(int)
            bat_inn: dict[str, dict]  = defaultdict(lambda: {"runs": 0, "balls": 0})
            bowl_inn: dict[str, dict] = defaultdict(lambda: {"runs": 0, "balls": 0, "wk": 0})
            current_striker = ""
            current_bowler  = ""
            prev_over = -1

            def _emit_snap(completed_over: int, cur_score: int, cur_wickets: int) -> dict:
                overs_done = completed_over + 1
                snap_rr    = cur_score / overs_done if overs_done > 0 else 0.0
                pp_so_far  = sum(over_runs[o] for o in range(min(6, completed_over + 1)))
                s_inn      = bat_inn.get(current_striker,  {"runs": 0, "balls": 0})
                s_career   = bat_career_snap.get(current_striker, {"runs": 0, "balls": 0})
                b_inn      = bowl_inn.get(current_bowler,  {"runs": 0, "balls": 0, "wk": 0})
                b_career   = bowl_career_snap.get(current_bowler, {"runs": 0, "balls": 0, "wickets": 0})
                return {
                    "match_id": mid,
                    "competition": competition,
                    "venue": venue,
                    "venue_avg_1st": venue_avg,
                    "home": meta["home"],
                    "away": meta["away"],
                    "toss_winner":  meta.get("toss_winner", ""),
                    "toss_decision": meta.get("toss_decision", ""),
                    "winner": meta.get("winner", ""),
                    "innings":  inn_num,
                    "over_num": completed_over,
                    "score":    cur_score,
                    "wickets":  cur_wickets,
                    "run_rate": round(snap_rr, 2),
                    "pp_runs_so_far": pp_so_far,
                    "last_over_runs": over_runs[completed_over],
                    "phase": phase_for_over(completed_over),
                    "striker": current_striker,
                    "striker_innings_runs":  s_inn["runs"],
                    "striker_innings_balls": s_inn["balls"],
                    "striker_innings_sr":    round(s_inn["runs"] / s_inn["balls"] * 100, 1) if s_inn["balls"] > 0 else 0.0,
                    "striker_career_runs":   s_career["runs"],
                    "striker_career_balls":  s_career["balls"],
                    "striker_career_sr":     round(s_career["runs"] / s_career["balls"] * 100, 1) if s_career["balls"] > 0 else 0.0,
                    "bowler": current_bowler,
                    "bowler_innings_runs":     b_inn["runs"],
                    "bowler_innings_balls":    b_inn["balls"],
                    "bowler_innings_wickets":  b_inn["wk"],
                    "bowler_innings_econ":     round(b_inn["runs"] / (b_inn["balls"] / 6), 2) if b_inn["balls"] > 0 else 0.0,
                    "bowler_career_runs":      b_career["runs"],
                    "bowler_career_balls":     b_career["balls"],
                    "bowler_career_wickets":   b_career["wickets"],
                    "bowler_career_econ":      round(b_career["runs"] / (b_career["balls"] / 6), 2) if b_career["balls"] > 0 else 0.0,
                    "batting_team_form":  round(batting_team_form, 1),
                    "bowling_team_form":  round(bowling_team_form, 1),
                    "actual_innings_total": actual_total,
                    "actual_pp_total":    phases.get("pp", 0),
                    "actual_7_15_total":  phases.get("middle", 0),
                    "actual_death_total": phases.get("death", 0),
                    "actual_runs_from_here": actual_total - cur_score,
                }

            for b in inn_balls:
                ov         = b["over"]
                bat_run    = b["bat_run"]
                total_run  = b["total_run"]
                is_wicket  = b["is_wicket"]
                is_wide    = b["is_wide"]
                batter     = b["batter"]
                bowler     = b["bowler"]

                score     += total_run
                over_runs[ov] += total_run

                bat_inn[batter]["runs"] += bat_run
                if not is_wide:
                    bat_inn[batter]["balls"] += 1

                if not is_wide:
                    bowl_inn[bowler]["balls"] += 1
                bowl_inn[bowler]["runs"] += total_run
                if is_wicket:
                    wickets += 1
                    bowl_inn[bowler]["wk"] += 1

                current_striker = batter
                current_bowler  = bowler

                # Emit snapshot at over boundary
                if ov != prev_over and prev_over >= 0:
                    snap_score   = sum(over_runs[o] for o in range(prev_over + 1))
                    snap_wickets = wickets - (1 if is_wicket else 0)
                    all_snapshots.append(_emit_snap(prev_over, snap_score, snap_wickets))

                prev_over = ov

            # Final over
            if prev_over >= 0:
                snap_score = sum(over_runs[o] for o in range(prev_over + 1))
                all_snapshots.append(_emit_snap(prev_over, snap_score, wickets))

        # Update career accumulators AFTER match (no leakage)
        for b in balls:
            bat_career[b["batter"]]["runs"]  += b["bat_run"]
            if not b["is_wide"]:
                bat_career[b["batter"]]["balls"] += 1
            bowl_career[b["bowler"]]["runs"]    += b["total_run"]
            if not b["is_wide"]:
                bowl_career[b["bowler"]]["balls"] += 1
            if b["is_wicket"]:
                bowl_career[b["bowler"]]["wickets"] += 1

        for inn_num, total in innings_totals.items():
            inn_b = innings_balls.get(inn_num, [])
            if inn_b:
                bt = inn_b[0].get("batting_team", "")
                if bt:
                    team_totals[bt].append(total)

        processed += 1
        if processed % 200 == 0:
            print(f"  {competition.upper()}: {processed}/{len(sorted_match_ids)} matches, {len(all_snapshots)} snapshots")

    print(f"  {competition.upper()}: {len(all_snapshots)} snapshots from {processed} matches")
    return all_snapshots


# ── IPL loader ───────────────────────────────────────────────────────

def load_ipl(bat_career: dict, bowl_career: dict) -> list[dict]:
    """Load IPL data from the custom CSV format."""
    if not IPL_BBB_CSV.exists() or not IPL_MATCH_CSV.exists():
        print(f"  IPL CSVs not found, skipping.")
        return []

    print("Loading IPL match metadata...")
    match_meta: dict[int, dict] = {}
    with open(IPL_MATCH_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
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

    print(f"  {len(match_meta)} IPL matches in metadata")
    print("Loading IPL ball-by-ball data...")

    balls_by_match: dict[int, list[dict]] = defaultdict(list)
    with open(IPL_BBB_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mid = int(row["ID"])
            balls_by_match[mid].append({
                "innings":     int(row["Innings"]),
                "over":        int(row["Overs"]),
                "ball_num":    int(row["BallNumber"]),
                "bat_run":     int(row["BatsmanRun"]),
                "total_run":   int(row["TotalRun"]),
                "is_wicket":   int(row["IsWicketDelivery"]),
                "is_wide":     row.get("ExtraType", "") == "wides",
                "batter":      row["Batter"],
                "bowler":      row["Bowler"],
                "batting_team": row.get("BattingTeam", ""),
            })

    total_balls = sum(len(v) for v in balls_by_match.values())
    print(f"  {total_balls} IPL balls across {len(balls_by_match)} matches")

    sorted_ids = sorted(match_meta.keys(), key=lambda m: match_meta[m]["date"])
    return _build_snapshots("ipl", sorted_ids, match_meta, balls_by_match, bat_career, bowl_career)


# ── Cricsheet loader (PSL, BBL, international, etc.) ─────────────────

def _parse_cricsheet_info(info_path: Path) -> dict:
    """Parse a Cricsheet *_info.csv file into a metadata dict."""
    meta: dict = {"teams": [], "toss_winner": "", "toss_decision": "", "winner": "", "venue": ""}
    if not info_path.exists():
        return meta
    try:
        with open(info_path, encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(",", 2)
                if len(parts) < 3 or parts[0] != "info":
                    continue
                key, val = parts[1].strip(), parts[2].strip()
                if key == "team":
                    meta["teams"].append(val)
                elif key == "toss_winner":
                    meta["toss_winner"] = val
                elif key == "toss_decision":
                    meta["toss_decision"] = val
                elif key == "winner":
                    meta["winner"] = val
                elif key == "venue":
                    meta["venue"] = val
    except Exception:
        pass
    return meta


def load_cricsheet(competition: str, bat_career: dict, bowl_career: dict) -> list[dict]:
    """Load data for a competition stored as Cricsheet-format CSVs."""
    comp_dir = CRICSHEET_DIR / competition
    if not comp_dir.exists():
        print(f"  No data directory for {competition} at {comp_dir}, skipping.")
        return []

    # Find all ball CSVs (exclude *_info.csv)
    ball_files = sorted(f for f in comp_dir.glob("*.csv") if not f.stem.endswith("_info"))
    if not ball_files:
        print(f"  No ball CSV files found in {comp_dir}")
        return []

    print(f"Loading {competition.upper()} data ({len(ball_files)} match files)...")

    match_meta:    dict[int, dict]       = {}
    balls_by_match: dict[int, list[dict]] = defaultdict(list)

    for ball_file in ball_files:
        info_file = ball_file.parent / (ball_file.stem + "_info.csv")
        info = _parse_cricsheet_info(info_file)

        first_row = None
        try:
            with open(ball_file, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            if not rows:
                continue

            first_row = rows[0]
            mid = int(first_row["match_id"])

            # Infer home/away from info teams, fall back to batting order
            teams = info.get("teams", [])
            home = teams[0] if len(teams) >= 1 else first_row.get("batting_team", "")
            away = teams[1] if len(teams) >= 2 else first_row.get("bowling_team", "")
            venue = info.get("venue") or first_row.get("venue", "")
            date  = first_row.get("start_date", "")

            match_meta[mid] = {
                "home": home,
                "away": away,
                "venue": venue,
                "toss_winner":  info.get("toss_winner", ""),
                "toss_decision": info.get("toss_decision", ""),
                "winner": info.get("winner", ""),
                "date": date,
            }

            for row in rows:
                # ball field: "0.1" → over=0, ball=1
                ball_str  = row.get("ball", "0.1")
                try:
                    over_f    = float(ball_str)
                    over_num  = int(over_f)
                    ball_num  = round((over_f - over_num) * 10)
                except ValueError:
                    over_num = ball_num = 0

                bat_run   = int(row.get("runs_off_bat", 0) or 0)
                extras    = int(row.get("extras", 0) or 0)
                total_run = bat_run + extras
                is_wicket = 1 if row.get("wicket_type", "").strip() else 0
                is_wide   = bool(row.get("wides", "").strip())

                balls_by_match[mid].append({
                    "innings":     int(row.get("innings", 1)),
                    "over":        over_num,
                    "ball_num":    ball_num,
                    "bat_run":     bat_run,
                    "total_run":   total_run,
                    "is_wicket":   is_wicket,
                    "is_wide":     is_wide,
                    "batter":      row.get("striker", ""),
                    "bowler":      row.get("bowler", ""),
                    "batting_team": row.get("batting_team", ""),
                })

        except Exception as e:
            print(f"  Warning: failed to parse {ball_file.name}: {e}")
            continue

    total_balls = sum(len(v) for v in balls_by_match.values())
    print(f"  {total_balls} {competition.upper()} balls across {len(match_meta)} matches")

    sorted_ids = sorted(match_meta.keys(), key=lambda m: match_meta[m].get("date", ""))
    return _build_snapshots(competition, sorted_ids, match_meta, balls_by_match, bat_career, bowl_career)


# ── DB writer ────────────────────────────────────────────────────────

def write_to_db(snapshots: list[dict], competition: str) -> int:
    """Upsert snapshots into the DB. Returns rows inserted."""
    conn = sqlite3.connect(str(OUT_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(CREATE_TABLE)

    # Delete existing rows for this competition so we do a clean rebuild
    deleted = conn.execute(
        "DELETE FROM real_over_snapshots WHERE competition = ?", (competition,)
    ).rowcount
    if deleted:
        print(f"  Cleared {deleted} existing {competition.upper()} rows")

    placeholders = ", ".join(["?"] * len(INSERT_COLS))
    insert_sql = (
        f"INSERT OR IGNORE INTO real_over_snapshots ({', '.join(INSERT_COLS)}) "
        f"VALUES ({placeholders})"
    )

    batch = []
    for snap in snapshots:
        batch.append(tuple(snap.get(c) for c in INSERT_COLS))
        if len(batch) >= 1000:
            conn.executemany(insert_sql, batch)
            batch.clear()
    if batch:
        conn.executemany(insert_sql, batch)
    conn.commit()

    count = conn.execute(
        "SELECT COUNT(*) FROM real_over_snapshots WHERE competition = ?", (competition,)
    ).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM real_over_snapshots").fetchone()[0]
    comp_counts = conn.execute(
        "SELECT competition, COUNT(*) FROM real_over_snapshots GROUP BY competition"
    ).fetchall()
    conn.close()

    print(f"  Wrote {count} {competition.upper()} snapshots (DB total: {total})")
    print(f"  Distribution: {dict(comp_counts)}")
    return count


# ── Main ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Build ML training snapshots")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--all",  action="store_true", help="Rebuild all competitions")
    grp.add_argument("--comp", metavar="NAME", help="Build one competition (e.g. psl, bbl)")
    args = parser.parse_args()

    # Career stats are shared across competitions — a player's overall career
    # history is the best prior regardless of which league they're playing in.
    bat_career:  dict = defaultdict(lambda: {"runs": 0, "balls": 0})
    bowl_career: dict = defaultdict(lambda: {"runs": 0, "balls": 0, "wickets": 0})

    # Auto-discover Cricsheet competition directories
    cricsheet_comps = sorted(
        d.name for d in CRICSHEET_DIR.iterdir()
        if d.is_dir() and d.name not in ("models",)
        and any(d.glob("*.csv"))
    )

    if args.comp:
        # Single competition
        comps_to_build = [args.comp]
    elif args.all:
        comps_to_build = ["ipl"] + [c for c in cricsheet_comps if c != "psl"] + ["psl"]
        # Build IPL first so career stats flow into PSL/BBL
    else:
        comps_to_build = ["ipl"]

    print(f"Building competitions: {comps_to_build}")
    print(f"Output: {OUT_DB}\n")

    for comp in comps_to_build:
        print(f"=== {comp.upper()} ===")
        if comp == "ipl":
            snapshots = load_ipl(bat_career, bowl_career)
        else:
            snapshots = load_cricsheet(comp, bat_career, bowl_career)
        if snapshots:
            write_to_db(snapshots, comp)
        print()

    # Final summary
    conn = sqlite3.connect(str(OUT_DB))
    summary = conn.execute(
        "SELECT competition, COUNT(*), COUNT(DISTINCT match_id) "
        "FROM real_over_snapshots GROUP BY competition ORDER BY competition"
    ).fetchall()
    conn.close()
    print("=== Final DB summary ===")
    print(f"{'Competition':<15} {'Snapshots':>10} {'Matches':>8}")
    print("-" * 36)
    for comp, snaps, matches in summary:
        print(f"{comp:<15} {snaps:>10,} {matches:>8,}")


if __name__ == "__main__":
    main()
