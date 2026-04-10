"""
Load PSL historical data from Cricsheet CSV files into StatsDB.

Data: data/raw/psl/ — 314 matches from cricsheet.org/downloads/psl_csv2.zip
Each match has {id}.csv (ball-by-ball) and {id}_info.csv (match metadata).

Usage:
    python load_psl_data.py
    python load_psl_data.py --db data/ipl_stats.db --players
"""

import argparse
import csv
import logging
from collections import defaultdict
from pathlib import Path

from modules.stats_db import StatsDB

logger = logging.getLogger("ipl_spotter.psl_loader")

PSL_DATA_DIR = Path(__file__).parent / "data" / "raw" / "psl"
NON_BOWLER_DISMISSALS = {
    "run out",
    "retired hurt",
    "retired out",
    "obstructing the field",
}


def parse_info_csv(info_path: Path) -> dict:
    """Parse a Cricsheet _info.csv into a match metadata dict."""
    meta = {"teams": [], "venue": "", "toss_winner": "", "toss_decision": "", "winner": "", "season": ""}
    with open(info_path, "r", encoding="utf-8") as f:
        for row in csv.reader(f):
            if len(row) < 3 or row[0] != "info":
                continue
            key = row[1]
            val = row[2]
            if key == "team":
                meta["teams"].append(val)
            elif key == "venue":
                meta["venue"] = val
            elif key == "toss_winner":
                meta["toss_winner"] = val
            elif key == "toss_decision":
                meta["toss_decision"] = val
            elif key == "winner":
                meta["winner"] = val
            elif key == "season":
                meta["season"] = val
    return meta


def compute_phase_runs(ball_path: Path) -> dict:
    """Compute phase runs from a Cricsheet ball CSV."""
    phases = defaultdict(int)
    with open(ball_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            innings = row.get("innings", "1")
            ball_str = row.get("ball", "0.0")
            over = int(float(ball_str))
            runs_off_bat = int(row.get("runs_off_bat", 0) or 0)
            extras = int(row.get("extras", 0) or 0)
            total_run = runs_off_bat + extras

            prefix = f"inn{innings}"
            phases[f"{prefix}_total"] += total_run
            if over < 6:
                phases[f"{prefix}_powerplay"] += total_run
            elif over < 16:
                phases[f"{prefix}_middle"] += total_run
            else:
                phases[f"{prefix}_death"] += total_run
    return dict(phases)


def compute_player_stats(ball_path: Path) -> dict:
    """Compute per-player batting stats from ball CSV."""
    players = defaultdict(lambda: {"runs": 0, "balls": 0, "fours": 0, "sixes": 0, "team": "", "opposition": ""})
    with open(ball_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            innings = row.get("innings", "1")
            striker = row.get("striker", "")
            if not striker:
                continue
            runs = int(row.get("runs_off_bat", 0) or 0)
            batting_team = row.get("batting_team", "")
            bowling_team = row.get("bowling_team", "")

            # Don't count wides as balls faced by batter
            wides = int(row.get("wides", 0) or 0)

            key = (innings, striker)
            players[key]["runs"] += runs
            if wides == 0:
                players[key]["balls"] += 1
            players[key]["team"] = batting_team
            players[key]["opposition"] = bowling_team
            if runs == 4:
                players[key]["fours"] += 1
            elif runs == 6:
                players[key]["sixes"] += 1
    return dict(players)


def load_psl_matches(db: StatsDB, data_dir: Path = PSL_DATA_DIR) -> int:
    """Load PSL match data into StatsDB."""
    info_files = sorted(data_dir.glob("*_info.csv"))
    if not info_files:
        logger.error("No PSL info files found in %s", data_dir)
        return 0

    loaded = 0
    for info_path in info_files:
        match_id_str = info_path.stem.replace("_info", "")
        ball_path = data_dir / f"{match_id_str}.csv"
        if not ball_path.exists():
            continue

        meta = parse_info_csv(info_path)
        phases = compute_phase_runs(ball_path)
        teams = meta.get("teams", [])
        team1 = teams[0] if len(teams) > 0 else ""
        team2 = teams[1] if len(teams) > 1 else ""

        # Use raw Cricsheet numeric ID; these are 7-digit IDs (1_000_000+)
        # which won't collide with IPL match numbers (1–1200).
        db.insert_match({
            "match_id": int(match_id_str),
            "venue": meta["venue"],
            "team1": team1,
            "team2": team2,
            "first_innings_total": phases.get("inn1_total", 0),
            "second_innings_total": phases.get("inn2_total", 0),
            "powerplay_runs_1st": phases.get("inn1_powerplay", 0),
            "powerplay_runs_2nd": phases.get("inn2_powerplay", 0),
            "middle_runs_1st": phases.get("inn1_middle", 0),
            "middle_runs_2nd": phases.get("inn2_middle", 0),
            "death_runs_1st": phases.get("inn1_death", 0),
            "death_runs_2nd": phases.get("inn2_death", 0),
            "toss_winner": meta["toss_winner"],
            "toss_decision": meta["toss_decision"],
            "winner": meta["winner"],
        })
        loaded += 1
        if loaded % 50 == 0:
            logger.info("  Loaded %d PSL matches...", loaded)

    logger.info("Loaded %d PSL matches into database", loaded)
    return loaded


def load_psl_player_stats(db: StatsDB, data_dir: Path = PSL_DATA_DIR) -> int:
    """Load PSL player batting stats into StatsDB."""
    ball_files = sorted(f for f in data_dir.glob("*.csv") if "_info" not in f.name)
    if not ball_files:
        return 0

    # Load venue info from info files
    match_venues = {}
    for info_path in data_dir.glob("*_info.csv"):
        mid = info_path.stem.replace("_info", "")
        meta = parse_info_csv(info_path)
        match_venues[mid] = meta.get("venue", "")

    db.delete_player_innings_for_matches(match_venues.keys())

    loaded = 0
    for ball_path in ball_files:
        mid = ball_path.stem
        venue = match_venues.get(mid, "")
        players = compute_player_stats(ball_path)

        for (innings, player), stats in players.items():
            db.insert_player_innings({
                "match_id": int(mid),
                "player": player,
                "team": stats["team"],
                "runs": stats["runs"],
                "balls": stats["balls"],
                "fours": stats["fours"],
                "sixes": stats["sixes"],
                "venue": venue,
                "phase": "full",
                "opposition": stats["opposition"],
            })
            loaded += 1

    logger.info("Loaded %d PSL player innings", loaded)
    return loaded


def compute_bowler_stats(ball_path: Path) -> dict:
    """Compute per-bowler innings stats from a Cricsheet ball CSV."""
    bowlers = defaultdict(
        lambda: {
            "legal_balls": 0,
            "runs_conceded": 0,
            "wickets": 0,
            "team": "",
            "opposition": "",
        }
    )
    with open(ball_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            innings = row.get("innings", "1")
            bowler = row.get("bowler", "")
            if not bowler:
                continue

            wides = int(row.get("wides", 0) or 0)
            noballs = int(row.get("noballs", 0) or 0)
            runs_off_bat = int(row.get("runs_off_bat", 0) or 0)
            extras = int(row.get("extras", 0) or 0)
            wicket_type = (row.get("wicket_type", "") or "").strip().lower()
            batting_team = row.get("batting_team", "")
            bowling_team = row.get("bowling_team", "")

            key = (innings, bowler)
            bowlers[key]["team"] = bowling_team
            bowlers[key]["opposition"] = batting_team
            bowlers[key]["runs_conceded"] += runs_off_bat + extras
            if wides == 0 and noballs == 0:
                bowlers[key]["legal_balls"] += 1
            if wicket_type and wicket_type not in NON_BOWLER_DISMISSALS:
                bowlers[key]["wickets"] += 1
    return dict(bowlers)


def load_psl_bowler_stats(db: StatsDB, data_dir: Path = PSL_DATA_DIR) -> int:
    """Load PSL bowling stats into StatsDB."""
    ball_files = sorted(f for f in data_dir.glob("*.csv") if "_info" not in f.name)
    if not ball_files:
        return 0

    match_venues = {}
    for info_path in data_dir.glob("*_info.csv"):
        mid = info_path.stem.replace("_info", "")
        meta = parse_info_csv(info_path)
        match_venues[mid] = meta.get("venue", "")

    db.delete_bowler_innings_for_matches(match_venues.keys())

    loaded = 0
    for ball_path in ball_files:
        mid = ball_path.stem
        venue = match_venues.get(mid, "")
        bowlers = compute_bowler_stats(ball_path)

        for (_innings, player), stats in bowlers.items():
            db.insert_bowler_innings({
                "match_id": int(mid),
                "player": player,
                "team": stats["team"],
                "overs": round(stats["legal_balls"] / 6.0, 3),
                "runs_conceded": stats["runs_conceded"],
                "wickets": stats["wickets"],
                "venue": venue,
                "phase": "full",
                "opposition": stats["opposition"],
            })
            loaded += 1

    logger.info("Loaded %d PSL bowler innings", loaded)
    return loaded


def main():
    parser = argparse.ArgumentParser(description="Load PSL data into StatsDB")
    parser.add_argument("--db", default="data/ipl_stats.db", help="Path to SQLite database")
    parser.add_argument("--players", action="store_true", help="Also load player batting stats")
    parser.add_argument("--bowlers", action="store_true", help="Also load player bowling stats")
    parser.add_argument("--data-dir", default=str(PSL_DATA_DIR), help="Path to PSL CSV directory")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    db = StatsDB(args.db)
    data_dir = Path(args.data_dir)

    matches = load_psl_matches(db, data_dir)
    print(f"Loaded {matches} PSL matches")

    if args.players:
        innings = load_psl_player_stats(db, data_dir)
        print(f"Loaded {innings} PSL player innings")
    if args.bowlers:
        bowlers = load_psl_bowler_stats(db, data_dir)
        print(f"Loaded {bowlers} PSL bowler innings")

    db.close()
    print("Done!")


if __name__ == "__main__":
    main()
