"""
IPL Edge Spotter — load_csv_data.py
Load IPL historical data from free CSV files (GitHub/Kaggle) into StatsDB.

Data source: https://github.com/ritesh-ojha/IPL-DATASET
Files needed in ipl_spotter/data/raw/:
  - Ball_By_Ball_Match_Data.csv (278K+ deliveries, 2008-2025)
  - Match_Info.csv (1169 matches)

Usage:
    python ipl_spotter/load_csv_data.py
    python ipl_spotter/load_csv_data.py --db data/ipl_stats.db
"""

import argparse
import csv
import logging
from collections import defaultdict
from pathlib import Path

from modules.stats_db import StatsDB

logger = logging.getLogger("ipl_spotter.csv_loader")

DATA_DIR = Path(__file__).parent / "data" / "raw"
NON_BOWLER_DISMISSALS = {
    "run out",
    "retired hurt",
    "retired out",
    "obstructing the field",
}


def load_matches_and_balls(db: StatsDB, data_dir: Path = DATA_DIR) -> int:
    """
    Load match info + ball-by-ball data, compute phase runs, and insert into DB.
    Returns number of matches loaded.
    """
    match_csv = data_dir / "Match_Info.csv"
    ball_csv = data_dir / "Ball_By_Ball_Match_Data.csv"

    if not match_csv.exists():
        logger.error("Match_Info.csv not found at %s", match_csv)
        return 0
    if not ball_csv.exists():
        logger.error("Ball_By_Ball_Match_Data.csv not found at %s", ball_csv)
        return 0

    # Step 1: Parse ball-by-ball data into per-match, per-innings phase runs
    logger.info("Parsing ball-by-ball data...")
    match_phases = _compute_all_phase_runs(ball_csv)
    logger.info("Computed phase runs for %d matches", len(match_phases))

    # Step 2: Parse match info and combine with phase data
    logger.info("Loading match info...")
    loaded = 0
    with open(match_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            match_id = row.get("match_number", "")
            if not match_id:
                continue

            phases = match_phases.get(match_id, {})
            venue = row.get("venue", "")
            team1 = row.get("team1", "")
            team2 = row.get("team2", "")

            db.insert_match({
                "match_id": match_id,
                "venue": venue,
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
                "toss_winner": row.get("toss_winner", ""),
                "toss_decision": row.get("toss_decision", ""),
                "winner": row.get("winner", ""),
            })
            loaded += 1

            if loaded % 200 == 0:
                logger.info("  Loaded %d matches...", loaded)

    logger.info("Loaded %d matches into database", loaded)
    return loaded


def load_player_stats(db: StatsDB, data_dir: Path = DATA_DIR) -> int:
    """
    Compute per-player batting stats from ball-by-ball data and insert into DB.
    Returns number of player innings loaded.
    """
    ball_csv = data_dir / "Ball_By_Ball_Match_Data.csv"
    match_csv = data_dir / "Match_Info.csv"

    if not ball_csv.exists() or not match_csv.exists():
        logger.error("CSV files not found")
        return 0

    # Load match metadata for venue and teams
    match_meta = {}
    with open(match_csv, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mid = row.get("match_number", "")
            match_meta[mid] = {
                "venue": row.get("venue", ""),
                "team1": row.get("team1", ""),
                "team2": row.get("team2", ""),
            }

    # Aggregate batting stats per player per match per innings
    logger.info("Computing player batting stats from ball data...")
    player_innings = defaultdict(lambda: {
        "runs": 0, "balls": 0, "fours": 0, "sixes": 0
    })

    with open(ball_csv, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mid = row.get("ID", "")
            innings = row.get("Innings", "1")
            batter = row.get("Batter", "")
            if not batter or not mid:
                continue

            key = (mid, innings, batter)
            batsman_run = int(row.get("BatsmanRun", 0) or 0)
            player_innings[key]["runs"] += batsman_run
            player_innings[key]["balls"] += 1
            player_innings[key]["team"] = row.get("BattingTeam", "")
            if batsman_run == 4:
                player_innings[key]["fours"] += 1
            elif batsman_run == 6:
                player_innings[key]["sixes"] += 1

    db.delete_player_innings_for_matches(match_meta.keys())

    # Insert into DB
    loaded = 0
    for (mid, innings, batter), stats in player_innings.items():
        meta = match_meta.get(mid, {})
        batting_team = stats.get("team", "")
        opposition = _opposition_for_team(
            batting_team,
            meta.get("team1", ""),
            meta.get("team2", ""),
        )

        db.insert_player_innings({
            "match_id": mid,
            "player": batter,
            "team": batting_team,
            "runs": stats["runs"],
            "balls": stats["balls"],
            "fours": stats["fours"],
            "sixes": stats["sixes"],
            "venue": meta.get("venue", ""),
            "phase": "full",
            "opposition": opposition,
        })
        loaded += 1

    logger.info("Loaded %d player innings", loaded)
    return loaded


def load_bowler_stats(db: StatsDB, data_dir: Path = DATA_DIR) -> int:
    """
    Compute per-bowler innings stats from IPL ball-by-ball data and insert into DB.
    Returns number of bowler innings loaded.
    """
    ball_csv = data_dir / "Ball_By_Ball_Match_Data.csv"
    match_csv = data_dir / "Match_Info.csv"

    if not ball_csv.exists() or not match_csv.exists():
        logger.error("CSV files not found")
        return 0

    match_meta = {}
    with open(match_csv, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mid = row.get("match_number", "")
            match_meta[mid] = {
                "venue": row.get("venue", ""),
                "team1": row.get("team1", ""),
                "team2": row.get("team2", ""),
            }

    bowler_innings = defaultdict(lambda: {
        "legal_balls": 0,
        "runs_conceded": 0,
        "wickets": 0,
        "team": "",
        "opposition": "",
    })

    logger.info("Computing bowler stats from ball data...")
    with open(ball_csv, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mid = row.get("ID", "")
            innings = row.get("Innings", "1")
            bowler = row.get("Bowler", "")
            batting_team = row.get("BattingTeam", "")
            meta = match_meta.get(mid, {})
            if not mid or not bowler or not batting_team or not meta:
                continue

            bowling_team = _opposition_for_team(
                batting_team,
                meta.get("team1", ""),
                meta.get("team2", ""),
            )
            if not bowling_team:
                continue

            key = (mid, innings, bowler)
            total_run = int(row.get("TotalRun", 0) or 0)
            extra_type = row.get("ExtraType", "") or ""
            dismissal_kind = (row.get("Kind", "") or "").strip().lower()
            is_wicket = str(row.get("IsWicketDelivery", "")).strip().lower() in {"1", "true", "yes"}

            bowler_innings[key]["runs_conceded"] += total_run
            bowler_innings[key]["team"] = bowling_team
            bowler_innings[key]["opposition"] = batting_team
            if _is_legal_ipl_ball(extra_type):
                bowler_innings[key]["legal_balls"] += 1
            if is_wicket and dismissal_kind not in NON_BOWLER_DISMISSALS:
                bowler_innings[key]["wickets"] += 1

    db.delete_bowler_innings_for_matches(match_meta.keys())

    loaded = 0
    for (mid, innings, bowler), stats in bowler_innings.items():
        meta = match_meta.get(mid, {})
        db.insert_bowler_innings({
            "match_id": mid,
            "player": bowler,
            "team": stats["team"],
            "overs": round(stats["legal_balls"] / 6.0, 3),
            "runs_conceded": stats["runs_conceded"],
            "wickets": stats["wickets"],
            "venue": meta.get("venue", ""),
            "phase": "full",
            "opposition": stats["opposition"],
        })
        loaded += 1

    logger.info("Loaded %d bowler innings", loaded)
    return loaded


def _compute_all_phase_runs(ball_csv: Path) -> dict:
    """
    Parse ball-by-ball CSV and compute phase runs for every match + innings.

    Returns: {match_id: {inn1_total, inn1_powerplay, inn1_middle, inn1_death, inn2_...}}
    """
    matches = defaultdict(lambda: defaultdict(int))

    with open(ball_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mid = row.get("ID", "")
            innings = row.get("Innings", "1")
            over = int(row.get("Overs", 0) or 0)
            total_run = int(row.get("TotalRun", 0) or 0)

            prefix = f"inn{innings}"
            matches[mid][f"{prefix}_total"] += total_run

            if over < 6:
                matches[mid][f"{prefix}_powerplay"] += total_run
            elif over < 15:
                matches[mid][f"{prefix}_middle"] += total_run
            else:
                matches[mid][f"{prefix}_death"] += total_run

    return dict(matches)


def _opposition_for_team(team: str, team1: str, team2: str) -> str:
    if team == team1:
        return team2
    if team == team2:
        return team1
    return ""


def _is_legal_ipl_ball(extra_type: str) -> bool:
    extra = str(extra_type or "").strip().lower()
    return "wides" not in extra and "noballs" not in extra and "no ball" not in extra


def main():
    parser = argparse.ArgumentParser(
        description="Load IPL historical data from CSV into StatsDB"
    )
    parser.add_argument(
        "--db",
        default="data/ipl_stats.db",
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--data-dir",
        default=str(DATA_DIR),
        help="Directory containing CSV files",
    )
    parser.add_argument(
        "--players",
        action="store_true",
        help="Also load player-level batting stats (slower)",
    )
    parser.add_argument(
        "--bowlers",
        action="store_true",
        help="Also load player-level bowling stats",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db = StatsDB(args.db)
    data_dir = Path(args.data_dir)

    try:
        matches = load_matches_and_balls(db, data_dir)
        logger.info("Matches loaded: %d", matches)

        if args.players:
            players = load_player_stats(db, data_dir)
            logger.info("Player innings loaded: %d", players)
        if args.bowlers:
            bowlers = load_bowler_stats(db, data_dir)
            logger.info("Bowler innings loaded: %d", bowlers)
    finally:
        db.close()

    logger.info("Done!")


if __name__ == "__main__":
    main()
