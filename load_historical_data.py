"""
IPL Edge Spotter — load_historical_data.py
Script to populate the SQLite database with IPL historical data from Sportmonks API.

Usage:
    python ipl_spotter/load_historical_data.py --config ipl_spotter_config.json
    python ipl_spotter/load_historical_data.py --config ipl_spotter_config.json --season 123
    python ipl_spotter/load_historical_data.py --config ipl_spotter_config.json --all
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from modules.stats_db import StatsDB

logger = logging.getLogger("ipl_spotter.loader")

DEFAULT_BASE_URL = "https://cricket.sportmonks.com/api/v2.0"
IPL_LEAGUE_ID = 1
REQUEST_TIMEOUT = 20

# ── Phase boundaries (over values from ball.ball field) ──────────────────────

POWERPLAY_MAX_OVER = 5   # overs 0-5  -> ball 0.1 .. 5.6
MIDDLE_MAX_OVER = 14     # overs 6-14 -> ball 6.1 .. 14.6
DEATH_MAX_OVER = 19      # overs 15-19 -> ball 15.1 .. 19.6


# ── Sportmonks API helpers ───────────────────────────────────────────────────

def _api_get(
    base_url: str,
    path: str,
    api_key: str,
    params: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Make a GET request to Sportmonks and return JSON, or None on error."""
    url = f"{base_url}{path}"
    req_params = {"api_token": api_key}
    if params:
        req_params.update(params)

    try:
        resp = requests.get(url, params=req_params, timeout=REQUEST_TIMEOUT)
        if resp.status_code >= 400:
            logger.error("API error: GET %s -> HTTP %d", path, resp.status_code)
            return None
        return resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.error("API request failed: GET %s -> %s", path, exc)
        return None


def _extract_data(response: Optional[Dict[str, Any]]) -> Any:
    """Extract the 'data' key from a Sportmonks response."""
    if response is None:
        return []
    if isinstance(response, dict):
        return response.get("data", [])
    return response


def _nested_data(obj: Any) -> list:
    """Unwrap Sportmonks nested {data: [...]} includes."""
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        return obj.get("data", [])
    return []


# ── Phase-run computation ────────────────────────────────────────────────────

def _over_from_ball(ball_value: float) -> int:
    """
    Extract the over number from a Sportmonks ball value.
    ball_value is e.g. 0.1, 5.6, 19.3.  The integer part is the over number.
    """
    return int(ball_value)


def compute_phase_runs(balls: List[dict], scoreboard: str) -> Dict[str, int]:
    """
    Given ball-by-ball data for one match and a scoreboard identifier
    ('S1' or 'S2'), compute runs scored in each phase of that innings.

    Each ball dict is expected to have:
      - ball:       float like 0.1, 5.6, 19.3  (over.delivery)
      - scoreboard: 'S1' or 'S2'
      - score:      dict or {'data': {'runs': int, ...}}

    Returns: {'powerplay': int, 'middle': int, 'death': int}
    """
    powerplay = 0
    middle = 0
    death = 0

    for b in balls:
        if b.get("scoreboard") != scoreboard:
            continue

        ball_val = b.get("ball")
        if ball_val is None:
            continue

        try:
            ball_val = float(ball_val)
        except (TypeError, ValueError):
            continue

        # Extract runs from score data
        score = b.get("score") or {}
        if isinstance(score, dict) and "data" in score:
            score = score["data"]
        runs = score.get("runs", 0) if isinstance(score, dict) else 0

        over = _over_from_ball(ball_val)

        if over <= POWERPLAY_MAX_OVER:
            powerplay += runs
        elif over <= MIDDLE_MAX_OVER:
            middle += runs
        else:
            death += runs

    return {"powerplay": powerplay, "middle": middle, "death": death}


# ── Fixture processing ───────────────────────────────────────────────────────

def _extract_team_name(fixture: dict, key: str) -> str:
    """Extract team name from nested localteam/visitorteam data."""
    team = fixture.get(key) or {}
    team_data = team.get("data", team) if isinstance(team, dict) else {}
    return team_data.get("name", "Unknown")


def _extract_venue_name(fixture: dict) -> str:
    """Extract venue name from nested venue data."""
    venue = fixture.get("venue") or {}
    venue_data = venue.get("data", venue) if isinstance(venue, dict) else {}
    return venue_data.get("name", "Unknown")


def _extract_innings_total(runs_data: list, inning: int) -> int:
    """Get the total score for a given inning number from runs data."""
    for r in runs_data:
        if r.get("inning") == inning:
            return r.get("score", 0)
    return 0


def _determine_winner(fixture: dict, team1: str, team2: str) -> str:
    """Determine match winner from fixture data."""
    winner_team_id = fixture.get("winner_team_id")
    lt = fixture.get("localteam") or {}
    lt_data = lt.get("data", lt) if isinstance(lt, dict) else {}
    vt = fixture.get("visitorteam") or {}
    vt_data = vt.get("data", vt) if isinstance(vt, dict) else {}

    if winner_team_id == lt_data.get("id"):
        return team1
    elif winner_team_id == vt_data.get("id"):
        return team2
    return "Unknown"


def _process_fixture(fixture: dict, db: StatsDB) -> bool:
    """
    Process a single fixture dict (with includes) and insert data into the DB.
    Returns True if successful.
    """
    match_id = fixture.get("id")
    if not match_id:
        return False

    # Teams and venue
    team1 = _extract_team_name(fixture, "localteam")
    team2 = _extract_team_name(fixture, "visitorteam")
    venue = _extract_venue_name(fixture)

    # Runs data — innings totals
    runs_raw = _nested_data(fixture.get("runs"))
    first_total = _extract_innings_total(runs_raw, 1)
    second_total = _extract_innings_total(runs_raw, 2)

    # Ball data — phase runs
    balls = _nested_data(fixture.get("balls"))
    phase_1st = compute_phase_runs(balls, "S1")
    phase_2nd = compute_phase_runs(balls, "S2")

    # Toss
    toss_winner_id = fixture.get("toss_win_team_id")
    lt_data = _nested_data_obj(fixture.get("localteam"))
    vt_data = _nested_data_obj(fixture.get("visitorteam"))
    toss_winner = team1 if toss_winner_id == lt_data.get("id") else team2
    toss_decision = fixture.get("elected", "Unknown")

    winner = _determine_winner(fixture, team1, team2)

    db.insert_match({
        "match_id": match_id,
        "venue": venue,
        "team1": team1,
        "team2": team2,
        "first_innings_total": first_total,
        "second_innings_total": second_total,
        "powerplay_runs_1st": phase_1st["powerplay"],
        "powerplay_runs_2nd": phase_2nd["powerplay"],
        "middle_runs_1st": phase_1st["middle"],
        "middle_runs_2nd": phase_2nd["middle"],
        "death_runs_1st": phase_1st["death"],
        "death_runs_2nd": phase_2nd["death"],
        "toss_winner": toss_winner,
        "toss_decision": toss_decision,
        "winner": winner,
    })

    # Insert batting stats
    batting = _nested_data(fixture.get("batting"))
    for entry in batting:
        _insert_batting_to_db(entry, match_id, fixture, team1, team2, venue, db)

    # Insert bowling stats
    bowling = _nested_data(fixture.get("bowling"))
    for entry in bowling:
        _insert_bowling(entry, match_id, fixture, team1, team2, venue, db)

    return True


def _nested_data_obj(obj: Any) -> dict:
    """Unwrap a nested {data: {}} object (not list)."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        inner = obj.get("data", obj)
        return inner if isinstance(inner, dict) else obj
    return {}


def _resolve_team_and_opposition(
    scoreboard: str, team1: str, team2: str
) -> Tuple[str, str]:
    """Return (team, opposition) based on scoreboard id."""
    if scoreboard == "S1":
        return team1, team2
    return team2, team1


def _insert_batting_to_db(
    entry: dict,
    match_id: int,
    fixture: dict,
    team1: str,
    team2: str,
    venue: str,
    db: StatsDB,
) -> None:
    """Insert a batting entry into player_innings."""
    scoreboard = entry.get("scoreboard", "S1")
    team, opposition = _resolve_team_and_opposition(scoreboard, team1, team2)

    player_name = entry.get("player_name", f"player_{entry.get('player_id', 'unknown')}")

    db.insert_player_innings({
        "match_id": match_id,
        "player": player_name,
        "team": team,
        "runs": entry.get("score", 0),
        "balls": entry.get("ball", 0),
        "fours": entry.get("four_x", 0),
        "sixes": entry.get("six_x", 0),
        "venue": venue,
        "phase": "full",
        "opposition": opposition,
    })


def _insert_bowling(
    entry: dict,
    match_id: int,
    fixture: dict,
    team1: str,
    team2: str,
    venue: str,
    db: StatsDB,
) -> None:
    """Insert a bowling entry into bowler_innings."""
    scoreboard = entry.get("scoreboard", "S1")
    # Bowler is on the opposite team to the scoreboard batting side
    if scoreboard == "S1":
        team, opposition = team2, team1
    else:
        team, opposition = team1, team2

    player_name = entry.get("player_name", f"player_{entry.get('player_id', 'unknown')}")

    db.insert_bowler_innings({
        "match_id": match_id,
        "player": player_name,
        "team": team,
        "overs": entry.get("overs", 0),
        "runs_conceded": entry.get("runs", 0),
        "wickets": entry.get("wickets", 0),
        "venue": venue,
        "phase": "full",
        "opposition": opposition,
    })


# ── Season loading ───────────────────────────────────────────────────────────

def load_from_sportmonks(
    db: StatsDB,
    api_key: str,
    season_id: Optional[int] = None,
    base_url: str = DEFAULT_BASE_URL,
    league_id: int = IPL_LEAGUE_ID,
) -> int:
    """
    Fetch historical IPL match data from Sportmonks and load into StatsDB.

    If no season_id is provided, fetches the current season from the API.
    Returns the number of matches loaded.
    """
    if season_id is None:
        resp = _api_get(base_url, f"/leagues/{league_id}", api_key)
        league = _extract_data(resp)
        if not isinstance(league, dict):
            logger.error("Could not fetch league data to determine current season")
            return 0
        season_id = league.get("current_season_id")
        if season_id is None:
            logger.error("No current_season_id found in league data")
            return 0
        logger.info("Using current season: %d", season_id)

    # Fetch all finished fixtures for the season
    resp = _api_get(
        base_url,
        "/fixtures",
        api_key,
        params={
            "filter[league_id]": league_id,
            "filter[season_id]": season_id,
            "filter[status]": "Finished",
            "include": "runs,batting,bowling,balls.score,localteam,visitorteam,venue",
        },
    )

    fixtures = _extract_data(resp)
    if not isinstance(fixtures, list):
        logger.error("No fixture data returned for season %s", season_id)
        return 0

    loaded = 0
    for i, fixture in enumerate(fixtures, 1):
        match_id = fixture.get("id", "?")
        try:
            ok = _process_fixture(fixture, db)
            if ok:
                loaded += 1
                logger.info(
                    "Loaded match %s (%d/%d) — season %d",
                    match_id, i, len(fixtures), season_id,
                )
            else:
                logger.warning("Skipped fixture %s (no id)", match_id)
        except Exception:
            logger.exception("Error processing fixture %s", match_id)

    logger.info(
        "Season %d complete: %d/%d matches loaded", season_id, loaded, len(fixtures)
    )
    return loaded


def load_all_seasons(
    db: StatsDB,
    api_key: str,
    base_url: str = DEFAULT_BASE_URL,
    league_id: int = IPL_LEAGUE_ID,
) -> int:
    """
    Fetch all IPL seasons from /leagues/{id}?include=seasons and load each.
    Returns total matches loaded.
    """
    resp = _api_get(
        base_url,
        f"/leagues/{league_id}",
        api_key,
        params={"include": "seasons"},
    )
    league = _extract_data(resp)
    if not isinstance(league, dict):
        logger.error("Could not fetch league data with seasons")
        return 0

    seasons_raw = league.get("seasons") or {}
    seasons = _nested_data(seasons_raw)
    if not seasons:
        logger.error("No seasons found for league %d", league_id)
        return 0

    total = 0
    for season in seasons:
        sid = season.get("id")
        if sid is None:
            continue
        logger.info("Loading season %d (%s) ...", sid, season.get("name", ""))
        count = load_from_sportmonks(db, api_key, season_id=sid, base_url=base_url, league_id=league_id)
        total += count
        # Small delay to be kind to the API
        time.sleep(1)

    logger.info("All seasons loaded: %d total matches", total)
    return total


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load IPL historical data from Sportmonks into StatsDB"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to ipl_spotter_config.json",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Specific season ID to load (default: current season)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="all_seasons",
        help="Load all IPL seasons",
    )
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Load config
    with open(args.config, "r") as f:
        config = json.load(f)

    api_key = config.get("sportmonks_api_key", "")
    base_url = config.get("sportmonks_base_url", DEFAULT_BASE_URL)
    db_path = config.get("db_path", "data/ipl_stats.db")

    if not api_key:
        logger.error("No sportmonks_api_key in config")
        sys.exit(1)

    db = StatsDB(db_path)

    try:
        if args.all_seasons:
            total = load_all_seasons(db, api_key, base_url=base_url)
            logger.info("Done — %d matches loaded across all seasons", total)
        else:
            total = load_from_sportmonks(db, api_key, season_id=args.season, base_url=base_url)
            logger.info("Done — %d matches loaded", total)
    finally:
        db.close()


if __name__ == "__main__":
    main()
