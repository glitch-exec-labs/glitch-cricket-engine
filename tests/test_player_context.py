from __future__ import annotations

from pathlib import Path

import pytest

from modules.name_matcher import NameMatcher
from modules.player_context import PlayerContext
from modules.stats_db import StatsDB


@pytest.fixture
def player_context(tmp_path: Path) -> PlayerContext:
    db = StatsDB(str(tmp_path / "player_context.db"))

    def add_batting(
        player: str,
        runs: int,
        balls: int,
        count: int = 6,
        venue: str = "M.Chinnaswamy Stadium",
    ) -> None:
        for innings in range(count):
            db.insert_player_innings({
                "match_id": innings + 1,
                "player": player,
                "team": "RCB",
                "runs": runs,
                "balls": balls,
                "fours": 4,
                "sixes": 2,
                "venue": venue,
                "phase": "full",
                "opposition": "SRH",
            })

    def add_bowling(
        player: str,
        overs: float,
        runs_conceded: int,
        count: int = 6,
        venue: str = "M.Chinnaswamy Stadium",
    ) -> None:
        for innings in range(count):
            db.insert_bowler_innings({
                "match_id": innings + 100,
                "player": player,
                "team": "SRH",
                "overs": overs,
                "runs_conceded": runs_conceded,
                "wickets": 1,
                "venue": venue,
                "phase": "full",
                "opposition": "RCB",
            })

    add_batting("V Kohli", runs=58, balls=40)       # SR 145
    add_batting("GJ Maxwell", runs=42, balls=28)    # SR 150
    add_batting("MS Dhoni", runs=33, balls=30)      # SR 110
    add_batting("A Player", runs=39, balls=30)      # SR 130
    add_batting("B Player", runs=39, balls=30)      # SR 130
    add_bowling("JJ Bumrah", overs=4.0, runs_conceded=26)   # econ 6.5
    add_bowling("A Bowler", overs=4.0, runs_conceded=40)    # econ 10.0
    add_bowling("Average Bowler", overs=4.0, runs_conceded=34)  # econ 8.5

    context = PlayerContext(db, NameMatcher(db))
    yield context
    db.close()


def test_above_avg_batsman_positive_adj(player_context: PlayerContext) -> None:
    assert player_context.get_batting_adjustment("Virat Kohli") > 0


def test_below_avg_batsman_negative_adj(player_context: PlayerContext) -> None:
    assert player_context.get_batting_adjustment("MS Dhoni") < 0


def test_good_bowler_negative_adj(player_context: PlayerContext) -> None:
    assert player_context.get_bowling_adjustment("Jasprit Bumrah") < 0


def test_bad_bowler_positive_adj(player_context: PlayerContext) -> None:
    assert player_context.get_bowling_adjustment("A Bowler") > 0


def test_form_multiplier_hot_form(player_context: PlayerContext) -> None:
    assert player_context.get_form_multiplier(160.0, 130.0) > 1.0


def test_combined_bumrah_bowling_reduces_prediction(player_context: PlayerContext) -> None:
    adj = player_context.get_combined_adjustment(
        active_batsmen=[
            {"name": "A Player", "score": 20, "balls": 18, "rate": 111.1},
            {"name": "MS Dhoni", "score": 12, "balls": 11, "rate": 109.0},
        ],
        active_bowler={"name": "Jasprit Bumrah", "overs": 3.0, "runs": 18, "rate": 6.0},
        venue="M.Chinnaswamy Stadium",
        overs_completed=12.0,
    )
    assert adj["confidence"] == "HIGH"
    assert adj["over_adjustment"] < 0
    assert adj["innings_adjustment"] < 0


def test_combined_two_hitters_increases_prediction(player_context: PlayerContext) -> None:
    adj = player_context.get_combined_adjustment(
        active_batsmen=[
            {"name": "Virat Kohli", "score": 45, "balls": 28, "rate": 160.7},
            {"name": "Glenn Maxwell", "score": 12, "balls": 8, "rate": 150.0},
        ],
        active_bowler={"name": "Average Bowler", "overs": 2.0, "runs": 17, "rate": 8.5},
        venue="M.Chinnaswamy Stadium",
        overs_completed=8.0,
    )
    assert adj["confidence"] == "HIGH"
    assert adj["over_adjustment"] > 0
    assert adj["innings_adjustment"] > 0


def test_unknown_players_low_confidence(player_context: PlayerContext) -> None:
    adj = player_context.get_combined_adjustment(
        active_batsmen=[
            {"name": "Unknown One", "score": 10, "balls": 9, "rate": 111.1},
            {"name": "Unknown Two", "score": 14, "balls": 10, "rate": 140.0},
        ],
        active_bowler={"name": "Unknown Bowler", "overs": 1.0, "runs": 8, "rate": 8.0},
        venue="M.Chinnaswamy Stadium",
        overs_completed=5.0,
    )
    assert adj["confidence"] == "LOW"
    assert adj["over_adjustment"] == 0.0
