from __future__ import annotations

from pathlib import Path

import pytest

from modules.name_matcher import NameMatcher
from modules.stats_db import StatsDB


@pytest.fixture
def stats_db(tmp_path: Path) -> StatsDB:
    db = StatsDB(str(tmp_path / "names.db"))

    def add_batting(player: str, count: int) -> None:
        for innings in range(count):
            db.insert_player_innings({
                "match_id": innings + 1,
                "player": player,
                "team": "TeamA",
                "runs": 30,
                "balls": 20,
                "fours": 3,
                "sixes": 1,
                "venue": "Venue",
                "phase": "full",
                "opposition": "TeamB",
            })

    def add_bowling(player: str, count: int) -> None:
        for innings in range(count):
            db.insert_bowler_innings({
                "match_id": innings + 1,
                "player": player,
                "team": "TeamB",
                "overs": 4.0,
                "runs_conceded": 28,
                "wickets": 1,
                "venue": "Venue",
                "phase": "full",
                "opposition": "TeamA",
            })

    add_batting("V Kohli", 5)
    add_batting("RG Sharma", 6)
    add_batting("R Sharma", 1)
    add_batting("MS Dhoni", 4)
    add_batting("Rashid Khan", 3)
    add_bowling("JJ Bumrah", 5)
    add_bowling("Rashid Khan", 4)

    yield db
    db.close()


def test_exact_match(stats_db: StatsDB) -> None:
    matcher = NameMatcher(stats_db)
    assert matcher.match_batsman("V Kohli") == "V Kohli"


def test_full_to_initial(stats_db: StatsDB) -> None:
    matcher = NameMatcher(stats_db)
    assert matcher.match_batsman("Virat Kohli") == "V Kohli"


def test_multiple_surnames_picks_most_prolific(stats_db: StatsDB) -> None:
    matcher = NameMatcher(stats_db)
    assert matcher.match_batsman("Rohit Sharma") == "RG Sharma"


def test_already_abbreviated(stats_db: StatsDB) -> None:
    matcher = NameMatcher(stats_db)
    assert matcher.match_batsman("MS Dhoni") == "MS Dhoni"


def test_unknown_player_returns_none(stats_db: StatsDB) -> None:
    matcher = NameMatcher(stats_db)
    assert matcher.match_batsman("Unknown Player") is None


def test_does_not_force_wrong_same_surname_match(stats_db: StatsDB) -> None:
    # Add only Shadab Khan to DB; Sarfaraz Khan should stay unmatched (None)
    for innings in range(3):
        stats_db.insert_player_innings({
            "match_id": 1000 + innings,
            "player": "Shadab Khan",
            "team": "TeamA",
            "runs": 28,
            "balls": 20,
            "fours": 2,
            "sixes": 1,
            "venue": "Venue",
            "phase": "full",
            "opposition": "TeamB",
        })

    matcher = NameMatcher(stats_db)
    assert matcher.match_batsman("Sarfaraz Khan") is None


def test_cache_works(stats_db: StatsDB, monkeypatch: pytest.MonkeyPatch) -> None:
    matcher = NameMatcher(stats_db)
    assert matcher.match_bowler("Jasprit Bumrah") == "JJ Bumrah"

    def fail(*_args: object, **_kwargs: object) -> str:
        raise AssertionError("cache miss")

    monkeypatch.setattr(matcher, "_fuzzy_match", fail)
    assert matcher.match_bowler("Jasprit Bumrah") == "JJ Bumrah"
