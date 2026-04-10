"""
Tests for ipl_spotter/load_historical_data.py

Covers:
  - compute_phase_runs with various ball distributions
  - Fixture processing / data transformation (mocked API)
  - load_from_sportmonks with mocked HTTP
  - load_all_seasons with mocked HTTP
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from unittest import mock

import pytest

# Ensure project root is on the path so we can import ipl_spotter modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from load_historical_data import (
    compute_phase_runs,
    load_all_seasons,
    load_from_sportmonks,
    _extract_team_name,
    _extract_venue_name,
    _extract_innings_total,
    _determine_winner,
    _process_fixture,
    _nested_data,
    _over_from_ball,
)
from modules.stats_db import StatsDB


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_ball(ball_val: float, scoreboard: str, runs: int) -> dict:
    """Create a minimal Sportmonks ball dict."""
    return {
        "ball": ball_val,
        "scoreboard": scoreboard,
        "score": {"data": {"runs": runs, "four": False, "six": False}},
    }


def _make_fixture(match_id=1001, balls=None, batting=None, bowling=None):
    """Create a minimal Sportmonks fixture dict with nested includes."""
    return {
        "id": match_id,
        "winner_team_id": 10,
        "toss_win_team_id": 10,
        "elected": "batting",
        "localteam": {"data": {"id": 10, "name": "Mumbai Indians"}},
        "visitorteam": {"data": {"id": 20, "name": "Chennai Super Kings"}},
        "venue": {"data": {"name": "Wankhede Stadium"}},
        "runs": {"data": [
            {"inning": 1, "score": 180, "wickets": 6, "overs": 20.0},
            {"inning": 2, "score": 170, "wickets": 8, "overs": 20.0},
        ]},
        "balls": {"data": balls or []},
        "batting": {"data": batting or []},
        "bowling": {"data": bowling or []},
    }


@pytest.fixture
def tmp_db():
    """Yield a StatsDB backed by a temp file, cleaned up after test."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = StatsDB(path)
    yield db
    db.close()
    os.unlink(path)


# ── compute_phase_runs tests ────────────────────────────────────────────────

class TestComputePhaseRuns:
    def test_empty_balls(self):
        result = compute_phase_runs([], "S1")
        assert result == {"powerplay": 0, "middle": 0, "death": 0}

    def test_powerplay_only(self):
        balls = [
            _make_ball(0.1, "S1", 4),
            _make_ball(0.2, "S1", 1),
            _make_ball(5.6, "S1", 6),
        ]
        result = compute_phase_runs(balls, "S1")
        assert result["powerplay"] == 11
        assert result["middle"] == 0
        assert result["death"] == 0

    def test_middle_overs(self):
        balls = [
            _make_ball(6.1, "S1", 2),
            _make_ball(10.3, "S1", 4),
            _make_ball(14.6, "S1", 1),
        ]
        result = compute_phase_runs(balls, "S1")
        assert result["powerplay"] == 0
        assert result["middle"] == 7
        assert result["death"] == 0

    def test_death_overs(self):
        balls = [
            _make_ball(15.1, "S1", 6),
            _make_ball(19.6, "S1", 4),
        ]
        result = compute_phase_runs(balls, "S1")
        assert result["powerplay"] == 0
        assert result["middle"] == 0
        assert result["death"] == 10

    def test_all_phases(self):
        balls = [
            _make_ball(0.1, "S1", 1),   # powerplay
            _make_ball(3.4, "S1", 4),   # powerplay
            _make_ball(7.2, "S1", 2),   # middle
            _make_ball(12.1, "S1", 3),  # middle
            _make_ball(16.3, "S1", 6),  # death
            _make_ball(19.1, "S1", 4),  # death
        ]
        result = compute_phase_runs(balls, "S1")
        assert result["powerplay"] == 5
        assert result["middle"] == 5
        assert result["death"] == 10

    def test_filters_by_scoreboard(self):
        balls = [
            _make_ball(0.1, "S1", 4),
            _make_ball(0.2, "S2", 6),
            _make_ball(1.1, "S1", 2),
        ]
        s1 = compute_phase_runs(balls, "S1")
        s2 = compute_phase_runs(balls, "S2")
        assert s1["powerplay"] == 6
        assert s2["powerplay"] == 6

    def test_none_ball_value_skipped(self):
        balls = [
            {"ball": None, "scoreboard": "S1", "score": {"data": {"runs": 99}}},
            _make_ball(0.1, "S1", 1),
        ]
        result = compute_phase_runs(balls, "S1")
        assert result["powerplay"] == 1

    def test_score_without_data_wrapper(self):
        """Score provided directly (not wrapped in {data: ...})."""
        balls = [
            {"ball": 0.1, "scoreboard": "S1", "score": {"runs": 3}},
        ]
        result = compute_phase_runs(balls, "S1")
        assert result["powerplay"] == 3

    def test_boundary_overs_5_and_6(self):
        """Over 5 is powerplay, over 6 is middle."""
        balls = [
            _make_ball(5.6, "S1", 4),  # over 5 -> powerplay
            _make_ball(6.1, "S1", 2),  # over 6 -> middle
        ]
        result = compute_phase_runs(balls, "S1")
        assert result["powerplay"] == 4
        assert result["middle"] == 2

    def test_boundary_overs_14_and_15(self):
        """Over 14 is middle, over 15 is death."""
        balls = [
            _make_ball(14.6, "S1", 3),  # over 14 -> middle
            _make_ball(15.1, "S1", 6),  # over 15 -> death
        ]
        result = compute_phase_runs(balls, "S1")
        assert result["middle"] == 3
        assert result["death"] == 6


# ── Data extraction helper tests ────────────────────────────────────────────

class TestDataExtraction:
    def test_extract_team_name(self):
        fixture = {"localteam": {"data": {"id": 10, "name": "MI"}}}
        assert _extract_team_name(fixture, "localteam") == "MI"

    def test_extract_team_name_no_data_wrapper(self):
        fixture = {"localteam": {"id": 10, "name": "MI"}}
        assert _extract_team_name(fixture, "localteam") == "MI"

    def test_extract_team_name_missing(self):
        assert _extract_team_name({}, "localteam") == "Unknown"

    def test_extract_venue_name(self):
        fixture = {"venue": {"data": {"name": "Eden Gardens"}}}
        assert _extract_venue_name(fixture) == "Eden Gardens"

    def test_extract_venue_missing(self):
        assert _extract_venue_name({}) == "Unknown"

    def test_extract_innings_total(self):
        runs = [
            {"inning": 1, "score": 185},
            {"inning": 2, "score": 160},
        ]
        assert _extract_innings_total(runs, 1) == 185
        assert _extract_innings_total(runs, 2) == 160
        assert _extract_innings_total(runs, 3) == 0

    def test_determine_winner_localteam(self):
        fixture = {
            "winner_team_id": 10,
            "localteam": {"data": {"id": 10}},
            "visitorteam": {"data": {"id": 20}},
        }
        assert _determine_winner(fixture, "MI", "CSK") == "MI"

    def test_determine_winner_visitorteam(self):
        fixture = {
            "winner_team_id": 20,
            "localteam": {"data": {"id": 10}},
            "visitorteam": {"data": {"id": 20}},
        }
        assert _determine_winner(fixture, "MI", "CSK") == "CSK"

    def test_determine_winner_unknown(self):
        fixture = {
            "winner_team_id": 99,
            "localteam": {"data": {"id": 10}},
            "visitorteam": {"data": {"id": 20}},
        }
        assert _determine_winner(fixture, "MI", "CSK") == "Unknown"

    def test_nested_data_list(self):
        assert _nested_data([1, 2]) == [1, 2]

    def test_nested_data_dict(self):
        assert _nested_data({"data": [3, 4]}) == [3, 4]

    def test_nested_data_none(self):
        assert _nested_data(None) == []

    def test_over_from_ball(self):
        assert _over_from_ball(0.1) == 0
        assert _over_from_ball(5.6) == 5
        assert _over_from_ball(19.3) == 19


# ── Fixture processing (integration with real StatsDB) ──────────────────────

class TestProcessFixture:
    def test_basic_fixture(self, tmp_db):
        balls = [
            _make_ball(0.1, "S1", 4),
            _make_ball(7.1, "S1", 2),
            _make_ball(16.1, "S1", 6),
            _make_ball(0.1, "S2", 3),
        ]
        batting = [
            {
                "player_id": 100,
                "player_name": "Rohit Sharma",
                "score": 45,
                "ball": 30,
                "four_x": 5,
                "six_x": 2,
                "scoreboard": "S1",
            },
        ]
        bowling = [
            {
                "player_id": 200,
                "player_name": "Jasprit Bumrah",
                "overs": 4.0,
                "runs": 28,
                "wickets": 3,
                "scoreboard": "S2",
            },
        ]
        fixture = _make_fixture(match_id=5001, balls=balls, batting=batting, bowling=bowling)
        ok = _process_fixture(fixture, tmp_db)
        assert ok is True

        # Verify match was inserted
        row = tmp_db.conn.execute("SELECT * FROM matches WHERE match_id=5001").fetchone()
        assert row is not None
        assert row["venue"] == "Wankhede Stadium"
        assert row["team1"] == "Mumbai Indians"
        assert row["team2"] == "Chennai Super Kings"
        assert row["powerplay_runs_1st"] == 4
        assert row["middle_runs_1st"] == 2
        assert row["death_runs_1st"] == 6
        assert row["powerplay_runs_2nd"] == 3
        assert row["winner"] == "Mumbai Indians"

        # Verify player innings
        pi = tmp_db.conn.execute(
            "SELECT * FROM player_innings WHERE match_id=5001"
        ).fetchall()
        assert len(pi) == 1
        assert pi[0]["player"] == "Rohit Sharma"
        assert pi[0]["runs"] == 45

        # Verify bowler innings
        bi = tmp_db.conn.execute(
            "SELECT * FROM bowler_innings WHERE match_id=5001"
        ).fetchall()
        assert len(bi) == 1
        assert bi[0]["player"] == "Jasprit Bumrah"
        assert bi[0]["wickets"] == 3

    def test_fixture_no_id_returns_false(self, tmp_db):
        fixture = {"no_id": True}
        assert _process_fixture(fixture, tmp_db) is False


# ── load_from_sportmonks (mocked API) ───────────────────────────────────────

class TestLoadFromSportmonks:
    def test_loads_fixtures_for_season(self, tmp_db):
        fixture = _make_fixture(match_id=7001, balls=[_make_ball(0.1, "S1", 2)])

        def mock_get(url, params=None, timeout=None):
            resp = mock.Mock()
            resp.status_code = 200
            if "/fixtures" in url:
                resp.json.return_value = {"data": [fixture]}
            elif "/leagues/" in url:
                resp.json.return_value = {"data": {"current_season_id": 99}}
            else:
                resp.json.return_value = {"data": []}
            return resp

        with mock.patch("load_historical_data.requests.get", side_effect=mock_get):
            count = load_from_sportmonks(tmp_db, "fake_key", season_id=99)

        assert count == 1
        row = tmp_db.conn.execute("SELECT * FROM matches WHERE match_id=7001").fetchone()
        assert row is not None

    def test_fetches_current_season_when_none(self, tmp_db):
        fixture = _make_fixture(match_id=7002)

        def mock_get(url, params=None, timeout=None):
            resp = mock.Mock()
            resp.status_code = 200
            if "/fixtures" in url:
                resp.json.return_value = {"data": [fixture]}
            elif "/leagues/" in url:
                resp.json.return_value = {"data": {"current_season_id": 42}}
            else:
                resp.json.return_value = {"data": []}
            return resp

        with mock.patch("load_historical_data.requests.get", side_effect=mock_get):
            count = load_from_sportmonks(tmp_db, "fake_key", season_id=None)

        assert count == 1

    def test_returns_zero_when_no_league_data(self, tmp_db):
        def mock_get(url, params=None, timeout=None):
            resp = mock.Mock()
            resp.status_code = 200
            resp.json.return_value = {"data": []}
            return resp

        with mock.patch("load_historical_data.requests.get", side_effect=mock_get):
            count = load_from_sportmonks(tmp_db, "fake_key", season_id=None)

        assert count == 0


# ── load_all_seasons (mocked API) ───────────────────────────────────────────

class TestLoadAllSeasons:
    def test_loads_multiple_seasons(self, tmp_db):
        fixture_s1 = _make_fixture(match_id=8001)
        fixture_s2 = _make_fixture(match_id=8002)

        def mock_get(url, params=None, timeout=None):
            resp = mock.Mock()
            resp.status_code = 200

            if "/leagues/" in url and "include" in (params or {}) and "seasons" in params.get("include", ""):
                resp.json.return_value = {
                    "data": {
                        "id": 1,
                        "seasons": {"data": [
                            {"id": 100, "name": "2023"},
                            {"id": 101, "name": "2024"},
                        ]},
                    }
                }
            elif "/leagues/" in url:
                resp.json.return_value = {"data": {"current_season_id": 100}}
            elif "/fixtures" in url:
                sid = params.get("filter[season_id]")
                if sid == 100:
                    resp.json.return_value = {"data": [fixture_s1]}
                elif sid == 101:
                    resp.json.return_value = {"data": [fixture_s2]}
                else:
                    resp.json.return_value = {"data": []}
            else:
                resp.json.return_value = {"data": []}
            return resp

        with mock.patch("load_historical_data.requests.get", side_effect=mock_get):
            with mock.patch("load_historical_data.time.sleep"):
                count = load_all_seasons(tmp_db, "fake_key")

        assert count == 2

    def test_returns_zero_no_seasons(self, tmp_db):
        def mock_get(url, params=None, timeout=None):
            resp = mock.Mock()
            resp.status_code = 200
            resp.json.return_value = {"data": {"id": 1, "seasons": {"data": []}}}
            return resp

        with mock.patch("load_historical_data.requests.get", side_effect=mock_get):
            count = load_all_seasons(tmp_db, "fake_key")

        assert count == 0
