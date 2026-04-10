"""
Tests for ipl_spotter.modules.cricket_client — CricketClient.

All tests mock the network layer so no real API calls are made.
"""

import pytest
import requests
from unittest.mock import patch, MagicMock

from modules.cricket_client import CricketClient


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def config():
    return {
        "sportmonks_api_key": "test_key_123",
        "sportmonks_base_url": "https://cricket.sportmonks.com/api/v2.0",
        "ipl_league_id": 1,
    }


@pytest.fixture
def client(config):
    return CricketClient(config)


# ── Initialization tests ─────────────────────────────────────────────────────

class TestInit:
    def test_basic_init(self, config):
        c = CricketClient(config)
        assert c.api_key == "test_key_123"
        assert c.base_url == "https://cricket.sportmonks.com/api/v2.0"
        assert c.league_id == 1
        assert c.timeout == 10

    def test_default_base_url(self):
        c = CricketClient({"sportmonks_api_key": "k"})
        assert c.base_url == "https://cricket.sportmonks.com/api/v2.0"

    def test_trailing_slash_stripped(self):
        c = CricketClient({
            "sportmonks_api_key": "k",
            "sportmonks_base_url": "https://example.com/api/",
        })
        assert c.base_url == "https://example.com/api"

    def test_missing_api_key_warns(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            CricketClient({})
        assert "sportmonks_api_key not set" in caplog.text

    def test_custom_league_id(self):
        c = CricketClient({"sportmonks_api_key": "k", "ipl_league_id": 99})
        assert c.league_id == 99


# ── _extract_data tests ──────────────────────────────────────────────────────

class TestExtractData:
    def test_none_returns_empty_list(self):
        assert CricketClient._extract_data(None) == []

    def test_dict_with_data(self):
        assert CricketClient._extract_data({"data": [1, 2, 3]}) == [1, 2, 3]

    def test_dict_without_data(self):
        assert CricketClient._extract_data({"other": "value"}) == []

    def test_list_passthrough(self):
        assert CricketClient._extract_data([1, 2]) == [1, 2]


# ── _extract_nested_data tests ───────────────────────────────────────────────

class TestExtractNestedData:
    def test_none(self):
        assert CricketClient._extract_nested_data(None) == []

    def test_list_passthrough(self):
        assert CricketClient._extract_nested_data([{"ball": 0.1}]) == [{"ball": 0.1}]

    def test_dict_with_data(self):
        result = CricketClient._extract_nested_data({"data": [{"ball": 0.2}]})
        assert result == [{"ball": 0.2}]

    def test_dict_without_data(self):
        assert CricketClient._extract_nested_data({"other": 1}) == []


# ── parse_ball_event tests ───────────────────────────────────────────────────

class TestParseBallEvent:
    def test_normal_ball(self):
        ball = {
            "ball": 0.1,
            "scoreboard": "S1",
            "batsman_id": 3431,
            "bowler_id": 4880,
            "batsmanout_id": None,
            "score": {
                "name": "1 Run",
                "runs": 1,
                "four": False,
                "six": False,
                "is_wicket": False,
                "ball": True,
            },
        }
        parsed = CricketClient.parse_ball_event(ball)
        assert parsed["over_ball"] == 0.1
        assert parsed["innings"] == "S1"
        assert parsed["batsman_id"] == 3431
        assert parsed["bowler_id"] == 4880
        assert parsed["is_wicket"] is False
        assert parsed["runs"] == 1
        assert parsed["is_four"] is False
        assert parsed["is_six"] is False
        assert parsed["is_legal"] is True

    def test_wicket_ball(self):
        ball = {
            "ball": 3.4,
            "scoreboard": "S1",
            "batsman_id": 100,
            "bowler_id": 200,
            "batsmanout_id": 100,
            "score": {
                "name": "Wicket",
                "runs": 0,
                "four": False,
                "six": False,
                "is_wicket": True,
                "ball": True,
            },
        }
        parsed = CricketClient.parse_ball_event(ball)
        assert parsed["is_wicket"] is True
        assert parsed["batsmanout_id"] == 100
        assert parsed["runs"] == 0

    def test_wide_ball(self):
        ball = {
            "ball": 0.1,
            "scoreboard": "S1",
            "batsman_id": 3431,
            "bowler_id": 4880,
            "batsmanout_id": None,
            "score": {
                "name": "1 Wide",
                "runs": 1,
                "four": False,
                "six": False,
                "is_wicket": False,
                "ball": False,
            },
        }
        parsed = CricketClient.parse_ball_event(ball)
        assert parsed["is_legal"] is False
        assert parsed["runs"] == 1
        assert parsed["score_name"] == "1 Wide"

    def test_six_ball(self):
        ball = {
            "ball": 19.5,
            "scoreboard": "S2",
            "batsman_id": 500,
            "bowler_id": 600,
            "batsmanout_id": None,
            "score": {
                "name": "6 Runs",
                "runs": 6,
                "four": False,
                "six": True,
                "is_wicket": False,
                "ball": True,
            },
        }
        parsed = CricketClient.parse_ball_event(ball)
        assert parsed["is_six"] is True
        assert parsed["runs"] == 6
        assert parsed["innings"] == "S2"

    def test_nested_score_data(self):
        """Score wrapped in {"data": {...}} format."""
        ball = {
            "ball": 1.1,
            "scoreboard": "S1",
            "batsman_id": 10,
            "bowler_id": 20,
            "batsmanout_id": None,
            "score": {
                "data": {
                    "name": "4 Runs",
                    "runs": 4,
                    "four": True,
                    "six": False,
                    "is_wicket": False,
                    "ball": True,
                }
            },
        }
        parsed = CricketClient.parse_ball_event(ball)
        assert parsed["is_four"] is True
        assert parsed["runs"] == 4

    def test_missing_score(self):
        ball = {"ball": 0.1, "scoreboard": "S1", "batsman_id": 1, "bowler_id": 2, "batsmanout_id": None}
        parsed = CricketClient.parse_ball_event(ball)
        assert parsed["runs"] == 0
        assert parsed["is_legal"] is True


# ── parse_innings_runs tests ─────────────────────────────────────────────────

class TestParseInningsRuns:
    def test_normal_runs(self):
        runs = [
            {"inning": 1, "score": 190, "wickets": 9, "overs": 20},
            {"inning": 2, "score": 145, "wickets": 10, "overs": 18.3},
        ]
        parsed = CricketClient.parse_innings_runs(runs)
        assert len(parsed) == 2
        assert parsed[0]["score"] == 190
        assert parsed[1]["wickets"] == 10

    def test_wrapped_data(self):
        runs = {"data": [{"inning": 1, "score": 100, "wickets": 3, "overs": 12}]}
        parsed = CricketClient.parse_innings_runs(runs)
        assert len(parsed) == 1
        assert parsed[0]["inning"] == 1

    def test_none_input(self):
        assert CricketClient.parse_innings_runs(None) == []

    def test_invalid_input(self):
        assert CricketClient.parse_innings_runs("invalid") == []


# ── get_current_phase tests ──────────────────────────────────────────────────

class TestGetCurrentPhase:
    def test_powerplay(self):
        assert CricketClient.get_current_phase(0) == "powerplay"
        assert CricketClient.get_current_phase(3.2) == "powerplay"
        assert CricketClient.get_current_phase(6) == "powerplay"

    def test_middle(self):
        assert CricketClient.get_current_phase(6.1) == "middle"
        assert CricketClient.get_current_phase(10) == "middle"
        assert CricketClient.get_current_phase(15) == "middle"

    def test_death(self):
        assert CricketClient.get_current_phase(15.1) == "death"
        assert CricketClient.get_current_phase(19.5) == "death"
        assert CricketClient.get_current_phase(20) == "death"


# ── API method tests (mocked) ────────────────────────────────────────────────

class TestGetLiveIPLMatches:
    @patch.object(CricketClient, "_request")
    def test_filters_ipl_only(self, mock_req, client):
        mock_req.return_value = {
            "data": [
                {"id": 1, "league_id": 1, "localteam": {"name": "CSK"}},
                {"id": 2, "league_id": 5, "localteam": {"name": "Other"}},
                {"id": 3, "league_id": 1, "localteam": {"name": "MI"}},
            ]
        }
        result = client.get_live_ipl_matches()
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 3

    @patch.object(CricketClient, "_request")
    def test_empty_response(self, mock_req, client):
        mock_req.return_value = {"data": []}
        assert client.get_live_ipl_matches() == []

    @patch.object(CricketClient, "_request")
    def test_api_failure(self, mock_req, client):
        mock_req.return_value = None
        assert client.get_live_ipl_matches() == []


class TestGetMatchBalls:
    @patch.object(CricketClient, "_request")
    def test_returns_balls(self, mock_req, client):
        mock_req.return_value = {
            "data": {
                "id": 123,
                "balls": {
                    "data": [
                        {"ball": 0.1, "scoreboard": "S1"},
                        {"ball": 0.2, "scoreboard": "S1"},
                    ]
                },
            }
        }
        balls = client.get_match_balls(123)
        assert len(balls) == 2
        assert balls[0]["ball"] == 0.1

    @patch.object(CricketClient, "_request")
    def test_no_balls(self, mock_req, client):
        mock_req.return_value = {"data": {"id": 123}}
        assert client.get_match_balls(123) == []

    @patch.object(CricketClient, "_request")
    def test_api_failure(self, mock_req, client):
        mock_req.return_value = None
        assert client.get_match_balls(999) == []


class TestGetMatchDetails:
    @patch.object(CricketClient, "_request")
    def test_returns_fixture(self, mock_req, client):
        mock_req.return_value = {
            "data": {
                "id": 456,
                "localteam": {"data": {"name": "RCB"}},
                "venue": {"data": {"name": "Chinnaswamy"}},
            }
        }
        details = client.get_match_details(456)
        assert details is not None
        assert details["id"] == 456

    @patch.object(CricketClient, "_request")
    def test_api_failure(self, mock_req, client):
        mock_req.return_value = None
        assert client.get_match_details(999) is None


class TestGetIPLFixtures:
    @patch.object(CricketClient, "_request")
    def test_returns_fixtures(self, mock_req, client):
        mock_req.return_value = {
            "data": [
                {"id": 1, "round": "1"},
                {"id": 2, "round": "2"},
            ]
        }
        fixtures = client.get_ipl_fixtures(season_id=50)
        assert len(fixtures) == 2
        mock_req.assert_called_once_with(
            "/fixtures",
            params={"filter[league_id]": 1, "filter[season_id]": 50},
        )

    @patch.object(CricketClient, "_request")
    def test_api_failure(self, mock_req, client):
        mock_req.return_value = None
        assert client.get_ipl_fixtures(season_id=50) == []


class TestGetCurrentSeasonId:
    @patch.object(CricketClient, "_request")
    def test_returns_season_id(self, mock_req, client):
        mock_req.return_value = {
            "data": {"id": 1, "name": "IPL", "current_season_id": 872}
        }
        assert client.get_current_season_id() == 872

    @patch.object(CricketClient, "_request")
    def test_missing_season_id(self, mock_req, client):
        mock_req.return_value = {"data": {"id": 1, "name": "IPL"}}
        assert client.get_current_season_id() is None

    @patch.object(CricketClient, "_request")
    def test_api_failure(self, mock_req, client):
        mock_req.return_value = None
        assert client.get_current_season_id() is None

    @patch.object(CricketClient, "_request")
    def test_invalid_season_id(self, mock_req, client):
        mock_req.return_value = {
            "data": {"id": 1, "current_season_id": "not_a_number"}
        }
        assert client.get_current_season_id() is None


# ── _request integration test (mocked at requests level) ────────────────────

class TestRequestMethod:
    @patch("modules.cricket_client.requests.Session")
    def test_request_passes_api_token(self, mock_session_cls, config):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": []}
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        c = CricketClient(config)
        c._session = mock_session
        result = c._request("/livescores", params={"include": "balls"})

        mock_session.get.assert_called_once()
        call_kwargs = mock_session.get.call_args
        assert call_kwargs[1]["params"]["api_token"] == "test_key_123"
        assert call_kwargs[1]["params"]["include"] == "balls"
        assert call_kwargs[1]["timeout"] == 10

    @patch("modules.cricket_client.requests.Session")
    def test_request_http_error(self, mock_session_cls, config):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.text = "Unauthorized"
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        c = CricketClient(config)
        c._session = mock_session
        result = c._request("/livescores")
        assert result is None

    @patch("modules.cricket_client.requests.Session")
    def test_request_network_error(self, mock_session_cls, config):
        mock_session = MagicMock()
        mock_session.get.side_effect = requests.ConnectionError("timeout")
        mock_session_cls.return_value = mock_session

        c = CricketClient(config)
        c._session = mock_session
        result = c._request("/livescores")
        assert result is None
