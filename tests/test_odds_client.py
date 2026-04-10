"""
Tests for ipl_spotter.modules.odds_client -- OddsClient.

All tests mock the network layer so no real Cloudbet API calls are made.
"""

import pytest
from unittest.mock import patch, MagicMock

from modules.odds_client import (
    OddsClient,
    CRICKET_MARKETS,
    _parse_params,
    _safe_float,
    _extract_player_name,
    _team_similarity,
)


# -- Fixtures ------------------------------------------------------------------

@pytest.fixture
def config():
    return {"cloudbet_api_key": "test_cb_key_456"}


@pytest.fixture
def client(config):
    return OddsClient(config)


def _make_event(
    home_name="Chennai Super Kings",
    away_name="Mumbai Indians",
    markets=None,
):
    """Helper to build a Cloudbet event dict."""
    return {
        "id": 12345,
        "home": {"name": home_name},
        "away": {"name": away_name},
        "markets": markets or {},
    }


def _make_match_winner_market(home_price=1.85, away_price=2.05):
    return {
        "cricket.winner": {
            "submarkets": {
                "main": {
                    "selections": [
                        {"outcome": "home", "params": "", "price": home_price, "probability": 0.54, "status": "open", "marketUrl": ""},
                        {"outcome": "away", "params": "", "price": away_price, "probability": 0.46, "status": "open", "marketUrl": ""},
                    ]
                }
            }
        }
    }


def _make_over_under_market(key="cricket.team_totals", line=185.5, over_price=1.90, under_price=1.90, team="home"):
    return {
        key: {
            "submarkets": {
                "main": {
                    "selections": [
                        {"outcome": "over", "params": f"total={line}&team={team}", "price": over_price, "status": "open", "marketUrl": ""},
                        {"outcome": "under", "params": f"total={line}&team={team}", "price": under_price, "status": "open", "marketUrl": ""},
                    ]
                }
            }
        }
    }


def _make_player_market(player_slug="virat-kohli", line=30.5, over_price=1.85, under_price=1.95):
    return {
        "cricket.player_total": {
            "submarkets": {
                "main": {
                    "selections": [
                        {
                            "outcome": "over",
                            "params": f"total={line}",
                            "price": over_price,
                            "status": "open",
                            "marketUrl": f"cricket/player-total/{player_slug}/over",
                        },
                        {
                            "outcome": "under",
                            "params": f"total={line}",
                            "price": under_price,
                            "status": "open",
                            "marketUrl": f"cricket/player-total/{player_slug}/under",
                        },
                    ]
                }
            }
        }
    }


def _make_session_market():
    return {
        "cricket.team_total_from_0_over_to_x_over": {
            "submarkets": {
                "main": {
                    "selections": [
                        {"outcome": "over", "params": "team=home&to_over=6&total=53.5", "price": 1.85, "status": "open", "marketUrl": "cricket.team_total_from_0_over_to_x_over/over?team=home&to_over=6&total=53.5"},
                        {"outcome": "under", "params": "team=home&to_over=6&total=53.5", "price": 1.80, "status": "open", "marketUrl": "cricket.team_total_from_0_over_to_x_over/under?team=home&to_over=6&total=53.5"},
                        {"outcome": "over", "params": "team=home&to_over=10&total=85.5", "price": 1.80, "status": "open", "marketUrl": "cricket.team_total_from_0_over_to_x_over/over?team=home&to_over=10&total=85.5"},
                        {"outcome": "under", "params": "team=home&to_over=10&total=85.5", "price": 1.85, "status": "open", "marketUrl": "cricket.team_total_from_0_over_to_x_over/under?team=home&to_over=10&total=85.5"},
                        {"outcome": "over", "params": "team=home&to_over=15&total=133.5", "price": 1.82, "status": "open", "marketUrl": "cricket.team_total_from_0_over_to_x_over/over?team=home&to_over=15&total=133.5"},
                        {"outcome": "under", "params": "team=home&to_over=15&total=133.5", "price": 1.88, "status": "open", "marketUrl": "cricket.team_total_from_0_over_to_x_over/under?team=home&to_over=15&total=133.5"},
                    ]
                }
            }
        }
    }


# -- Initialization tests ------------------------------------------------------

class TestInit:
    def test_basic_init(self, config):
        c = OddsClient(config)
        assert c.api_key == "test_cb_key_456"
        assert c.timeout == 12

    def test_session_has_api_key_header(self, config):
        c = OddsClient(config)
        assert c._session.headers["X-API-Key"] == "test_cb_key_456"
        assert c._session.headers["Accept"] == "application/json"

    def test_missing_api_key_warns(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            OddsClient({})
        assert "cloudbet_api_key not set" in caplog.text


# -- get_ipl_events tests ------------------------------------------------------

class TestGetIPLEvents:
    @patch.object(OddsClient, "_get")
    def test_returns_events(self, mock_get, client):
        mock_get.return_value = {
            "events": [
                _make_event("CSK", "MI"),
                _make_event("RCB", "KKR"),
            ]
        }
        events = client.get_ipl_events()
        assert len(events) == 2
        assert events[0]["home"]["name"] == "CSK"

    @patch.object(OddsClient, "_get")
    def test_empty_events(self, mock_get, client):
        mock_get.return_value = {"events": []}
        assert client.get_ipl_events() == []

    @patch.object(OddsClient, "_get")
    def test_api_failure(self, mock_get, client):
        mock_get.return_value = None
        assert client.get_ipl_events() == []

    @patch.object(OddsClient, "_get")
    def test_missing_events_key(self, mock_get, client):
        mock_get.return_value = {"other": "data"}
        assert client.get_ipl_events() == []

    @patch.object(OddsClient, "_get")
    def test_non_list_events(self, mock_get, client):
        mock_get.return_value = {"events": "invalid"}
        assert client.get_ipl_events() == []


# -- get_market_odds tests ------------------------------------------------------

class TestGetMarketOdds:
    def test_match_winner(self, client):
        event = _make_event(markets=_make_match_winner_market(1.85, 2.05))
        result = client.get_market_odds(event, "match_winner")
        assert result is not None
        assert result["market"] == "match_winner"
        assert result["selections"]["home"]["price"] == 1.85
        assert result["selections"]["away"]["price"] == 2.05
        assert result["home_odds"] == 1.85
        assert result["away_odds"] == 2.05

    def test_over_under_innings_total(self, client):
        event = _make_event(markets=_make_over_under_market("cricket.team_totals", 185.5, 1.90, 1.90, "home"))
        result = client.get_market_odds(event, "innings_total")
        assert result is not None
        assert result["market"] == "innings_total"
        assert len(result["lines"]) == 1
        line = result["lines"][0]
        assert line["line"] == 185.5
        assert line["over_odds"] == 1.90
        assert line["under_odds"] == 1.90
        assert line["team"] == "home"

    def test_player_runs(self, client):
        event = _make_event(markets=_make_player_market("virat-kohli", 30.5, 1.85, 1.95))
        result = client.get_market_odds(event, "player_runs")
        assert result is not None
        assert result["market"] == "player_runs"
        assert len(result["players"]) == 1
        player = result["players"][0]
        assert player["player"] == "virat kohli"
        assert player["line"] == 30.5
        assert player["over_odds"] == 1.85
        assert player["under_odds"] == 1.95

    def test_unknown_market_type(self, client):
        event = _make_event()
        result = client.get_market_odds(event, "nonexistent_market")
        assert result is None

    def test_market_not_in_event(self, client):
        event = _make_event(markets={})
        result = client.get_market_odds(event, "match_winner")
        assert result is None

    def test_empty_submarkets(self, client):
        event = _make_event(markets={"cricket.winner": {"submarkets": {}}})
        result = client.get_market_odds(event, "match_winner")
        assert result is None


# -- get_all_market_odds tests --------------------------------------------------

class TestGetAllMarketOdds:
    def test_multiple_markets(self, client):
        combined_markets = {}
        combined_markets.update(_make_match_winner_market(1.85, 2.05))
        combined_markets.update(_make_over_under_market("cricket.team_totals", 185.5, 1.90, 1.90))
        event = _make_event(markets=combined_markets)

        result = client.get_all_market_odds(event)
        assert "match_winner" in result
        assert "innings_total" in result
        assert result["match_winner"]["market"] == "match_winner"
        assert result["innings_total"]["market"] == "innings_total"

    def test_empty_markets(self, client):
        event = _make_event(markets={})
        assert client.get_all_market_odds(event) == {}

    def test_no_markets_key(self, client):
        event = {"id": 1, "home": {"name": "A"}, "away": {"name": "B"}}
        assert client.get_all_market_odds(event) == {}

    def test_extracts_session_markets_by_over(self, client):
        markets = _make_session_market()
        event = _make_event(markets=markets)
        result = client.get_all_market_odds(event, batting_team_side="home")
        assert result["6_over"]["line"] == 53.5
        assert result["10_over"]["line"] == 85.5
        assert result["15_over"]["line"] == 133.5
        assert result["powerplay_runs"]["line"] == 53.5

    def test_powerplay_alias_does_not_reuse_later_session_line(self, client):
        markets = _make_session_market()
        event = _make_event(markets=markets)
        result = client.get_all_market_odds(event, batting_team_side="home")
        assert result["powerplay_runs"]["line"] == result["6_over"]["line"]
        assert result["powerplay_runs"]["line"] != result["15_over"]["line"]


# -- Over/under parsing tests ---------------------------------------------------

class TestOverUnderParsing:
    def test_multiple_lines(self, client):
        market_data = {
            "submarkets": {
                "main": {
                    "selections": [
                        {"outcome": "over", "params": "total=6.5&team=home", "price": 1.80, "marketUrl": ""},
                        {"outcome": "under", "params": "total=6.5&team=home", "price": 2.00, "marketUrl": ""},
                        {"outcome": "over", "params": "total=7.5&team=home", "price": 2.10, "marketUrl": ""},
                        {"outcome": "under", "params": "total=7.5&team=home", "price": 1.70, "marketUrl": ""},
                    ]
                }
            }
        }
        event = _make_event(markets={"cricket.over_team_total": market_data})
        result = client.get_market_odds(event, "over_runs")
        assert result is not None
        assert len(result["lines"]) == 2
        lines_by_val = {l["line"]: l for l in result["lines"]}
        assert lines_by_val[6.5]["over_odds"] == 1.80
        assert lines_by_val[7.5]["under_odds"] == 1.70

    def test_invalid_price_defaults_to_zero(self, client):
        market_data = {
            "submarkets": {
                "main": {
                    "selections": [
                        {"outcome": "over", "params": "total=10.5&team=away", "price": "bad", "marketUrl": ""},
                    ]
                }
            }
        }
        event = _make_event(markets={"cricket.team_totals": market_data})
        result = client.get_market_odds(event, "innings_total")
        assert result["lines"][0]["over_odds"] == 0.0


# -- Player market parsing tests -----------------------------------------------

class TestPlayerMarketParsing:
    def test_player_milestone(self, client):
        market_data = {
            "submarkets": {
                "main": {
                    "selections": [
                        {
                            "outcome": "over",
                            "params": "total=0.5",
                            "price": 1.50,
                            "marketUrl": "cricket/player-to-score-milestone/ms-dhoni/over",
                        },
                        {
                            "outcome": "under",
                            "params": "total=0.5",
                            "price": 2.50,
                            "marketUrl": "cricket/player-to-score-milestone/ms-dhoni/under",
                        },
                    ]
                }
            }
        }
        event = _make_event(markets={"cricket.player_to_score_milestone": market_data})
        result = client.get_market_odds(event, "player_milestone")
        assert result is not None
        assert result["players"][0]["player"] == "ms dhoni"
        assert result["players"][0]["over_odds"] == 1.50

    def test_multiple_players(self, client):
        market_data = {
            "submarkets": {
                "main": {
                    "selections": [
                        {"outcome": "over", "params": "total=25.5", "price": 1.85, "marketUrl": "cricket/player-total/virat-kohli/over"},
                        {"outcome": "under", "params": "total=25.5", "price": 1.95, "marketUrl": "cricket/player-total/virat-kohli/under"},
                        {"outcome": "over", "params": "total=20.5", "price": 1.90, "marketUrl": "cricket/player-total/rohit-sharma/over"},
                        {"outcome": "under", "params": "total=20.5", "price": 1.90, "marketUrl": "cricket/player-total/rohit-sharma/under"},
                    ]
                }
            }
        }
        event = _make_event(markets={"cricket.player_total": market_data})
        result = client.get_market_odds(event, "player_runs")
        assert len(result["players"]) == 2
        names = {p["player"] for p in result["players"]}
        assert "virat kohli" in names
        assert "rohit sharma" in names


# -- match_cloudbet_to_sportmonks tests -----------------------------------------

class TestMatchCloudbetToSportmonks:
    def test_exact_match(self):
        cb = _make_event("Chennai Super Kings", "Mumbai Indians")
        sm = {
            "localteam": {"data": {"name": "Chennai Super Kings"}},
            "visitorteam": {"data": {"name": "Mumbai Indians"}},
        }
        assert OddsClient.match_cloudbet_to_sportmonks(cb, sm) is True

    def test_close_match(self):
        cb = _make_event("Chennai Super Kings", "Mumbai Indians")
        sm = {
            "localteam": {"name": "Chennai Super Kings"},
            "visitorteam": {"name": "Mumbai Indians"},
        }
        assert OddsClient.match_cloudbet_to_sportmonks(cb, sm) is True

    def test_abbreviation_vs_full(self):
        """RCB name change: Bangalore vs Bengaluru."""
        cb = _make_event("Royal Challengers Bangalore", "Kolkata Knight Riders")
        sm = {
            "localteam": {"data": {"name": "Royal Challengers Bengaluru"}},
            "visitorteam": {"data": {"name": "Kolkata Knight Riders"}},
        }
        assert OddsClient.match_cloudbet_to_sportmonks(cb, sm) is True

    def test_no_match(self):
        cb = _make_event("Chennai Super Kings", "Mumbai Indians")
        sm = {
            "localteam": {"data": {"name": "Rajasthan Royals"}},
            "visitorteam": {"data": {"name": "Delhi Capitals"}},
        }
        assert OddsClient.match_cloudbet_to_sportmonks(cb, sm) is False

    def test_missing_cloudbet_names(self):
        cb = {"home": {}, "away": {}}
        sm = {
            "localteam": {"data": {"name": "CSK"}},
            "visitorteam": {"data": {"name": "MI"}},
        }
        assert OddsClient.match_cloudbet_to_sportmonks(cb, sm) is False

    def test_missing_sportmonks_names(self):
        cb = _make_event("CSK", "MI")
        sm = {"localteam": {}, "visitorteam": {}}
        assert OddsClient.match_cloudbet_to_sportmonks(cb, sm) is False


# -- Helper function tests ------------------------------------------------------

class TestParseParams:
    def test_simple_param(self):
        assert _parse_params("total=185.5") == {"total": "185.5"}

    def test_multiple_params(self):
        result = _parse_params("over=1&team=home&total=7.5")
        assert result == {"over": "1", "team": "home", "total": "7.5"}

    def test_empty_string(self):
        assert _parse_params("") == {}


class TestSafeFloat:
    def test_valid(self):
        assert _safe_float("185.5") == 185.5

    def test_invalid(self):
        assert _safe_float("bad") == 0.0

    def test_none(self):
        assert _safe_float(None) == 0.0


class TestExtractPlayerName:
    def test_player_total_url(self):
        assert _extract_player_name("cricket/player-total/virat-kohli/over") == "virat kohli"

    def test_milestone_url(self):
        assert _extract_player_name("cricket/player-to-score-milestone/ms-dhoni/over") == "ms dhoni"

    def test_empty(self):
        assert _extract_player_name("") == ""


class TestTeamSimilarity:
    def test_identical(self):
        assert _team_similarity("mumbai indians", "mumbai indians") == 1.0

    def test_alias(self):
        assert _team_similarity("royal challengers bangalore", "royal challengers bengaluru") == 1.0

    def test_different(self):
        score = _team_similarity("chennai super kings", "kolkata knight riders")
        assert score < 0.6


# -- _get method tests (mocked at requests level) ------------------------------

class TestGetMethod:
    def test_successful_request(self, config):
        c = OddsClient(config)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"events": []}
        c._session.get = MagicMock(return_value=mock_resp)

        result = c._get("https://example.com/api")
        assert result == {"events": []}
        c._session.get.assert_called_once_with(
            "https://example.com/api", params=None, timeout=12
        )

    def test_http_error(self, config):
        c = OddsClient(config)
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = "Forbidden"
        c._session.get = MagicMock(return_value=mock_resp)

        assert c._get("https://example.com/api") is None

    def test_connection_error(self, config):
        import requests as req
        c = OddsClient(config)
        c._session.get = MagicMock(side_effect=req.ConnectionError("timeout"))

        assert c._get("https://example.com/api") is None

    def test_json_decode_error(self, config):
        c = OddsClient(config)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.side_effect = ValueError("bad json")
        c._session.get = MagicMock(return_value=mock_resp)

        assert c._get("https://example.com/api") is None
