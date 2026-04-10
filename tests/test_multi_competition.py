"""Tests for multi-competition (IPL + PSL) support."""

import pytest
from modules.odds_client import OddsClient, COMPETITION_URLS
from modules.theodds_client import TheOddsClient, SPORT_KEYS
from modules.cricket_client import CricketClient, LEAGUE_IDS


class TestCompetitionMappings:
    def test_cloudbet_urls(self):
        assert "ipl" in COMPETITION_URLS
        assert "psl" in COMPETITION_URLS
        assert "indian-premier-league" in COMPETITION_URLS["ipl"]
        assert "pakistan-super-league" in COMPETITION_URLS["psl"]

    def test_theodds_sport_keys(self):
        assert SPORT_KEYS["ipl"] == "cricket_ipl"
        assert SPORT_KEYS["psl"] == "cricket_psl"

    def test_sportmonks_league_ids(self):
        assert LEAGUE_IDS["ipl"] == 1
        assert LEAGUE_IDS["psl"] == 8


class TestOddsClientMultiComp:
    def test_get_events_defaults_to_ipl(self):
        client = OddsClient({"cloudbet_api_key": "test"})
        # Just verify it doesn't crash with no key
        events = client.get_events("ipl")
        assert isinstance(events, list)

    def test_get_events_psl(self):
        client = OddsClient({"cloudbet_api_key": "test"})
        events = client.get_events("psl")
        assert isinstance(events, list)

    def test_backward_compat_get_ipl_events(self):
        client = OddsClient({"cloudbet_api_key": "test"})
        events = client.get_ipl_events()
        assert isinstance(events, list)

    def test_get_psl_events(self):
        client = OddsClient({"cloudbet_api_key": "test"})
        events = client.get_psl_events()
        assert isinstance(events, list)


class TestTheOddsMultiComp:
    def test_get_odds_defaults(self):
        client = TheOddsClient({})
        assert client.enabled is False
        result = client.get_odds("psl")
        assert result == []

    def test_backward_compat(self):
        client = TheOddsClient({})
        assert client.get_ipl_odds() == []
        assert client.get_psl_odds() == []


class TestCricketClientMultiComp:
    def test_default_league_id(self):
        client = CricketClient({"sportmonks_api_key": "test"})
        assert client.league_id == 1  # IPL default

    def test_league_ids_mapping(self):
        assert LEAGUE_IDS["ipl"] == 1
        assert LEAGUE_IDS["psl"] == 8

    def test_backward_compat(self):
        client = CricketClient({})
        # These should not crash even without API key
        result = client.get_live_ipl_matches()
        assert isinstance(result, list)
