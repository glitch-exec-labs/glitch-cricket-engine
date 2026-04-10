"""Regression tests for live session-market filtering in the spotter."""

from __future__ import annotations

from modules.edge_detector import EdgeDetector
from modules.match_copilot import MatchCopilot
from modules.match_state import MatchState
from modules.predictor import Predictor
from modules.speed_edge import SpeedEdge
import spotter
from spotter import IPLEdgeSpotter


def _make_state(overs_completed: float, runs: int, wickets: int = 0) -> MatchState:
    state = MatchState("Quetta Gladiators", "Karachi Kings", "Gaddafi Stadium")
    state.total_runs = runs
    state.wickets = wickets
    state.overs_completed = overs_completed
    state.current_innings = 2
    return state


def test_skip_completed_session_edge() -> None:
    """At over 11.5, powerplay edge should not be detected."""
    spotter = IPLEdgeSpotter.__new__(IPLEdgeSpotter)
    spotter.edge_detector = EdgeDetector({"min_ev_pct": 0.0, "min_edge_runs": 0.0})
    spotter.predictor = Predictor({})
    spotter.bet_executor = None

    state = _make_state(overs_completed=11.5, runs=107, wickets=3)
    predictions = {
        "powerplay_total": {"expected": 60.0, "std_dev": 4.0},
        "ten_over_total": {"expected": 92.0, "std_dev": 6.0},
        "fifteen_over_total": {"expected": 136.0, "std_dev": 8.0},
        "innings_total": {"expected": 178.0, "std_dev": 12.0},
    }
    cloudbet_odds = {
        "powerplay_runs": {"line": 133.5, "over_odds": 1.85, "under_odds": 1.85},
        "15_over": {"line": 133.5, "over_odds": 1.82, "under_odds": 1.88},
    }

    edges = spotter._iter_session_edges(
        match_id=1,
        home="Quetta Gladiators",
        away="Karachi Kings",
        state=state,
        predictions=predictions,
        cloudbet_odds=cloudbet_odds,
    )

    assert all(edge["market"] != "powerplay_runs" for edge in edges)
    assert any(edge["market"] == "15_over" for edge in edges)


def test_speed_edge_suppressed_final_overs() -> None:
    """No speed edge alert sent after over 18."""
    spotter_bot = IPLEdgeSpotter.__new__(IPLEdgeSpotter)
    spotter_bot.speed_edge = SpeedEdge()
    state = _make_state(overs_completed=18.0, runs=152, wickets=7)
    cloudbet_odds = {
        "innings_total": {"line": 158.5, "over_odds": 1.9, "under_odds": 1.9},
        "match_winner": {"home_odds": 2.2, "away_odds": 1.6},
    }

    suppress, short_alert, reason = spotter_bot._classify_speed_trigger(
        state,
        cloudbet_odds,
        {"recommended_action": "Check UNDER on innings total"},
    )

    assert suppress is True
    assert short_alert is False
    assert "final overs" in reason


def test_speed_edge_suppressed_suspended_markets() -> None:
    """No speed edge alert when Cloudbet odds are 0."""
    spotter_bot = IPLEdgeSpotter.__new__(IPLEdgeSpotter)
    spotter_bot.speed_edge = SpeedEdge()
    state = _make_state(overs_completed=16.2, runs=140, wickets=5)
    cloudbet_odds = {
        "innings_total": {"line": 158.5, "over_odds": 0.0, "under_odds": 0.0},
        "match_winner": {"home_odds": 0.0, "away_odds": 0.0},
    }

    suppress, short_alert, reason = spotter_bot._classify_speed_trigger(
        state,
        cloudbet_odds,
        {"recommended_action": "Check UNDER on innings total"},
    )

    assert suppress is True
    assert short_alert is False
    assert reason == "markets suspended"


class _TelegramRecorder:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send_alert_sync(self, message: str, **kwargs) -> bool:
        self.messages.append(message)
        return True


def test_mw_call_sends_telegram(monkeypatch) -> None:
    """MW call with value generates a Telegram message."""
    spotter_bot = IPLEdgeSpotter.__new__(IPLEdgeSpotter)
    spotter_bot.config = {
        "match_winner_tracking": True,
        "over_by_over_updates": False,
    }
    spotter_bot.copilot = MatchCopilot({
        "copilot_enabled": True,
        "shadow_mw_default_stake_inr": 500,
        "min_ev_pct": 5.0,
        "message_throttle_seconds": 999,
    })
    spotter_bot.telegram = _TelegramRecorder()
    spotter_bot.match_info = {1: {"home": "RCB", "away": "SRH", "competition": "ipl"}}

    monkeypatch.setattr(spotter, "fmt_session_call", None)
    monkeypatch.setattr(spotter, "fmt_book_alert", None)
    monkeypatch.setattr(spotter, "fmt_over_update", None)
    monkeypatch.setattr(spotter, "fmt_mw_swing", None)
    monkeypatch.setattr(
        spotter,
        "fmt_mw_call",
        lambda **kwargs: f"MW ALERT {kwargs['team']} {kwargs['odds']}",
    )

    state = _make_state(overs_completed=8.0, runs=72, wickets=1)
    predictions = {"match_winner": {"home_prob": 0.50}}
    cloudbet_odds = {"match_winner": {"home_odds": 1.65, "away_odds": 2.30}}

    spotter_bot._run_copilot(1, "RCB", "SRH", state, predictions, cloudbet_odds)

    assert any(message.startswith("MW ALERT SRH 2.3") for message in spotter_bot.telegram.messages)


def test_mw_call_not_throttled_by_session(monkeypatch) -> None:
    """MW call sends even if session calls just sent."""
    spotter_bot = IPLEdgeSpotter.__new__(IPLEdgeSpotter)
    spotter_bot.config = {
        "match_winner_tracking": True,
        "over_by_over_updates": False,
    }
    spotter_bot.copilot = MatchCopilot({
        "copilot_enabled": True,
        "shadow_default_stake_inr": 500,
        "shadow_min_stake_inr": 200,
        "shadow_max_stake_inr": 1000,
        "shadow_mw_default_stake_inr": 500,
        "min_ev_pct": 5.0,
        "message_throttle_seconds": 999,
    })
    spotter_bot.telegram = _TelegramRecorder()
    spotter_bot.match_info = {1: {"home": "RCB", "away": "SRH", "competition": "ipl"}}

    monkeypatch.setattr(
        spotter,
        "fmt_session_call",
        lambda **kwargs: f"SESSION ALERT {kwargs['market']} {kwargs['direction']}",
    )
    monkeypatch.setattr(
        spotter,
        "fmt_mw_call",
        lambda **kwargs: f"MW ALERT {kwargs['team']} {kwargs['odds']}",
    )
    monkeypatch.setattr(spotter, "fmt_book_alert", None)
    monkeypatch.setattr(spotter, "fmt_over_update", None)
    monkeypatch.setattr(spotter, "fmt_mw_swing", None)

    state = _make_state(overs_completed=2.0, runs=21, wickets=0)
    predictions = {
        "powerplay_total": {"expected": 62.0, "std_dev": 8.0},
        "match_winner": {"home_prob": 0.50},
    }
    cloudbet_odds = {
        "6_over": {"line": 56.0},
        "match_winner": {"home_odds": 1.65, "away_odds": 2.30},
    }

    spotter_bot._run_copilot(1, "RCB", "SRH", state, predictions, cloudbet_odds)

    assert any(message.startswith("SESSION ALERT") for message in spotter_bot.telegram.messages)
    assert any(message.startswith("MW ALERT") for message in spotter_bot.telegram.messages)


def _make_first_innings_state(overs_completed: float, runs: int, batting_team: str) -> MatchState:
    state = MatchState("Rajasthan Royals", "Chennai Super Kings", "Sawai Mansingh Stadium")
    state.total_runs = runs
    state.wickets = 0
    state.overs_completed = overs_completed
    state.current_innings = 1
    state.batting_team = batting_team
    return state


def test_check_edges_suppresses_match_winner_before_min_overs() -> None:
    """MODEL_EDGE MW should not fire in very early overs (default gate: 2.0 ov)."""
    spotter_bot = IPLEdgeSpotter.__new__(IPLEdgeSpotter)
    spotter_bot.config = {}
    spotter_bot.edge_detector = EdgeDetector({"min_ev_pct": 0.0, "min_ev_pct_mw": 0.0, "min_edge_runs": 2.0})
    spotter_bot.predictor = Predictor({})
    spotter_bot.bet_executor = None
    spotter_bot.copilot = None
    spotter_bot.theodds = None

    captured_edges: list[dict] = []

    def _capture_edge(match_id, edge, *args, **kwargs):
        captured_edges.append(edge)

    spotter_bot._send_edge_alert = _capture_edge

    state = _make_first_innings_state(overs_completed=0.1, runs=0, batting_team="Chennai Super Kings")
    predictions = {
        "innings_total": {"expected": 154.5, "std_dev": 20.0},
        "match_winner": {"home_prob": 0.47},
    }
    cloudbet_odds = {
        "innings_total": {"line": 184.5, "over_odds": 1.85, "under_odds": 1.85},
        "match_winner": {
            "selections": {
                "home": {"price": 1.60},
                "away": {"price": 2.35},
            }
        },
    }

    spotter_bot._check_edges(
        match_id=69520,
        home="Rajasthan Royals",
        away="Chennai Super Kings",
        state=state,
        predictions=predictions,
        cloudbet_odds=cloudbet_odds,
    )

    assert all(edge.get("market") != "match_winner" for edge in captured_edges)


def test_check_edges_allows_match_winner_after_min_overs_when_consistent() -> None:
    """MODEL_EDGE MW can fire after gate when innings projection and MW side agree."""
    spotter_bot = IPLEdgeSpotter.__new__(IPLEdgeSpotter)
    spotter_bot.config = {"match_winner_min_overs": 2.0}
    spotter_bot.edge_detector = EdgeDetector({"min_ev_pct": 0.0, "min_ev_pct_mw": 0.0, "min_edge_runs": 2.0})
    spotter_bot.predictor = Predictor({})
    spotter_bot.bet_executor = None
    spotter_bot.copilot = None
    spotter_bot.theodds = None

    captured_edges: list[dict] = []

    def _capture_edge(match_id, edge, *args, **kwargs):
        captured_edges.append(edge)

    spotter_bot._send_edge_alert = _capture_edge

    state = _make_first_innings_state(overs_completed=3.0, runs=24, batting_team="Chennai Super Kings")
    predictions = {
        "innings_total": {"expected": 178.0, "std_dev": 20.0},
        "match_winner": {"home_prob": 0.45},
    }
    cloudbet_odds = {
        "innings_total": {"line": 170.5, "over_odds": 1.85, "under_odds": 1.85},
        "match_winner": {
            "selections": {
                "home": {"price": 1.60},
                "away": {"price": 2.35},
            }
        },
    }

    spotter_bot._check_edges(
        match_id=69520,
        home="Rajasthan Royals",
        away="Chennai Super Kings",
        state=state,
        predictions=predictions,
        cloudbet_odds=cloudbet_odds,
    )

    assert any(
        edge.get("market") == "match_winner" and edge.get("team") == "Chennai Super Kings"
        for edge in captured_edges
    )


def test_check_edges_session_signals_are_direction_consistent() -> None:
    """When session edges conflict in one scan, only one direction should be emitted."""
    spotter_bot = IPLEdgeSpotter.__new__(IPLEdgeSpotter)
    spotter_bot.config = {"strict_session_direction_consistency": True, "match_winner_min_overs": 2.0}
    spotter_bot.edge_detector = EdgeDetector({"min_ev_pct": 0.0, "min_ev_pct_mw": 0.0, "min_edge_runs": 0.0})
    spotter_bot.predictor = Predictor({})
    spotter_bot.bet_executor = None
    spotter_bot.copilot = None
    spotter_bot.theodds = None

    captured_edges: list[dict] = []

    def _capture_edge(match_id, edge, *args, **kwargs):
        captured_edges.append(edge)

    spotter_bot._send_edge_alert = _capture_edge

    state = _make_first_innings_state(overs_completed=3.5, runs=29, batting_team="Chennai Super Kings")
    predictions = {
        "powerplay_total": {"expected": 43.4, "std_dev": 6.0},
        "ten_over_total": {"expected": 66.1, "std_dev": 10.0},
        "fifteen_over_total": {"expected": 124.9, "std_dev": 12.0},
        "innings_total": {"expected": 167.0, "std_dev": 22.0},
        "match_winner": {"home_prob": 0.80},
    }
    cloudbet_odds = {
        "6_over": {"line": 46.5, "over_odds": 1.8, "under_odds": 1.9},
        "10_over": {"line": 77.5, "over_odds": 1.85, "under_odds": 1.8},
        "15_over": {"line": 119.5, "over_odds": 1.8, "under_odds": 1.85},
        "match_winner": {
            "selections": {
                "home": {"price": 1.22},
                "away": {"price": 4.15},
            }
        },
    }

    spotter_bot._check_edges(
        match_id=69520,
        home="Rajasthan Royals",
        away="Chennai Super Kings",
        state=state,
        predictions=predictions,
        cloudbet_odds=cloudbet_odds,
    )

    session_markets = {"6_over", "powerplay_runs", "10_over", "12_over", "15_over", "20_over"}
    session_directions = {
        edge.get("direction")
        for edge in captured_edges
        if edge.get("market") in session_markets and edge.get("direction") in ("OVER", "UNDER")
    }
    assert len(session_directions) <= 1
    assert session_directions == {"UNDER"}
