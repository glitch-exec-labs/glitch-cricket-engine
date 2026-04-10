"""
Tests for ipl_spotter.modules.odds_tracker -- OddsTracker.

All tests use in-memory data; no external calls are made.
"""

import pytest
from datetime import datetime, timezone, timedelta

from modules.odds_tracker import (
    OddsTracker,
    _classify_direction,
    _direction_arrow,
    _market_label,
)


# -- Fixtures / helpers ---------------------------------------------------------

@pytest.fixture
def tracker():
    return OddsTracker()


def _ts(minutes: int = 0) -> datetime:
    """Return a UTC timestamp offset by `minutes` from a fixed base."""
    base = datetime(2025, 4, 10, 19, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(minutes=minutes)


def _match_winner_data(home_price: float, away_price: float) -> dict:
    return {
        "market": "match_winner",
        "selections": {
            "home": {"price": home_price},
            "away": {"price": away_price},
        },
    }


def _over_under_data(
    market: str = "innings_total",
    line: float = 185.5,
    over_odds: float = 1.90,
    under_odds: float = 1.90,
    team: str = "home",
) -> dict:
    return {
        "market": market,
        "lines": [
            {
                "market": market,
                "line": line,
                "over_odds": over_odds,
                "under_odds": under_odds,
                "team": team,
            }
        ],
    }


def _player_data(
    player: str = "virat kohli",
    line: float = 30.5,
    over_odds: float = 1.85,
    under_odds: float = 1.95,
) -> dict:
    return {
        "market": "player_runs",
        "players": [
            {
                "player": player,
                "line": line,
                "over_odds": over_odds,
                "under_odds": under_odds,
            }
        ],
    }


# -- Initialization tests -------------------------------------------------------

class TestInit:
    def test_empty_history(self, tracker):
        assert len(tracker.history) == 0

    def test_history_is_defaultdict(self, tracker):
        assert tracker.history[("x", "y", "z")] == []


# -- record_snapshot tests -------------------------------------------------------

class TestRecordSnapshot:
    def test_record_match_winner(self, tracker):
        data = _match_winner_data(1.85, 2.05)
        tracker.record_snapshot("m1", "match_winner", data, timestamp=_ts(0))
        key = ("m1", "match_winner", "main")
        assert len(tracker.history[key]) == 1
        assert tracker.history[key][0][1]["selections"]["home"]["price"] == 1.85

    def test_record_over_under(self, tracker):
        data = _over_under_data("innings_total", 185.5, 1.90, 1.90, "home")
        tracker.record_snapshot("m1", "innings_total", data, timestamp=_ts(0))
        key = ("m1", "innings_total", "home:185.5")
        assert len(tracker.history[key]) == 1
        assert tracker.history[key][0][1]["line"] == 185.5

    def test_record_player_market(self, tracker):
        data = _player_data("virat kohli", 30.5, 1.85, 1.95)
        tracker.record_snapshot("m1", "player_runs", data, timestamp=_ts(0))
        key = ("m1", "player_runs", "virat kohli:30.5")
        assert len(tracker.history[key]) == 1
        assert tracker.history[key][0][1]["player"] == "virat kohli"

    def test_multiple_snapshots_append(self, tracker):
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.85, 2.05), timestamp=_ts(0))
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.75, 2.20), timestamp=_ts(5))
        key = ("m1", "match_winner", "main")
        assert len(tracker.history[key]) == 2

    def test_default_timestamp(self, tracker):
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.85, 2.05))
        key = ("m1", "match_winner", "main")
        ts = tracker.history[key][0][0]
        assert ts.tzinfo is not None  # Should be UTC

    def test_match_id_coerced_to_string(self, tracker):
        tracker.record_snapshot(12345, "match_winner", _match_winner_data(1.85, 2.05), timestamp=_ts(0))
        key = ("12345", "match_winner", "main")
        assert len(tracker.history[key]) == 1

    def test_over_under_no_team(self, tracker):
        data = {
            "market": "over_runs",
            "lines": [{"line": 7.5, "over_odds": 1.80, "under_odds": 2.00, "team": ""}],
        }
        tracker.record_snapshot("m1", "over_runs", data, timestamp=_ts(0))
        key = ("m1", "over_runs", "7.5")
        assert len(tracker.history[key]) == 1


# -- get_movement tests ----------------------------------------------------------

class TestGetMovement:
    def test_match_winner_movement(self, tracker):
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.80, 2.10), timestamp=_ts(0))
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.60, 2.40), timestamp=_ts(10))
        mv = tracker.get_movement("m1", "match_winner")
        assert mv is not None
        assert mv["home_opening"] == 1.80
        assert mv["home_current"] == 1.60
        assert mv["home_direction"] == "SHORTENING"
        assert mv["away_direction"] == "DRIFTING"
        assert mv["snapshots_count"] == 2

    def test_over_under_movement(self, tracker):
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.85, 1.95, "home"),
            timestamp=_ts(0),
        )
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 180.5, 1.95, 1.85, "home"),
            timestamp=_ts(5),
        )
        mv = tracker.get_movement("m1", "innings_total", "home:185.5")

        # First snapshot line is 185.5, second changes to 180.5
        # But they are stored under the SAME key "home:185.5" because
        # line_key is derived from the first snapshot's line -- actually
        # the second snapshot will create a new key "home:180.5".
        # Let's check what we have:
        assert mv is not None
        assert mv["opening_line"] == 185.5
        assert mv["snapshots_count"] == 1

    def test_over_under_same_line_movement(self, tracker):
        """When the line stays the same but odds change."""
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.85, 1.95, "home"),
            timestamp=_ts(0),
        )
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 2.00, 1.80, "home"),
            timestamp=_ts(5),
        )
        mv = tracker.get_movement("m1", "innings_total", "home:185.5")
        assert mv is not None
        assert mv["opening_over_odds"] == 1.85
        assert mv["current_over_odds"] == 2.00
        assert mv["line_change"] == 0
        assert mv["direction"] == "DRIFTING"
        assert mv["snapshots_count"] == 2

    def test_no_data_returns_none(self, tracker):
        assert tracker.get_movement("m1", "match_winner") is None

    def test_single_snapshot(self, tracker):
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.85, 2.05), timestamp=_ts(0))
        mv = tracker.get_movement("m1", "match_winner")
        assert mv is not None
        assert mv["snapshots_count"] == 1
        assert mv["home_direction"] == "STABLE"

    def test_stable_odds(self, tracker):
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.90, 1.90, "home"),
            timestamp=_ts(0),
        )
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.90, 1.90, "home"),
            timestamp=_ts(5),
        )
        mv = tracker.get_movement("m1", "innings_total", "home:185.5")
        assert mv["direction"] == "STABLE"
        assert mv["odds_change_pct"] == 0.0

    def test_shortening_over_odds(self, tracker):
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 2.00, 1.80, "home"),
            timestamp=_ts(0),
        )
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.80, 2.00, "home"),
            timestamp=_ts(5),
        )
        mv = tracker.get_movement("m1", "innings_total", "home:185.5")
        assert mv["direction"] == "SHORTENING"
        assert mv["odds_change_pct"] == -10.0


# -- get_all_movements tests -----------------------------------------------------

class TestGetAllMovements:
    def test_multiple_markets(self, tracker):
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.85, 2.05), timestamp=_ts(0))
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.90, 1.90, "home"),
            timestamp=_ts(0),
        )
        movements = tracker.get_all_movements("m1")
        assert len(movements) == 2
        market_keys = {m["market_key"] for m in movements}
        assert "match_winner" in market_keys
        assert "innings_total" in market_keys

    def test_empty_match(self, tracker):
        assert tracker.get_all_movements("nonexistent") == []

    def test_ignores_other_matches(self, tracker):
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.85, 2.05), timestamp=_ts(0))
        tracker.record_snapshot("m2", "match_winner", _match_winner_data(1.70, 2.30), timestamp=_ts(0))
        movements = tracker.get_all_movements("m1")
        assert len(movements) == 1
        assert movements[0]["market_key"] == "match_winner"


# -- format_odds_update tests ----------------------------------------------------

class TestFormatOddsUpdate:
    def test_basic_format(self, tracker):
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.75, 2.05), timestamp=_ts(0))
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.60, 2.30), timestamp=_ts(10))
        msg = tracker.format_odds_update("m1", "MI", "CSK")
        assert "MI vs CSK" in msg
        assert "Match Winner" in msg
        assert "1.75" in msg
        assert "1.60" in msg
        assert "SHORTENING" in msg
        assert "DRIFTING" in msg

    def test_over_under_format(self, tracker):
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.85, 1.95, "home"),
            timestamp=_ts(0),
        )
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.95, 1.85, "home"),
            timestamp=_ts(5),
        )
        msg = tracker.format_odds_update("m1", "MI", "CSK")
        assert "Innings Total" in msg
        assert "1.85" in msg
        assert "1.95" in msg

    def test_no_data_message(self, tracker):
        msg = tracker.format_odds_update("m1", "MI", "CSK")
        assert "No odds data" in msg

    def test_unchanged_line_shows_unchanged(self, tracker):
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.90, 1.90, "home"),
            timestamp=_ts(0),
        )
        msg = tracker.format_odds_update("m1", "MI", "CSK")
        assert "unchanged" in msg


# -- get_sharp_moves tests -------------------------------------------------------

class TestGetSharpMoves:
    def test_detects_sharp_over_under(self, tracker):
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.85, 1.95, "home"),
            timestamp=_ts(0),
        )
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 2.15, 1.70, "home"),
            timestamp=_ts(5),
        )
        sharp = tracker.get_sharp_moves("m1", threshold_pct=5.0)
        assert len(sharp) == 1
        assert sharp[0]["market_key"] == "innings_total"

    def test_no_sharp_moves_below_threshold(self, tracker):
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.90, 1.90, "home"),
            timestamp=_ts(0),
        )
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.91, 1.89, "home"),
            timestamp=_ts(5),
        )
        sharp = tracker.get_sharp_moves("m1", threshold_pct=5.0)
        assert len(sharp) == 0

    def test_detects_sharp_match_winner(self, tracker):
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(2.00, 1.85), timestamp=_ts(0))
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.60, 2.50), timestamp=_ts(5))
        sharp = tracker.get_sharp_moves("m1", threshold_pct=5.0)
        assert len(sharp) == 1
        assert sharp[0]["market_key"] == "match_winner"

    def test_custom_threshold(self, tracker):
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.90, 1.90, "home"),
            timestamp=_ts(0),
        )
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.95, 1.85, "home"),
            timestamp=_ts(5),
        )
        # ~2.6% change -- above 2% threshold but below 5%
        sharp_2 = tracker.get_sharp_moves("m1", threshold_pct=2.0)
        sharp_5 = tracker.get_sharp_moves("m1", threshold_pct=5.0)
        assert len(sharp_2) == 1
        assert len(sharp_5) == 0

    def test_empty_match(self, tracker):
        assert tracker.get_sharp_moves("nonexistent") == []


# -- format_sharp_move_alert tests -----------------------------------------------

class TestFormatSharpMoveAlert:
    def test_over_under_alert(self, tracker):
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 1.85, 1.95, "home"),
            timestamp=_ts(0),
        )
        tracker.record_snapshot(
            "m1", "innings_total",
            _over_under_data("innings_total", 185.5, 2.15, 1.70, "home"),
            timestamp=_ts(5),
        )
        sharp = tracker.get_sharp_moves("m1", threshold_pct=5.0)
        assert len(sharp) == 1
        alert = tracker.format_sharp_move_alert("m1", "MI", "CSK", sharp[0])
        assert "SHARP MOVE" in alert
        assert "MI vs CSK" in alert
        assert "1.85" in alert
        assert "2.15" in alert
        assert "snapshots recorded" in alert

    def test_match_winner_alert(self, tracker):
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(2.00, 1.85), timestamp=_ts(0))
        tracker.record_snapshot("m1", "match_winner", _match_winner_data(1.60, 2.50), timestamp=_ts(5))
        sharp = tracker.get_sharp_moves("m1", threshold_pct=5.0)
        alert = tracker.format_sharp_move_alert("m1", "MI", "CSK", sharp[0])
        assert "SHARP MOVE" in alert
        assert "MI" in alert
        assert "CSK" in alert


# -- Helper function tests -------------------------------------------------------

class TestClassifyDirection:
    def test_shortening(self):
        assert _classify_direction(2.0, 1.8) == "SHORTENING"

    def test_drifting(self):
        assert _classify_direction(1.8, 2.0) == "DRIFTING"

    def test_stable(self):
        assert _classify_direction(1.90, 1.90) == "STABLE"

    def test_zero_opening(self):
        assert _classify_direction(0.0, 1.90) == "STABLE"

    def test_near_equal(self):
        assert _classify_direction(1.90, 1.905) == "STABLE"


class TestDirectionArrow:
    def test_shortening(self):
        assert _direction_arrow("SHORTENING") == "\u2193"

    def test_drifting(self):
        assert _direction_arrow("DRIFTING") == "\u2191"

    def test_stable(self):
        assert _direction_arrow("STABLE") == "\u2194"


class TestMarketLabel:
    def test_known_market(self):
        assert _market_label("match_winner", "main") == "Match Winner"

    def test_with_team(self):
        assert _market_label("innings_total", "home:185.5") == "Innings Total (home)"

    def test_unknown_market(self):
        assert _market_label("unknown_thing", "main") == "unknown_thing"

    def test_no_team_in_line_key(self):
        assert _market_label("over_runs", "7.5") == "Over Runs"
