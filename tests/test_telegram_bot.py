"""Tests for ipl_spotter.modules.telegram_bot formatting functions."""

from __future__ import annotations

import asyncio

import pytest

from modules.telegram_bot import (
    MARKET_DISPLAY,
    format_edge_alert,
    format_pre_match_report,
    TelegramNotifier,
)


# ── fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture
def over_edge() -> dict:
    return {
        "market": "total_runs",
        "direction": "OVER",
        "bookmaker_line": 170.0,
        "model_expected": 180.0,
        "edge_runs": 10.0,
        "odds": 2.10,
        "ev_pct": 15.5,
        "confidence": "HIGH",
    }


@pytest.fixture
def under_edge() -> dict:
    return {
        "market": "powerplay_runs",
        "direction": "UNDER",
        "bookmaker_line": 55.0,
        "model_expected": 48.0,
        "edge_runs": -7.0,
        "odds": 1.95,
        "ev_pct": 8.3,
        "confidence": "MEDIUM",
    }


@pytest.fixture
def winner_edge() -> dict:
    return {
        "market": "match_winner",
        "team": "MI",
        "model_prob": 0.65,
        "implied_prob": 0.5263,
        "odds": 1.90,
        "ev_pct": 23.5,
        "confidence": "HIGH",
    }


@pytest.fixture
def pre_match_report() -> dict:
    return {
        "home": "CSK",
        "away": "MI",
        "venue": "MA Chidambaram Stadium",
        "venue_avg_score": 165,
        "venue_avg_first_innings": 172,
        "venue_avg_second_innings": 158,
        "model_predicted_total": 340,
        "model_home_score": 178,
        "model_away_score": 162,
        "toss_winner": "CSK",
        "toss_decision": "bat",
    }


# ── format_edge_alert — line markets ─────────────────────────────────────

class TestFormatLineAlert:
    def test_contains_header(self, over_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", over_edge)
        assert "EDGE:" in msg
        assert "CSK vs MI" in msg

    def test_contains_market_display(self, over_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", over_edge)
        assert "Total Runs" in msg

    def test_contains_direction(self, over_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", over_edge)
        assert "OVER" in msg

    def test_contains_bookmaker_line(self, over_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", over_edge)
        assert "170" in msg

    def test_contains_model_expected(self, over_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", over_edge)
        assert "180" in msg

    def test_contains_edge_runs(self, over_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", over_edge)
        assert "10 runs" in msg

    def test_contains_odds_and_ev(self, over_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", over_edge)
        assert "2.1" in msg
        assert "+15.5%" in msg

    def test_contains_confidence(self, over_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", over_edge)
        assert "HIGH" in msg

    def test_timestamp_included_when_provided(self, over_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", over_edge, timestamp="2025-04-10 19:30 IST")
        assert "2025-04-10 19:30 IST" in msg

    def test_timestamp_absent_when_empty(self, over_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", over_edge)
        # Should end with confidence tag
        assert msg.endswith("[HIGH]")

    def test_under_edge_formatting(self, under_edge: dict) -> None:
        msg = format_edge_alert("RCB", "DC", under_edge)
        assert "UNDER" in msg
        assert "6 Over Runs (Powerplay)" in msg
        assert "-7 runs" in msg

    def test_15_over_market_labeled_correctly(self) -> None:
        """15-over market should show as '15 Over Runs' not 'Powerplay Runs'."""
        edge = {
            "market": "15_over",
            "direction": "UNDER",
            "bookmaker_line": 133.5,
            "model_expected": 128.0,
            "edge_runs": -5.5,
            "odds": 1.88,
            "ev_pct": 12.0,
            "confidence": "HIGH",
        }
        msg = format_edge_alert("QG", "KK", edge)
        assert "15 Over Runs" in msg
        assert "Powerplay Runs" not in msg

    def test_unknown_market_uses_raw_key(self) -> None:
        edge = {
            "market": "some_custom_market",
            "direction": "OVER",
            "bookmaker_line": 10.0,
            "model_expected": 15.0,
            "edge_runs": 5.0,
            "odds": 2.0,
            "ev_pct": 10.0,
            "confidence": "LOW",
        }
        msg = format_edge_alert("A", "B", edge)
        assert "some_custom_market" in msg

    def test_player_adjustment_details_render(self) -> None:
        edge = {
            "market": "over_runs",
            "direction": "UNDER",
            "bookmaker_line": 9.5,
            "model_expected": 7.1,
            "base_expected": 8.5,
            "player_adj": -1.4,
            "edge_runs": -2.4,
            "odds": 1.85,
            "ev_pct": 32.5,
            "confidence": "HIGH",
            "player_context": {
                "bowler": {"name": "Jasprit Bumrah", "career_econ": 6.5},
            },
        }
        msg = format_edge_alert("RCB", "SRH", edge)
        assert "Model: 7 (base 8 -1)" in msg
        assert "Bowler: Jasprit Bumrah (econ 6.5)" in msg


# ── format_edge_alert — match_winner ─────────────────────────────────────

class TestFormatMatchWinnerAlert:
    def test_contains_header(self, winner_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", winner_edge)
        assert "EDGE: MW" in msg
        assert "CSK vs MI" in msg

    def test_contains_match_winner_label(self, winner_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", winner_edge)
        assert "MW" in msg

    def test_contains_team(self, winner_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", winner_edge)
        assert "MI" in msg

    def test_contains_model_probability(self, winner_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", winner_edge)
        assert "65%" in msg

    def test_contains_implied_probability(self, winner_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", winner_edge)
        assert "53%" in msg

    def test_contains_odds_and_ev(self, winner_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", winner_edge)
        assert "1.9" in msg
        assert "+23.5%" in msg

    def test_contains_confidence(self, winner_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", winner_edge)
        assert "HIGH" in msg

    def test_timestamp_included(self, winner_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", winner_edge, timestamp="2025-04-10 20:00 IST")
        assert "2025-04-10 20:00 IST" in msg

    def test_no_bookmaker_line_in_winner(self, winner_edge: dict) -> None:
        msg = format_edge_alert("CSK", "MI", winner_edge)
        assert "Bookmaker line" not in msg


# ── format_pre_match_report ──────────────────────────────────────────────

class TestFormatPreMatchReport:
    def test_contains_header(self, pre_match_report: dict) -> None:
        msg = format_pre_match_report(pre_match_report)
        assert "PRE-MATCH REPORT" in msg

    def test_contains_teams(self, pre_match_report: dict) -> None:
        msg = format_pre_match_report(pre_match_report)
        assert "CSK vs MI" in msg

    def test_contains_venue(self, pre_match_report: dict) -> None:
        msg = format_pre_match_report(pre_match_report)
        assert "MA Chidambaram Stadium" in msg

    def test_contains_venue_averages(self, pre_match_report: dict) -> None:
        msg = format_pre_match_report(pre_match_report)
        assert "165" in msg
        assert "172" in msg
        assert "158" in msg

    def test_contains_model_predictions(self, pre_match_report: dict) -> None:
        msg = format_pre_match_report(pre_match_report)
        assert "340" in msg
        assert "178" in msg
        assert "162" in msg

    def test_contains_toss_info(self, pre_match_report: dict) -> None:
        msg = format_pre_match_report(pre_match_report)
        assert "CSK won" in msg
        assert "bat" in msg

    def test_handles_missing_fields(self) -> None:
        msg = format_pre_match_report({})
        assert "PRE-MATCH REPORT" in msg
        assert "TBD vs TBD" in msg
        assert "Unknown" in msg
        assert "N/A" in msg

    def test_rounds_raw_float_fields(self) -> None:
        msg = format_pre_match_report({
            "home": "QG",
            "away": "KK",
            "venue": "Gaddafi Stadium",
            "venue_avg_score": 163.53846153846155,
            "venue_avg_first_innings": 168.8,
            "venue_avg_second_innings": 157.2,
            "model_predicted_total": 164.4,
            "model_home_score": 82.6,
            "model_away_score": 81.8,
            "toss_winner": "QG",
            "toss_decision": "bowl",
        })
        assert "Overall: 164" in msg
        assert "Predicted Total: 164" in msg

    def test_shows_toss_data_unavailable_when_flagged(self) -> None:
        msg = format_pre_match_report({
            "home": "QG",
            "away": "KK",
            "venue": "Gaddafi Stadium",
            "toss_available": False,
        })
        assert "Toss:* Data unavailable" in msg


# ── MARKET_DISPLAY ───────────────────────────────────────────────────────

class TestMarketDisplay:
    def test_total_runs_mapping(self) -> None:
        assert MARKET_DISPLAY["total_runs"] == "Total Runs"

    def test_match_winner_mapping(self) -> None:
        assert MARKET_DISPLAY["match_winner"] == "Match Winner"

    def test_session_market_mapping(self) -> None:
        assert MARKET_DISPLAY["15_over"] == "15 Over Runs"

    def test_all_values_are_strings(self) -> None:
        for key, value in MARKET_DISPLAY.items():
            assert isinstance(key, str)
            assert isinstance(value, str)


# ── TelegramNotifier init ────────────────────────────────────────────────

class TestTelegramNotifierInit:
    def test_enabled_when_both_present(self) -> None:
        notifier = TelegramNotifier({
            "telegram_bot_token": "123:ABC",
            "telegram_chat_id": "-100123",
        })
        assert notifier.enabled is True

    def test_disabled_when_token_missing(self) -> None:
        notifier = TelegramNotifier({"telegram_chat_id": "-100123"})
        assert notifier.enabled is False

    def test_disabled_when_chat_id_missing(self) -> None:
        notifier = TelegramNotifier({"telegram_bot_token": "123:ABC"})
        assert notifier.enabled is False

    def test_disabled_when_empty_config(self) -> None:
        notifier = TelegramNotifier({})
        assert notifier.enabled is False

    def test_disabled_when_none_config(self) -> None:
        notifier = TelegramNotifier(None)
        assert notifier.enabled is False


class TestTelegramNotifierSending:
    def test_send_alert_switches_to_http_after_bot_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        notifier = TelegramNotifier({
            "telegram_bot_token": "123:ABC",
            "telegram_chat_id": "-100123",
        })
        calls: list[dict] = []

        class BrokenBot:
            async def send_message(self, **kwargs: dict) -> None:
                raise RuntimeError("loop is closed")

        class DummyResponse:
            ok = True
            status_code = 200
            text = '{"ok":true}'

        def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
            calls.append(json)
            return DummyResponse()

        notifier._bot = BrokenBot()
        monkeypatch.setattr("modules.telegram_bot.requests.post", fake_post)

        result = asyncio.run(notifier.send_alert("Fallback after bot failure"))

        assert result is True
        assert notifier._bot is None
        assert notifier._bot_import_failed is False  # transient failure should NOT permanently disable bot
        assert calls[-1]["text"] == "Fallback after bot failure"

    def test_send_alert_sync_falls_back_to_http_api(self, monkeypatch: pytest.MonkeyPatch) -> None:
        notifier = TelegramNotifier({
            "telegram_bot_token": "123:ABC",
            "telegram_chat_id": "-100123",
        })
        notifier._bot_import_failed = True
        calls: list[dict] = []

        class DummyResponse:
            ok = True
            status_code = 200
            text = '{"ok":true}'

        def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
            assert url.endswith("/sendMessage")
            assert timeout == 20
            calls.append(json)
            return DummyResponse()

        monkeypatch.setattr("modules.telegram_bot.requests.post", fake_post)

        result = notifier.send_alert_sync("Test message")

        assert result is True
        assert calls == [{
            "chat_id": "-100123",
            "text": "Test message",
            "parse_mode": "Markdown",
        }]

    def test_send_alert_sync_retries_without_parse_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        notifier = TelegramNotifier({
            "telegram_bot_token": "123:ABC",
            "telegram_chat_id": "-100123",
        })
        notifier._bot_import_failed = True
        calls: list[dict] = []

        class DummyResponse:
            def __init__(self, ok: bool, status_code: int, text: str) -> None:
                self.ok = ok
                self.status_code = status_code
                self.text = text

        responses = [
            DummyResponse(False, 400, "Bad Request: can't parse entities"),
            DummyResponse(True, 200, '{"ok":true}'),
        ]

        def fake_post(url: str, json: dict, timeout: int) -> DummyResponse:
            assert url.endswith("/sendMessage")
            assert timeout == 20
            calls.append(json)
            return responses.pop(0)

        monkeypatch.setattr("modules.telegram_bot.requests.post", fake_post)

        result = notifier.send_alert_sync("*Broken markdown [")

        assert result is True
        assert calls[0]["parse_mode"] == "Markdown"
        assert calls[1] == {
            "chat_id": "-100123",
            "text": "*Broken markdown [",
        }
