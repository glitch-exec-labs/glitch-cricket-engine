"""
Tests for ipl_spotter.modules.bet_executor -- BetExecutor.

Covers paper mode (no network), and live mode with mocked Cloudbet API.
"""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from modules.bet_executor import BetExecutor, LiveBet


# -- Fixtures ------------------------------------------------------------------

@pytest.fixture
def config():
    return {
        "cloudbet_api_key": "test_key_123",
        "default_currency": "USDC",
        "accept_price_change": "ALL",
        "allowed_currencies": ["USDC", "USDT"],
        "max_position_size_usd": 25.0,
    }


@pytest.fixture
def paper_executor(config):
    return BetExecutor(config, paper_mode=True)


@pytest.fixture
def live_executor(config):
    return BetExecutor(config, paper_mode=False)


BET_KWARGS = dict(
    event_id="33515323",
    market_url="cricket.team_totals/over?team=home&total=185.5",
    price=1.85,
    stake=0.15,
    market="innings_total",
    direction="OVER",
    line=185.5,
    home="RCB",
    away="SRH",
    ev_pct=12.4,
    trigger="SPEED_EDGE",
)


# -- Paper Mode ----------------------------------------------------------------

class TestPaperMode:
    def test_place_bet_returns_live_bet(self, paper_executor):
        bet = paper_executor.place_bet(**BET_KWARGS)
        assert bet is not None
        assert isinstance(bet, LiveBet)

    def test_place_bet_fields(self, paper_executor):
        bet = paper_executor.place_bet(**BET_KWARGS)
        assert bet.event_id == "33515323"
        assert bet.home_team == "RCB"
        assert bet.away_team == "SRH"
        assert bet.market == "innings_total"
        assert bet.direction == "OVER"
        assert bet.line == 185.5
        assert bet.price == 1.85
        assert bet.stake_usd == 0.15
        assert bet.ev_pct == 12.4
        assert bet.trigger == "SPEED_EDGE"
        assert bet.paper is True
        assert bet.status == "ACCEPTED"
        assert bet.pnl == 0.0
        assert bet.settled_at is None

    def test_place_bet_tracked_in_open_bets(self, paper_executor):
        bet = paper_executor.place_bet(**BET_KWARGS)
        assert bet.reference_id in paper_executor.open_bets
        assert paper_executor.open_bets[bet.reference_id] is bet

    def test_place_bet_uuid_reference_id(self, paper_executor):
        bet = paper_executor.place_bet(**BET_KWARGS)
        # Should be a valid UUID
        parsed = uuid.UUID(bet.reference_id)
        assert str(parsed) == bet.reference_id

    def test_place_different_markets(self, paper_executor):
        bet1 = paper_executor.place_bet(**BET_KWARGS)
        kwargs2 = {**BET_KWARGS, "market": "powerplay_runs"}
        bet2 = paper_executor.place_bet(**kwargs2)
        assert bet1.reference_id != bet2.reference_id
        assert len(paper_executor.open_bets) == 2

    def test_duplicate_bet_blocked(self, paper_executor):
        bet1 = paper_executor.place_bet(**BET_KWARGS)
        assert bet1 is not None
        bet2 = paper_executor.place_bet(**BET_KWARGS)
        assert bet2 is None
        assert len(paper_executor.open_bets) == 1

    def test_stake_clamped_to_hard_cap(self, paper_executor):
        kwargs = {**BET_KWARGS, "stake": 100.00, "market": "over_runs"}
        bet = paper_executor.place_bet(**kwargs)
        assert bet.stake_usd == 25.0

    def test_disallowed_currency_returns_none(self, paper_executor):
        bet = paper_executor.place_bet(**BET_KWARGS, currency="BTC")
        assert bet is None

    def test_check_settlements_paper_noop(self, paper_executor):
        paper_executor.place_bet(**BET_KWARGS)
        settled = paper_executor.check_settlements()
        assert settled == []
        assert len(paper_executor.open_bets) == 1

    def test_get_status_empty(self, paper_executor):
        status = paper_executor.get_status()
        assert status["open_bets"] == 0
        assert status["daily_pnl"] == 0.0
        assert status["total_pnl"] == 0.0
        assert status["trades_today"] == 0
        assert status["win_rate"] == 0.0

    def test_get_status_with_open_bet(self, paper_executor):
        paper_executor.place_bet(**BET_KWARGS)
        status = paper_executor.get_status()
        assert status["open_bets"] == 1
        assert status["trades_today"] == 1

    def test_format_bet_placed(self, paper_executor):
        bet = paper_executor.place_bet(**BET_KWARGS)
        msg = paper_executor.format_bet_placed(bet)
        assert "BET PLACED" in msg
        assert "[PAPER]" in msg
        assert "RCB vs SRH" in msg
        assert "Innings Total" in msg
        assert "OVER" in msg
        assert "185.5" in msg
        assert "$0.15" in msg
        assert "1.85" in msg
        assert "+12.4%" in msg
        assert "SPEED_EDGE" in msg
        assert bet.reference_id[:8] in msg

    def test_format_bet_settled_won(self, paper_executor):
        bet = paper_executor.place_bet(**BET_KWARGS)
        bet.status = "WON"
        bet.pnl = 0.13
        paper_executor.daily_pnl = 0.28
        paper_executor.total_pnl = 1.45
        msg = paper_executor.format_bet_settled(bet)
        assert "BET WON" in msg
        assert "RCB vs SRH" in msg
        assert "+$0.13" in msg
        assert "+$0.28" in msg
        assert "+$1.45" in msg

    def test_format_bet_settled_lost(self, paper_executor):
        bet = paper_executor.place_bet(**BET_KWARGS)
        bet.status = "LOST"
        bet.pnl = -0.15
        paper_executor.daily_pnl = -0.15
        paper_executor.total_pnl = -0.15
        msg = paper_executor.format_bet_settled(bet)
        assert "BET LOST" in msg
        assert "-$0.15" in msg


# -- Live Mode (Mocked API) ---------------------------------------------------

class TestLiveMode:
    def _mock_post_accepted(self, *args, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "referenceId": "some-ref",
            "status": "ACCEPTED",
        }
        return resp

    def _mock_post_rejected(self, *args, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "referenceId": "some-ref",
            "status": "REJECTED",
        }
        return resp

    def _mock_post_http_error(self, *args, **kwargs):
        resp = MagicMock()
        resp.status_code = 500
        resp.text = "Internal Server Error"
        return resp

    def _mock_get_odds(self, *args, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "markets": {
                "cricket.team_totals": {
                    "submarkets": {
                        "main": {
                            "selections": [
                                {
                                    "outcome": "over",
                                    "params": "team=home&total=185.5",
                                    "price": "1.90",
                                },
                                {
                                    "outcome": "under",
                                    "params": "team=home&total=185.5",
                                    "price": "1.95",
                                },
                            ]
                        }
                    }
                }
            }
        }
        return resp

    def _mock_get_odds_404(self, *args, **kwargs):
        resp = MagicMock()
        resp.status_code = 404
        resp.text = "Not Found"
        return resp

    @patch("modules.bet_executor.requests.Session")
    def test_live_bet_accepted(self, mock_session_cls, config):
        session = MagicMock()
        mock_session_cls.return_value = session

        # First call = GET odds, second call = POST bet
        session.get.return_value = self._mock_get_odds()
        session.post.return_value = self._mock_post_accepted()

        executor = BetExecutor(config, paper_mode=False)
        executor._session = session

        bet = executor.place_bet(**BET_KWARGS)
        assert bet is not None
        assert bet.status == "ACCEPTED"
        assert bet.paper is False
        # Price should be updated to live price
        assert bet.price == 1.90
        assert bet.reference_id in executor.open_bets

    @patch("modules.bet_executor.requests.Session")
    def test_live_bet_rejected(self, mock_session_cls, config):
        session = MagicMock()
        mock_session_cls.return_value = session

        session.get.return_value = self._mock_get_odds()
        session.post.return_value = self._mock_post_rejected()

        executor = BetExecutor(config, paper_mode=False)
        executor._session = session

        bet = executor.place_bet(**BET_KWARGS)
        assert bet is None
        assert len(executor.open_bets) == 0

    @patch("modules.bet_executor.requests.Session")
    def test_live_bet_http_error(self, mock_session_cls, config):
        session = MagicMock()
        mock_session_cls.return_value = session

        session.get.return_value = self._mock_get_odds()
        session.post.return_value = self._mock_post_http_error()

        executor = BetExecutor(config, paper_mode=False)
        executor._session = session

        bet = executor.place_bet(**BET_KWARGS)
        assert bet is None

    @patch("modules.bet_executor.requests.Session")
    def test_live_bet_falls_back_when_odds_unavailable(self, mock_session_cls, config):
        session = MagicMock()
        mock_session_cls.return_value = session

        session.get.return_value = self._mock_get_odds_404()
        session.post.return_value = self._mock_post_accepted()

        executor = BetExecutor(config, paper_mode=False)
        executor._session = session

        bet = executor.place_bet(**BET_KWARGS)
        assert bet is not None
        # Falls back to original price when live odds unavailable
        assert bet.price == 1.85

    @patch("modules.bet_executor.requests.Session")
    def test_check_settlements_won(self, mock_session_cls, config):
        session = MagicMock()
        mock_session_cls.return_value = session

        # Setup: place a bet
        session.get.side_effect = [
            self._mock_get_odds(),  # for _get_live_price during placement
        ]
        session.post.return_value = self._mock_post_accepted()

        executor = BetExecutor(config, paper_mode=False)
        executor._session = session

        bet = executor.place_bet(**BET_KWARGS)
        assert bet is not None
        # Backdate so the bet passes the minimum age check
        bet.placed_at = datetime.now(timezone.utc) - timedelta(seconds=30)

        # Now mock the settlement check
        won_resp = MagicMock()
        won_resp.status_code = 200
        won_resp.json.return_value = {"status": "WON"}
        session.get.side_effect = [won_resp]

        settled = executor.check_settlements()
        assert len(settled) == 1
        assert settled[0].status == "WON"
        assert settled[0].pnl == pytest.approx(0.15 * (1.90 - 1))
        assert len(executor.open_bets) == 0
        assert len(executor.closed_bets) == 1
        assert executor.total_pnl == pytest.approx(0.15 * 0.90)

    @patch("modules.bet_executor.requests.Session")
    def test_check_settlements_lost(self, mock_session_cls, config):
        session = MagicMock()
        mock_session_cls.return_value = session

        session.get.side_effect = [self._mock_get_odds()]
        session.post.return_value = self._mock_post_accepted()

        executor = BetExecutor(config, paper_mode=False)
        executor._session = session

        bet = executor.place_bet(**BET_KWARGS)
        bet.placed_at = datetime.now(timezone.utc) - timedelta(seconds=30)

        lost_resp = MagicMock()
        lost_resp.status_code = 200
        lost_resp.json.return_value = {"status": "LOST"}
        session.get.side_effect = [lost_resp]

        settled = executor.check_settlements()
        assert len(settled) == 1
        assert settled[0].status == "LOST"
        assert settled[0].pnl == pytest.approx(-0.15)
        assert executor.total_pnl == pytest.approx(-0.15)

    @patch("modules.bet_executor.requests.Session")
    def test_check_settlements_void(self, mock_session_cls, config):
        session = MagicMock()
        mock_session_cls.return_value = session

        session.get.side_effect = [self._mock_get_odds()]
        session.post.return_value = self._mock_post_accepted()

        executor = BetExecutor(config, paper_mode=False)
        executor._session = session

        bet = executor.place_bet(**BET_KWARGS)
        bet.placed_at = datetime.now(timezone.utc) - timedelta(seconds=30)

        void_resp = MagicMock()
        void_resp.status_code = 200
        void_resp.json.return_value = {"status": "VOID"}
        session.get.side_effect = [void_resp]

        settled = executor.check_settlements()
        assert len(settled) == 1
        assert settled[0].pnl == 0.0
        assert executor.total_pnl == 0.0

    @patch("modules.bet_executor.requests.Session")
    def test_status_check_falls_back_to_history_on_404(self, mock_session_cls, config):
        session = MagicMock()
        mock_session_cls.return_value = session

        session.get.side_effect = [self._mock_get_odds()]
        session.post.return_value = self._mock_post_accepted()

        executor = BetExecutor(config, paper_mode=False)
        executor._session = session

        bet = executor.place_bet(**BET_KWARGS)
        assert bet is not None

        status_404 = MagicMock()
        status_404.status_code = 404
        status_404.text = "Not Found"

        history_resp = MagicMock()
        history_resp.status_code = 200
        history_resp.json.return_value = {
            "bets": [
                {"referenceId": bet.reference_id, "status": "ACCEPTED"},
            ]
        }

        session.get.side_effect = [status_404, history_resp]
        assert executor._get_bet_status(bet.reference_id) == "ACCEPTED"

    def test_no_bet_on_past_session(self, live_executor):
        """Bet executor rejects bets on sessions that already passed."""
        bet = live_executor.place_bet(
            event_id="33515323",
            market_url="cricket.team_total_from_0_over_to_x_over/under?team=home&to_over=6&total=53.5",
            price=1.85,
            stake=0.15,
            market="6_over",
            direction="UNDER",
            line=53.5,
            home="RCB",
            away="SRH",
            ev_pct=12.0,
            trigger="MODEL_EDGE",
            innings=2,
            current_overs=8.0,
        )
        assert bet is None


# -- Price Extraction ----------------------------------------------------------

class TestPriceExtraction:
    def test_extract_price_from_odds(self, paper_executor):
        event_data = {
            "markets": {
                "cricket.team_totals": {
                    "submarkets": {
                        "main": {
                            "selections": [
                                {
                                    "outcome": "over",
                                    "params": "team=home&total=185.5",
                                    "price": "2.10",
                                },
                                {
                                    "outcome": "under",
                                    "params": "team=home&total=185.5",
                                    "price": "1.75",
                                },
                            ]
                        }
                    }
                }
            }
        }
        price = paper_executor._extract_price_from_odds(
            event_data, "cricket.team_totals/over?team=home&total=185.5"
        )
        assert price == 2.10

    def test_extract_price_under(self, paper_executor):
        event_data = {
            "markets": {
                "cricket.team_totals": {
                    "submarkets": {
                        "main": {
                            "selections": [
                                {
                                    "outcome": "over",
                                    "params": "team=home&total=185.5",
                                    "price": "2.10",
                                },
                                {
                                    "outcome": "under",
                                    "params": "team=home&total=185.5",
                                    "price": "1.75",
                                },
                            ]
                        }
                    }
                }
            }
        }
        price = paper_executor._extract_price_from_odds(
            event_data, "cricket.team_totals/under?team=home&total=185.5"
        )
        assert price == 1.75

    def test_extract_price_missing_market(self, paper_executor):
        event_data = {"markets": {}}
        price = paper_executor._extract_price_from_odds(
            event_data, "cricket.team_totals/over?team=home&total=185.5"
        )
        assert price is None

    def test_extract_price_no_matching_params(self, paper_executor):
        event_data = {
            "markets": {
                "cricket.team_totals": {
                    "submarkets": {
                        "main": {
                            "selections": [
                                {
                                    "outcome": "over",
                                    "params": "team=away&total=170.5",
                                    "price": "2.10",
                                },
                            ]
                        }
                    }
                }
            }
        }
        price = paper_executor._extract_price_from_odds(
            event_data, "cricket.team_totals/over?team=home&total=185.5"
        )
        assert price is None
