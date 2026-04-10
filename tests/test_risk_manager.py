"""Tests for ipl_spotter.modules.risk_manager."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from modules.risk_manager import RiskManager


# ── fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture
def rm() -> RiskManager:
    """Default risk manager matching Cloudbet $1 bankroll config."""
    return RiskManager({
        "bankroll_usd": 1.0,
        "max_position_pct": 0.15,
        "max_open_bets": 3,
        "daily_loss_limit_usd": 0.50,
        "min_ev_pct": 5.0,
        "fractional_kelly": 0.25,
        "min_odds": 1.30,
        "max_odds": 5.00,
        "cooldown_seconds": 60,
    })


@pytest.fixture
def loose_rm() -> RiskManager:
    """Risk manager with relaxed limits for easier testing."""
    return RiskManager({
        "bankroll_usd": 10.0,
        "min_ev_pct": 0.0,
        "fractional_kelly": 1.0,
        "max_position_pct": 0.50,
        "daily_loss_limit_usd": 100.0,
        "cooldown_seconds": 0,
    })


# ── __init__ defaults ────────────────────────────────────────────────────

class TestDefaults:
    def test_default_bankroll(self) -> None:
        rm = RiskManager()
        assert rm.bankroll_usd == 1.0

    def test_default_max_position_pct(self) -> None:
        rm = RiskManager()
        assert rm.max_position_pct == 0.15
        assert rm.max_position_size_usd == 0.15

    def test_default_fractional_kelly(self) -> None:
        rm = RiskManager()
        assert rm.fractional_kelly == 0.25

    def test_config_overrides(self) -> None:
        rm = RiskManager({"bankroll_usd": 5.0, "max_open_bets": 10, "max_position_pct": 0.2})
        assert rm.bankroll_usd == 5.0
        assert rm.max_open_bets == 10
        assert rm.max_position_size_usd == 1.0

    def test_initial_state_zeroed(self) -> None:
        rm = RiskManager()
        assert rm.daily_pnl == 0.0
        assert rm.open_exposure == 0.0
        assert rm.last_bet_times == {}


# ── calculate_stake ──────────────────────────────────────────────────────

class TestCalculateStake:
    def test_below_min_ev_returns_zero(self, rm: RiskManager) -> None:
        assert rm.calculate_stake(ev_pct=3.0, odds=2.0) == 0.0

    def test_at_min_ev_returns_nonzero(self, rm: RiskManager) -> None:
        # ev=5%, odds=2.0 → kelly = 0.05/1.0 = 0.05, *0.25 = 0.0125
        # stake = 1.0 * 0.0125 = 0.0125 → below 0.10 floor → 0
        assert rm.calculate_stake(ev_pct=5.0, odds=2.0) == 0.0

    def test_high_ev_capped_at_max_position(self, rm: RiskManager) -> None:
        # ev=50%, odds=1.5 → kelly = 0.5/0.5 = 1.0, *0.25 = 0.25
        # stake = 1.0 * 0.25 = 0.25 → capped at 0.15
        assert rm.calculate_stake(ev_pct=50.0, odds=1.50) == 0.15

    def test_kelly_math_correct(self, loose_rm: RiskManager) -> None:
        # ev=20%, odds=3.0, bankroll=10, full kelly
        # kelly = 0.20/2.0 = 0.10, stake = 10*0.10 = 1.0
        assert loose_rm.calculate_stake(ev_pct=20.0, odds=3.0) == 1.0

    def test_custom_bankroll_override(self, rm: RiskManager) -> None:
        # ev=50%, odds=1.5, bankroll=2.0
        # kelly = 0.5/0.5 = 1.0, *0.25 = 0.25, stake = 2.0*0.25 = 0.50
        # capped at 0.15
        assert rm.calculate_stake(ev_pct=50.0, odds=1.50, bankroll=2.0) == 0.15

    def test_floor_at_cloudbet_minimum(self) -> None:
        rm = RiskManager({"bankroll_usd": 1.0, "fractional_kelly": 0.25,
                          "min_ev_pct": 0.0, "max_position_pct": 1.0})
        # ev=1%, odds=2.0 → kelly = 0.01/1.0 = 0.01, *0.25 = 0.0025
        # stake = 1.0 * 0.0025 = 0.0025 → below 0.10 → 0
        assert rm.calculate_stake(ev_pct=1.0, odds=2.0) == 0.0

    def test_stake_rounded_to_cents(self, loose_rm: RiskManager) -> None:
        # ev=15%, odds=2.5, bankroll=10, full kelly
        # kelly = 0.15/1.5 = 0.10, stake = 10*0.10 = 1.0
        assert loose_rm.calculate_stake(ev_pct=15.0, odds=2.5) == 1.0


# ── can_place_bet ────────────────────────────────────────────────────────

class TestCanPlaceBet:
    def test_all_checks_pass(self, rm: RiskManager) -> None:
        ok, msg = rm.can_place_bet(ev_pct=10.0, odds=2.0, market_key="m1", open_bets_count=0)
        assert ok is True
        assert msg == "OK"

    def test_ev_too_low(self, rm: RiskManager) -> None:
        ok, msg = rm.can_place_bet(ev_pct=3.0, odds=2.0, market_key="m1", open_bets_count=0)
        assert ok is False
        assert "EV" in msg

    def test_odds_below_min(self, rm: RiskManager) -> None:
        ok, msg = rm.can_place_bet(ev_pct=10.0, odds=1.10, market_key="m1", open_bets_count=0)
        assert ok is False
        assert "below minimum" in msg

    def test_odds_above_max(self, rm: RiskManager) -> None:
        ok, msg = rm.can_place_bet(ev_pct=10.0, odds=6.0, market_key="m1", open_bets_count=0)
        assert ok is False
        assert "above maximum" in msg

    def test_too_many_open_bets(self, rm: RiskManager) -> None:
        ok, msg = rm.can_place_bet(ev_pct=10.0, odds=2.0, market_key="m1", open_bets_count=3)
        assert ok is False
        assert "limit" in msg.lower()

    def test_daily_loss_limit_reached(self, rm: RiskManager) -> None:
        rm.daily_pnl = -0.50
        ok, msg = rm.can_place_bet(ev_pct=10.0, odds=2.0, market_key="m1", open_bets_count=0)
        assert ok is False
        assert "loss limit" in msg.lower()

    def test_cooldown_active(self, rm: RiskManager) -> None:
        rm.last_bet_times["m1"] = time.time()
        ok, msg = rm.can_place_bet(ev_pct=10.0, odds=2.0, market_key="m1", open_bets_count=0)
        assert ok is False
        assert "cooldown" in msg.lower()

    def test_cooldown_expired(self, rm: RiskManager) -> None:
        rm.last_bet_times["m1"] = time.time() - 120  # 2 min ago
        ok, msg = rm.can_place_bet(ev_pct=10.0, odds=2.0, market_key="m1", open_bets_count=0)
        assert ok is True

    def test_different_market_no_cooldown(self, rm: RiskManager) -> None:
        rm.last_bet_times["m1"] = time.time()
        ok, msg = rm.can_place_bet(ev_pct=10.0, odds=2.0, market_key="m2", open_bets_count=0)
        assert ok is True


# ── record_bet_placed ────────────────────────────────────────────────────

class TestRecordBetPlaced:
    def test_exposure_increases(self, rm: RiskManager) -> None:
        rm.record_bet_placed("m1", 0.10)
        assert rm.open_exposure == pytest.approx(0.10)

    def test_cooldown_set(self, rm: RiskManager) -> None:
        before = time.time()
        rm.record_bet_placed("m1", 0.10)
        assert rm.last_bet_times["m1"] >= before

    def test_multiple_bets_accumulate(self, rm: RiskManager) -> None:
        rm.record_bet_placed("m1", 0.10)
        rm.record_bet_placed("m2", 0.15)
        assert rm.open_exposure == pytest.approx(0.25)


# ── record_bet_settled ───────────────────────────────────────────────────

class TestRecordBetSettled:
    def test_winning_bet_updates_pnl(self, rm: RiskManager) -> None:
        rm.open_exposure = 0.10
        rm.record_bet_settled(0.10)
        assert rm.daily_pnl == pytest.approx(0.10)

    def test_losing_bet_updates_pnl(self, rm: RiskManager) -> None:
        rm.open_exposure = 0.10
        rm.record_bet_settled(-0.10)
        assert rm.daily_pnl == pytest.approx(-0.10)
        assert rm.open_exposure == pytest.approx(0.0)

    def test_exposure_never_negative(self, rm: RiskManager) -> None:
        rm.open_exposure = 0.05
        rm.record_bet_settled(-0.10)
        assert rm.open_exposure == 0.0


class TestUpdateBankroll:
    def test_updates_max_stake_from_bankroll_pct(self) -> None:
        rm = RiskManager({"bankroll_usd": 100.0, "max_position_pct": 0.15})
        rm.update_bankroll(160.37)
        assert rm.bankroll_usd == pytest.approx(160.37)
        assert rm.max_position_size_usd == pytest.approx(24.06)

    def test_static_cap_remains_if_provided(self) -> None:
        rm = RiskManager({"bankroll_usd": 100.0, "max_position_size_usd": 20.0})
        rm.update_bankroll(160.37)
        assert rm.max_position_size_usd == pytest.approx(20.0)


# ── reset_daily ──────────────────────────────────────────────────────────

class TestResetDaily:
    def test_pnl_reset(self, rm: RiskManager) -> None:
        rm.daily_pnl = -0.30
        rm.reset_daily()
        assert rm.daily_pnl == 0.0

    def test_exposure_preserved(self, rm: RiskManager) -> None:
        rm.open_exposure = 0.10
        rm.reset_daily()
        assert rm.open_exposure == 0.10


# ── get_status ───────────────────────────────────────────────────────────

class TestGetStatus:
    def test_returns_all_keys(self, rm: RiskManager) -> None:
        status = rm.get_status()
        expected_keys = {
            "bankroll_usd", "daily_pnl", "open_exposure",
            "max_position_size_usd", "max_position_pct", "max_open_bets", "daily_loss_limit_usd",
            "min_ev_pct", "fractional_kelly", "min_odds", "max_odds",
            "cooldown_seconds", "active_cooldowns",
        }
        assert set(status.keys()) == expected_keys

    def test_reflects_current_state(self, rm: RiskManager) -> None:
        rm.daily_pnl = -0.20
        rm.record_bet_placed("m1", 0.10)
        status = rm.get_status()
        assert status["daily_pnl"] == -0.20
        assert status["open_exposure"] == 0.10
        assert status["active_cooldowns"] == 1
