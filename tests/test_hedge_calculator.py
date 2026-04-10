"""Tests for hedge_calculator — session and match-winner booking math."""

import pytest
from modules.hedge_calculator import HedgeCalculator, BookOpportunity


class TestSessionBooking:
    def setup_method(self):
        self.calc = HedgeCalculator()

    def test_session_book_profit_yes_then_no(self):
        """YES 56 then NO 64 at Rs 200/run = Rs 1600 guaranteed."""
        result = self.calc.calculate_session_book(
            entry_direction="YES", entry_line=56.0,
            current_line_no=64.0, stake_per_run=200.0,
        )
        assert result["guaranteed_profit"] == 1600.0
        assert result["action"] == "Khai NO 64"
        assert result["exit_stake_per_run"] == 200.0

    def test_session_book_profit_no_then_yes(self):
        """NO 64 then YES 58 at Rs 300/run = Rs 1800 guaranteed."""
        result = self.calc.calculate_session_book(
            entry_direction="NO", entry_line=64.0,
            current_line_yes=58.0, stake_per_run=300.0,
        )
        assert result["guaranteed_profit"] == 1800.0
        assert result["action"] == "Lagai YES 58"

    def test_session_no_book_when_line_moved_against(self):
        """YES 56, line dropped to 52-53 — no book, negative profit."""
        result = self.calc.calculate_session_book(
            entry_direction="YES", entry_line=56.0,
            current_line_no=53.0, stake_per_run=200.0,
        )
        assert result["guaranteed_profit"] < 0

    def test_session_verification_all_outcomes(self):
        """Verify the booking math holds for any actual total."""
        entry_line = 56.0
        exit_line = 64.0
        stake = 100.0
        result = self.calc.calculate_session_book(
            entry_direction="YES", entry_line=entry_line,
            current_line_no=exit_line, stake_per_run=stake,
        )
        for actual in [50, 60, 70]:
            yes_pnl = (actual - entry_line) * stake if actual > entry_line else -(entry_line - actual) * stake
            no_pnl = (exit_line - actual) * stake if actual < exit_line else -(actual - exit_line) * stake
            net = yes_pnl + no_pnl
            assert net == result["guaranteed_profit"]


class TestMatchWinnerBooking:
    def setup_method(self):
        self.calc = HedgeCalculator()

    def test_mw_book_lagai_then_khai(self):
        """Lagai SRH @ 2.30 for Rs 500, Khai SRH @ 1.75."""
        result = self.calc.calculate_mw_book(
            entry_direction="LAGAI", entry_odds=2.30,
            entry_stake=500.0, exit_odds=1.75,
        )
        assert abs(result["exit_stake"] - 657.14) < 1.0
        assert abs(result["if_wins"] - 157.14) < 1.0
        assert abs(result["if_loses"] - 157.14) < 1.0
        assert abs(result["guaranteed_profit"] - 157.14) < 1.0
        assert "Khai" in result["action"]

    def test_mw_book_khai_then_lagai(self):
        """Khai RCB @ 1.50 for Rs 600, Lagai RCB @ 2.00."""
        result = self.calc.calculate_mw_book(
            entry_direction="KHAI", entry_odds=1.50,
            entry_stake=600.0, exit_odds=2.00,
        )
        assert abs(result["exit_stake"] - 450.0) < 1.0
        assert abs(result["if_loses"] - 150.0) < 1.0
        assert abs(result["guaranteed_profit"] - 150.0) < 1.0
        assert "Lagai" in result["action"]

    def test_mw_no_book_when_odds_moved_against(self):
        """Lagai SRH @ 2.30, now SRH @ 2.80 — odds moved against."""
        result = self.calc.calculate_mw_book(
            entry_direction="LAGAI", entry_odds=2.30,
            entry_stake=500.0, exit_odds=2.80,
        )
        assert result["guaranteed_profit"] < 0


class TestBookTriggers:
    def setup_method(self):
        self.calc = HedgeCalculator(config={
            "hedge_session_min_runs": 4,
            "hedge_mw_min_odds_move": 0.20,
            "hedge_min_profit_inr": 100,
        })

    def test_session_trigger_fires(self):
        opp = self.calc.check_session_book_opportunity(
            entry_direction="YES", entry_line=56.0,
            current_line_yes=62.0, current_line_no=63.0,
            stake_per_run=200.0,
        )
        assert opp is not None
        assert opp.guaranteed_profit >= 100

    def test_session_trigger_too_small(self):
        opp = self.calc.check_session_book_opportunity(
            entry_direction="YES", entry_line=56.0,
            current_line_yes=57.0, current_line_no=58.0,
            stake_per_run=200.0,
        )
        assert opp is None

    def test_mw_trigger_fires(self):
        opp = self.calc.check_mw_book_opportunity(
            entry_direction="LAGAI", entry_odds=2.30,
            entry_stake=500.0, current_odds=1.75,
        )
        assert opp is not None
        assert opp.guaranteed_profit >= 100

    def test_mw_trigger_too_small(self):
        opp = self.calc.check_mw_book_opportunity(
            entry_direction="LAGAI", entry_odds=2.30,
            entry_stake=500.0, current_odds=2.20,
        )
        assert opp is None
