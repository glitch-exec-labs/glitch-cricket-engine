"""Tests for position_book — tracks shadow (10wicket) and real (Cloudbet) positions."""

import pytest
from modules.position_book import PositionBook, SessionPosition, MWPosition


class TestSessionPositions:
    def setup_method(self):
        self.book = PositionBook()

    def test_add_session_position(self):
        pos = self.book.add_session_call(match_id=1, market="6_over", direction="YES", line=56.0, stake_per_run=200.0)
        assert pos.direction == "YES"
        assert pos.entry_line == 56.0
        assert pos.status == "OPEN"
        assert len(self.book.get_open_sessions()) == 1

    def test_book_session_position(self):
        pos = self.book.add_session_call(1, "6_over", "YES", 56.0, 200.0)
        profit = self.book.book_session(pos, exit_line=64.0)
        assert profit == 1600.0
        assert pos.status == "BOOKED"
        assert pos.exit_line == 64.0

    def test_settle_session_won(self):
        pos = self.book.add_session_call(1, "6_over", "YES", 56.0, 200.0)
        self.book.settle_session(pos, actual_total=65.0)
        assert pos.status == "WON"
        assert pos.pnl == (65.0 - 56.0) * 200.0

    def test_settle_session_lost(self):
        pos = self.book.add_session_call(1, "6_over", "YES", 56.0, 200.0)
        self.book.settle_session(pos, actual_total=50.0)
        assert pos.status == "LOST"
        assert pos.pnl == -(56.0 - 50.0) * 200.0

    def test_settle_booked_session(self):
        pos = self.book.add_session_call(1, "6_over", "YES", 56.0, 200.0)
        self.book.book_session(pos, exit_line=64.0)
        self.book.settle_session(pos, actual_total=60.0)
        assert pos.status == "SETTLED"
        assert pos.pnl == 1600.0

    def test_multiple_sessions(self):
        self.book.add_session_call(1, "6_over", "YES", 56.0, 200.0)
        self.book.add_session_call(1, "20_over", "NO", 170.0, 300.0)
        assert len(self.book.get_open_sessions()) == 2
        assert len(self.book.get_open_sessions(match_id=1)) == 2


class TestMWPositions:
    def setup_method(self):
        self.book = PositionBook()

    def test_add_mw_position(self):
        pos = self.book.add_mw_call(match_id=1, team="SRH", direction="LAGAI", odds=2.30, stake=500.0)
        assert pos.team == "SRH"
        assert pos.direction == "LAGAI"
        assert pos.status == "OPEN"

    def test_book_mw_position(self):
        pos = self.book.add_mw_call(1, "SRH", "LAGAI", 2.30, 500.0)
        profit = self.book.book_mw(pos, exit_odds=1.75)
        assert abs(profit - 157.14) < 1.0
        assert pos.status == "BOOKED"

    def test_settle_mw_won(self):
        pos = self.book.add_mw_call(1, "SRH", "LAGAI", 2.30, 500.0)
        self.book.settle_mw(pos, team_won=True)
        assert pos.status == "WON"
        assert pos.pnl == 500.0 * (2.30 - 1)

    def test_settle_mw_lost(self):
        pos = self.book.add_mw_call(1, "SRH", "LAGAI", 2.30, 500.0)
        self.book.settle_mw(pos, team_won=False)
        assert pos.status == "LOST"
        assert pos.pnl == -500.0


class TestPnLSummary:
    def test_total_pnl(self):
        book = PositionBook()
        p1 = book.add_session_call(1, "6_over", "YES", 56.0, 200.0)
        book.settle_session(p1, actual_total=65.0)
        p2 = book.add_mw_call(1, "SRH", "LAGAI", 2.30, 500.0)
        book.settle_mw(p2, team_won=True)
        pnl = book.get_total_shadow_pnl()
        assert pnl == 2450.0

    def test_pnl_only_settled(self):
        book = PositionBook()
        book.add_session_call(1, "6_over", "YES", 56.0, 200.0)
        assert book.get_total_shadow_pnl() == 0.0
