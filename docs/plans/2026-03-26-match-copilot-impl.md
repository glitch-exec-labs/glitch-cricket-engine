# Match Co-Pilot Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a live match co-pilot that auto-bets on Cloudbet AND guides the user on their Indian book (10wicket) via Telegram with session calls, match winner calls, hedge/book alerts, and over-by-over updates.

**Architecture:** Four new modules (hedge_calculator, position_book, copilot_telegram, match_copilot) built bottom-up with TDD. Each module is self-contained with no external deps beyond what's already installed. The MatchCopilot hooks into spotter.py's `_process_match()` after existing edge detection.

**Tech Stack:** Python 3.9+, dataclasses, pytest. No new dependencies.

---

### Task 1: HedgeCalculator — Session Booking Math

**Files:**
- Create: `modules/hedge_calculator.py`
- Test: `tests/test_hedge_calculator.py`

**Step 1: Write failing tests for session booking**

```python
# tests/test_hedge_calculator.py
"""Tests for hedge_calculator — session and match-winner booking math."""

import pytest
from modules.hedge_calculator import HedgeCalculator, BookOpportunity


class TestSessionBooking:
    def setup_method(self):
        self.calc = HedgeCalculator()

    def test_session_book_profit_yes_then_no(self):
        """YES 56 then NO 64 at Rs 200/run = Rs 1600 guaranteed."""
        result = self.calc.calculate_session_book(
            entry_direction="YES",
            entry_line=56.0,
            current_line_no=64.0,
            stake_per_run=200.0,
        )
        assert result["guaranteed_profit"] == 1600.0
        assert result["action"] == "Khai NO 64"
        assert result["exit_stake_per_run"] == 200.0

    def test_session_book_profit_no_then_yes(self):
        """NO 64 then YES 58 at Rs 300/run = Rs 1800 guaranteed."""
        result = self.calc.calculate_session_book(
            entry_direction="NO",
            entry_line=64.0,
            current_line_yes=58.0,
            stake_per_run=300.0,
        )
        assert result["guaranteed_profit"] == 1800.0
        assert result["action"] == "Lagai YES 58"

    def test_session_no_book_when_line_moved_against(self):
        """YES 56, line dropped to 52-53 — no book, negative profit."""
        result = self.calc.calculate_session_book(
            entry_direction="YES",
            entry_line=56.0,
            current_line_no=53.0,
            stake_per_run=200.0,
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
        # Check three scenarios: below entry, between, above exit
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
            entry_direction="LAGAI",
            entry_odds=2.30,
            entry_stake=500.0,
            exit_odds=1.75,
        )
        # exit_stake = 500 * 2.30 / 1.75 = 657.14
        assert abs(result["exit_stake"] - 657.14) < 1.0
        # if_wins = 500*1.30 - 657.14*0.75 = 650 - 492.86 = 157.14
        assert abs(result["if_wins"] - 157.14) < 1.0
        # if_loses = -500 + 657.14 = 157.14
        assert abs(result["if_loses"] - 157.14) < 1.0
        assert abs(result["guaranteed_profit"] - 157.14) < 1.0
        assert "Khai" in result["action"]

    def test_mw_book_khai_then_lagai(self):
        """Khai RCB @ 1.50 for Rs 600, Lagai RCB @ 2.00."""
        result = self.calc.calculate_mw_book(
            entry_direction="KHAI",
            entry_odds=1.50,
            entry_stake=600.0,
            exit_odds=2.00,
        )
        # exit_stake = 600 * 1.50 / 2.00 = 450
        assert abs(result["exit_stake"] - 450.0) < 1.0
        # if_loses (team loses, khai wins): +600 - 450 = 150
        assert abs(result["if_loses"] - 150.0) < 1.0
        assert abs(result["guaranteed_profit"] - 150.0) < 1.0
        assert "Lagai" in result["action"]

    def test_mw_no_book_when_odds_moved_against(self):
        """Lagai SRH @ 2.30, now SRH @ 2.80 — odds moved against."""
        result = self.calc.calculate_mw_book(
            entry_direction="LAGAI",
            entry_odds=2.30,
            entry_stake=500.0,
            exit_odds=2.80,
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
        """Line moved 6 runs — should trigger."""
        opp = self.calc.check_session_book_opportunity(
            entry_direction="YES", entry_line=56.0,
            current_line_yes=62.0, current_line_no=63.0,
            stake_per_run=200.0,
        )
        assert opp is not None
        assert opp.guaranteed_profit >= 100

    def test_session_trigger_too_small(self):
        """Line moved only 2 runs — should not trigger."""
        opp = self.calc.check_session_book_opportunity(
            entry_direction="YES", entry_line=56.0,
            current_line_yes=57.0, current_line_no=58.0,
            stake_per_run=200.0,
        )
        assert opp is None

    def test_mw_trigger_fires(self):
        """Odds moved 0.55 — should trigger."""
        opp = self.calc.check_mw_book_opportunity(
            entry_direction="LAGAI", entry_odds=2.30,
            entry_stake=500.0, current_odds=1.75,
        )
        assert opp is not None
        assert opp.guaranteed_profit >= 100

    def test_mw_trigger_too_small(self):
        """Odds moved only 0.10 — should not trigger."""
        opp = self.calc.check_mw_book_opportunity(
            entry_direction="LAGAI", entry_odds=2.30,
            entry_stake=500.0, current_odds=2.20,
        )
        assert opp is None
```

**Step 2: Run tests to verify they fail**

Run: `cd /Users/tejaskaranagrawal/Downloads/Krait/ipl_bot && python -m pytest tests/test_hedge_calculator.py -v`
Expected: FAIL — ModuleNotFoundError (hedge_calculator doesn't exist yet)

**Step 3: Implement hedge_calculator.py**

```python
# modules/hedge_calculator.py
"""Hedge Calculator — booking math for Indian book sessions and match winner.

Session bets: line moves, payout at even money per run.
  Booking formula: (exit_line - entry_line) * stake_per_run

Match Winner (khai-lagai): decimal odds, can back and lay.
  Booking formula: stake * (back_odds - lay_odds) / lay_odds
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class BookOpportunity:
    """A profitable booking opportunity."""
    market: str
    action: str
    guaranteed_profit: float
    exit_line: float | None = None
    exit_odds: float | None = None
    exit_stake: float | None = None
    math_breakdown: str = ""


class HedgeCalculator:
    """Computes booking math for Indian-book session and match-winner bets."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        config = config or {}
        self.session_min_runs: float = config.get("hedge_session_min_runs", 4.0)
        self.mw_min_odds_move: float = config.get("hedge_mw_min_odds_move", 0.20)
        self.min_profit_inr: float = config.get("hedge_min_profit_inr", 100.0)

    # ── Session booking ──────────────────────────────────────────────────

    def calculate_session_book(
        self,
        entry_direction: str,
        entry_line: float,
        stake_per_run: float,
        current_line_no: float | None = None,
        current_line_yes: float | None = None,
    ) -> dict[str, Any]:
        """Calculate session booking profit.

        For YES entry: book by taking NO at current_line_no.
        For NO entry: book by taking YES at current_line_yes.
        """
        if entry_direction == "YES":
            exit_line = current_line_no or 0.0
            profit = (exit_line - entry_line) * stake_per_run
            action = f"Khai NO {exit_line:.0f}"
        else:  # NO
            exit_line = current_line_yes or 0.0
            profit = (entry_line - exit_line) * stake_per_run
            action = f"Lagai YES {exit_line:.0f}"

        return {
            "guaranteed_profit": profit,
            "action": action,
            "exit_line": exit_line,
            "exit_stake_per_run": stake_per_run,
            "math_breakdown": self._session_math_str(
                entry_direction, entry_line, exit_line, stake_per_run, profit,
            ),
        }

    def _session_math_str(
        self, direction: str, entry: float, exit_: float,
        stake: float, profit: float,
    ) -> str:
        if direction == "YES":
            return (
                f"YES {entry:.0f} + NO {exit_:.0f} @ Rs {stake:.0f}/run\n"
                f"  Guaranteed: ({exit_:.0f} - {entry:.0f}) x {stake:.0f} = Rs {profit:.0f}"
            )
        return (
            f"NO {entry:.0f} + YES {exit_:.0f} @ Rs {stake:.0f}/run\n"
            f"  Guaranteed: ({entry:.0f} - {exit_:.0f}) x {stake:.0f} = Rs {profit:.0f}"
        )

    # ── Match Winner booking (khai-lagai) ────────────────────────────────

    def calculate_mw_book(
        self,
        entry_direction: str,
        entry_odds: float,
        entry_stake: float,
        exit_odds: float,
    ) -> dict[str, Any]:
        """Calculate match-winner booking profit.

        LAGAI (back) then KHAI (lay): profit when odds shortened.
        KHAI (lay) then LAGAI (back): profit when odds drifted.
        """
        exit_stake = round(entry_stake * entry_odds / exit_odds, 2)

        if entry_direction == "LAGAI":
            # Entry: back, Exit: lay
            if_wins = entry_stake * (entry_odds - 1) - exit_stake * (exit_odds - 1)
            if_loses = -entry_stake + exit_stake
            action = f"Khai @ {exit_odds:.2f} for Rs {exit_stake:.0f}"
        else:  # KHAI
            # Entry: lay, Exit: back
            if_wins = -entry_stake * (entry_odds - 1) + exit_stake * (exit_odds - 1)
            if_loses = entry_stake - exit_stake
            action = f"Lagai @ {exit_odds:.2f} for Rs {exit_stake:.0f}"

        guaranteed = min(if_wins, if_loses)

        return {
            "guaranteed_profit": round(guaranteed, 2),
            "exit_stake": exit_stake,
            "if_wins": round(if_wins, 2),
            "if_loses": round(if_loses, 2),
            "action": action,
            "math_breakdown": (
                f"  Wins: Rs {if_wins:+.0f} | Loses: Rs {if_loses:+.0f}\n"
                f"  Guaranteed: Rs {guaranteed:.0f}"
            ),
        }

    # ── Trigger checks ───────────────────────────────────────────────────

    def check_session_book_opportunity(
        self,
        entry_direction: str,
        entry_line: float,
        current_line_yes: float,
        current_line_no: float,
        stake_per_run: float,
    ) -> BookOpportunity | None:
        """Check if a session position can be booked profitably."""
        if entry_direction == "YES":
            line_moved = current_line_no - entry_line
            result = self.calculate_session_book(
                "YES", entry_line, stake_per_run, current_line_no=current_line_no,
            )
        else:
            line_moved = entry_line - current_line_yes
            result = self.calculate_session_book(
                "NO", entry_line, stake_per_run, current_line_yes=current_line_yes,
            )

        if line_moved < self.session_min_runs:
            return None
        if result["guaranteed_profit"] < self.min_profit_inr:
            return None

        return BookOpportunity(
            market="session",
            action=result["action"],
            guaranteed_profit=result["guaranteed_profit"],
            exit_line=result["exit_line"],
            math_breakdown=result["math_breakdown"],
        )

    def check_mw_book_opportunity(
        self,
        entry_direction: str,
        entry_odds: float,
        entry_stake: float,
        current_odds: float,
    ) -> BookOpportunity | None:
        """Check if a match-winner position can be booked profitably."""
        if entry_direction == "LAGAI":
            odds_moved = entry_odds - current_odds  # positive = in our favor
        else:
            odds_moved = current_odds - entry_odds

        if odds_moved < self.mw_min_odds_move:
            return None

        result = self.calculate_mw_book(entry_direction, entry_odds, entry_stake, current_odds)
        if result["guaranteed_profit"] < self.min_profit_inr:
            return None

        return BookOpportunity(
            market="match_winner",
            action=result["action"],
            guaranteed_profit=result["guaranteed_profit"],
            exit_odds=current_odds,
            exit_stake=result["exit_stake"],
            math_breakdown=result["math_breakdown"],
        )
```

**Step 4: Run tests to verify they pass**

Run: `cd /Users/tejaskaranagrawal/Downloads/Krait/ipl_bot && python -m pytest tests/test_hedge_calculator.py -v`
Expected: All 10 tests PASS

**Step 5: Commit**

```bash
cd /Users/tejaskaranagrawal/Downloads/Krait/ipl_bot
git add modules/hedge_calculator.py tests/test_hedge_calculator.py
git commit -m "feat: add hedge calculator for session and match winner booking"
```

---

### Task 2: PositionBook — Dual Portfolio Tracker

**Files:**
- Create: `modules/position_book.py`
- Test: `tests/test_position_book.py`

**Step 1: Write failing tests**

```python
# tests/test_position_book.py
"""Tests for position_book — tracks shadow (10wicket) and real (Cloudbet) positions."""

import pytest
from modules.position_book import PositionBook, SessionPosition, MWPosition


class TestSessionPositions:
    def setup_method(self):
        self.book = PositionBook()

    def test_add_session_position(self):
        pos = self.book.add_session_call(
            match_id=1, market="6_over", direction="YES",
            line=56.0, stake_per_run=200.0,
        )
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
        assert pos.pnl == (65.0 - 56.0) * 200.0  # 1800

    def test_settle_session_lost(self):
        pos = self.book.add_session_call(1, "6_over", "YES", 56.0, 200.0)
        self.book.settle_session(pos, actual_total=50.0)
        assert pos.status == "LOST"
        assert pos.pnl == -(56.0 - 50.0) * 200.0  # -1200

    def test_settle_booked_session(self):
        """Booked sessions always profit the guaranteed amount."""
        pos = self.book.add_session_call(1, "6_over", "YES", 56.0, 200.0)
        self.book.book_session(pos, exit_line=64.0)
        self.book.settle_session(pos, actual_total=60.0)
        assert pos.status == "SETTLED"
        # YES: (60-56)*200 = 800, NO: (64-60)*200 = 800, net = 1600
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
        pos = self.book.add_mw_call(
            match_id=1, team="SRH", direction="LAGAI",
            odds=2.30, stake=500.0,
        )
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
        assert pos.pnl == 500.0 * (2.30 - 1)  # 650

    def test_settle_mw_lost(self):
        pos = self.book.add_mw_call(1, "SRH", "LAGAI", 2.30, 500.0)
        self.book.settle_mw(pos, team_won=False)
        assert pos.status == "LOST"
        assert pos.pnl == -500.0


class TestPnLSummary:
    def test_total_pnl(self):
        book = PositionBook()
        p1 = book.add_session_call(1, "6_over", "YES", 56.0, 200.0)
        book.settle_session(p1, actual_total=65.0)  # +1800
        p2 = book.add_mw_call(1, "SRH", "LAGAI", 2.30, 500.0)
        book.settle_mw(p2, team_won=True)  # +650
        pnl = book.get_total_shadow_pnl()
        assert pnl == 2450.0

    def test_pnl_only_settled(self):
        book = PositionBook()
        book.add_session_call(1, "6_over", "YES", 56.0, 200.0)  # still OPEN
        assert book.get_total_shadow_pnl() == 0.0
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_position_book.py -v`
Expected: FAIL — ModuleNotFoundError

**Step 3: Implement position_book.py**

```python
# modules/position_book.py
"""Position Book — tracks shadow (Indian book) and Cloudbet positions.

Shadow positions are assumed: when the bot sends a call, it assumes the user followed.
Session positions track line-based bets at even money per run.
MW positions track khai-lagai match winner bets.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, List, Optional

logger = logging.getLogger("ipl_spotter.position_book")


@dataclass
class SessionPosition:
    match_id: int
    market: str        # "6_over", "10_over", "20_over", "player_runs_kohli"
    direction: str     # "YES" or "NO"
    entry_line: float
    stake_per_run: float
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "OPEN"      # OPEN, BOOKED, WON, LOST, SETTLED
    exit_line: Optional[float] = None
    booked_profit: Optional[float] = None
    pnl: float = 0.0


@dataclass
class MWPosition:
    match_id: int
    team: str
    direction: str     # "LAGAI" (back) or "KHAI" (lay)
    odds: float
    stake: float
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "OPEN"      # OPEN, BOOKED, WON, LOST, SETTLED
    exit_odds: Optional[float] = None
    exit_stake: Optional[float] = None
    booked_profit: Optional[float] = None
    pnl: float = 0.0


class PositionBook:
    """Dual portfolio: shadow (10wicket) and Cloudbet positions."""

    def __init__(self) -> None:
        self.shadow_sessions: List[SessionPosition] = []
        self.shadow_mw: List[MWPosition] = []

    # ── Session positions ────────────────────────────────────────────────

    def add_session_call(
        self, match_id: int, market: str, direction: str,
        line: float, stake_per_run: float,
    ) -> SessionPosition:
        pos = SessionPosition(
            match_id=match_id, market=market, direction=direction,
            entry_line=line, stake_per_run=stake_per_run,
        )
        self.shadow_sessions.append(pos)
        logger.info("Shadow session: %s %s %.0f @ Rs %.0f/run", direction, market, line, stake_per_run)
        return pos

    def book_session(self, pos: SessionPosition, exit_line: float) -> float:
        """Book a session position. Returns guaranteed profit."""
        if pos.direction == "YES":
            profit = (exit_line - pos.entry_line) * pos.stake_per_run
        else:
            profit = (pos.entry_line - exit_line) * pos.stake_per_run
        pos.exit_line = exit_line
        pos.booked_profit = profit
        pos.status = "BOOKED"
        logger.info("Booked session %s %.0f → %.0f: Rs %.0f", pos.market, pos.entry_line, exit_line, profit)
        return profit

    def settle_session(self, pos: SessionPosition, actual_total: float) -> None:
        """Settle a session based on actual runs."""
        if pos.status == "BOOKED":
            # Booked = guaranteed profit regardless of actual
            pos.pnl = pos.booked_profit or 0.0
            pos.status = "SETTLED"
        elif pos.direction == "YES":
            if actual_total > pos.entry_line:
                pos.pnl = (actual_total - pos.entry_line) * pos.stake_per_run
                pos.status = "WON"
            else:
                pos.pnl = -(pos.entry_line - actual_total) * pos.stake_per_run
                pos.status = "LOST"
        else:  # NO
            if actual_total < pos.entry_line:
                pos.pnl = (pos.entry_line - actual_total) * pos.stake_per_run
                pos.status = "WON"
            else:
                pos.pnl = -(actual_total - pos.entry_line) * pos.stake_per_run
                pos.status = "LOST"

    # ── Match Winner positions ───────────────────────────────────────────

    def add_mw_call(
        self, match_id: int, team: str, direction: str,
        odds: float, stake: float,
    ) -> MWPosition:
        pos = MWPosition(
            match_id=match_id, team=team, direction=direction,
            odds=odds, stake=stake,
        )
        self.shadow_mw.append(pos)
        logger.info("Shadow MW: %s %s @ %.2f for Rs %.0f", direction, team, odds, stake)
        return pos

    def book_mw(self, pos: MWPosition, exit_odds: float) -> float:
        """Book a MW position. Returns guaranteed profit."""
        exit_stake = round(pos.stake * pos.odds / exit_odds, 2)
        if pos.direction == "LAGAI":
            if_wins = pos.stake * (pos.odds - 1) - exit_stake * (exit_odds - 1)
            if_loses = -pos.stake + exit_stake
        else:
            if_wins = -pos.stake * (pos.odds - 1) + exit_stake * (exit_odds - 1)
            if_loses = pos.stake - exit_stake
        profit = round(min(if_wins, if_loses), 2)
        pos.exit_odds = exit_odds
        pos.exit_stake = exit_stake
        pos.booked_profit = profit
        pos.status = "BOOKED"
        logger.info("Booked MW %s %s @ %.2f → %.2f: Rs %.0f", pos.direction, pos.team, pos.odds, exit_odds, profit)
        return profit

    def settle_mw(self, pos: MWPosition, team_won: bool) -> None:
        """Settle a MW position based on whether the team won."""
        if pos.status == "BOOKED":
            pos.pnl = pos.booked_profit or 0.0
            pos.status = "SETTLED"
        elif pos.direction == "LAGAI":
            pos.pnl = pos.stake * (pos.odds - 1) if team_won else -pos.stake
            pos.status = "WON" if team_won else "LOST"
        else:  # KHAI
            pos.pnl = pos.stake if not team_won else -pos.stake * (pos.odds - 1)
            pos.status = "WON" if not team_won else "LOST"

    # ── Queries ──────────────────────────────────────────────────────────

    def get_open_sessions(self, match_id: int | None = None) -> List[SessionPosition]:
        return [p for p in self.shadow_sessions
                if p.status == "OPEN"
                and (match_id is None or p.match_id == match_id)]

    def get_open_mw(self, match_id: int | None = None) -> List[MWPosition]:
        return [p for p in self.shadow_mw
                if p.status == "OPEN"
                and (match_id is None or p.match_id == match_id)]

    def get_all_positions(self, match_id: int | None = None) -> List:
        all_pos = self.shadow_sessions + self.shadow_mw
        if match_id is not None:
            return [p for p in all_pos if p.match_id == match_id]
        return all_pos

    def get_total_shadow_pnl(self) -> float:
        session_pnl = sum(p.pnl for p in self.shadow_sessions if p.status in ("WON", "LOST", "SETTLED"))
        mw_pnl = sum(p.pnl for p in self.shadow_mw if p.status in ("WON", "LOST", "SETTLED"))
        return session_pnl + mw_pnl
```

**Step 4: Run tests**

Run: `python -m pytest tests/test_position_book.py -v`
Expected: All 11 tests PASS

**Step 5: Commit**

```bash
git add modules/position_book.py tests/test_position_book.py
git commit -m "feat: add position book for dual portfolio tracking"
```

---

### Task 3: CopilotTelegram — Formatted Match Messages

**Files:**
- Create: `modules/copilot_telegram.py`
- Test: `tests/test_copilot_telegram.py`

**Step 1: Write failing tests**

```python
# tests/test_copilot_telegram.py
"""Tests for copilot_telegram — formatted Telegram messages for match co-pilot."""

import pytest
from modules.copilot_telegram import (
    format_session_call,
    format_mw_call,
    format_over_update,
    format_book_alert,
    format_mw_swing,
    format_session_summary,
    format_pre_match_copilot,
    format_toss_update,
)


class TestSessionCall:
    def test_basic_yes_call(self):
        msg = format_session_call(
            market="6 Over", direction="YES", line=56.0,
            stake_per_run=300, model_prediction=59.0,
            home="RCB", away="SRH",
        )
        assert "YES 56" in msg
        assert "300" in msg
        assert "59" in msg

    def test_includes_cloudbet_auto(self):
        msg = format_session_call(
            market="6 Over", direction="YES", line=56.0,
            stake_per_run=300, model_prediction=59.0,
            home="RCB", away="SRH",
            cloudbet_info="Auto-placed YES 54.5 @ 1.88 for $3.20",
        )
        assert "Auto-placed" in msg


class TestMWCall:
    def test_lagai_call(self):
        msg = format_mw_call(
            team="SRH", direction="LAGAI", odds=2.30,
            stake=500, fair_prob=0.43, home="RCB", away="SRH",
        )
        assert "Lagai SRH" in msg
        assert "2.30" in msg
        assert "500" in msg

    def test_khai_call(self):
        msg = format_mw_call(
            team="RCB", direction="KHAI", odds=1.50,
            stake=600, fair_prob=0.60, home="RCB", away="SRH",
        )
        assert "Khai RCB" in msg


class TestOverUpdate:
    def test_basic_update(self):
        msg = format_over_update(
            over_num=3, innings=1, batting_team="RCB",
            score=34, wickets=0, run_rate=11.3,
            projected_total=178,
            mw_home_odds=1.45, mw_away_odds=2.65,
            home="RCB", away="SRH",
            positions_summary="YES 56: +3 runs ahead",
        )
        assert "Over 3" in msg
        assert "34/0" in msg
        assert "11.3" in msg


class TestBookAlert:
    def test_session_book(self):
        msg = format_book_alert(
            market_type="session",
            market_name="6 Over",
            action="Khai NO 64 @ Rs 300/run",
            guaranteed_profit=1800.0,
            math_breakdown="YES 56 + NO 64 @ Rs 300/run\n  Guaranteed: Rs 1800",
        )
        assert "BOOK" in msg
        assert "1800" in msg or "1,800" in msg
        assert "NO 64" in msg

    def test_mw_book(self):
        msg = format_book_alert(
            market_type="match_winner",
            action="Khai SRH @ 1.75 for Rs 657",
            guaranteed_profit=157.0,
            math_breakdown="Wins: Rs +158 | Loses: Rs +157\n  Guaranteed: Rs 157",
        )
        assert "BOOK" in msg
        assert "157" in msg


class TestMWSswing:
    def test_swing_alert(self):
        msg = format_mw_swing(
            team_moved="SRH", old_odds=2.30, new_odds=1.75,
            home="RCB", away="SRH",
            home_odds=2.05, away_odds=1.75,
            model_prob=0.58,
        )
        assert "SRH" in msg
        assert "2.30" in msg
        assert "1.75" in msg


class TestSummary:
    def test_session_summary(self):
        msg = format_session_summary(
            cloudbet_pnl=10.28, cloudbet_bets=3,
            shadow_pnl=3757.0, shadow_bets=5,
            shadow_currency="INR",
            positions=[],
        )
        assert "10.28" in msg or "10" in msg
        assert "3757" in msg or "3,757" in msg
```

**Step 2: Run tests**

Run: `python -m pytest tests/test_copilot_telegram.py -v`
Expected: FAIL

**Step 3: Implement copilot_telegram.py**

```python
# modules/copilot_telegram.py
"""Copilot Telegram — formatted Telegram messages for the match co-pilot.

Pure formatting functions, no Telegram API dependency.
Each function returns a Markdown string ready for Telegram.
"""

from __future__ import annotations

from typing import Any, List, Optional


def format_pre_match_copilot(
    home: str, away: str, venue: str,
    cloudbet_home_odds: float, cloudbet_away_odds: float,
    est_home_odds: str, est_away_odds: str,
    consensus_home_prob: float, consensus_away_prob: float,
    pp_line_est: str, model_pp: float,
    venue_avg_pp: float, venue_modifier: float,
) -> str:
    return (
        f"\U0001f3cf *PRE-MATCH: {home} vs {away}*\n"
        f"Venue: {venue}\n\n"
        f"*Match Winner:*\n"
        f"Cloudbet: {home} {cloudbet_home_odds:.2f} | {away} {cloudbet_away_odds:.2f}\n"
        f"10wicket est: {home} {est_home_odds} | {away} {est_away_odds}\n"
        f"Consensus ({27} books): {home} {consensus_home_prob*100:.0f}% | {away} {consensus_away_prob*100:.0f}%\n\n"
        f"*6 Over Session:*\n"
        f"Est line: {pp_line_est} | Model: {model_pp:.0f}\n"
        f"Venue avg PP: {venue_avg_pp:.1f} | Modifier: {venue_modifier:+.0f}"
    )


def format_toss_update(
    winner: str, decision: str, home: str, away: str,
    adjustment: str = "",
) -> str:
    return (
        f"\U0001fa99 *TOSS: {winner} win, {decision}*\n"
        f"{adjustment}"
    )


def format_session_call(
    market: str, direction: str, line: float,
    stake_per_run: float, model_prediction: float,
    home: str, away: str,
    cloudbet_info: str = "",
) -> str:
    dir_word = "Lagai YES" if direction == "YES" else "Khai NO"
    msg = (
        f"\U0001f3af *SESSION: {dir_word} {line:.0f} ({market}) @ Rs {stake_per_run:.0f}/run*\n"
        f"Model: {model_prediction:.0f} | Line {line:.0f} = "
        f"{abs(model_prediction - line):.0f} runs {'under' if direction == 'YES' else 'over'}priced"
    )
    if cloudbet_info:
        msg += f"\n\U0001f916 Cloudbet: {cloudbet_info}"
    return msg


def format_mw_call(
    team: str, direction: str, odds: float, stake: float,
    fair_prob: float, home: str, away: str,
    cloudbet_info: str = "",
) -> str:
    dir_word = "Lagai" if direction == "LAGAI" else "Khai"
    fair_odds = 1 / fair_prob if fair_prob > 0 else 0
    msg = (
        f"\U0001f3af *MW: {dir_word} {team} @ {odds:.2f} for Rs {stake:.0f}*\n"
        f"Model: {team} {fair_prob*100:.0f}% = fair {fair_odds:.2f} | "
        f"EV: {((odds / fair_odds) - 1) * 100:.1f}%"
    )
    if cloudbet_info:
        msg += f"\n\U0001f916 Cloudbet: {cloudbet_info}"
    return msg


def format_over_update(
    over_num: int, innings: int, batting_team: str,
    score: int, wickets: int, run_rate: float,
    projected_total: float,
    mw_home_odds: float, mw_away_odds: float,
    home: str, away: str,
    positions_summary: str = "",
    last_over_detail: str = "",
) -> str:
    msg = (
        f"\U0001f4ca *Over {over_num}* | {batting_team} {score}/{wickets} | "
        f"RR {run_rate:.1f} | Proj: {projected_total:.0f}\n"
    )
    if last_over_detail:
        msg += f"Last over: {last_over_detail}\n"
    msg += f"\U0001f3c6 Win: {home} {mw_home_odds:.2f} | {away} {mw_away_odds:.2f}"
    if positions_summary:
        msg += f"\n\U0001f4cd {positions_summary}"
    return msg


def format_book_alert(
    market_type: str,
    action: str,
    guaranteed_profit: float,
    math_breakdown: str,
    market_name: str = "",
) -> str:
    header = f"\U0001f504 *BOOK {'SESSION' if market_type == 'session' else 'MW'}"
    if market_name:
        header += f" ({market_name})"
    header += " — Lock Profit NOW!*\n"
    return (
        f"{header}"
        f"➡️ {action}\n"
        f"✅ *GUARANTEED: Rs {guaranteed_profit:,.0f}*\n\n"
        f"{math_breakdown}"
    )


def format_mw_swing(
    team_moved: str, old_odds: float, new_odds: float,
    home: str, away: str,
    home_odds: float, away_odds: float,
    model_prob: float = 0.0,
) -> str:
    direction = "shortened" if new_odds < old_odds else "drifted"
    return (
        f"\U0001f3c6 *MW SWING: {team_moved} {direction} {old_odds:.2f} → {new_odds:.2f}*\n"
        f"{home} {home_odds:.2f} | {away} {away_odds:.2f}"
        + (f"\nModel: {team_moved} {model_prob*100:.0f}%" if model_prob else "")
    )


def format_session_summary(
    cloudbet_pnl: float, cloudbet_bets: int,
    shadow_pnl: float, shadow_bets: int,
    shadow_currency: str = "INR",
    positions: list | None = None,
) -> str:
    msg = (
        f"\U0001f4b0 *MATCH SUMMARY*\n\n"
        f"*Cloudbet (auto):*\n"
        f"Bets: {cloudbet_bets} | P&L: ${cloudbet_pnl:+.2f}\n\n"
        f"*10wicket (shadow):*\n"
        f"Bets: {shadow_bets} | P&L: {shadow_currency} {shadow_pnl:+,.0f}\n"
    )
    if positions:
        msg += "\n*Positions:*\n"
        for p in positions:
            msg += f"  {p}\n"
    return msg
```

**Step 4: Run tests**

Run: `python -m pytest tests/test_copilot_telegram.py -v`
Expected: All 10 tests PASS

**Step 5: Commit**

```bash
git add modules/copilot_telegram.py tests/test_copilot_telegram.py
git commit -m "feat: add copilot Telegram message formatters"
```

---

### Task 4: MatchCopilot — Main Orchestrator

**Files:**
- Create: `modules/match_copilot.py`
- Test: `tests/test_match_copilot.py`

**Step 1: Write failing tests**

```python
# tests/test_match_copilot.py
"""Tests for match_copilot — orchestrates the match co-pilot experience."""

import pytest
from unittest.mock import MagicMock, patch
from modules.match_copilot import MatchCopilot


class TestPhaseDetection:
    def setup_method(self):
        self.copilot = MatchCopilot(config={
            "copilot_enabled": True,
            "shadow_default_stake_inr": 500,
            "shadow_mw_default_stake_inr": 500,
            "hedge_min_profit_inr": 100,
            "hedge_session_min_runs": 4,
            "hedge_mw_min_odds_move": 0.20,
            "message_throttle_seconds": 0,  # no throttle in tests
        })

    def test_innings_1_pp_phase(self):
        phase = self.copilot._detect_phase(innings=1, overs=3.2, wickets=0)
        assert phase == "INNINGS_1_PP"

    def test_innings_1_middle_phase(self):
        phase = self.copilot._detect_phase(innings=1, overs=8.0, wickets=2)
        assert phase == "INNINGS_1_MIDDLE"

    def test_innings_1_death_phase(self):
        phase = self.copilot._detect_phase(innings=1, overs=16.3, wickets=4)
        assert phase == "INNINGS_1_DEATH"

    def test_innings_2_pp_phase(self):
        phase = self.copilot._detect_phase(innings=2, overs=4.0, wickets=0)
        assert phase == "INNINGS_2_PP"

    def test_innings_2_chase_phase(self):
        phase = self.copilot._detect_phase(innings=2, overs=17.0, wickets=5)
        assert phase == "INNINGS_2_CHASE"


class TestStakeSizing:
    def setup_method(self):
        self.copilot = MatchCopilot(config={
            "copilot_enabled": True,
            "shadow_min_stake_inr": 200,
            "shadow_max_stake_inr": 1000,
            "shadow_default_stake_inr": 500,
        })

    def test_default_stake(self):
        stake = self.copilot._calculate_shadow_stake(edge_size=3.0, confidence="MEDIUM")
        assert 200 <= stake <= 1000

    def test_high_edge_gets_higher_stake(self):
        low = self.copilot._calculate_shadow_stake(edge_size=2.0, confidence="MEDIUM")
        high = self.copilot._calculate_shadow_stake(edge_size=8.0, confidence="HIGH")
        assert high >= low

    def test_stake_clamped_to_bounds(self):
        stake = self.copilot._calculate_shadow_stake(edge_size=100.0, confidence="HIGH")
        assert stake <= 1000
        stake = self.copilot._calculate_shadow_stake(edge_size=0.1, confidence="LOW")
        assert stake >= 200


class TestSessionCallDecision:
    def setup_method(self):
        self.copilot = MatchCopilot(config={
            "copilot_enabled": True,
            "shadow_default_stake_inr": 500,
            "shadow_min_stake_inr": 200,
            "shadow_max_stake_inr": 1000,
            "min_ev_pct": 5.0,
            "message_throttle_seconds": 0,
        })

    def test_generates_call_when_edge_exists(self):
        calls = self.copilot.evaluate_session_calls(
            match_id=1,
            model_predictions={"powerplay_total": {"expected": 59.0, "std_dev": 8.0}},
            estimated_lines={"6_over": {"yes": 55.0, "no": 56.0}},
            overs_completed=1.0,
        )
        assert len(calls) >= 1
        assert calls[0]["direction"] == "YES"
        assert calls[0]["line"] == 56.0

    def test_no_call_when_no_edge(self):
        calls = self.copilot.evaluate_session_calls(
            match_id=1,
            model_predictions={"powerplay_total": {"expected": 55.0, "std_dev": 8.0}},
            estimated_lines={"6_over": {"yes": 55.0, "no": 56.0}},
            overs_completed=1.0,
        )
        assert len(calls) == 0


class TestBookChecking:
    def setup_method(self):
        self.copilot = MatchCopilot(config={
            "copilot_enabled": True,
            "shadow_default_stake_inr": 300,
            "shadow_min_stake_inr": 200,
            "shadow_max_stake_inr": 1000,
            "hedge_min_profit_inr": 100,
            "hedge_session_min_runs": 4,
            "hedge_mw_min_odds_move": 0.20,
            "message_throttle_seconds": 0,
        })

    def test_session_book_opportunity_found(self):
        # Simulate: called YES 56, now line is 62-63
        self.copilot.position_book.add_session_call(1, "6_over", "YES", 56.0, 300.0)
        books = self.copilot.check_book_opportunities(
            match_id=1,
            current_session_lines={"6_over": {"yes": 62.0, "no": 63.0}},
            current_mw_odds={},
        )
        assert len(books) >= 1
        assert books[0].guaranteed_profit >= 100

    def test_mw_book_opportunity_found(self):
        self.copilot.position_book.add_mw_call(1, "SRH", "LAGAI", 2.30, 500.0)
        books = self.copilot.check_book_opportunities(
            match_id=1,
            current_session_lines={},
            current_mw_odds={"SRH": 1.75},
        )
        assert len(books) >= 1
        assert books[0].guaranteed_profit >= 100
```

**Step 2: Run tests**

Run: `python -m pytest tests/test_match_copilot.py -v`
Expected: FAIL

**Step 3: Implement match_copilot.py**

```python
# modules/match_copilot.py
"""Match Co-Pilot — orchestrates live match guidance.

Manages match phases, generates session/MW calls for 10wicket,
checks booking opportunities, and formats over-by-over updates.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from modules.hedge_calculator import HedgeCalculator, BookOpportunity
from modules.position_book import PositionBook

logger = logging.getLogger("ipl_spotter.copilot")

# Session market mapping: market_key -> (prediction_key, description)
SESSION_MARKETS = {
    "6_over": ("powerplay_total", "6 Over"),
    "10_over": ("ten_over_total", "10 Over"),
    "15_over": ("fifteen_over_total", "15 Over"),
    "20_over": ("innings_total", "20 Over"),
}


class MatchCopilot:
    """Orchestrates the match co-pilot experience."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        config = config or {}
        self.enabled = config.get("copilot_enabled", True)
        self.min_stake = config.get("shadow_min_stake_inr", 200)
        self.max_stake = config.get("shadow_max_stake_inr", 1000)
        self.default_stake = config.get("shadow_default_stake_inr", 500)
        self.mw_default_stake = config.get("shadow_mw_default_stake_inr", 500)
        self.min_ev_pct = config.get("min_ev_pct", 5.0)
        self.throttle_seconds = config.get("message_throttle_seconds", 20)

        self.hedge_calculator = HedgeCalculator(config)
        self.position_book = PositionBook()

        # Dedup: avoid sending same call twice
        self._calls_sent: set[str] = set()
        self._last_message_time: float = 0.0
        # Track last over update sent per match
        self._last_over_sent: Dict[int, int] = {}
        # Track MW odds for swing detection
        self._prev_mw_odds: Dict[int, Dict[str, float]] = {}

    # ── Phase detection ──────────────────────────────────────────────────

    def _detect_phase(self, innings: int, overs: float, wickets: int) -> str:
        if innings == 1:
            if overs <= 6.0:
                return "INNINGS_1_PP"
            elif overs <= 15.0:
                return "INNINGS_1_MIDDLE"
            else:
                return "INNINGS_1_DEATH"
        else:
            if overs <= 6.0:
                return "INNINGS_2_PP"
            elif overs <= 15.0:
                return "INNINGS_2_MIDDLE"
            else:
                return "INNINGS_2_CHASE"

    # ── Stake sizing ─────────────────────────────────────────────────────

    def _calculate_shadow_stake(
        self, edge_size: float, confidence: str = "MEDIUM",
    ) -> float:
        """Scale stake based on edge size and confidence."""
        base = self.default_stake
        if confidence == "HIGH":
            base = self.default_stake * 1.5
        elif confidence == "LOW":
            base = self.default_stake * 0.6

        # Scale with edge: bigger edge → higher stake
        scale = min(edge_size / 5.0, 2.0)  # cap at 2x
        stake = base * max(scale, 0.4)

        return max(self.min_stake, min(self.max_stake, round(stake / 50) * 50))

    # ── Session call evaluation ──────────────────────────────────────────

    def evaluate_session_calls(
        self,
        match_id: int,
        model_predictions: Dict[str, Any],
        estimated_lines: Dict[str, Dict[str, float]],
        overs_completed: float,
    ) -> List[Dict[str, Any]]:
        """Evaluate which session markets have edge for 10wicket calls.

        Returns list of call dicts: {market, direction, line, stake_per_run, model, edge}.
        """
        calls = []
        for market_key, (pred_key, display_name) in SESSION_MARKETS.items():
            pred = model_predictions.get(pred_key, {})
            if not pred:
                continue
            expected = pred.get("expected", 0)
            std_dev = pred.get("std_dev", 10)
            if expected == 0:
                continue

            lines = estimated_lines.get(market_key, {})
            line_yes = lines.get("yes", 0)
            line_no = lines.get("no", 0)
            if line_yes == 0 or line_no == 0:
                continue

            # Check dedup
            call_key = f"{match_id}:{market_key}"
            if call_key in self._calls_sent:
                continue

            # Edge = model - line. YES if model > line_no, NO if model < line_yes
            edge_yes = expected - line_no
            edge_no = line_yes - expected

            if edge_yes >= 3.0:  # Model says more runs than NO line
                ev_pct = (edge_yes / std_dev * 100) if std_dev > 0 else 0
                if ev_pct >= self.min_ev_pct or edge_yes >= 4.0:
                    stake = self._calculate_shadow_stake(edge_yes)
                    calls.append({
                        "market": market_key,
                        "market_display": display_name,
                        "direction": "YES",
                        "line": line_no,
                        "stake_per_run": stake,
                        "model": expected,
                        "edge": edge_yes,
                        "confidence": "HIGH" if edge_yes >= 5 else "MEDIUM",
                    })
                    self._calls_sent.add(call_key)

            elif edge_no >= 3.0:  # Model says fewer runs
                ev_pct = (edge_no / std_dev * 100) if std_dev > 0 else 0
                if ev_pct >= self.min_ev_pct or edge_no >= 4.0:
                    stake = self._calculate_shadow_stake(edge_no)
                    calls.append({
                        "market": market_key,
                        "market_display": display_name,
                        "direction": "NO",
                        "line": line_yes,
                        "stake_per_run": stake,
                        "model": expected,
                        "edge": edge_no,
                        "confidence": "HIGH" if edge_no >= 5 else "MEDIUM",
                    })
                    self._calls_sent.add(call_key)

        return calls

    # ── Match Winner call evaluation ─────────────────────────────────────

    def evaluate_mw_call(
        self,
        match_id: int,
        home: str, away: str,
        model_home_prob: float,
        current_home_odds: float, current_away_odds: float,
    ) -> Optional[Dict[str, Any]]:
        """Check if match winner has value for a 10wicket call."""
        call_key = f"{match_id}:mw"
        if call_key in self._calls_sent:
            return None

        # Check each team for value
        for team, prob, odds in [
            (home, model_home_prob, current_home_odds),
            (away, 1 - model_home_prob, current_away_odds),
        ]:
            if prob <= 0 or odds <= 0:
                continue
            fair_odds = 1.0 / prob
            ev_pct = (odds / fair_odds - 1) * 100
            if ev_pct >= self.min_ev_pct:
                self._calls_sent.add(call_key)
                return {
                    "team": team,
                    "direction": "LAGAI",
                    "odds": odds,
                    "stake": self.mw_default_stake,
                    "fair_prob": prob,
                    "ev_pct": ev_pct,
                }
        return None

    # ── Book opportunity checking ────────────────────────────────────────

    def check_book_opportunities(
        self,
        match_id: int,
        current_session_lines: Dict[str, Dict[str, float]],
        current_mw_odds: Dict[str, float],
    ) -> List[BookOpportunity]:
        """Check all open positions for booking opportunities."""
        opportunities = []

        # Check session positions
        for pos in self.position_book.get_open_sessions(match_id):
            lines = current_session_lines.get(pos.market, {})
            if not lines:
                continue
            opp = self.hedge_calculator.check_session_book_opportunity(
                entry_direction=pos.direction,
                entry_line=pos.entry_line,
                current_line_yes=lines.get("yes", 0),
                current_line_no=lines.get("no", 0),
                stake_per_run=pos.stake_per_run,
            )
            if opp:
                opp.market = pos.market
                opportunities.append(opp)

        # Check MW positions
        for pos in self.position_book.get_open_mw(match_id):
            odds = current_mw_odds.get(pos.team, 0)
            if odds <= 0:
                continue
            opp = self.hedge_calculator.check_mw_book_opportunity(
                entry_direction=pos.direction,
                entry_odds=pos.odds,
                entry_stake=pos.stake,
                current_odds=odds,
            )
            if opp:
                opportunities.append(opp)

        return opportunities

    # ── MW swing detection ───────────────────────────────────────────────

    def check_mw_swing(
        self, match_id: int, home: str, away: str,
        home_odds: float, away_odds: float,
        threshold_pct: float = 10.0,
    ) -> Optional[Dict[str, Any]]:
        """Detect significant match winner odds swings."""
        prev = self._prev_mw_odds.get(match_id, {})
        self._prev_mw_odds[match_id] = {home: home_odds, away: away_odds}

        if not prev:
            return None

        for team, odds in [(home, home_odds), (away, away_odds)]:
            old = prev.get(team, 0)
            if old <= 0:
                continue
            old_prob = 1.0 / old * 100
            new_prob = 1.0 / odds * 100
            swing = abs(new_prob - old_prob)
            if swing >= threshold_pct:
                return {
                    "team": team,
                    "old_odds": old,
                    "new_odds": odds,
                    "swing_pct": swing,
                    "direction": "shortened" if odds < old else "drifted",
                }
        return None

    # ── Over update tracking ─────────────────────────────────────────────

    def should_send_over_update(self, match_id: int, over_num: int) -> bool:
        """Check if we should send an over update (avoid duplicates)."""
        last = self._last_over_sent.get(match_id, -1)
        if over_num > last:
            self._last_over_sent[match_id] = over_num
            return True
        return False

    # ── Throttle ─────────────────────────────────────────────────────────

    def can_send_message(self) -> bool:
        """Rate-limit messages."""
        now = time.time()
        if now - self._last_message_time >= self.throttle_seconds:
            self._last_message_time = now
            return True
        return False

    # ── Session line estimation ──────────────────────────────────────────

    def estimate_session_lines(
        self,
        overs_completed: float,
        current_score: int,
        cloudbet_lines: Dict[str, Any],
        model_predictions: Dict[str, Any],
    ) -> Dict[str, Dict[str, float]]:
        """Estimate 10wicket session lines from Cloudbet + run rate.

        Indian book lines are typically close to Cloudbet but with a
        1-2 run gap (book margin). We estimate: yes = cloudbet_line - 1, no = cloudbet_line.
        """
        lines = {}

        for market_key, (pred_key, _) in SESSION_MARKETS.items():
            cb_line = cloudbet_lines.get(market_key, {}).get("line", 0)
            if cb_line > 0:
                lines[market_key] = {
                    "yes": cb_line - 1,  # YES line 1 below
                    "no": cb_line,       # NO line = Cloudbet line
                }
            else:
                # Fallback: use model prediction with 1-run gap
                pred = model_predictions.get(pred_key, {})
                expected = pred.get("expected", 0)
                if expected > 0:
                    lines[market_key] = {
                        "yes": round(expected) - 1,
                        "no": round(expected),
                    }

        return lines

    # ── Reset for new match ──────────────────────────────────────────────

    def reset_match(self, match_id: int) -> None:
        """Clear state for a new match."""
        keys_to_remove = [k for k in self._calls_sent if k.startswith(f"{match_id}:")]
        for k in keys_to_remove:
            self._calls_sent.discard(k)
        self._last_over_sent.pop(match_id, None)
        self._prev_mw_odds.pop(match_id, None)
```

**Step 4: Run tests**

Run: `python -m pytest tests/test_match_copilot.py -v`
Expected: All 12 tests PASS

**Step 5: Commit**

```bash
git add modules/match_copilot.py tests/test_match_copilot.py
git commit -m "feat: add match copilot orchestrator"
```

---

### Task 5: Integrate Co-Pilot Into Spotter

**Files:**
- Modify: `spotter.py` (imports, init, _process_match hook)
- Modify: `ipl_spotter_config.json` (new config keys)
- Modify: `config.py` (no changes needed — it returns raw dict)

**Step 1: Add copilot config to ipl_spotter_config.json**

Add these keys to the existing config JSON:

```json
{
    "copilot_enabled": true,
    "shadow_currency": "INR",
    "shadow_min_stake_inr": 200,
    "shadow_max_stake_inr": 1000,
    "shadow_default_stake_inr": 500,
    "shadow_mw_default_stake_inr": 500,
    "hedge_min_profit_inr": 100,
    "hedge_session_min_runs": 4,
    "hedge_mw_min_odds_move": 0.20,
    "over_by_over_updates": true,
    "match_winner_tracking": true,
    "win_prob_swing_threshold_pct": 10,
    "message_throttle_seconds": 20
}
```

**Step 2: Add copilot imports to spotter.py**

After existing try/except import blocks (~line 60), add:

```python
try:
    from modules.match_copilot import MatchCopilot
except ImportError:
    MatchCopilot = None

try:
    from modules.copilot_telegram import (
        format_session_call,
        format_mw_call,
        format_over_update,
        format_book_alert,
        format_mw_swing,
        format_session_summary,
    )
except ImportError:
    format_session_call = None
```

**Step 3: Initialize copilot in IPLSpotter.__init__**

After existing module initializations, add:

```python
# Match Co-Pilot
self.copilot: Optional[MatchCopilot] = None
if MatchCopilot and config.get("copilot_enabled", False):
    self.copilot = MatchCopilot(config)
    logger.info("Match Co-Pilot ENABLED")
```

**Step 4: Hook copilot into _process_match()**

After the existing `_check_edges()` call (~line 275), add the copilot block:

```python
            # ── Match Co-Pilot ──────────────────────────────────────────
            if self.copilot and self.copilot.enabled:
                self._run_copilot(match_id, home, away, state, predictions, cloudbet_odds)
```

**Step 5: Add _run_copilot method to IPLSpotter**

```python
    def _run_copilot(
        self, match_id: int, home: str, away: str,
        state: MatchState, predictions: dict,
        cloudbet_odds: Optional[dict],
    ) -> None:
        """Run co-pilot logic: session calls, MW calls, book alerts, over updates."""
        if not self.copilot:
            return

        config = self.config
        overs = state.overs_completed
        innings = state.current_innings

        # 1. Estimate 10wicket session lines from Cloudbet
        cb_lines = {}
        if cloudbet_odds:
            for market_key in ["6_over", "10_over", "15_over", "20_over"]:
                cb_market = cloudbet_odds.get(market_key, {})
                if cb_market and "line" in cb_market:
                    cb_lines[market_key] = cb_market

        est_lines = self.copilot.estimate_session_lines(
            overs, state.total_runs, cb_lines, predictions,
        )

        # 2. Evaluate session calls
        calls = self.copilot.evaluate_session_calls(
            match_id, predictions, est_lines, overs,
        )
        for call in calls:
            # Add to shadow portfolio
            pos = self.copilot.position_book.add_session_call(
                match_id, call["market"], call["direction"],
                call["line"], call["stake_per_run"],
            )
            # Format and send Telegram
            if format_session_call and self.copilot.can_send_message():
                cloudbet_info = ""
                # Reference the Cloudbet auto-bet if one was placed this scan
                msg = format_session_call(
                    market=call["market_display"],
                    direction=call["direction"],
                    line=call["line"],
                    stake_per_run=call["stake_per_run"],
                    model_prediction=call["model"],
                    home=home, away=away,
                    cloudbet_info=cloudbet_info,
                )
                self.telegram.send_alert_sync(msg)
                logger.info("COPILOT CALL: %s %s %.0f", call["direction"], call["market"], call["line"])

        # 3. Evaluate MW call
        if config.get("match_winner_tracking", True):
            mw_odds = cloudbet_odds or {}
            home_odds = mw_odds.get("match_winner", {}).get("home_odds", 0)
            away_odds = mw_odds.get("match_winner", {}).get("away_odds", 0)
            # Use TheOddsAPI consensus if available
            model_home_prob = predictions.get("match_winner", {}).get("home_prob", 0.5)

            mw_call = self.copilot.evaluate_mw_call(
                match_id, home, away, model_home_prob, home_odds, away_odds,
            )
            if mw_call and format_mw_call and self.copilot.can_send_message():
                self.copilot.position_book.add_mw_call(
                    match_id, mw_call["team"], mw_call["direction"],
                    mw_call["odds"], mw_call["stake"],
                )
                msg = format_mw_call(**mw_call, home=home, away=away)
                self.telegram.send_alert_sync(msg)
                logger.info("COPILOT MW: %s %s @ %.2f", mw_call["direction"], mw_call["team"], mw_call["odds"])

            # MW swing detection
            if home_odds > 0 and away_odds > 0:
                swing = self.copilot.check_mw_swing(
                    match_id, home, away, home_odds, away_odds,
                    config.get("win_prob_swing_threshold_pct", 10),
                )
                if swing and format_mw_swing and self.copilot.can_send_message():
                    msg = format_mw_swing(
                        team_moved=swing["team"],
                        old_odds=swing["old_odds"],
                        new_odds=swing["new_odds"],
                        home=home, away=away,
                        home_odds=home_odds, away_odds=away_odds,
                    )
                    self.telegram.send_alert_sync(msg)

        # 4. Check book opportunities
        current_mw_odds = {}
        if cloudbet_odds and "match_winner" in cloudbet_odds:
            mw = cloudbet_odds["match_winner"]
            if mw.get("home_odds"):
                current_mw_odds[home] = mw["home_odds"]
            if mw.get("away_odds"):
                current_mw_odds[away] = mw["away_odds"]

        books = self.copilot.check_book_opportunities(match_id, est_lines, current_mw_odds)
        for book in books:
            if format_book_alert and self.copilot.can_send_message():
                msg = format_book_alert(
                    market_type=book.market,
                    market_name=book.market if book.market != "match_winner" else "",
                    action=book.action,
                    guaranteed_profit=book.guaranteed_profit,
                    math_breakdown=book.math_breakdown,
                )
                self.telegram.send_alert_sync(msg)
                logger.info("COPILOT BOOK: %s profit=%.0f", book.market, book.guaranteed_profit)

                # Auto-book the shadow position
                if book.market == "match_winner":
                    for pos in self.copilot.position_book.get_open_mw(match_id):
                        if book.exit_odds:
                            self.copilot.position_book.book_mw(pos, book.exit_odds)
                            break
                else:
                    for pos in self.copilot.position_book.get_open_sessions(match_id):
                        if pos.market == book.market and book.exit_line:
                            self.copilot.position_book.book_session(pos, book.exit_line)
                            break

        # 5. Over-by-over update
        if config.get("over_by_over_updates", True):
            over_num = int(overs)
            if over_num > 0 and self.copilot.should_send_over_update(match_id, over_num):
                # Build positions summary
                pos_lines = []
                for p in self.copilot.position_book.get_open_sessions(match_id):
                    est_current = est_lines.get(p.market, {}).get("no", 0)
                    diff = est_current - p.entry_line if p.direction == "YES" else p.entry_line - est_current
                    status = f"+{diff:.0f} runs" if diff > 0 else f"{diff:.0f} runs"
                    pos_lines.append(f"{p.direction} {p.entry_line:.0f} ({p.market}): {status}")

                mw_data = cloudbet_odds.get("match_winner", {}) if cloudbet_odds else {}

                if format_over_update and self.copilot.can_send_message():
                    msg = format_over_update(
                        over_num=over_num,
                        innings=innings,
                        batting_team=state.batting_team,
                        score=state.total_runs,
                        wickets=state.wickets,
                        run_rate=state.current_run_rate,
                        projected_total=predictions.get("innings_total", {}).get("expected", 0),
                        mw_home_odds=mw_data.get("home_odds", 0),
                        mw_away_odds=mw_data.get("away_odds", 0),
                        home=home, away=away,
                        positions_summary=" | ".join(pos_lines) if pos_lines else "",
                    )
                    self.telegram.send_alert_sync(msg)
```

**Step 6: Run all tests**

Run: `python -m pytest tests/ -q`
Expected: All tests pass (406 existing + ~43 new = ~449)

**Step 7: Commit**

```bash
git add spotter.py ipl_spotter_config.json
git commit -m "feat: integrate match copilot into spotter main loop"
```

---

### Task 6: Full Integration Test

**Files:**
- Create: `tests/test_copilot_integration.py`

**Step 1: Write integration test**

```python
# tests/test_copilot_integration.py
"""Integration test: simulate a mini-match through the copilot pipeline."""

import pytest
from modules.match_copilot import MatchCopilot
from modules.copilot_telegram import format_session_call, format_book_alert


def test_full_session_lifecycle():
    """Simulate: call YES 56, line moves to 64, book it, settle."""
    copilot = MatchCopilot(config={
        "copilot_enabled": True,
        "shadow_default_stake_inr": 300,
        "shadow_min_stake_inr": 200,
        "shadow_max_stake_inr": 1000,
        "hedge_min_profit_inr": 100,
        "hedge_session_min_runs": 4,
        "hedge_mw_min_odds_move": 0.20,
        "min_ev_pct": 5.0,
        "message_throttle_seconds": 0,
    })

    # 1. Bot evaluates and finds edge
    calls = copilot.evaluate_session_calls(
        match_id=1,
        model_predictions={"powerplay_total": {"expected": 62.0, "std_dev": 8.0}},
        estimated_lines={"6_over": {"yes": 55.0, "no": 56.0}},
        overs_completed=1.0,
    )
    assert len(calls) == 1
    assert calls[0]["direction"] == "YES"

    # 2. Add to shadow portfolio
    pos = copilot.position_book.add_session_call(
        1, "6_over", "YES", 56.0, calls[0]["stake_per_run"],
    )
    assert pos.status == "OPEN"

    # 3. Line moves up — check book
    books = copilot.check_book_opportunities(
        match_id=1,
        current_session_lines={"6_over": {"yes": 63.0, "no": 64.0}},
        current_mw_odds={},
    )
    assert len(books) == 1
    assert books[0].guaranteed_profit > 0

    # 4. Book it
    profit = copilot.position_book.book_session(pos, exit_line=64.0)
    assert profit == (64 - 56) * pos.stake_per_run

    # 5. Settle
    copilot.position_book.settle_session(pos, actual_total=68.0)
    assert pos.status == "SETTLED"
    assert pos.pnl == profit  # Booked = guaranteed

    # 6. Verify Telegram message formats work
    msg = format_session_call(
        market="6 Over", direction="YES", line=56.0,
        stake_per_run=pos.stake_per_run, model_prediction=62.0,
        home="RCB", away="SRH",
    )
    assert "YES 56" in msg

    book_msg = format_book_alert(
        market_type="session", market_name="6 Over",
        action="Khai NO 64", guaranteed_profit=profit,
        math_breakdown=f"Rs {profit:.0f} guaranteed",
    )
    assert "BOOK" in book_msg


def test_full_mw_lifecycle():
    """Simulate: lagai SRH @ 2.30, odds shorten to 1.75, book it."""
    copilot = MatchCopilot(config={
        "copilot_enabled": True,
        "shadow_mw_default_stake_inr": 500,
        "shadow_min_stake_inr": 200,
        "shadow_max_stake_inr": 1000,
        "hedge_min_profit_inr": 100,
        "hedge_mw_min_odds_move": 0.20,
        "min_ev_pct": 5.0,
        "message_throttle_seconds": 0,
    })

    # 1. MW call
    mw = copilot.evaluate_mw_call(
        match_id=1, home="RCB", away="SRH",
        model_home_prob=0.55,
        current_home_odds=1.65, current_away_odds=2.30,
    )
    assert mw is not None
    assert mw["team"] == "SRH"

    # 2. Add to portfolio
    pos = copilot.position_book.add_mw_call(1, "SRH", "LAGAI", 2.30, 500.0)

    # 3. Odds shorten
    books = copilot.check_book_opportunities(
        match_id=1,
        current_session_lines={},
        current_mw_odds={"SRH": 1.75},
    )
    assert len(books) == 1
    assert books[0].guaranteed_profit >= 100

    # 4. Book
    copilot.position_book.book_mw(pos, exit_odds=1.75)
    assert pos.status == "BOOKED"

    # 5. Total PnL
    copilot.position_book.settle_mw(pos, team_won=True)
    assert pos.pnl == pos.booked_profit
```

**Step 2: Run all tests**

Run: `python -m pytest tests/ -q`
Expected: All pass

**Step 3: Commit**

```bash
git add tests/test_copilot_integration.py
git commit -m "feat: add copilot integration tests"
```

---

### Task 7: Update Banner and Status Display

**Files:**
- Modify: `spotter.py` (banner, _print_match_state)

**Step 1: Update banner to show copilot status**

In the startup banner section, after the existing lines, add:

```python
    copilot_status = "ON" if (spotter.copilot and spotter.copilot.enabled) else "OFF"
    # Add to banner:
    # ║  Co-Pilot: ON | Shadow: INR 200-1000/bet              ║
```

**Step 2: Add shadow portfolio to terminal match display**

In `_print_match_state()`, after existing odds display, add a copilot positions section showing open shadow positions and their current status.

**Step 3: Run all tests and commit**

```bash
python -m pytest tests/ -q
git add spotter.py
git commit -m "feat: update banner and terminal display with copilot status"
```

---

### Task 8: Run Full Test Suite and Deploy to Windows VM

**Step 1: Run complete test suite**

Run: `python -m pytest tests/ -v --tb=short`
Expected: All tests pass (~450+)

**Step 2: Commit any remaining changes**

```bash
git add -A
git commit -m "chore: match copilot v1 complete"
```

**Step 3: Deploy to Windows VM**

Copy updated files to `C:\Users\Administrator\.openclaw\workspace\ipl_bot\` and restart the bot.

**Step 4: Verify on VM**

```cmd
python -m pytest tests/ -q
python spotter.py
```
Expected: Bot starts with Co-Pilot enabled, shows in banner, ready for RCB vs SRH on March 28.
