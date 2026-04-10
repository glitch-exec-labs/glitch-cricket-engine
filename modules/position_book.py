"""Position Book — tracks shadow (Indian book) and Cloudbet positions.

Shadow positions are assumed: when the bot sends a call, it assumes the user followed.
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
    market: str
    direction: str
    entry_line: float
    stake_per_run: float
    innings: int = 1
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "OPEN"
    exit_line: Optional[float] = None
    booked_profit: Optional[float] = None
    pnl: float = 0.0


@dataclass
class MWPosition:
    match_id: int
    team: str
    direction: str
    odds: float
    stake: float
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "OPEN"
    exit_odds: Optional[float] = None
    exit_stake: Optional[float] = None
    booked_profit: Optional[float] = None
    pnl: float = 0.0


class PositionBook:
    def __init__(self) -> None:
        self.shadow_sessions: List[SessionPosition] = []
        self.shadow_mw: List[MWPosition] = []

    def add_session_call(self, match_id, market, direction, line, stake_per_run, innings: int = 1) -> SessionPosition:
        pos = SessionPosition(
            match_id=match_id,
            market=market,
            innings=innings,
            direction=direction,
            entry_line=line,
            stake_per_run=stake_per_run,
        )
        self.shadow_sessions.append(pos)
        logger.info(
            "Shadow session: innings=%d %s %s %.0f @ Rs %.0f/run",
            innings,
            direction,
            market,
            line,
            stake_per_run,
        )
        return pos

    def book_session(self, pos: SessionPosition, exit_line: float) -> float:
        if pos.direction == "YES":
            profit = (exit_line - pos.entry_line) * pos.stake_per_run
        else:
            profit = (pos.entry_line - exit_line) * pos.stake_per_run
        pos.exit_line = exit_line
        pos.booked_profit = profit
        pos.status = "BOOKED"
        return profit

    def settle_session(self, pos: SessionPosition, actual_total: float) -> None:
        if pos.status == "BOOKED":
            pos.pnl = pos.booked_profit or 0.0
            pos.status = "SETTLED"
        elif pos.direction == "YES":
            if actual_total > pos.entry_line:
                pos.pnl = (actual_total - pos.entry_line) * pos.stake_per_run
                pos.status = "WON"
            else:
                pos.pnl = -(pos.entry_line - actual_total) * pos.stake_per_run
                pos.status = "LOST"
        else:
            if actual_total < pos.entry_line:
                pos.pnl = (pos.entry_line - actual_total) * pos.stake_per_run
                pos.status = "WON"
            else:
                pos.pnl = -(actual_total - pos.entry_line) * pos.stake_per_run
                pos.status = "LOST"

    def add_mw_call(self, match_id, team, direction, odds, stake) -> MWPosition:
        pos = MWPosition(match_id=match_id, team=team, direction=direction, odds=odds, stake=stake)
        self.shadow_mw.append(pos)
        logger.info("Shadow MW: %s %s @ %.2f for Rs %.0f", direction, team, odds, stake)
        return pos

    def book_mw(self, pos: MWPosition, exit_odds: float) -> float:
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
        return profit

    def settle_mw(self, pos: MWPosition, team_won: bool) -> None:
        if pos.status == "BOOKED":
            pos.pnl = pos.booked_profit or 0.0
            pos.status = "SETTLED"
        elif pos.direction == "LAGAI":
            pos.pnl = pos.stake * (pos.odds - 1) if team_won else -pos.stake
            pos.status = "WON" if team_won else "LOST"
        else:
            pos.pnl = pos.stake if not team_won else -pos.stake * (pos.odds - 1)
            pos.status = "WON" if not team_won else "LOST"

    def get_open_sessions(self, match_id=None, innings: int | None = None) -> List[SessionPosition]:
        return [
            p for p in self.shadow_sessions
            if p.status == "OPEN"
            and (match_id is None or p.match_id == match_id)
            and (innings is None or p.innings == innings)
        ]

    def get_open_mw(self, match_id=None) -> List[MWPosition]:
        return [p for p in self.shadow_mw if p.status == "OPEN" and (match_id is None or p.match_id == match_id)]

    def get_all_positions(self, match_id=None) -> List:
        all_pos = self.shadow_sessions + self.shadow_mw
        if match_id is not None:
            return [p for p in all_pos if p.match_id == match_id]
        return all_pos

    def get_total_shadow_pnl(self) -> float:
        session_pnl = sum(p.pnl for p in self.shadow_sessions if p.status in ("WON", "LOST", "SETTLED"))
        mw_pnl = sum(p.pnl for p in self.shadow_mw if p.status in ("WON", "LOST", "SETTLED"))
        return session_pnl + mw_pnl
