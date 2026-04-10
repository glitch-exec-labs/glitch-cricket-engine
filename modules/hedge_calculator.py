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

    def calculate_session_book(
        self, entry_direction: str, entry_line: float, stake_per_run: float,
        current_line_no: float | None = None, current_line_yes: float | None = None,
    ) -> dict[str, Any]:
        if entry_direction == "YES":
            exit_line = current_line_no or 0.0
            profit = (exit_line - entry_line) * stake_per_run
            action = f"Khai NO {exit_line:.0f}"
        else:
            exit_line = current_line_yes or 0.0
            profit = (entry_line - exit_line) * stake_per_run
            action = f"Lagai YES {exit_line:.0f}"
        return {
            "guaranteed_profit": profit,
            "action": action,
            "exit_line": exit_line,
            "exit_stake_per_run": stake_per_run,
            "math_breakdown": self._session_math_str(entry_direction, entry_line, exit_line, stake_per_run, profit),
        }

    def _session_math_str(self, direction, entry, exit_, stake, profit):
        if direction == "YES":
            return f"YES {entry:.0f} + NO {exit_:.0f} @ Rs {stake:.0f}/run\n  Guaranteed: ({exit_:.0f} - {entry:.0f}) x {stake:.0f} = Rs {profit:.0f}"
        return f"NO {entry:.0f} + YES {exit_:.0f} @ Rs {stake:.0f}/run\n  Guaranteed: ({entry:.0f} - {exit_:.0f}) x {stake:.0f} = Rs {profit:.0f}"

    def calculate_mw_book(
        self, entry_direction: str, entry_odds: float, entry_stake: float, exit_odds: float,
    ) -> dict[str, Any]:
        if exit_odds <= 0:
            return {
                "guaranteed_profit": 0.0,
                "exit_stake": 0.0,
                "if_wins": 0.0,
                "if_loses": 0.0,
                "action": "N/A — market suspended",
                "math_breakdown": "",
            }
        exit_stake = round(entry_stake * entry_odds / exit_odds, 2)
        if entry_direction == "LAGAI":
            if_wins = entry_stake * (entry_odds - 1) - exit_stake * (exit_odds - 1)
            if_loses = -entry_stake + exit_stake
            action = f"Khai @ {exit_odds:.2f} for Rs {exit_stake:.0f}"
        else:
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
            "math_breakdown": f"  Wins: Rs {if_wins:+.0f} | Loses: Rs {if_loses:+.0f}\n  Guaranteed: Rs {guaranteed:.0f}",
        }

    def check_session_book_opportunity(
        self, entry_direction: str, entry_line: float,
        current_line_yes: float, current_line_no: float, stake_per_run: float,
    ) -> BookOpportunity | None:
        if entry_direction == "YES":
            line_moved = current_line_no - entry_line
            result = self.calculate_session_book("YES", entry_line, stake_per_run, current_line_no=current_line_no)
        else:
            line_moved = entry_line - current_line_yes
            result = self.calculate_session_book("NO", entry_line, stake_per_run, current_line_yes=current_line_yes)
        if line_moved < self.session_min_runs:
            return None
        if result["guaranteed_profit"] < self.min_profit_inr:
            return None
        return BookOpportunity(
            market="session", action=result["action"],
            guaranteed_profit=result["guaranteed_profit"],
            exit_line=result["exit_line"], math_breakdown=result["math_breakdown"],
        )

    def check_mw_book_opportunity(
        self, entry_direction: str, entry_odds: float, entry_stake: float, current_odds: float,
    ) -> BookOpportunity | None:
        if entry_direction == "LAGAI":
            odds_moved = entry_odds - current_odds
        else:
            odds_moved = current_odds - entry_odds
        if odds_moved < self.mw_min_odds_move:
            return None
        result = self.calculate_mw_book(entry_direction, entry_odds, entry_stake, current_odds)
        if result["guaranteed_profit"] < self.min_profit_inr:
            return None
        return BookOpportunity(
            market="match_winner", action=result["action"],
            guaranteed_profit=result["guaranteed_profit"],
            exit_odds=current_odds, exit_stake=result["exit_stake"],
            math_breakdown=result["math_breakdown"],
        )
