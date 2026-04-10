"""Smart Staking Engine — percentage-based capital management with streak logic.

Replaces fixed-dollar or pure-Kelly sizing with a system that:
  - Uses % of bankroll (5-10%) for entries, scalable from $1 to $1M.
  - Tracks consecutive win/loss streaks per market and overall.
  - Increases bet size after wins (momentum), decreases after losses (preservation).
  - Keeps a full bet history to learn from past performance.
  - Integrates with the series profile for per-competition tuning.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("ipl_spotter.smart_staking")


@dataclass
class BetRecord:
    """A single historical bet for learning."""
    market: str
    direction: str
    line: float
    odds: float
    stake_usd: float
    stake_pct: float       # % of bankroll at time of bet
    ev_pct: float
    result: str            # WIN, LOSS, PUSH, PENDING
    pnl: float
    bankroll_before: float
    bankroll_after: float
    timestamp: float
    match_id: int = 0
    innings: int = 1
    trigger: str = ""


@dataclass
class StakingState:
    """Mutable state tracked across bets within a session."""
    # Overall streak
    current_streak: int = 0        # positive = wins, negative = losses
    max_win_streak: int = 0
    max_loss_streak: int = 0

    # Per-market streaks
    market_streaks: Dict[str, int] = field(default_factory=dict)

    # Session stats
    bets_placed: int = 0
    bets_won: int = 0
    bets_lost: int = 0
    session_pnl: float = 0.0

    # Bet history for learning
    history: List[BetRecord] = field(default_factory=list)

    # Last bet per market (for timing analysis)
    last_bet_time: Dict[str, float] = field(default_factory=dict)


class SmartStakingEngine:
    """Percentage-based stake sizing with streak-aware adjustments.

    Config keys (all optional, with sensible defaults):
      - smart_staking_base_pct: float (default 0.07 = 7% of bankroll)
      - smart_staking_min_pct: float (default 0.05 = 5%)
      - smart_staking_max_pct: float (default 0.15 = 15%)
      - smart_staking_loss_scale: float (default 0.7 — multiply by this after loss)
      - smart_staking_win_scale: float (default 1.2 — multiply by this after win)
      - smart_staking_max_streak_boost: int (default 3 — cap streak bonus at 3 wins)
      - smart_staking_recovery_mode_after: int (default 3 — enter recovery after 3 losses)
      - smart_staking_recovery_pct: float (default 0.05 — 5% in recovery mode)
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        config = config or {}
        self.base_pct: float = config.get("smart_staking_base_pct", 0.07)
        self.min_pct: float = config.get("smart_staking_min_pct", 0.05)
        self.max_pct: float = config.get("smart_staking_max_pct", 0.15)
        self.loss_scale: float = config.get("smart_staking_loss_scale", 0.7)
        self.win_scale: float = config.get("smart_staking_win_scale", 1.2)
        self.max_streak_boost: int = config.get("smart_staking_max_streak_boost", 3)
        self.recovery_after: int = config.get("smart_staking_recovery_mode_after", 3)
        self.recovery_pct: float = config.get("smart_staking_recovery_pct", 0.05)

        self.state = StakingState()

    # ── Stake calculation ─────────────────────────────────────────────

    def calculate_stake(
        self,
        bankroll: float,
        market: str = "",
        ev_pct: float = 0.0,
        odds: float = 1.85,
        is_first_entry: bool = False,
    ) -> tuple[float, float, str]:
        """Calculate the optimal stake based on bankroll %, streaks, and context.

        Returns:
            (stake_usd, stake_pct, reason) — the dollar amount, the percentage
            of bankroll used, and a human-readable reason for the sizing decision.
        """
        if bankroll <= 0:
            return 0.0, 0.0, "bankroll_zero"

        # Start with base percentage
        pct = self.base_pct
        reason_parts = [f"base={self.base_pct:.0%}"]

        # First entry of a match — be more conservative
        if is_first_entry:
            pct = self.min_pct
            reason_parts = [f"first_entry={self.min_pct:.0%}"]

        # Recovery mode: after N consecutive losses, drop to minimum
        if self.state.current_streak <= -self.recovery_after:
            pct = self.recovery_pct
            reason_parts = [f"recovery_mode={self.recovery_pct:.0%} (streak={self.state.current_streak})"]

        # Loss adjustment: scale down after each consecutive loss
        elif self.state.current_streak < 0:
            loss_count = abs(self.state.current_streak)
            scale = self.loss_scale ** loss_count
            pct = max(self.min_pct, pct * scale)
            reason_parts.append(f"loss_adj={scale:.2f} (streak={self.state.current_streak})")

        # Win adjustment: scale up after each consecutive win (capped)
        elif self.state.current_streak > 0:
            win_count = min(self.state.current_streak, self.max_streak_boost)
            scale = self.win_scale ** win_count
            pct = min(self.max_pct, pct * scale)
            reason_parts.append(f"win_adj={scale:.2f} (streak=+{self.state.current_streak})")

        # Per-market streak adjustment
        market_streak = self.state.market_streaks.get(market, 0)
        if market_streak <= -2:
            # This specific market is cold — reduce further
            pct = max(self.min_pct, pct * 0.8)
            reason_parts.append(f"market_cold={market}({market_streak})")
        elif market_streak >= 2:
            # This market is hot — allow slight boost
            pct = min(self.max_pct, pct * 1.1)
            reason_parts.append(f"market_hot={market}(+{market_streak})")

        # EV bonus: higher EV → slight boost (above 15% EV)
        if ev_pct > 15.0:
            ev_boost = min(1.3, 1.0 + (ev_pct - 15.0) / 50.0)
            pct = min(self.max_pct, pct * ev_boost)
            reason_parts.append(f"ev_boost={ev_boost:.2f}")

        # Clamp
        pct = max(self.min_pct, min(self.max_pct, pct))

        stake = round(bankroll * pct, 2)
        reason = " | ".join(reason_parts)

        logger.info(
            "SmartStake: bankroll=$%.2f pct=%.1f%% stake=$%.2f [%s]",
            bankroll, pct * 100, stake, reason,
        )

        return stake, pct, reason

    # ── Result recording ──────────────────────────────────────────────

    def record_result(
        self,
        market: str,
        result: str,
        pnl: float,
        stake: float,
        bankroll_before: float,
        odds: float = 0.0,
        ev_pct: float = 0.0,
        direction: str = "",
        line: float = 0.0,
        match_id: int = 0,
        innings: int = 1,
        trigger: str = "",
    ) -> None:
        """Record a bet result and update streaks.

        result: "WIN", "LOSS", "PUSH" (or Cloudbet: "WIN", "LOSS", "PUSH",
                "HALF_WIN", "HALF_LOSS", "PARTIAL")
        """
        # Normalize result for streak tracking
        is_win = result in ("WIN", "HALF_WIN", "WON")
        is_loss = result in ("LOSS", "HALF_LOSS", "LOST")

        # Update overall streak
        if is_win:
            self.state.bets_won += 1
            if self.state.current_streak >= 0:
                self.state.current_streak += 1
            else:
                self.state.current_streak = 1
            self.state.max_win_streak = max(self.state.max_win_streak, self.state.current_streak)
        elif is_loss:
            self.state.bets_lost += 1
            if self.state.current_streak <= 0:
                self.state.current_streak -= 1
            else:
                self.state.current_streak = -1
            self.state.max_loss_streak = max(self.state.max_loss_streak, abs(self.state.current_streak))
        # PUSH/PARTIAL/VOID don't change streak

        # Update per-market streak
        if is_win:
            prev = self.state.market_streaks.get(market, 0)
            self.state.market_streaks[market] = (prev + 1) if prev >= 0 else 1
        elif is_loss:
            prev = self.state.market_streaks.get(market, 0)
            self.state.market_streaks[market] = (prev - 1) if prev <= 0 else -1

        self.state.bets_placed += 1
        self.state.session_pnl += pnl

        # Record in history
        bankroll_after = bankroll_before + pnl
        stake_pct = (stake / bankroll_before) if bankroll_before > 0 else 0
        record = BetRecord(
            market=market,
            direction=direction,
            line=line,
            odds=odds,
            stake_usd=stake,
            stake_pct=stake_pct,
            ev_pct=ev_pct,
            result=result,
            pnl=pnl,
            bankroll_before=bankroll_before,
            bankroll_after=bankroll_after,
            timestamp=time.time(),
            match_id=match_id,
            innings=innings,
            trigger=trigger,
        )
        self.state.history.append(record)

        # Keep history bounded
        if len(self.state.history) > 500:
            self.state.history = self.state.history[-500:]

        logger.info(
            "SmartStake result: %s %s %s pnl=$%.2f streak=%+d market_streak(%s)=%+d session_pnl=$%.2f",
            result, market, direction, pnl, self.state.current_streak,
            market, self.state.market_streaks.get(market, 0), self.state.session_pnl,
        )

    # ── Analytics ─────────────────────────────────────────────────────

    def get_win_rate(self, last_n: int = 0) -> float:
        """Return win rate as a fraction (0.0 - 1.0).  last_n=0 means all."""
        history = self.state.history[-last_n:] if last_n > 0 else self.state.history
        if not history:
            return 0.0
        wins = sum(1 for r in history if r.result in ("WIN", "HALF_WIN", "WON"))
        total = sum(1 for r in history if r.result not in ("PUSH", "PARTIAL", "PENDING"))
        return wins / total if total > 0 else 0.0

    def get_market_win_rate(self, market: str) -> float:
        """Win rate for a specific market."""
        relevant = [r for r in self.state.history if r.market == market]
        if not relevant:
            return 0.0
        wins = sum(1 for r in relevant if r.result in ("WIN", "HALF_WIN", "WON"))
        total = sum(1 for r in relevant if r.result not in ("PUSH", "PARTIAL", "PENDING"))
        return wins / total if total > 0 else 0.0

    def get_status(self) -> dict[str, Any]:
        """Return a snapshot for logging / Telegram."""
        return {
            "streak": self.state.current_streak,
            "max_win_streak": self.state.max_win_streak,
            "max_loss_streak": self.state.max_loss_streak,
            "bets_placed": self.state.bets_placed,
            "bets_won": self.state.bets_won,
            "bets_lost": self.state.bets_lost,
            "win_rate": round(self.get_win_rate() * 100, 1),
            "session_pnl": round(self.state.session_pnl, 2),
            "market_streaks": dict(self.state.market_streaks),
        }

    def format_telegram_status(self) -> str:
        """Format a short Telegram-friendly status line."""
        s = self.state
        wr = self.get_win_rate()
        streak_icon = "🔥" if s.current_streak >= 2 else "❄️" if s.current_streak <= -2 else "➡️"
        return (
            f"{streak_icon} Streak: {s.current_streak:+d} | "
            f"W/L: {s.bets_won}/{s.bets_lost} ({wr:.0%}) | "
            f"Session: ${s.session_pnl:+.2f}"
        )
