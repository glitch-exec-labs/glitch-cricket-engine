"""
Risk Manager — controls stake sizing and enforces betting limits.

Uses fractional Kelly criterion for position sizing and tracks
daily P&L, open exposure, and per-market cooldowns.

Pure Python, no external dependencies.
"""

from __future__ import annotations

import time
from typing import Any


class RiskManager:
    """Manages bankroll, stake sizing, and risk limits for live betting."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        config = config or {}
        self.bankroll_usd: float = float(config.get("bankroll_usd", 1.0))
        self.max_position_pct: float = float(config.get("max_position_pct", 0.15))
        self._static_max_position_size_usd: float | None = config.get("max_position_size_usd")
        self.max_position_size_usd: float = 0.0
        self.max_open_bets: int = config.get("max_open_bets", 3)
        self.daily_loss_limit_usd: float = config.get("daily_loss_limit_usd", 0.50)
        self.min_ev_pct: float = config.get("min_ev_pct", 5.0)
        self.fractional_kelly: float = config.get("fractional_kelly", 0.25)
        self.min_stake_usd: float = config.get("min_stake_usd", 0.10)
        self.default_stake_usd: float = config.get("default_stake_usd", 0.0)
        self.min_odds: float = config.get("min_odds", 1.30)
        self.max_odds: float = config.get("max_odds", 5.00)
        self.cooldown_seconds: int = config.get("cooldown_seconds", 60)

        # Mutable state
        self.daily_pnl: float = 0.0
        self.open_exposure: float = 0.0
        self.last_bet_times: dict[str, float] = {}
        self._recalculate_max_position_size()

    def _recalculate_max_position_size(self) -> None:
        """Update the max stake cap from the current bankroll."""
        if self._static_max_position_size_usd is not None:
            self.max_position_size_usd = float(self._static_max_position_size_usd)
            return
        self.max_position_size_usd = round(self.bankroll_usd * self.max_position_pct, 2)

    def update_bankroll(self, balance: float) -> None:
        """Set bankroll from the latest live balance and refresh dependent caps."""
        try:
            bankroll = float(balance)
        except (TypeError, ValueError):
            return
        if bankroll <= 0:
            return
        self.bankroll_usd = bankroll
        self._recalculate_max_position_size()

    # ── stake sizing ──────────────────────────────────────────────────────

    # Market-based stake multipliers:
    #   Session markets (10-over, 15-over) = full size — more data, safer bets
    #   Single-over bets = smaller — volatile, riskier
    #   6-over = medium — some data but still early
    MARKET_STAKE_MULT: dict[str, float] = {
        "10_over": 1.5,
        "15_over": 1.5,
        "20_over": 1.3,
        "innings_total": 1.3,
        "6_over": 1.0,
        "powerplay_runs": 1.0,
        "over_runs": 0.5,
        "match_winner": 0.8,
    }

    def calculate_stake(
        self,
        ev_pct: float,
        odds: float,
        bankroll: float | None = None,
        market: str = "",
    ) -> float:
        """Return optimal stake in USD using fractional Kelly criterion.

        Scales stake based on market type:
          10/15-over sessions → larger (safer, more data)
          single-over bets → smaller (volatile)

        Returns 0.0 if EV is below the minimum threshold.
        """
        if ev_pct < self.min_ev_pct:
            return 0.0
        if odds <= 1.0:
            return 0.0

        bankroll = bankroll if bankroll is not None else self.bankroll_usd
        ev = ev_pct / 100.0
        kelly_fraction = ev / (odds - 1.0)
        kelly_fraction *= self.fractional_kelly
        stake = bankroll * kelly_fraction

        # Scale by market type
        market_mult = self.MARKET_STAKE_MULT.get(market, 1.0)
        stake *= market_mult

        # Apply default stake as floor if configured — but only when the floor
        # doesn't exceed the available bankroll. With a depleted primary bankroll
        # ($0.31) we must NOT inflate the stake beyond what can actually be placed;
        # client accounts size their own bets independently via _get_account_stake.
        if self.default_stake_usd > 0:
            floor = self.default_stake_usd * market_mult
            if floor <= bankroll:
                stake = max(stake, floor)

        # Cap at max position size
        stake = min(stake, self.max_position_size_usd)

        # Floor at configured minimum
        if stake < self.min_stake_usd:
            return 0.0

        return round(stake, 2)

    # ── pre-bet checks ────────────────────────────────────────────────────

    def can_place_bet(
        self,
        ev_pct: float,
        odds: float,
        market_key: str,
        open_bets_count: int,
    ) -> tuple[bool, str]:
        """Check whether a new bet is allowed under current risk limits.

        Returns (True, "OK") or (False, "<reason>").
        """
        if ev_pct < self.min_ev_pct:
            return False, f"EV {ev_pct:.1f}% below minimum {self.min_ev_pct:.1f}%"

        if odds < self.min_odds:
            return False, f"Odds {odds:.2f} below minimum {self.min_odds:.2f}"

        if odds > self.max_odds:
            return False, f"Odds {odds:.2f} above maximum {self.max_odds:.2f}"

        if open_bets_count >= self.max_open_bets:
            return False, f"Open bets ({open_bets_count}) at limit ({self.max_open_bets})"

        if self.daily_pnl <= -self.daily_loss_limit_usd:
            return False, f"Daily loss limit reached (PnL: ${self.daily_pnl:.2f})"

        now = time.time()
        last_time = self.last_bet_times.get(market_key)
        if last_time is not None:
            elapsed = now - last_time
            if elapsed < self.cooldown_seconds:
                remaining = int(self.cooldown_seconds - elapsed)
                return False, f"Cooldown active on {market_key} ({remaining}s remaining)"

        return True, "OK"

    # ── state tracking ────────────────────────────────────────────────────

    def record_bet_placed(self, market_key: str, stake: float) -> None:
        """Record that a bet was placed — update exposure and cooldown."""
        self.open_exposure += stake
        self.last_bet_times[market_key] = time.time()

    def record_bet_settled(self, pnl: float, stake: float = 0.0) -> None:
        """Record a settled bet — update daily P&L and release exposure.

        Args:
            pnl:   profit/loss amount (negative for loss, positive for win).
            stake: original stake placed; used to correctly release exposure.
                   If not provided, falls back to abs(pnl) as an approximation.
        """
        self.daily_pnl += pnl
        # Release exactly the stake that was locked when the bet was placed.
        # pnl alone is wrong for wins: pnl = stake*(odds-1), not stake itself.
        release = stake if stake > 0 else abs(pnl)
        self.open_exposure = max(0.0, self.open_exposure - release)

    def reset_daily(self) -> None:
        """Reset daily P&L — call at the start of each new day."""
        self.daily_pnl = 0.0

    # ── status ────────────────────────────────────────────────────────────

    def get_status(self) -> dict[str, Any]:
        """Return a snapshot of current risk state and configuration."""
        return {
            "bankroll_usd": self.bankroll_usd,
            "daily_pnl": round(self.daily_pnl, 2),
            "open_exposure": round(self.open_exposure, 2),
            "max_position_size_usd": self.max_position_size_usd,
            "max_position_pct": self.max_position_pct,
            "max_open_bets": self.max_open_bets,
            "daily_loss_limit_usd": self.daily_loss_limit_usd,
            "min_ev_pct": self.min_ev_pct,
            "fractional_kelly": self.fractional_kelly,
            "min_odds": self.min_odds,
            "max_odds": self.max_odds,
            "cooldown_seconds": self.cooldown_seconds,
            "active_cooldowns": len(self.last_bet_times),
        }
