"""Per-series tunable parameters for the Cricket Edge Spotter.

Each series (IPL, PSL, BBL, etc.) gets its own frozen SeriesProfile that
controls thresholds, cooldowns, enabled markets, Kelly sizing, and model
flags.  The engine code reads values from the profile instead of
hard-coded constants.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet


@dataclass(frozen=True)
class SeriesProfile:
    """All per-series tunable parameters.  Immutable after creation."""

    # ── Identity ──────────────────────────────────────────────────────
    series_key: str                     # "ipl", "psl", "bbl", …
    display_name: str                   # "Indian Premier League"

    # ── Markets enabled for auto-betting ──────────────────────────────
    enabled_markets: FrozenSet[str] = frozenset({
        "match_winner", "innings_total", "6_over", "10_over",
        "15_over", "over_runs",
    })

    # ── Edge thresholds ───────────────────────────────────────────────
    entry_edge: float = 0.05            # min edge to consider
    exit_edge: float = 0.02             # below this → deactivate signal
    min_edge_balls: int = 3             # consecutive scans above entry
    min_edge_balls_powerplay: int = 5   # stricter during powerplay
    reversal_edge_jump: float = 0.12    # required swing to flip direction
    signal_min_edge_runs: float = 2.0   # min edge (runs) for Telegram signal
    autobet_min_edge_runs: float = 4.0  # min edge (runs) for auto-bet
    signal_min_ev_pct: float = 5.0      # min EV% for Telegram signal
    autobet_min_ev_pct: float = 10.0    # min EV% for auto-bet

    # ── Cooldowns (seconds) ───────────────────────────────────────────
    cooldown_match_winner_s: int = 60
    cooldown_session_s: int = 90        # 6_over, 10_over, 15_over
    cooldown_innings_total_s: int = 120
    cooldown_over_runs_s: int = 30
    signal_direction_cooldown_s: int = 900   # 15 min between opposite signals
    signal_flip_min_edge: float = 15.0       # runs needed to override flip cooldown
    speed_edge_direction_cooldown_s: int = 600
    speed_edge_flip_override_edge: float = 20.0

    # ── Model stability gate ─────────────────────────────────────────
    model_stability_window: int = 3     # number of scans to check
    model_max_shift_per_scan: float = 15.0  # max acceptable shift (runs)

    # ── Risk / Kelly ─────────────────────────────────────────────────
    fractional_kelly: float = 0.25
    max_position_size_usd: float = 25.0
    max_open_bets: int = 10
    min_odds: float = 1.30
    max_odds: float = 5.00
    daily_loss_limit_usd: float = 50.0

    # ── Session auto-bet minimum overs ───────────────────────────────
    session_autobet_min_overs_6: float = 2.0
    session_autobet_min_overs_10: float = 4.0
    session_autobet_min_overs_15: float = 7.0
    session_autobet_early_ev: float = 20.0  # override early gate if EV this high

    # ── Innings total gates ──────────────────────────────────────────
    innings_total_min_overs: float = 10.0
    innings_total_max_overs_inn1: float = 19.0
    innings_total_max_overs_inn2: float = 17.0

    # ── Phase thresholds ─────────────────────────────────────────────
    chase_easy_rrr: float = 7.5         # below this = CHASE_EASY
    death_over_start: int = 16

    # ── Telegram ─────────────────────────────────────────────────────
    alert_on_edge_only: bool = False     # True = alerts only, no auto-bet
    suppress_speed_edge: bool = False

    # ── Helpers ───────────────────────────────────────────────────────

    def cooldown_for_market(self, market: str) -> int:
        """Return the cooldown in seconds for a given market key."""
        if market == "match_winner":
            return self.cooldown_match_winner_s
        if market == "innings_total":
            return self.cooldown_innings_total_s
        if market == "over_runs":
            return self.cooldown_over_runs_s
        return self.cooldown_session_s

    def min_edge_balls_for_phase(self, phase: str) -> int:
        """Return stability requirement based on match phase."""
        if phase in ("POWERPLAY", "powerplay"):
            return self.min_edge_balls_powerplay
        return self.min_edge_balls
