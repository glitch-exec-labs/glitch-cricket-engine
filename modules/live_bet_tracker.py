"""Live Bet Tracker — monitors open bets against live scores in real time.

Core logic:
  - Knows what bets are open (e.g. "60 runs YES 10-over, $10 stake")
  - Every scan, checks live score against each bet's target
  - Detects "already won" (score passed target before deadline over)
  - Detects "likely lost" (too many runs needed in remaining balls)
  - Suggests follow-up bets (hedge UNDER after early win, push MORE OVER)
  - Sends Telegram updates for all state changes
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("ipl_spotter.live_tracker")


@dataclass
class TrackedBet:
    """A bet being monitored against live scores."""
    reference_id: str
    match_id: int
    market: str           # "6_over", "10_over", "15_over", "innings_total"
    direction: str        # "OVER" or "UNDER"
    target_line: float    # e.g. 60.5 (runs target)
    target_over: float    # e.g. 10.0 (the over at which this settles)
    stake_usd: float
    odds: float
    innings: int
    home_team: str
    away_team: str

    # Live tracking state
    status: str = "LIVE"  # LIVE, EARLY_WIN, LIKELY_LOSS, SETTLED_WIN, SETTLED_LOSS
    last_score: int = 0
    last_wickets: int = 0
    last_overs: float = 0.0
    runs_needed: float = 0.0       # runs still needed to hit target (OVER bets)
    balls_remaining: float = 0.0   # balls left until target over
    win_probability: float = 0.5   # rough estimate based on required rate
    follow_up_sent: bool = False   # have we sent a follow-up suggestion?
    created_at: float = field(default_factory=time.time)

    # Score snapshots for database
    score_snapshots: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class FollowUpSuggestion:
    """A suggested follow-up bet based on live tracking analysis."""
    bet_ref: str          # reference_id of the triggering bet
    market: str           # suggested market
    direction: str        # suggested direction
    reason: str           # human-readable reason
    confidence: str       # HIGH, MEDIUM, LOW
    context: Dict[str, Any] = field(default_factory=dict)


class LiveBetTracker:
    """Monitors open bets against live match scores.

    Integrated into the scan loop: on each scan, call update() with the
    current match state.  The tracker compares each open bet's target
    against the live score and fires events when bets cross thresholds.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        config = config or {}
        # Map market name → target over for that market
        self._market_target_overs: Dict[str, float] = {
            "6_over": 6.0,
            "powerplay_runs": 6.0,
            "10_over": 10.0,
            "15_over": 15.0,
            "20_over": 20.0,
            "innings_total": 20.0,
        }
        # How many overs before deadline to consider "likely lost" for OVER bets
        self._likely_loss_threshold_rr: float = config.get("tracker_likely_loss_rr", 15.0)
        # Tracked bets: reference_id → TrackedBet
        self._bets: Dict[str, TrackedBet] = {}

    # ── Public API ────────────────────────────────────────────────────

    def add_bet(
        self,
        reference_id: str,
        match_id: int,
        market: str,
        direction: str,
        line: float,
        stake: float,
        odds: float,
        innings: int,
        home: str,
        away: str,
    ) -> None:
        """Start tracking a newly placed bet."""
        target_over = self._market_target_overs.get(market, 20.0)
        bet = TrackedBet(
            reference_id=reference_id,
            match_id=match_id,
            market=market,
            direction=direction,
            target_line=line,
            target_over=target_over,
            stake_usd=stake,
            odds=odds,
            innings=innings,
            home_team=home,
            away_team=away,
        )
        self._bets[reference_id] = bet
        logger.info(
            "TRACKER: monitoring %s %s %.1f (target over %.1f) stake=$%.2f ref=%s",
            market, direction, line, target_over, stake, reference_id[:8],
        )

    def remove_bet(self, reference_id: str) -> None:
        """Stop tracking a bet (e.g. after settlement)."""
        self._bets.pop(reference_id, None)

    def update(
        self,
        match_id: int,
        innings: int,
        runs: int,
        wickets: int,
        overs: float,
    ) -> List[Tuple[TrackedBet, str, Optional[FollowUpSuggestion]]]:
        """Update all tracked bets for a match with the latest score.

        Returns a list of (bet, event_type, follow_up) tuples for each
        bet that changed state.  event_type is one of:
          - "EARLY_WIN"   — score already passed target, bet is won early
          - "LIKELY_LOSS" — required run rate is impossibly high
          - "SETTLED"     — reached the target over (deadline passed)
          - "PROGRESS"    — score update logged (no state change)
        """
        events: List[Tuple[TrackedBet, str, Optional[FollowUpSuggestion]]] = []

        for bet in list(self._bets.values()):
            if bet.match_id != match_id or bet.innings != innings:
                continue

            # Record snapshot (cap at 150 entries ≈ full innings at 8s interval)
            bet.last_score = runs
            bet.last_wickets = wickets
            bet.last_overs = overs
            bet.score_snapshots.append({
                "overs": overs, "runs": runs, "wickets": wickets,
                "timestamp": time.time(),
            })
            if len(bet.score_snapshots) > 150:
                bet.score_snapshots = bet.score_snapshots[-150:]

            # Skip if already in a terminal state
            if bet.status in ("SETTLED_WIN", "SETTLED_LOSS", "EARLY_WIN"):
                continue

            # Calculate live metrics
            balls_bowled_in_session = max(0, overs * 6)
            balls_at_target = bet.target_over * 6
            bet.balls_remaining = max(0, balls_at_target - balls_bowled_in_session)

            event = self._evaluate_bet(bet, runs, wickets, overs)
            if event:
                event_type, follow_up = event
                events.append((bet, event_type, follow_up))

        return events

    def get_tracked_bets(self, match_id: int = 0) -> List[TrackedBet]:
        """Return all tracked bets, optionally filtered by match."""
        if match_id == 0:
            return list(self._bets.values())
        return [b for b in self._bets.values() if b.match_id == match_id]

    def get_bet(self, reference_id: str) -> Optional[TrackedBet]:
        return self._bets.get(reference_id)

    # ── Core evaluation logic ─────────────────────────────────────────

    def _evaluate_bet(
        self, bet: TrackedBet, runs: int, wickets: int, overs: float,
    ) -> Optional[Tuple[str, Optional[FollowUpSuggestion]]]:
        """Evaluate a single bet against the current score.

        Returns (event_type, follow_up_suggestion) or None if no state change.
        """
        target = bet.target_line

        # Has the target over passed? (bet is settled)
        if overs >= bet.target_over:
            return self._handle_settlement(bet, runs)

        if bet.direction == "OVER":
            return self._evaluate_over_bet(bet, runs, wickets, overs, target)
        elif bet.direction == "UNDER":
            return self._evaluate_under_bet(bet, runs, wickets, overs, target)

        return None

    def _evaluate_over_bet(
        self, bet: TrackedBet, runs: int, wickets: int, overs: float, target: float,
    ) -> Optional[Tuple[str, Optional[FollowUpSuggestion]]]:
        """OVER bet: we need score >= target by target_over."""
        bet.runs_needed = max(0, target - runs)

        # EARLY WIN: score already passed target, and there are still overs left
        if runs >= target and bet.status != "EARLY_WIN":
            bet.status = "EARLY_WIN"
            overs_remaining = bet.target_over - overs
            logger.info(
                "EARLY WIN: %s %s %.1f — score %d already >= %.1f with %.1f overs left (ref=%s)",
                bet.market, bet.direction, target, runs, target, overs_remaining,
                bet.reference_id[:8],
            )
            follow_up = self._suggest_after_early_win(bet, runs, wickets, overs)
            return "EARLY_WIN", follow_up

        # LIKELY LOSS: required run rate is impossibly high
        if bet.balls_remaining > 0:
            required_rr = (bet.runs_needed / bet.balls_remaining) * 6
            bet.win_probability = max(0.0, min(1.0, 1.0 - (required_rr / 36.0)))

            if (
                required_rr > self._likely_loss_threshold_rr
                and bet.status != "LIKELY_LOSS"
                and wickets >= 5  # strengthen the signal
            ):
                bet.status = "LIKELY_LOSS"
                logger.info(
                    "LIKELY LOSS: %s OVER %.1f — need %.0f more runs at RR %.1f with %d wickets down (ref=%s)",
                    bet.market, target, bet.runs_needed, required_rr, wickets,
                    bet.reference_id[:8],
                )
                return "LIKELY_LOSS", None

        return None

    def _evaluate_under_bet(
        self, bet: TrackedBet, runs: int, wickets: int, overs: float, target: float,
    ) -> Optional[Tuple[str, Optional[FollowUpSuggestion]]]:
        """UNDER bet: we need score < target by target_over."""
        runs_margin = target - runs  # how many runs of margin we still have

        # EARLY WIN: team all out, or so many wickets that target is safe
        if wickets >= 10 and runs < target and bet.status != "EARLY_WIN":
            bet.status = "EARLY_WIN"
            logger.info(
                "EARLY WIN: %s UNDER %.1f — all out at %d (ref=%s)",
                bet.market, target, runs, bet.reference_id[:8],
            )
            return "EARLY_WIN", None

        # LIKELY LOSS: score already passed target for UNDER
        if runs >= target and bet.status != "LIKELY_LOSS":
            bet.status = "LIKELY_LOSS"
            logger.info(
                "LIKELY LOSS (now effective LOSS): %s UNDER %.1f — score %d already >= target (ref=%s)",
                bet.market, target, runs, bet.reference_id[:8],
            )
            # Suggest an OVER follow-up since batting is strong
            follow_up = self._suggest_after_under_bust(bet, runs, wickets, overs)
            return "LIKELY_LOSS", follow_up

        # Warn when margin is thin
        if bet.balls_remaining > 0:
            overs_left = bet.balls_remaining / 6
            margin_rr = (runs_margin / overs_left) if overs_left > 0 else 0
            bet.win_probability = max(0.0, min(1.0, margin_rr / 12.0))

        return None

    def _handle_settlement(
        self, bet: TrackedBet, runs: int,
    ) -> Tuple[str, None]:
        """Target over has passed — bet is settled."""
        if bet.direction == "OVER":
            won = runs >= bet.target_line
        else:
            won = runs < bet.target_line

        bet.status = "SETTLED_WIN" if won else "SETTLED_LOSS"
        logger.info(
            "SETTLED: %s %s %.1f → score=%d → %s (ref=%s)",
            bet.market, bet.direction, bet.target_line, runs, bet.status,
            bet.reference_id[:8],
        )
        return "SETTLED", None

    # ── Follow-up suggestions ─────────────────────────────────────────

    def _suggest_after_early_win(
        self, bet: TrackedBet, runs: int, wickets: int, overs: float,
    ) -> Optional[FollowUpSuggestion]:
        """After an OVER bet wins early, suggest a follow-up."""
        if bet.follow_up_sent:
            return None
        bet.follow_up_sent = True

        overs_left = bet.target_over - overs
        current_rr = runs / overs if overs > 0 else 0

        # If wickets are falling, UNDER on a later market is attractive
        if wickets >= 4 and overs_left > 2:
            return FollowUpSuggestion(
                bet_ref=bet.reference_id,
                market=self._next_session_market(bet.market),
                direction="UNDER",
                reason=(
                    f"{bet.market} OVER {bet.target_line} WON EARLY at {overs:.1f} ov "
                    f"(score {runs}/{wickets}). Wickets falling — consider UNDER on next session."
                ),
                confidence="MEDIUM",
                context={"score": runs, "wickets": wickets, "overs": overs, "rr": current_rr},
            )

        # If batting is dominant, suggest OVER on a later market
        if current_rr > 9.0 and wickets < 3:
            return FollowUpSuggestion(
                bet_ref=bet.reference_id,
                market=self._next_session_market(bet.market),
                direction="OVER",
                reason=(
                    f"{bet.market} OVER {bet.target_line} WON EARLY at {overs:.1f} ov "
                    f"(score {runs}/{wickets}, RR {current_rr:.1f}). Batting strong — push OVER."
                ),
                confidence="MEDIUM",
                context={"score": runs, "wickets": wickets, "overs": overs, "rr": current_rr},
            )

        return None

    def _suggest_after_under_bust(
        self, bet: TrackedBet, runs: int, wickets: int, overs: float,
    ) -> Optional[FollowUpSuggestion]:
        """After an UNDER bet busts, suggest a follow-up OVER."""
        if bet.follow_up_sent:
            return None
        bet.follow_up_sent = True

        current_rr = runs / overs if overs > 0 else 0
        if current_rr > 8.0 and wickets < 4:
            return FollowUpSuggestion(
                bet_ref=bet.reference_id,
                market=self._next_session_market(bet.market),
                direction="OVER",
                reason=(
                    f"{bet.market} UNDER {bet.target_line} BUSTED at {overs:.1f} ov "
                    f"(score {runs}/{wickets}, RR {current_rr:.1f}). Batting dominant — "
                    f"consider OVER on next session to recover."
                ),
                confidence="LOW",
                context={"score": runs, "wickets": wickets, "overs": overs, "rr": current_rr},
            )
        return None

    def _next_session_market(self, current_market: str) -> str:
        """Given a market, return the next logical session market."""
        progression = ["6_over", "10_over", "15_over", "innings_total"]
        try:
            idx = progression.index(current_market)
            if idx + 1 < len(progression):
                return progression[idx + 1]
        except ValueError:
            pass
        return "innings_total"

    # ── Telegram formatting ───────────────────────────────────────────

    def format_early_win(self, bet: TrackedBet) -> str:
        """Format an EARLY WIN Telegram notification."""
        overs_left = bet.target_over - bet.last_overs
        return (
            f"🏆 EARLY WIN — {bet.home_team} vs {bet.away_team}\n"
            f"\n"
            f"📊 {bet.market} {bet.direction} {bet.target_line}\n"
            f"📈 Score: {bet.last_score}/{bet.last_wickets} ({bet.last_overs:.1f} ov)\n"
            f"⏱ {overs_left:.1f} overs remaining in session\n"
            f"💰 Stake: ${bet.stake_usd:.2f} @ {bet.odds:.2f}\n"
            f"🔖 Ref: {bet.reference_id[:8]}..."
        )

    def format_likely_loss(self, bet: TrackedBet) -> str:
        """Format a LIKELY LOSS Telegram notification."""
        return (
            f"⚠️ LIKELY LOSS — {bet.home_team} vs {bet.away_team}\n"
            f"\n"
            f"📊 {bet.market} {bet.direction} {bet.target_line}\n"
            f"📉 Score: {bet.last_score}/{bet.last_wickets} ({bet.last_overs:.1f} ov)\n"
            f"🎯 Need: {bet.runs_needed:.0f} more runs | {bet.balls_remaining:.0f} balls left\n"
            f"💰 Stake: ${bet.stake_usd:.2f} @ {bet.odds:.2f}\n"
            f"🔖 Ref: {bet.reference_id[:8]}..."
        )

    def format_follow_up(self, suggestion: FollowUpSuggestion) -> str:
        """Format a follow-up suggestion for Telegram."""
        return (
            f"💡 FOLLOW-UP OPPORTUNITY [{suggestion.confidence}]\n"
            f"\n"
            f"📊 Suggest: {suggestion.market} {suggestion.direction}\n"
            f"📝 {suggestion.reason}\n"
            f"🔗 Triggered by: {suggestion.bet_ref[:8]}..."
        )

    def format_settlement(self, bet: TrackedBet) -> str:
        """Format a settlement notification."""
        icon = "✅" if bet.status == "SETTLED_WIN" else "❌"
        return (
            f"{icon} SETTLED — {bet.home_team} vs {bet.away_team}\n"
            f"\n"
            f"📊 {bet.market} {bet.direction} {bet.target_line}\n"
            f"📈 Final: {bet.last_score}/{bet.last_wickets} ({bet.last_overs:.1f} ov)\n"
            f"💰 Stake: ${bet.stake_usd:.2f} @ {bet.odds:.2f}\n"
            f"🔖 Ref: {bet.reference_id[:8]}..."
        )
