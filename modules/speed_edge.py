"""
Speed Edge Module — detects trigger events in live ball-by-ball data
that should cause odds to shift, then checks if the bookmaker has adjusted.

The 15-45 second gap between Sportmonks data and bookmaker adjustment IS the edge.
"""

from __future__ import annotations

from typing import Any, Optional

from modules.match_state import MatchState


# ── Trigger severity constants ────────────────────────────────────────────────

HIGH = "HIGH"
MEDIUM = "MEDIUM"


# ── Trigger type constants ────────────────────────────────────────────────────

WICKET = "WICKET"
BIG_OVER = "BIG_OVER"
HUGE_OVER = "HUGE_OVER"
BOUNDARY_CLUSTER = "BOUNDARY_CLUSTER"
COLLAPSE = "COLLAPSE"
BIG_PARTNERSHIP = "BIG_PARTNERSHIP"
DOT_BALL_PRESSURE = "DOT_BALL_PRESSURE"
DEATH_OVER_EXPLOSION = "DEATH_OVER_EXPLOSION"


class SpeedEdge:
    """Detects speed-edge trigger events from live ball-by-ball data."""

    FINAL_OVERS_SUPPRESS_FROM = 18.0
    LATE_OVERS_SHORT_ALERT_FROM = 15.0
    INNINGS_TOTAL_SUGGESTION_CUTOFF = 17.0

    # ── Construction ──────────────────────────────────────────────────────

    def __init__(self) -> None:
        # Recent trigger history (for de-duplication / cooldown)
        self.recent_triggers: list[dict[str, Any]] = []

        # Thresholds — exposed as attributes so callers can tune
        self.big_over_threshold = 15
        self.huge_over_threshold = 20
        self.boundary_cluster_count = 3
        self.boundary_cluster_window = 6  # last N balls
        self.collapse_wickets = 3
        self.collapse_window = 18  # last N balls (3 overs)
        self.big_partnership_runs = 50
        self.dot_ball_count = 5
        self.dot_ball_window = 8
        self.death_over_explosion_threshold = 18

        # Deduplication state: BIG_PARTNERSHIP should only fire ONCE per partnership
        # (reset when the next wicket falls)
        self._partnership_fired_at_wickets: int = -1  # wicket count when last fired

        # Minimum edge size (runs) for evaluate_speed_opportunity to return a signal.
        # Prevents low-confidence noise from reaching Telegram.
        self.min_edge_size: float = 10.0

    # ── Public API ────────────────────────────────────────────────────────

    def detect_triggers(
        self, match_state: MatchState, last_n_balls: int = 6
    ) -> list[dict[str, Any]]:
        """Analyze recent balls and return a list of detected trigger events."""
        triggers: list[dict[str, Any]] = []
        balls = match_state.balls

        if not balls:
            return triggers

        recent = balls[-last_n_balls:] if len(balls) >= last_n_balls else list(balls)

        # --- WICKET (any wicket in recent balls) ---
        for b in recent:
            if b.get("is_wicket"):
                over_ball = f"{b['over']}.{b['ball']}"
                triggers.append({
                    "type": WICKET,
                    "severity": HIGH,
                    "detail": f"Wicket fell at {over_ball} "
                              f"({match_state.total_runs}/{match_state.wickets})",
                    "expected_impact": "Innings total should drop 5-10 runs",
                    "recommended_action": "Check UNDER on innings total — odds may not have adjusted",
                })

        # --- BIG_OVER / HUGE_OVER (completed over with high runs) ---
        if balls:
            latest_over = balls[-1]["over"]
            over_total = match_state.over_runs.get(latest_over, 0)
            is_death = latest_over >= 15

            # Only fire on a COMPLETED over (6+ legal deliveries).
            # Firing mid-over on partial run totals creates premature signals.
            legal_in_over = sum(
                1 for b in balls
                if b.get("over") == latest_over and b.get("is_legal", True)
            )
            over_complete = legal_in_over >= 6

            if not over_complete:
                pass  # skip BIG/HUGE_OVER — over still in progress
            elif over_total >= self.huge_over_threshold:
                triggers.append({
                    "type": HUGE_OVER,
                    "severity": HIGH,
                    "detail": f"Over {latest_over} went for {over_total} runs",
                    "expected_impact": "Innings total should rise 8-12 runs",
                    "recommended_action": "Check OVER on innings total — line may still be low",
                })
            elif over_total >= self.big_over_threshold:
                triggers.append({
                    "type": BIG_OVER,
                    "severity": MEDIUM,
                    "detail": f"Over {latest_over} went for {over_total} runs",
                    "expected_impact": "Innings total should rise 5-8 runs",
                    "recommended_action": "Check OVER on innings total — line may still be high",
                })

            # --- DEATH OVER EXPLOSION ---
            if over_complete and is_death and over_total >= self.death_over_explosion_threshold:
                triggers.append({
                    "type": DEATH_OVER_EXPLOSION,
                    "severity": HIGH,
                    "detail": f"Death over {latest_over} exploded for {over_total} runs",
                    "expected_impact": "Innings total should rise 10-15 runs",
                    "recommended_action": "Check OVER on innings total — death over carnage",
                })

        # --- BOUNDARY_CLUSTER (3+ boundaries in last N balls) ---
        window = balls[-self.boundary_cluster_window:] if len(balls) >= self.boundary_cluster_window else list(balls)
        boundary_count = sum(
            1 for b in window
            if b.get("is_legal", True) and b["runs"] - b.get("extras", 0) in (4, 6)
        )
        if boundary_count >= self.boundary_cluster_count:
            triggers.append({
                "type": BOUNDARY_CLUSTER,
                "severity": MEDIUM,
                "detail": f"{boundary_count} boundaries in last {len(window)} balls",
                "expected_impact": "Momentum shift — total should rise 5-8 runs",
                "recommended_action": "Check OVER on innings total — momentum building",
            })

        # --- COLLAPSE (3+ wickets in last 18 balls) ---
        collapse_window = balls[-self.collapse_window:] if len(balls) >= self.collapse_window else list(balls)
        wickets_in_window = sum(1 for b in collapse_window if b.get("is_wicket"))
        if wickets_in_window >= self.collapse_wickets:
            triggers.append({
                "type": COLLAPSE,
                "severity": HIGH,
                "detail": f"{wickets_in_window} wickets in last {len(collapse_window)} balls",
                "expected_impact": "Innings total should drop 15-25 runs",
                "recommended_action": "Check UNDER on innings total — collapse in progress",
            })

        # --- BIG_PARTNERSHIP (50+ runs since last wicket) ---
        # Only fire ONCE per partnership — re-fires only after next wicket falls.
        runs_since_wicket = 0
        for b in reversed(balls):
            if b.get("is_wicket"):
                break
            runs_since_wicket += b["runs"]
        if runs_since_wicket >= self.big_partnership_runs:
            current_wickets = match_state.wickets
            if current_wickets != self._partnership_fired_at_wickets:
                # New partnership threshold crossed — fire and mark this wicket count
                self._partnership_fired_at_wickets = current_wickets
                triggers.append({
                    "type": BIG_PARTNERSHIP,
                    "severity": MEDIUM,
                    "detail": f"Partnership of {runs_since_wicket} runs without a wicket",
                    "expected_impact": "Innings total should rise 8-12 runs",
                    "recommended_action": "Check OVER on innings total — big partnership building",
                })
            # else: same partnership already fired — skip

        # --- DOT_BALL_PRESSURE (5+ dots in last 8 balls) ---
        dot_window = balls[-self.dot_ball_window:] if len(balls) >= self.dot_ball_window else list(balls)
        dot_count = sum(
            1 for b in dot_window
            if b.get("is_legal", True) and b["runs"] == 0
        )
        if dot_count >= self.dot_ball_count:
            triggers.append({
                "type": DOT_BALL_PRESSURE,
                "severity": MEDIUM,
                "detail": f"{dot_count} dot balls in last {len(dot_window)} balls",
                "expected_impact": "Run rate dropping — total should drop 3-6 runs",
                "recommended_action": "Check UNDER on innings total — dot ball pressure mounting",
            })

        self.recent_triggers.extend(triggers)
        return triggers

    def format_speed_alert(
        self,
        home: str,
        away: str,
        trigger: dict[str, Any],
        match_state: MatchState,
        *,
        include_action: bool = True,
        include_window: bool = True,
    ) -> str:
        """Format a trigger into a Telegram-ready speed alert message."""
        overs_display = self._format_overs(match_state)
        projected = match_state.projected_innings_total()

        sev = "!" if trigger["severity"] == HIGH else "~"

        lines = [
            f"SPEED EDGE [{sev}] {home} vs {away}",
            f"  {trigger['type']} — {trigger['detail']}",
            f"  {match_state.total_runs}/{match_state.wickets} ({overs_display}) RR:{match_state.current_run_rate:.1f} Proj:{projected:.0f}",
            f"  Impact: {trigger['expected_impact']}",
        ]
        if include_action and trigger.get("recommended_action"):
            lines.append(f"  Action: {trigger['recommended_action']}")
        if include_window:
            lines.append(f"  Window: ~30-60s before odds adjust")
        return "\n".join(lines)

    def should_trigger_odds_fetch(self, triggers: list[dict[str, Any]]) -> bool:
        """Return True if any trigger is HIGH severity."""
        return any(t["severity"] == HIGH for t in triggers)

    def should_suppress_alert(self, overs_completed: float) -> bool:
        """Return True when the match is too late for an actionable speed alert."""
        return overs_completed >= self.FINAL_OVERS_SUPPRESS_FROM

    def should_shorten_alert(self, overs_completed: float) -> bool:
        """Return True when only a reduced informational alert should be shown."""
        return self.LATE_OVERS_SHORT_ALERT_FROM <= overs_completed < self.FINAL_OVERS_SUPPRESS_FROM

    def should_suggest_innings_total(self, overs_completed: float) -> bool:
        """Return True when innings-total advice is still actionable."""
        return overs_completed <= self.INNINGS_TOTAL_SUGGESTION_CUTOFF

    def evaluate_speed_opportunity(
        self,
        trigger: dict[str, Any],
        pre_event_prediction: float | dict[str, Any],
        current_prediction: float | dict[str, Any],
        cloudbet_odds: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        """Compare model shift against Cloudbet line to confirm a speed edge.

        Parameters
        ----------
        trigger : dict
            The trigger event that fired.
        pre_event_prediction : float
            Our model's predicted innings total *before* the trigger event.
        current_prediction : float
            Our model's predicted innings total *after* the trigger event.
        cloudbet_odds : dict
            Cloudbet market data. Expected keys:
                line  (float) — current over/under line
                over  (float) — odds for over
                under (float) — odds for under

        Returns
        -------
        dict or None
            Speed opportunity details if an edge is confirmed, else None.
        """
        def _prediction_value(prediction: float | dict[str, Any]) -> float | None:
            if isinstance(prediction, dict):
                innings_total = prediction.get("innings_total", {})
                if isinstance(innings_total, dict):
                    expected = innings_total.get("expected")
                    if expected is not None:
                        return float(expected)
                expected = prediction.get("expected")
                if expected is not None:
                    return float(expected)
                return None
            return float(prediction)

        market_odds = cloudbet_odds.get("innings_total", cloudbet_odds)
        pre_event_prediction = _prediction_value(pre_event_prediction)
        current_prediction = _prediction_value(current_prediction)
        if pre_event_prediction is None or current_prediction is None:
            return None
        model_shift = current_prediction - pre_event_prediction
        cloudbet_line = market_odds.get("line", 0.0)
        if not cloudbet_line or cloudbet_line <= 0:
            return None

        # Determine if Cloudbet has moved to account for the trigger
        # We consider Cloudbet "moved" if their line shifted in the same
        # direction as our model by at least half the model's shift.
        if abs(model_shift) < 1.0:
            # Model didn't shift meaningfully — no edge
            return None

        # Edge size = how far Cloudbet's line is from our current prediction
        edge_size = abs(current_prediction - cloudbet_line)

        # Cloudbet has caught up if their line is within 2 runs of our prediction
        cloudbet_moved = edge_size < 2.0

        if cloudbet_moved:
            # No edge — bookmaker already adjusted
            return None

        # Edge too small to be actionable — suppress to avoid noise
        if edge_size < self.min_edge_size:
            return None

        # Determine recommendation
        if model_shift < 0:
            # Model says total should drop → bet UNDER if Cloudbet line still high
            if cloudbet_line > current_prediction:
                recommendation = (
                    f"UNDER {cloudbet_line} — model says {current_prediction:.1f}, "
                    f"Cloudbet still at {cloudbet_line}"
                )
            else:
                return None
        else:
            # Model says total should rise → bet OVER if Cloudbet line still low
            if cloudbet_line < current_prediction:
                recommendation = (
                    f"OVER {cloudbet_line} — model says {current_prediction:.1f}, "
                    f"Cloudbet still at {cloudbet_line}"
                )
            else:
                return None

        return {
            "trigger": trigger["type"],
            "model_shift": round(model_shift, 1),
            "cloudbet_line": cloudbet_line,
            "cloudbet_moved": False,
            "edge_size": round(edge_size, 1),
            "recommendation": recommendation,
        }

    # ── Internal helpers ──────────────────────────────────────────────────

    @staticmethod
    def _format_overs(ms: MatchState) -> str:
        """Format overs as '7.2 ov' style string."""
        completed = int(ms.overs_completed)
        remaining_balls = ms.balls_faced - completed * 6
        return f"{completed}.{remaining_balls} ov"
