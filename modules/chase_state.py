"""Chase Pressure State Machine — 5-state classifier for 2nd innings.

Replaces the binary "hopeless/playable" logic with graduated pressure bands
that adjust signal thresholds, win probability, and Telegram messaging.

States: CRUISE → CONTROLLED → PRESSURE → CRISIS → DEAD
"""
from __future__ import annotations

import logging
import math
from enum import Enum
from typing import Any

logger = logging.getLogger("ipl_spotter.chase_state")


class ChasePhase(Enum):
    NOT_CHASE = "not_chase"
    CRUISE = "cruise"
    CONTROLLED = "controlled"
    PRESSURE = "pressure"
    CRISIS = "crisis"
    DEAD = "dead"


# Min EV% threshold by chase phase (edge detector / spotter uses this)
CHASE_EV_THRESHOLDS: dict[str, float] = {
    "not_chase": 5.0,
    "cruise": 3.0,
    "controlled": 5.0,
    "pressure": 8.0,
    "crisis": 15.0,
    "dead": 999.0,
}

# Sigmoid steepness for win probability by chase phase
CHASE_K_FACTORS: dict[str, float] = {
    "not_chase": 0.5,
    "cruise": 0.3,
    "controlled": 0.5,
    "pressure": 0.7,
    "crisis": 1.2,
    "dead": 2.0,
}

_EMOJI = {
    "not_chase": "",
    "cruise": "🟢",
    "controlled": "🟡",
    "pressure": "🟠",
    "crisis": "🔴",
    "dead": "💀",
}

_LABEL = {
    "not_chase": "1st Innings",
    "cruise": "Chase: Cruising",
    "controlled": "Chase: Controlled",
    "pressure": "Chase: Under Pressure",
    "crisis": "Chase: Crisis",
    "dead": "Chase: Dead",
}


class ChaseStateMachine:
    """Classify 2nd innings chase into pressure bands."""

    def classify(
        self,
        match_state: Any,
        innings_state: Any = None,
    ) -> dict[str, Any]:
        """Classify current chase situation.

        Returns dict with: phase, rrr, runs_needed, overs_left, wickets_in_hand,
        batting_depth, min_ev_pct, k_factor, suppress_signals, emoji, label.
        """
        innings = getattr(match_state, "current_innings", 1)
        target = getattr(match_state, "target_runs", None)

        if innings < 2 or not target:
            phase = ChasePhase.NOT_CHASE
            return self._build_result(phase, 0.0, 0, 20.0, 10, 5)

        score = int(getattr(match_state, "total_runs", 0))
        overs = float(getattr(match_state, "overs_completed", 0))
        wickets = int(getattr(match_state, "wickets", 0))
        runs_needed = max(0, target - score)
        overs_left = max(0.1, 20.0 - overs)
        rrr = runs_needed / overs_left
        wickets_in_hand = 10 - wickets
        batting_depth = getattr(innings_state, "batting_depth", wickets_in_hand) if innings_state else wickets_in_hand

        # Won already
        if runs_needed <= 0:
            return self._build_result(ChasePhase.CRUISE, 0.0, 0, overs_left, wickets_in_hand, batting_depth)

        # Classify
        phase = self._classify_phase(rrr, wickets_in_hand, batting_depth, runs_needed, overs_left)

        return self._build_result(phase, rrr, runs_needed, overs_left, wickets_in_hand, batting_depth)

    def _classify_phase(
        self,
        rrr: float,
        wickets_in_hand: int,
        batting_depth: int,
        runs_needed: int,
        overs_left: float,
    ) -> ChasePhase:
        # DEAD: mathematically impossible or near-impossible
        if rrr > 16.0:
            return ChasePhase.DEAD
        if runs_needed > overs_left * 36:  # max theoretical 36 per over
            return ChasePhase.DEAD
        if wickets_in_hand <= 1 and runs_needed > 15:
            return ChasePhase.DEAD

        # CRISIS: extremely difficult
        if rrr >= 12.0:
            return ChasePhase.CRISIS
        if wickets_in_hand <= 3 and runs_needed > 30:
            return ChasePhase.CRISIS
        if wickets_in_hand <= 2 and runs_needed > 15:
            return ChasePhase.CRISIS

        # PRESSURE: challenging
        if rrr >= 9.5:
            # Batting depth can upgrade PRESSURE → CONTROLLED
            if batting_depth >= 3 and rrr < 11.0:
                return ChasePhase.CONTROLLED
            return ChasePhase.PRESSURE
        if wickets_in_hand <= 5 and rrr >= 7.5:
            return ChasePhase.PRESSURE

        # CONTROLLED: manageable but not easy
        if rrr >= 7.0:
            return ChasePhase.CONTROLLED

        # CRUISE: comfortable
        return ChasePhase.CRUISE

    def _build_result(
        self,
        phase: ChasePhase,
        rrr: float,
        runs_needed: int,
        overs_left: float,
        wickets_in_hand: int,
        batting_depth: int,
    ) -> dict[str, Any]:
        pv = phase.value
        return {
            "phase": phase,
            "rrr": round(rrr, 1),
            "runs_needed": runs_needed,
            "overs_left": round(overs_left, 1),
            "wickets_in_hand": wickets_in_hand,
            "batting_depth": batting_depth,
            "min_ev_pct": CHASE_EV_THRESHOLDS.get(pv, 5.0),
            "k_factor": CHASE_K_FACTORS.get(pv, 0.5),
            "suppress_signals": pv == "dead",
            "emoji": _EMOJI.get(pv, ""),
            "label": _LABEL.get(pv, ""),
        }

    def adjusted_win_probability(
        self,
        match_state: Any,
        innings_state: Any = None,
    ) -> float:
        """Compute chase win probability using state-dependent k factor.

        Replaces the flat k=0.5 in IPLPredictor.chase_win_probability().
        """
        target = getattr(match_state, "target_runs", None)
        if not target:
            return 0.5

        score = int(getattr(match_state, "total_runs", 0))
        overs = float(getattr(match_state, "overs_completed", 0))
        wickets = int(getattr(match_state, "wickets", 0))
        runs_needed = max(0, target - score)
        overs_left = max(0.1, 20.0 - overs)
        wickets_in_hand = 10 - wickets

        if runs_needed <= 0:
            return 1.0
        if wickets_in_hand <= 0:
            return 0.0

        chase_info = self.classify(match_state, innings_state)
        k = chase_info["k_factor"]

        # Resource model: weighted (overs more predictive than wickets for chase)
        overs_pct = overs_left / 20.0
        wickets_pct = wickets_in_hand / 10.0
        resource_pct = 0.4 * overs_pct + 0.6 * wickets_pct  # wickets weighted more in chase

        # Quality adjustment
        if innings_state is not None:
            rq = getattr(innings_state, "remaining_bat_quality", 0.5)
            resource_pct *= 0.7 + 0.6 * rq  # 0.7-1.3x
            dbq = getattr(innings_state, "death_bowling_quality", 0.5)
            if overs_left <= 5 and dbq > 0.7:
                resource_pct *= 0.9

        expected_remaining = 172.0 * resource_pct
        rrr = runs_needed / overs_left
        expected_rr = expected_remaining / overs_left if overs_left > 0 else 0.0

        diff = rrr - expected_rr
        prob = 1.0 / (1.0 + math.exp(k * diff))

        return round(max(0.01, min(0.99, prob)), 3)

    def should_suppress_signal(
        self,
        match_state: Any,
        edge_ev_pct: float,
        innings_state: Any = None,
    ) -> tuple[bool, str]:
        """Return (suppress, reason) based on chase phase and EV threshold."""
        info = self.classify(match_state, innings_state)
        phase = info["phase"]

        if phase == ChasePhase.DEAD:
            return True, f"Chase DEAD (RRR {info['rrr']:.1f}) — suppress all signals"

        min_ev = info["min_ev_pct"]
        if edge_ev_pct < min_ev:
            return True, f"Chase {phase.value} — EV {edge_ev_pct:.1f}% below threshold {min_ev:.0f}%"

        return False, ""
