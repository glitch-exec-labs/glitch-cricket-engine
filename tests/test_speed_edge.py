"""
Tests for ipl_spotter.modules.speed_edge — Speed Edge trigger detection.

Pure-Python, no external dependencies beyond pytest.
"""

from typing import Optional

import pytest

from modules.match_state import MatchState
from modules.speed_edge import (
    SpeedEdge,
    HIGH,
    MEDIUM,
    WICKET,
    BIG_OVER,
    HUGE_OVER,
    BOUNDARY_CLUSTER,
    COLLAPSE,
    BIG_PARTNERSHIP,
    DOT_BALL_PRESSURE,
    DEATH_OVER_EXPLOSION,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_state() -> MatchState:
    return MatchState("RCB", "SRH", "M. Chinnaswamy Stadium")


def _ball(
    over: int,
    ball: int,
    runs: int,
    *,
    is_wicket: bool = False,
    extras: int = 0,
    is_legal: bool = True,
) -> dict:
    return {
        "over": over,
        "ball": ball,
        "runs": runs,
        "is_wicket": is_wicket,
        "extras": extras,
        "is_legal": is_legal,
    }


def _add_over(
    ms: MatchState,
    over_num: int,
    run_per_ball: int = 1,
    wicket_on: "Optional[int]" = None,
) -> None:
    """Add six legal deliveries for a given over."""
    for b in range(1, 7):
        ms.add_ball(
            _ball(
                over_num,
                b,
                run_per_ball,
                is_wicket=(b == wicket_on) if wicket_on else False,
            )
        )


def _add_balls(ms: MatchState, over_num: int, run_list: list[int]) -> None:
    """Add balls to an over with specific runs per ball."""
    for i, runs in enumerate(run_list, start=1):
        ms.add_ball(_ball(over_num, i, runs))


# ── Init ─────────────────────────────────────────────────────────────────────


class TestInit:
    def test_default_thresholds(self):
        se = SpeedEdge()
        assert se.big_over_threshold == 15
        assert se.huge_over_threshold == 20
        assert se.boundary_cluster_count == 3
        assert se.collapse_wickets == 3
        assert se.big_partnership_runs == 50
        assert se.dot_ball_count == 5
        assert se.death_over_explosion_threshold == 18
        assert se.recent_triggers == []


# ── Wicket trigger ───────────────────────────────────────────────────────────


class TestWicketTrigger:
    def test_wicket_detected(self):
        ms = _make_state()
        _add_over(ms, 0)
        ms.add_ball(_ball(1, 1, 0, is_wicket=True))
        se = SpeedEdge()
        triggers = se.detect_triggers(ms, last_n_balls=6)
        wicket_triggers = [t for t in triggers if t["type"] == WICKET]
        assert len(wicket_triggers) == 1
        assert wicket_triggers[0]["severity"] == HIGH
        assert "Wicket fell" in wicket_triggers[0]["detail"]

    def test_no_wicket_no_trigger(self):
        ms = _make_state()
        _add_over(ms, 0)
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        wicket_triggers = [t for t in triggers if t["type"] == WICKET]
        assert len(wicket_triggers) == 0

    def test_wicket_outside_window_not_detected(self):
        ms = _make_state()
        ms.add_ball(_ball(0, 1, 0, is_wicket=True))
        # Add 10 more balls so wicket is outside last_n_balls=6
        for b in range(2, 7):
            ms.add_ball(_ball(0, b, 1))
        _add_over(ms, 1)
        se = SpeedEdge()
        triggers = se.detect_triggers(ms, last_n_balls=6)
        wicket_triggers = [t for t in triggers if t["type"] == WICKET]
        assert len(wicket_triggers) == 0


# ── Big Over / Huge Over ─────────────────────────────────────────────────────


class TestBigOverTrigger:
    def test_big_over_15_runs(self):
        ms = _make_state()
        _add_balls(ms, 0, [4, 4, 1, 4, 1, 1])  # 15 runs
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        big = [t for t in triggers if t["type"] == BIG_OVER]
        assert len(big) == 1
        assert big[0]["severity"] == MEDIUM

    def test_huge_over_20_runs(self):
        ms = _make_state()
        _add_balls(ms, 0, [4, 6, 4, 2, 2, 4])  # 22 runs
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        huge = [t for t in triggers if t["type"] == HUGE_OVER]
        assert len(huge) == 1
        assert huge[0]["severity"] == HIGH

    def test_huge_over_does_not_also_trigger_big_over(self):
        ms = _make_state()
        _add_balls(ms, 0, [4, 6, 4, 2, 2, 4])  # 22 runs
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        big = [t for t in triggers if t["type"] == BIG_OVER]
        assert len(big) == 0  # huge_over takes precedence

    def test_normal_over_no_trigger(self):
        ms = _make_state()
        _add_over(ms, 0, run_per_ball=1)  # 6 runs
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        over_triggers = [t for t in triggers if t["type"] in (BIG_OVER, HUGE_OVER)]
        assert len(over_triggers) == 0


# ── Boundary Cluster ─────────────────────────────────────────────────────────


class TestBoundaryCluster:
    def test_three_boundaries_in_six_balls(self):
        ms = _make_state()
        # 3 fours in 6 balls
        ms.add_ball(_ball(0, 1, 4))
        ms.add_ball(_ball(0, 2, 0))
        ms.add_ball(_ball(0, 3, 4))
        ms.add_ball(_ball(0, 4, 0))
        ms.add_ball(_ball(0, 5, 4))
        ms.add_ball(_ball(0, 6, 0))
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        bc = [t for t in triggers if t["type"] == BOUNDARY_CLUSTER]
        assert len(bc) == 1
        assert bc[0]["severity"] == MEDIUM

    def test_sixes_count_as_boundaries(self):
        ms = _make_state()
        ms.add_ball(_ball(0, 1, 6))
        ms.add_ball(_ball(0, 2, 6))
        ms.add_ball(_ball(0, 3, 6))
        ms.add_ball(_ball(0, 4, 0))
        ms.add_ball(_ball(0, 5, 0))
        ms.add_ball(_ball(0, 6, 0))
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        bc = [t for t in triggers if t["type"] == BOUNDARY_CLUSTER]
        assert len(bc) == 1

    def test_extras_not_counted_as_boundaries(self):
        ms = _make_state()
        # 4 runs but all extras — not a boundary
        ms.add_ball(_ball(0, 1, 4, extras=4))
        ms.add_ball(_ball(0, 2, 4, extras=4))
        ms.add_ball(_ball(0, 3, 4, extras=4))
        ms.add_ball(_ball(0, 4, 0))
        ms.add_ball(_ball(0, 5, 0))
        ms.add_ball(_ball(0, 6, 0))
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        bc = [t for t in triggers if t["type"] == BOUNDARY_CLUSTER]
        assert len(bc) == 0


# ── Collapse ─────────────────────────────────────────────────────────────────


class TestCollapse:
    def test_three_wickets_in_18_balls(self):
        ms = _make_state()
        # 3 overs with a wicket each = 3 wickets in 18 balls
        _add_over(ms, 0, wicket_on=3)
        _add_over(ms, 1, wicket_on=5)
        _add_over(ms, 2, wicket_on=1)
        se = SpeedEdge()
        triggers = se.detect_triggers(ms, last_n_balls=6)
        collapse = [t for t in triggers if t["type"] == COLLAPSE]
        assert len(collapse) == 1
        assert collapse[0]["severity"] == HIGH

    def test_two_wickets_no_collapse(self):
        ms = _make_state()
        _add_over(ms, 0, wicket_on=3)
        _add_over(ms, 1, wicket_on=5)
        _add_over(ms, 2)  # no wicket
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        collapse = [t for t in triggers if t["type"] == COLLAPSE]
        assert len(collapse) == 0


# ── Big Partnership ──────────────────────────────────────────────────────────


class TestBigPartnership:
    def test_50_run_partnership(self):
        ms = _make_state()
        # ~8 overs of ~6 runs each = ~50 runs, no wicket
        for over_num in range(9):
            _add_over(ms, over_num, run_per_ball=1)
        # total = 54 runs, 0 wickets
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        bp = [t for t in triggers if t["type"] == BIG_PARTNERSHIP]
        assert len(bp) == 1
        assert bp[0]["severity"] == MEDIUM

    def test_partnership_resets_on_wicket(self):
        ms = _make_state()
        for over_num in range(7):
            _add_over(ms, over_num, run_per_ball=1)
        # 42 runs so far, now a wicket
        ms.add_ball(_ball(7, 1, 0, is_wicket=True))
        # Then 5 more runs
        for b in range(2, 7):
            ms.add_ball(_ball(7, b, 1))
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        bp = [t for t in triggers if t["type"] == BIG_PARTNERSHIP]
        assert len(bp) == 0  # only 5 runs since last wicket


# ── Dot Ball Pressure ────────────────────────────────────────────────────────


class TestDotBallPressure:
    def test_five_dots_in_eight_balls(self):
        ms = _make_state()
        # 8 balls, 5 dots
        for b in range(1, 6):
            ms.add_ball(_ball(0, b, 0))
        ms.add_ball(_ball(0, 6, 1))
        ms.add_ball(_ball(1, 1, 1))
        ms.add_ball(_ball(1, 2, 1))
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        dp = [t for t in triggers if t["type"] == DOT_BALL_PRESSURE]
        assert len(dp) == 1
        assert dp[0]["severity"] == MEDIUM

    def test_few_dots_no_trigger(self):
        ms = _make_state()
        # 8 balls, only 2 dots
        ms.add_ball(_ball(0, 1, 0))
        ms.add_ball(_ball(0, 2, 0))
        for b in range(3, 7):
            ms.add_ball(_ball(0, b, 2))
        ms.add_ball(_ball(1, 1, 1))
        ms.add_ball(_ball(1, 2, 1))
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        dp = [t for t in triggers if t["type"] == DOT_BALL_PRESSURE]
        assert len(dp) == 0


# ── Death Over Explosion ─────────────────────────────────────────────────────


class TestDeathOverExplosion:
    def test_death_over_18_runs(self):
        ms = _make_state()
        # Build up to over 15 (death phase)
        for over_num in range(15):
            _add_over(ms, over_num, run_per_ball=1)
        # Death over with 18 runs
        _add_balls(ms, 15, [6, 4, 4, 2, 1, 1])  # 18 runs
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        doe = [t for t in triggers if t["type"] == DEATH_OVER_EXPLOSION]
        assert len(doe) == 1
        assert doe[0]["severity"] == HIGH

    def test_big_over_not_in_death_no_explosion(self):
        ms = _make_state()
        # Over 5 (powerplay) with 20 runs — should be HUGE_OVER but not DEATH_OVER_EXPLOSION
        _add_balls(ms, 5, [6, 6, 4, 2, 1, 1])  # 20 runs
        se = SpeedEdge()
        triggers = se.detect_triggers(ms)
        doe = [t for t in triggers if t["type"] == DEATH_OVER_EXPLOSION]
        assert len(doe) == 0


# ── Empty match state ────────────────────────────────────────────────────────


class TestEmptyState:
    def test_no_balls_no_triggers(self):
        ms = _make_state()
        se = SpeedEdge()
        assert se.detect_triggers(ms) == []


# ── format_speed_alert ───────────────────────────────────────────────────────


class TestFormatSpeedAlert:
    def test_format_contains_key_elements(self):
        ms = _make_state()
        _add_over(ms, 0, run_per_ball=2)
        _add_over(ms, 1, run_per_ball=1)
        ms.add_ball(_ball(2, 1, 0, is_wicket=True))

        trigger = {
            "type": WICKET,
            "severity": HIGH,
            "detail": "Wicket fell at 2.1 (18/1)",
            "expected_impact": "Innings total should drop 5-10 runs",
            "recommended_action": "Check UNDER on innings total",
        }

        se = SpeedEdge()
        alert = se.format_speed_alert("RCB", "SRH", trigger, ms)

        assert "SPEED EDGE" in alert
        assert "RCB vs SRH" in alert
        assert "WICKET" in alert
        assert "Wicket fell" in alert
        assert "drop 5-10 runs" in alert
        assert "UNDER" in alert
        assert "30-60s" in alert
        assert "18/1" in alert
        assert "RR:" in alert
        assert "Proj:" in alert

    def test_format_medium_severity_uses_yellow(self):
        ms = _make_state()
        _add_over(ms, 0, run_per_ball=1)

        trigger = {
            "type": DOT_BALL_PRESSURE,
            "severity": MEDIUM,
            "detail": "5 dots in 8 balls",
            "expected_impact": "Run rate dropping",
            "recommended_action": "Check UNDER",
        }

        se = SpeedEdge()
        alert = se.format_speed_alert("MI", "CSK", trigger, ms)
        # Tilde for MEDIUM severity
        assert "[~]" in alert


# ── should_trigger_odds_fetch ────────────────────────────────────────────────


class TestShouldTriggerOddsFetch:
    def test_high_severity_returns_true(self):
        se = SpeedEdge()
        triggers = [{"type": WICKET, "severity": HIGH}]
        assert se.should_trigger_odds_fetch(triggers) is True

    def test_medium_only_returns_false(self):
        se = SpeedEdge()
        triggers = [{"type": DOT_BALL_PRESSURE, "severity": MEDIUM}]
        assert se.should_trigger_odds_fetch(triggers) is False

    def test_mixed_returns_true(self):
        se = SpeedEdge()
        triggers = [
            {"type": DOT_BALL_PRESSURE, "severity": MEDIUM},
            {"type": WICKET, "severity": HIGH},
        ]
        assert se.should_trigger_odds_fetch(triggers) is True

    def test_empty_returns_false(self):
        se = SpeedEdge()
        assert se.should_trigger_odds_fetch([]) is False


# ── evaluate_speed_opportunity ───────────────────────────────────────────────


class TestEvaluateSpeedOpportunity:
    def test_edge_confirmed_under(self):
        """Model drops, Cloudbet line still high → UNDER edge."""
        se = SpeedEdge()
        trigger = {"type": WICKET, "severity": HIGH}
        result = se.evaluate_speed_opportunity(
            trigger=trigger,
            pre_event_prediction=170.0,
            current_prediction=162.0,
            cloudbet_odds={"line": 169.5, "over": 1.90, "under": 1.90},
        )
        assert result is not None
        assert result["trigger"] == WICKET
        assert result["model_shift"] == -8.0
        assert result["cloudbet_line"] == 169.5
        assert result["cloudbet_moved"] is False
        assert result["edge_size"] == 7.5
        assert "UNDER" in result["recommendation"]

    def test_edge_confirmed_over(self):
        """Model rises, Cloudbet line still low → OVER edge."""
        se = SpeedEdge()
        trigger = {"type": HUGE_OVER, "severity": HIGH}
        result = se.evaluate_speed_opportunity(
            trigger=trigger,
            pre_event_prediction=165.0,
            current_prediction=175.0,
            cloudbet_odds={"line": 166.5, "over": 1.90, "under": 1.90},
        )
        assert result is not None
        assert result["trigger"] == HUGE_OVER
        assert result["model_shift"] == 10.0
        assert result["cloudbet_moved"] is False
        assert "OVER" in result["recommendation"]

    def test_no_edge_cloudbet_already_moved(self):
        """Cloudbet has already adjusted — no edge."""
        se = SpeedEdge()
        trigger = {"type": WICKET, "severity": HIGH}
        result = se.evaluate_speed_opportunity(
            trigger=trigger,
            pre_event_prediction=170.0,
            current_prediction=162.0,
            cloudbet_odds={"line": 162.5, "over": 1.90, "under": 1.90},
        )
        assert result is None

    def test_no_edge_small_model_shift(self):
        """Model barely shifted — not meaningful."""
        se = SpeedEdge()
        trigger = {"type": DOT_BALL_PRESSURE, "severity": MEDIUM}
        result = se.evaluate_speed_opportunity(
            trigger=trigger,
            pre_event_prediction=170.0,
            current_prediction=170.5,
            cloudbet_odds={"line": 170.5, "over": 1.90, "under": 1.90},
        )
        assert result is None

    def test_no_edge_wrong_direction(self):
        """Model drops but Cloudbet line already lower — no opportunity."""
        se = SpeedEdge()
        trigger = {"type": WICKET, "severity": HIGH}
        result = se.evaluate_speed_opportunity(
            trigger=trigger,
            pre_event_prediction=170.0,
            current_prediction=162.0,
            cloudbet_odds={"line": 160.0, "over": 1.90, "under": 1.90},
        )
        assert result is None


# ── recent_triggers accumulation ─────────────────────────────────────────────


class TestRecentTriggers:
    def test_triggers_accumulate(self):
        ms = _make_state()
        ms.add_ball(_ball(0, 1, 0, is_wicket=True))
        se = SpeedEdge()
        se.detect_triggers(ms)
        assert len(se.recent_triggers) >= 1
        # Second call
        ms.add_ball(_ball(0, 2, 0, is_wicket=True))
        se.detect_triggers(ms)
        assert len(se.recent_triggers) >= 2
