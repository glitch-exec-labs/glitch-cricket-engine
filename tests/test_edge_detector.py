"""Tests for ipl_spotter.modules.edge_detector."""

from __future__ import annotations

import math

import pytest

from modules.edge_detector import EdgeDetector


# ── fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture
def detector() -> EdgeDetector:
    return EdgeDetector({"min_ev_pct": 5.0, "min_ev_pct_mw": 3.0, "min_edge_runs": 2.0})


@pytest.fixture
def loose_detector() -> EdgeDetector:
    """Detector with very low thresholds so edges pass easily."""
    return EdgeDetector({"min_ev_pct": 0.0, "min_edge_runs": 0.0})


# ── _normal_cdf ──────────────────────────────────────────────────────────

class TestNormalCdf:
    def test_cdf_at_zero(self) -> None:
        assert EdgeDetector._normal_cdf(0.0) == pytest.approx(0.5)

    def test_cdf_at_large_positive(self) -> None:
        assert EdgeDetector._normal_cdf(4.0) == pytest.approx(1.0, abs=1e-4)

    def test_cdf_at_large_negative(self) -> None:
        assert EdgeDetector._normal_cdf(-4.0) == pytest.approx(0.0, abs=1e-4)

    def test_cdf_symmetry(self) -> None:
        val = EdgeDetector._normal_cdf(1.0)
        assert EdgeDetector._normal_cdf(-1.0) == pytest.approx(1.0 - val)

    def test_cdf_at_one(self) -> None:
        assert EdgeDetector._normal_cdf(1.0) == pytest.approx(0.8413, abs=1e-3)


# ── _edge_confidence ─────────────────────────────────────────────────────

class TestEdgeConfidence:
    def test_high_confidence(self, detector: EdgeDetector) -> None:
        # z = 5/8 = 0.625, ev = 15 → HIGH
        assert detector._edge_confidence(5.0, 8.0, 15.0) == "HIGH"

    def test_medium_confidence(self, detector: EdgeDetector) -> None:
        # z = 3/10 = 0.30, ev = 8 → MEDIUM
        assert detector._edge_confidence(3.0, 10.0, 8.0) == "MEDIUM"

    def test_low_confidence(self, detector: EdgeDetector) -> None:
        # z = 1/10 = 0.10, ev = 3 → LOW
        assert detector._edge_confidence(1.0, 10.0, 3.0) == "LOW"

    def test_high_ev_low_z_is_low(self, detector: EdgeDetector) -> None:
        # z = 0.1, ev = 20 → LOW (z too small)
        assert detector._edge_confidence(1.0, 10.0, 20.0) == "LOW"

    def test_zero_std_dev(self, detector: EdgeDetector) -> None:
        assert detector._edge_confidence(5.0, 0.0, 15.0) == "LOW"


# ── evaluate_line ────────────────────────────────────────────────────────

class TestEvaluateLine:
    def test_returns_none_when_edge_too_small(self, detector: EdgeDetector) -> None:
        result = detector.evaluate_line(
            market="total_runs",
            model_expected=170.0,
            model_std_dev=15.0,
            bookmaker_line=169.0,
            over_odds=1.90,
            under_odds=1.90,
        )
        assert result is None

    def test_returns_none_when_ev_too_low(self, detector: EdgeDetector) -> None:
        # Edge is large enough but odds are poor
        result = detector.evaluate_line(
            market="total_runs",
            model_expected=175.0,
            model_std_dev=15.0,
            bookmaker_line=170.0,
            over_odds=1.05,
            under_odds=1.05,
        )
        assert result is None

    def test_over_edge_detected(self, detector: EdgeDetector) -> None:
        result = detector.evaluate_line(
            market="total_runs",
            model_expected=180.0,
            model_std_dev=12.0,
            bookmaker_line=170.0,
            over_odds=2.10,
            under_odds=1.80,
        )
        assert result is not None
        assert result["direction"] == "OVER"
        assert result["market"] == "total_runs"
        assert result["edge_runs"] == 10.0
        assert result["bookmaker_line"] == 170.0
        assert result["model_expected"] == 180.0
        assert result["odds"] == 2.10
        assert result["ev_pct"] > 0

    def test_under_edge_detected(self, detector: EdgeDetector) -> None:
        result = detector.evaluate_line(
            market="total_runs",
            model_expected=155.0,
            model_std_dev=12.0,
            bookmaker_line=170.0,
            over_odds=1.80,
            under_odds=2.20,
        )
        assert result is not None
        assert result["direction"] == "UNDER"
        assert result["edge_runs"] == -15.0
        assert result["odds"] == 2.20

    def test_result_keys(self, loose_detector: EdgeDetector) -> None:
        result = loose_detector.evaluate_line(
            market="powerplay",
            model_expected=55.0,
            model_std_dev=8.0,
            bookmaker_line=50.0,
            over_odds=1.95,
            under_odds=1.85,
        )
        assert result is not None
        expected_keys = {
            "market", "direction", "bookmaker_line", "model_expected",
            "edge_runs", "model_prob", "odds", "fair_odds", "ev_pct",
            "confidence",
        }
        assert set(result.keys()) == expected_keys

    def test_probability_is_valid(self, loose_detector: EdgeDetector) -> None:
        result = loose_detector.evaluate_line(
            market="total_runs",
            model_expected=175.0,
            model_std_dev=15.0,
            bookmaker_line=170.0,
            over_odds=2.00,
            under_odds=1.80,
        )
        assert result is not None
        assert 0.0 < result["model_prob"] < 1.0


# ── evaluate_match_winner ────────────────────────────────────────────────

class TestEvaluateMatchWinner:
    def test_returns_none_when_ev_too_low(self, detector: EdgeDetector) -> None:
        result = detector.evaluate_match_winner(
            model_win_prob=0.50,
            bookmaker_odds=1.90,
            team="CSK",
        )
        assert result is None

    def test_positive_ev_detected(self, detector: EdgeDetector) -> None:
        result = detector.evaluate_match_winner(
            model_win_prob=0.65,
            bookmaker_odds=1.90,
            team="MI",
        )
        assert result is not None
        assert result["market"] == "match_winner"
        assert result["team"] == "MI"
        assert result["odds"] == 1.90
        # EV = (1.90 * 0.65 - 1) * 100 = 23.5%
        assert result["ev_pct"] == pytest.approx(23.5, abs=0.1)

    def test_implied_prob_calculation(self, detector: EdgeDetector) -> None:
        result = detector.evaluate_match_winner(
            model_win_prob=0.70,
            bookmaker_odds=2.00,
            team="RCB",
        )
        assert result is not None
        assert result["implied_prob"] == pytest.approx(0.50, abs=0.01)

    def test_edge_calculation(self, detector: EdgeDetector) -> None:
        result = detector.evaluate_match_winner(
            model_win_prob=0.70,
            bookmaker_odds=2.00,
            team="RCB",
        )
        assert result is not None
        assert result["edge"] == pytest.approx(0.20, abs=0.01)

    def test_result_keys(self, detector: EdgeDetector) -> None:
        result = detector.evaluate_match_winner(
            model_win_prob=0.70,
            bookmaker_odds=2.00,
            team="DC",
        )
        assert result is not None
        expected_keys = {
            "market", "team", "model_prob", "implied_prob",
            "odds", "ev_pct", "edge", "confidence",
        }
        assert set(result.keys()) == expected_keys

    def test_uses_match_winner_ev_threshold(self, detector: EdgeDetector) -> None:
        result = detector.evaluate_match_winner(
            model_win_prob=0.46,
            bookmaker_odds=2.30,
            team="SRH",
        )
        assert result is not None
        assert result["ev_pct"] == pytest.approx(5.8, abs=0.1)


# ── default config ───────────────────────────────────────────────────────

class TestDefaultConfig:
    def test_defaults_applied(self) -> None:
        d = EdgeDetector()
        assert d.min_ev_pct == 5.0
        assert d.min_ev_pct_mw == 5.0
        assert d.min_edge_runs == 2.0

    def test_config_override(self) -> None:
        d = EdgeDetector({"min_ev_pct": 3.0, "min_ev_pct_mw": 2.0, "min_edge_runs": 1.5})
        assert d.min_ev_pct == 3.0
        assert d.min_ev_pct_mw == 2.0
        assert d.min_edge_runs == 1.5


def test_no_opposite_bet_on_same_market(detector: EdgeDetector) -> None:
    """After betting OVER on powerplay_runs, UNDER should be blocked."""
    detector.lock_market(match_id=99, market_key="powerplay_runs", direction="OVER", innings=2)

    assert detector.get_locked_direction(99, "powerplay_runs", 2) == "OVER"
    assert detector.is_market_locked(99, "powerplay_runs", 2, proposed_direction="UNDER") is True


def test_directional_lock_persists(detector: EdgeDetector) -> None:
    """Lock stays for entire innings, not just cooldown period."""
    detector.lock_market(match_id=77, market_key="powerplay_runs", direction="OVER", innings=2)

    assert detector.get_locked_direction(77, "powerplay_runs", 2) == "OVER"
    detector.clear_locks(match_id=77, innings=1)
    assert detector.get_locked_direction(77, "powerplay_runs", 2) == "OVER"
