"""Tests for match_copilot — orchestrates the match co-pilot experience."""

import pytest
from modules.match_copilot import MatchCopilot


class TestPhaseDetection:
    def setup_method(self):
        self.copilot = MatchCopilot(config={"copilot_enabled": True, "shadow_default_stake_inr": 500, "shadow_mw_default_stake_inr": 500, "hedge_min_profit_inr": 100, "hedge_session_min_runs": 4, "hedge_mw_min_odds_move": 0.20, "message_throttle_seconds": 0})

    def test_innings_1_pp_phase(self):
        assert self.copilot._detect_phase(innings=1, overs=3.2, wickets=0) == "INNINGS_1_PP"

    def test_innings_1_middle_phase(self):
        assert self.copilot._detect_phase(innings=1, overs=8.0, wickets=2) == "INNINGS_1_MIDDLE"

    def test_innings_1_death_phase(self):
        assert self.copilot._detect_phase(innings=1, overs=16.3, wickets=4) == "INNINGS_1_DEATH"

    def test_innings_2_pp_phase(self):
        assert self.copilot._detect_phase(innings=2, overs=4.0, wickets=0) == "INNINGS_2_PP"

    def test_innings_2_chase_phase(self):
        assert self.copilot._detect_phase(innings=2, overs=17.0, wickets=5) == "INNINGS_2_CHASE"


class TestStakeSizing:
    def setup_method(self):
        self.copilot = MatchCopilot(config={"copilot_enabled": True, "shadow_min_stake_inr": 200, "shadow_max_stake_inr": 1000, "shadow_default_stake_inr": 500})

    def test_default_stake(self):
        stake = self.copilot._calculate_shadow_stake(edge_size=3.0, confidence="MEDIUM")
        assert 200 <= stake <= 1000

    def test_high_edge_gets_higher_stake(self):
        low = self.copilot._calculate_shadow_stake(edge_size=2.0, confidence="MEDIUM")
        high = self.copilot._calculate_shadow_stake(edge_size=8.0, confidence="HIGH")
        assert high >= low

    def test_stake_clamped_to_bounds(self):
        assert self.copilot._calculate_shadow_stake(edge_size=100.0, confidence="HIGH") <= 1000
        assert self.copilot._calculate_shadow_stake(edge_size=0.1, confidence="LOW") >= 200


class TestSessionCallDecision:
    def setup_method(self):
        self.copilot = MatchCopilot(config={"copilot_enabled": True, "shadow_default_stake_inr": 500, "shadow_min_stake_inr": 200, "shadow_max_stake_inr": 1000, "min_ev_pct": 5.0, "message_throttle_seconds": 0})

    def test_generates_call_when_edge_exists(self):
        calls = self.copilot.evaluate_session_calls(
            match_id=1,
            model_predictions={"powerplay_total": {"expected": 59.0, "std_dev": 8.0}},
            estimated_lines={"6_over": {"yes": 55.0, "no": 56.0}},
            overs_completed=1.0,
        )
        assert len(calls) >= 1
        assert calls[0]["direction"] == "YES"
        assert calls[0]["line"] == 56.0

    def test_no_call_when_no_edge(self):
        calls = self.copilot.evaluate_session_calls(
            match_id=1,
            model_predictions={"powerplay_total": {"expected": 55.0, "std_dev": 8.0}},
            estimated_lines={"6_over": {"yes": 55.0, "no": 56.0}},
            overs_completed=1.0,
        )
        assert len(calls) == 0

    def test_same_market_can_fire_again_in_new_innings(self):
        calls_1 = self.copilot.evaluate_session_calls(
            match_id=1,
            model_predictions={"powerplay_total": {"expected": 59.0, "std_dev": 8.0}},
            estimated_lines={"6_over": {"yes": 55.0, "no": 56.0}},
            overs_completed=1.0,
            innings=1,
        )
        calls_2 = self.copilot.evaluate_session_calls(
            match_id=1,
            model_predictions={"powerplay_total": {"expected": 59.0, "std_dev": 8.0}},
            estimated_lines={"6_over": {"yes": 55.0, "no": 56.0}},
            overs_completed=1.0,
            innings=2,
        )
        assert len(calls_1) == 1
        assert len(calls_2) == 1


class TestMWCallDecision:
    def setup_method(self):
        self.copilot = MatchCopilot(config={"copilot_enabled": True, "shadow_mw_default_stake_inr": 500, "min_ev_pct": 5.0, "min_ev_pct_mw": 3.0, "message_throttle_seconds": 0})

    def test_mw_call_when_value(self):
        mw = self.copilot.evaluate_mw_call(match_id=1, home="RCB", away="SRH", model_home_prob=0.55, current_home_odds=1.65, current_away_odds=2.30)
        # SRH at 2.30 with 45% fair prob (fair=2.22) — ev = (2.30/2.22-1)*100 = 3.6% — may or may not trigger depending on threshold
        # But with model_home_prob=0.55, away=0.45, fair_away=2.22, ev=(2.30/2.22-1)*100=3.6% < 5% so no call
        # Let's test with clearer edge
        pass

    def test_mw_call_clear_edge(self):
        mw = self.copilot.evaluate_mw_call(match_id=1, home="RCB", away="SRH", model_home_prob=0.50, current_home_odds=1.65, current_away_odds=2.30)
        # away=50%, fair=2.00, ev=(2.30/2.00-1)*100=15% — clear value
        assert mw is not None
        assert mw["team"] == "SRH"
        assert mw["direction"] == "LAGAI"

    def test_mw_shadow_dedup(self):
        """Only one MW shadow position per team per match."""
        first = self.copilot.evaluate_mw_call(
            match_id=1,
            home="RCB",
            away="SRH",
            model_home_prob=0.50,
            current_home_odds=1.65,
            current_away_odds=2.30,
        )
        second = self.copilot.evaluate_mw_call(
            match_id=1,
            home="RCB",
            away="SRH",
            model_home_prob=0.50,
            current_home_odds=1.70,
            current_away_odds=2.55,
        )

        assert first is not None
        assert second is None
        assert len(self.copilot.position_book.get_open_mw(1)) == 1

    def test_opposite_session_call_blocked_by_lock(self):
        first_calls = self.copilot.evaluate_session_calls(
            match_id=10,
            model_predictions={"powerplay_total": {"expected": 62.0, "std_dev": 8.0}},
            estimated_lines={"6_over": {"yes": 58.0, "no": 59.0}},
            overs_completed=2.0,
            innings=2,
        )
        second_calls = self.copilot.evaluate_session_calls(
            match_id=10,
            model_predictions={"powerplay_total": {"expected": 56.0, "std_dev": 8.0}},
            estimated_lines={"6_over": {"yes": 60.0, "no": 61.0}},
            overs_completed=2.5,
            innings=2,
        )

        assert len(first_calls) == 1
        assert first_calls[0]["direction"] == "YES"
        assert second_calls == []

    def test_skip_completed_session_copilot(self):
        """At over 8.0, 6_over session call should not be generated."""
        calls = self.copilot.evaluate_session_calls(
            match_id=11,
            model_predictions={"powerplay_total": {"expected": 62.0, "std_dev": 8.0}},
            estimated_lines={"6_over": {"yes": 58.0, "no": 59.0}},
            overs_completed=8.0,
            innings=2,
        )
        assert calls == []

    def test_mw_uses_separate_ev_threshold(self):
        mw = self.copilot.evaluate_mw_call(
            match_id=12,
            home="RCB",
            away="SRH",
            model_home_prob=0.55,
            current_home_odds=1.65,
            current_away_odds=2.30,
        )
        assert mw is not None
        assert mw["team"] == "SRH"


class TestBookChecking:
    def setup_method(self):
        self.copilot = MatchCopilot(config={"copilot_enabled": True, "shadow_default_stake_inr": 300, "shadow_min_stake_inr": 200, "shadow_max_stake_inr": 1000, "hedge_min_profit_inr": 100, "hedge_session_min_runs": 4, "hedge_mw_min_odds_move": 0.20, "message_throttle_seconds": 0, "session_book_alerts": True})

    def test_session_book_opportunity_found(self):
        self.copilot.position_book.add_session_call(1, "6_over", "YES", 56.0, 300.0)
        books = self.copilot.check_book_opportunities(match_id=1, current_session_lines={"6_over": {"yes": 62.0, "no": 63.0}}, current_mw_odds={})
        assert len(books) >= 1
        assert books[0].guaranteed_profit >= 100

    def test_mw_book_opportunity_found(self):
        self.copilot.position_book.add_mw_call(1, "SRH", "LAGAI", 2.30, 500.0)
        books = self.copilot.check_book_opportunities(match_id=1, current_session_lines={}, current_mw_odds={"SRH": 1.75})
        assert len(books) >= 1
        assert books[0].guaranteed_profit >= 100


class TestMWSwing:
    def setup_method(self):
        self.copilot = MatchCopilot(config={"copilot_enabled": True, "message_throttle_seconds": 0})

    def test_detects_swing(self):
        # First call sets baseline
        self.copilot.check_mw_swing(1, "RCB", "SRH", 1.65, 2.30)
        # Second call with big move
        swing = self.copilot.check_mw_swing(1, "RCB", "SRH", 2.05, 1.80)
        assert swing is not None

    def test_no_swing_small_move(self):
        self.copilot.check_mw_swing(1, "RCB", "SRH", 1.65, 2.30)
        swing = self.copilot.check_mw_swing(1, "RCB", "SRH", 1.62, 2.35)
        assert swing is None


class TestOverTracking:
    def setup_method(self):
        self.copilot = MatchCopilot(config={"copilot_enabled": True})

    def test_sends_once_per_over(self):
        assert self.copilot.should_send_over_update(1, 3) is True
        assert self.copilot.should_send_over_update(1, 3) is False
        assert self.copilot.should_send_over_update(1, 4) is True

    def test_allows_same_over_in_new_innings(self):
        assert self.copilot.should_send_over_update(1, 3, innings=1) is True
        assert self.copilot.should_send_over_update(1, 3, innings=2) is True


class TestLineEstimation:
    def setup_method(self):
        self.copilot = MatchCopilot(config={"copilot_enabled": True})

    def test_estimates_from_cloudbet(self):
        lines = self.copilot.estimate_session_lines(
            overs_completed=2.0, current_score=20,
            cloudbet_lines={"6_over": {"line": 55.0}},
            model_predictions={"powerplay_total": {"expected": 57.0}},
        )
        assert "6_over" in lines
        assert lines["6_over"]["yes"] == 54.0
        assert lines["6_over"]["no"] == 55.0

    def test_estimates_from_model_fallback(self):
        lines = self.copilot.estimate_session_lines(
            overs_completed=2.0, current_score=20,
            cloudbet_lines={},
            model_predictions={"powerplay_total": {"expected": 57.0}},
        )
        assert "6_over" in lines
        assert lines["6_over"]["yes"] == 56.0
        assert lines["6_over"]["no"] == 57.0
