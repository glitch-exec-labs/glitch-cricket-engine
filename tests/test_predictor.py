"""Tests for ipl_spotter.modules.predictor — IPL prediction models."""

from __future__ import annotations

from pathlib import Path

import pytest

from modules.match_state import MatchState
from modules.predictor import IPLPredictor, Predictor
from modules.stats_db import StatsDB


@pytest.fixture
def predictor() -> IPLPredictor:
    return IPLPredictor()


@pytest.fixture
def player_aware_predictor(tmp_path: Path) -> IPLPredictor:
    db = StatsDB(str(tmp_path / "predictor.db"))
    for innings in range(6):
        db.insert_player_innings({
            "match_id": innings + 1,
            "player": "V Kohli",
            "team": "RCB",
            "runs": 58,
            "balls": 40,
            "fours": 5,
            "sixes": 2,
            "venue": "M.Chinnaswamy Stadium",
            "phase": "full",
            "opposition": "SRH",
        })
        db.insert_player_innings({
            "match_id": innings + 20,
            "player": "GJ Maxwell",
            "team": "RCB",
            "runs": 42,
            "balls": 28,
            "fours": 3,
            "sixes": 2,
            "venue": "M.Chinnaswamy Stadium",
            "phase": "full",
            "opposition": "SRH",
        })
        db.insert_bowler_innings({
            "match_id": innings + 40,
            "player": "JJ Bumrah",
            "team": "SRH",
            "overs": 4.0,
            "runs_conceded": 26,
            "wickets": 1,
            "venue": "M.Chinnaswamy Stadium",
            "phase": "full",
            "opposition": "RCB",
        })
    predictor = IPLPredictor({}, stats_db=db)
    yield predictor
    db.close()


def _make_match_state(
    runs: int = 0,
    wickets: int = 0,
    balls: int = 0,
    venue: str = "Wankhede Stadium",
) -> MatchState:
    """Helper to build a MatchState with given totals without replaying balls."""
    ms = MatchState("TeamA", "TeamB", venue)
    ms.total_runs = runs
    ms.wickets = wickets
    ms.balls_faced = balls
    ms.overs_completed = balls / 6.0
    return ms


# ── Venue modifier ───────────────────────────────────────────────────────────


class TestGetVenueModifier:
    def test_wankhede(self, predictor: IPLPredictor) -> None:
        assert predictor.get_venue_modifier("Wankhede Stadium, Mumbai") == 8.0

    def test_chinnaswamy(self, predictor: IPLPredictor) -> None:
        assert predictor.get_venue_modifier("M Chinnaswamy Stadium") == 12.0

    def test_chepauk(self, predictor: IPLPredictor) -> None:
        assert predictor.get_venue_modifier("MA Chidambaram, Chepauk") == -8.0

    def test_unknown_venue(self, predictor: IPLPredictor) -> None:
        assert predictor.get_venue_modifier("Unknown Ground") == 0.0

    def test_case_insensitive(self, predictor: IPLPredictor) -> None:
        assert predictor.get_venue_modifier("WANKHEDE") == 8.0


# ── Confidence helper ────────────────────────────────────────────────────────


class TestConfidenceFromStd:
    def test_high(self, predictor: IPLPredictor) -> None:
        # CV = 5/50 = 0.10 < 0.15 -> HIGH
        assert predictor._confidence_from_std(5.0, 50.0) == "HIGH"

    def test_medium(self, predictor: IPLPredictor) -> None:
        # CV = 10/50 = 0.20 -> MEDIUM
        assert predictor._confidence_from_std(10.0, 50.0) == "MEDIUM"

    def test_low(self, predictor: IPLPredictor) -> None:
        # CV = 20/50 = 0.40 -> LOW
        assert predictor._confidence_from_std(20.0, 50.0) == "LOW"

    def test_zero_expected(self, predictor: IPLPredictor) -> None:
        assert predictor._confidence_from_std(5.0, 0.0) == "LOW"


# ── Powerplay total ─────────────────────────────────────────────────────────


class TestPredictPowerplayTotal:
    def test_first_innings_default(self, predictor: IPLPredictor) -> None:
        result = predictor.predict_powerplay_total(
            "MI", "CSK", "Unknown Ground", innings=1, toss_decision="field",
        )
        # Tuned from backtest: 46.8 for 1st innings (was 49.5)
        assert result["expected"] == 46.8
        assert result["std_dev"] == 12.0
        assert result["range_low"] == pytest.approx(46.8 - 12.0, abs=0.1)

    def test_second_innings_default(self, predictor: IPLPredictor) -> None:
        result = predictor.predict_powerplay_total(
            "MI", "CSK", "Unknown Ground", innings=2,
        )
        assert result["expected"] == pytest.approx(44.5, abs=0.5)

    def test_toss_bat_first_adjustment(self, predictor: IPLPredictor) -> None:
        result = predictor.predict_powerplay_total(
            "MI", "CSK", "Unknown Ground", innings=1, toss_decision="bat",
        )
        # 46.8 - 1.0 = 45.8
        assert result["expected"] == 45.8

    def test_venue_avg_pp_override(self, predictor: IPLPredictor) -> None:
        result = predictor.predict_powerplay_total(
            "MI", "CSK", "Unknown Ground", venue_avg_pp=55.0, innings=1,
            toss_decision="field",
        )
        assert result["expected"] == 55.0

    def test_venue_modifier_applied(self, predictor: IPLPredictor) -> None:
        result = predictor.predict_powerplay_total(
            "MI", "CSK", "Chinnaswamy Stadium", innings=1, toss_decision="field",
        )
        # 46.8 + 12 * 0.3 = 46.8 + 3.6 = 50.4
        assert result["expected"] == pytest.approx(50.4, abs=0.1)

    def test_returns_all_keys(self, predictor: IPLPredictor) -> None:
        result = predictor.predict_powerplay_total("MI", "CSK", "Wankhede")
        for key in ("expected", "std_dev", "confidence", "range_low", "range_high"):
            assert key in result


# ── Phase runs ───────────────────────────────────────────────────────────────


class TestPredictPhaseRuns:
    def test_middle_phase_no_wickets(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=50, balls=36)  # 6 overs, CRR ~8.33
        result = predictor.predict_phase_runs(ms, "middle")
        assert result["expected"] > 0
        assert "confidence" in result

    def test_death_phase_no_wickets(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=120, balls=90)  # 15 overs
        result = predictor.predict_phase_runs(ms, "death")
        assert result["expected"] > 0

    def test_wickets_reduce_expected(self, predictor: IPLPredictor) -> None:
        ms_few = _make_match_state(runs=50, balls=36, wickets=1)
        ms_many = _make_match_state(runs=50, balls=36, wickets=6)
        res_few = predictor.predict_phase_runs(ms_few, "middle")
        res_many = predictor.predict_phase_runs(ms_many, "middle")
        assert res_few["expected"] > res_many["expected"]

    def test_venue_avg_override(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=50, balls=36)
        result = predictor.predict_phase_runs(ms, "middle", venue_avg=90.0)
        # venue_avg 90 / 9 overs = 10 RR -> higher than default 8.2
        assert result["expected"] > 0

    def test_returns_all_keys(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=50, balls=36)
        result = predictor.predict_phase_runs(ms, "middle")
        for key in ("expected", "std_dev", "confidence", "range_low", "range_high"):
            assert key in result


# ── Innings total ────────────────────────────────────────────────────────────


class TestPredictInningsTotal:
    def test_no_overs_returns_venue_avg(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=0, balls=0)
        result = predictor.predict_innings_total(ms, venue_avg=180.0)
        assert result["expected"] == 180.0

    def test_midway_prediction(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=90, balls=60)  # 10 overs, CRR 9.0
        result = predictor.predict_innings_total(ms)
        assert result["expected"] > 150
        assert result["expected"] < 230

    def test_std_dev_decreases_with_progress(self, predictor: IPLPredictor) -> None:
        ms_early = _make_match_state(runs=30, balls=18)  # 3 overs
        ms_late = _make_match_state(runs=140, balls=102)  # 17 overs
        res_early = predictor.predict_innings_total(ms_early)
        res_late = predictor.predict_innings_total(ms_late)
        assert res_early["std_dev"] > res_late["std_dev"]

    def test_returns_all_keys(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=50, balls=36)
        result = predictor.predict_innings_total(ms)
        for key in ("expected", "std_dev", "confidence", "range_low", "range_high"):
            assert key in result

    def test_late_innings_projection_stays_above_current_score(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=146, wickets=6, balls=106, venue="Gaddafi Stadium")
        result = predictor.predict_innings_total(ms, venue_avg=163.5)
        assert result["expected"] >= 146
        assert result["expected"] > 150

    def test_predict_wrapper_returns_copilot_keys(self) -> None:
        ms = _make_match_state(runs=32, wickets=1, balls=24, venue="Gaddafi Stadium")
        ms.current_innings = 2
        ms.target_runs = 161
        result = Predictor({}).predict(ms, home="Quetta Gladiators", away="Karachi Kings", venue_avg=163.5)
        for key in (
            "powerplay_total",
            "ten_over_total",
            "fifteen_over_total",
            "innings_total",
            "next_over",
            "match_winner",
        ):
            assert key in result
        assert "home_prob" in result["match_winner"]

    def test_completed_session_returns_actual_milestone_score(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=107, wickets=3, balls=71, venue="Gaddafi Stadium")
        ms.over_runs = {
            0: 10,
            1: 11,
            2: 12,
            3: 9,
            4: 10,
            5: 8,
            6: 12,
            7: 10,
            8: 9,
            9: 8,
            10: 8,
            11: 0,
        }
        result = predictor.predict_total_at_over(ms, target_over=6.0, venue_avg=163.5)
        assert result["expected"] == 60.0

    def test_player_adjustment_applied_to_next_over_and_innings(
        self,
        player_aware_predictor: IPLPredictor,
    ) -> None:
        ms = _make_match_state(runs=72, wickets=1, balls=48, venue="M.Chinnaswamy Stadium")
        ms.active_batsmen = [
            {"name": "Virat Kohli", "score": 42, "balls": 29, "rate": 145.0},
            {"name": "Glenn Maxwell", "score": 18, "balls": 14, "rate": 128.6},
        ]
        ms.active_bowler = {"name": "Jasprit Bumrah", "overs": 2.0, "runs": 12, "rate": 6.0}

        result = player_aware_predictor.predict(ms, home="RCB", away="SRH", venue_avg=180.0)

        assert "player_context" in result
        assert result["next_over"]["base_expected"] == 8.0
        assert result["next_over"]["expected"] < result["next_over"]["base_expected"]
        assert result["innings_total"]["player_adj"] < 0
        assert result["innings_total"]["expected"] < result["innings_total"]["base_expected"]


# ── Next over runs ───────────────────────────────────────────────────────────


class TestPredictNextOverRuns:
    def test_powerplay_base(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=20, balls=18)  # 3 overs -> powerplay
        result = predictor.predict_next_over_runs(ms)
        assert result["expected"] == 8.5
        assert result["std_dev"] == 3.5

    def test_middle_base(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=60, balls=48)  # 8 overs -> middle
        result = predictor.predict_next_over_runs(ms)
        assert result["expected"] == 8.0

    def test_death_base(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=130, balls=96)  # 16 overs -> death
        result = predictor.predict_next_over_runs(ms)
        assert result["expected"] == 11.5

    def test_bowler_economy_adjustment(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=60, balls=48)  # middle
        result = predictor.predict_next_over_runs(ms, bowler_economy=6.0)
        # 0.5*8.0 + 0.5*6.0 = 7.0
        assert result["expected"] == 7.0

    def test_batsman_sr_adjustment(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=60, balls=48)
        result = predictor.predict_next_over_runs(ms, batsman_sr=150.0)
        # batsman_rpo = 150 * 6 / 100 = 9.0
        # 0.7*8.0 + 0.3*9.0 = 5.6 + 2.7 = 8.3
        assert result["expected"] == pytest.approx(8.3, abs=0.1)

    def test_both_adjustments(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=60, balls=48)
        result = predictor.predict_next_over_runs(ms, bowler_economy=6.0, batsman_sr=150.0)
        # bowler adj: 0.5*8.0 + 0.5*6.0 = 7.0
        # then batsman adj: 0.7*7.0 + 0.3*9.0 = 4.9 + 2.7 = 7.6
        assert result["expected"] == pytest.approx(7.6, abs=0.1)

    def test_range_low_not_negative(self, predictor: IPLPredictor) -> None:
        ms = _make_match_state(runs=10, balls=18)
        result = predictor.predict_next_over_runs(ms, bowler_economy=2.0)
        assert result["range_low"] >= 0


# ── Chase win probability ────────────────────────────────────────────────────


class TestChaseWinProbability:
    def test_already_won(self, predictor: IPLPredictor) -> None:
        prob = predictor.chase_win_probability(150, 150, 15.0, 3)
        assert prob == 1.0

    def test_no_overs_left(self, predictor: IPLPredictor) -> None:
        prob = predictor.chase_win_probability(200, 180, 20.0, 5)
        assert prob == 0.0

    def test_no_wickets_left(self, predictor: IPLPredictor) -> None:
        prob = predictor.chase_win_probability(200, 180, 15.0, 10)
        assert prob == 0.0

    def test_easy_chase(self, predictor: IPLPredictor) -> None:
        # Need 20 runs in 5 overs, 8 wickets in hand -> very gettable
        prob = predictor.chase_win_probability(170, 150, 15.0, 2)
        assert prob > 0.6

    def test_hard_chase(self, predictor: IPLPredictor) -> None:
        # Need 100 from 3 overs with 7 wickets lost -> very hard
        prob = predictor.chase_win_probability(250, 150, 17.0, 7)
        assert prob < 0.3

    def test_returns_bounded(self, predictor: IPLPredictor) -> None:
        prob = predictor.chase_win_probability(180, 90, 10.0, 3)
        assert 0.0 <= prob <= 1.0

    def test_more_wickets_lost_reduces_probability(self, predictor: IPLPredictor) -> None:
        prob_few = predictor.chase_win_probability(180, 100, 12.0, 2)
        prob_many = predictor.chase_win_probability(180, 100, 12.0, 7)
        assert prob_few > prob_many
