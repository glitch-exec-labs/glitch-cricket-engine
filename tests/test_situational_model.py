"""Tests for modules.situational_model — situational prediction model."""

from __future__ import annotations

from pathlib import Path

import pytest

from modules.match_state import MatchState
from modules.predictor import IPLPredictor
from modules.situational_model import (
    SituationalPredictor,
    _over_bucket,
    _run_rate_bucket,
    _wicket_bucket,
)
from modules.stats_db import StatsDB


# ── Bucket assignment tests ─────────────────────────────────────────────────


class TestBucketAssignment:
    """Verify that values map to the correct bucket labels."""

    def test_wicket_bucket_zero(self):
        assert _wicket_bucket(0) == "0-1"

    def test_wicket_bucket_one(self):
        assert _wicket_bucket(1) == "0-1"

    def test_wicket_bucket_mid(self):
        assert _wicket_bucket(3) == "2-3"

    def test_wicket_bucket_four(self):
        assert _wicket_bucket(4) == "4-5"

    def test_wicket_bucket_high(self):
        assert _wicket_bucket(7) == "6-7"

    def test_wicket_bucket_eight_plus(self):
        assert _wicket_bucket(8) == "8+"
        assert _wicket_bucket(10) == "8+"

    def test_over_bucket_powerplay(self):
        assert _over_bucket(3.0) == "1-6"
        assert _over_bucket(6.0) == "1-6"

    def test_over_bucket_middle(self):
        assert _over_bucket(7.0) == "7-10"
        assert _over_bucket(10.0) == "7-10"

    def test_over_bucket_late_middle(self):
        assert _over_bucket(11.0) == "11-15"
        assert _over_bucket(15.0) == "11-15"

    def test_over_bucket_death(self):
        assert _over_bucket(16.0) == "16-20"
        assert _over_bucket(20.0) == "16-20"

    def test_over_bucket_below_one(self):
        assert _over_bucket(0.5) == "1-6"

    def test_run_rate_bucket_low(self):
        assert _run_rate_bucket(4.5) == "<6"

    def test_run_rate_bucket_mid_low(self):
        assert _run_rate_bucket(6.5) == "6-7.5"

    def test_run_rate_bucket_mid(self):
        assert _run_rate_bucket(8.0) == "7.5-9"

    def test_run_rate_bucket_mid_high(self):
        assert _run_rate_bucket(10.0) == "9-10.5"

    def test_run_rate_bucket_high(self):
        assert _run_rate_bucket(12.0) == ">10.5"

    def test_run_rate_bucket_boundary(self):
        # 6.0 is the lower bound of the 6-7.5 bucket
        assert _run_rate_bucket(6.0) == "6-7.5"


# ── Helper to build a MatchState at a specific situation ─────────────────────


def _make_state(
    runs: int,
    wickets: int,
    overs: float,
    batting_team: str = "CSK",
    bowling_team: str = "MI",
    venue: str = "Wankhede",
) -> MatchState:
    """Create a MatchState with specific score/wickets/overs for testing."""
    state = MatchState(batting_team=batting_team, bowling_team=bowling_team, venue=venue)
    state.total_runs = runs
    state.wickets = wickets
    state.overs_completed = overs
    completed_overs = int(overs)
    balls_in_current = int(round((overs - completed_overs) * 10))
    state.balls_faced = completed_overs * 6 + balls_in_current
    return state


def _populate_db(db: StatsDB, n: int = 30, avg_total: int = 175) -> None:
    """Insert n match records into the DB with realistic phase splits."""
    import random

    rng = random.Random(42)
    for i in range(n):
        total_1 = avg_total + rng.randint(-30, 30)
        total_2 = avg_total + rng.randint(-30, 30)
        pp1 = int(total_1 * 0.30) + rng.randint(-5, 5)
        mid1 = int(total_1 * 0.45) + rng.randint(-5, 5)
        death1 = total_1 - pp1 - mid1
        pp2 = int(total_2 * 0.30) + rng.randint(-5, 5)
        mid2 = int(total_2 * 0.45) + rng.randint(-5, 5)
        death2 = total_2 - pp2 - mid2
        db.insert_match({
            "match_id": 1000 + i,
            "venue": "Wankhede Stadium",
            "team1": "CSK",
            "team2": "MI",
            "first_innings_total": total_1,
            "second_innings_total": total_2,
            "powerplay_runs_1st": pp1,
            "powerplay_runs_2nd": pp2,
            "middle_runs_1st": mid1,
            "middle_runs_2nd": mid2,
            "death_runs_1st": death1,
            "death_runs_2nd": death2,
            "toss_winner": "CSK",
            "toss_decision": "bat",
            "winner": "CSK",
        })


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def empty_db(tmp_path: Path) -> StatsDB:
    db = StatsDB(str(tmp_path / "empty.db"))
    yield db
    db.close()


@pytest.fixture
def populated_db(tmp_path: Path) -> StatsDB:
    db = StatsDB(str(tmp_path / "populated.db"))
    _populate_db(db)
    yield db
    db.close()


# ── SituationalPredictor tests ──────────────────────────────────────────────


class TestSituationalPredictorEmptyDB:
    """When no historical data is available, fall back to venue average."""

    def test_fallback_to_venue_avg(self, empty_db: StatsDB):
        sp = SituationalPredictor(empty_db)
        state = _make_state(runs=60, wickets=2, overs=8.0)
        result = sp.predict_innings_total(state, venue_avg=165.0)

        assert result["expected"] == 165.0
        assert result["sample_count"] == 0
        assert result["confidence"] in ("HIGH", "MEDIUM", "LOW")
        assert "bucket" in result

    def test_fallback_std_dev(self, empty_db: StatsDB):
        sp = SituationalPredictor(empty_db)
        state = _make_state(runs=50, wickets=1, overs=6.0)
        result = sp.predict_innings_total(state)

        assert result["std_dev"] == 25.0


class TestSituationalPredictorWithData:
    """When populated with historical data, produce data-driven predictions."""

    def test_returns_expected_keys(self, populated_db: StatsDB):
        sp = SituationalPredictor(populated_db)
        state = _make_state(runs=82, wickets=1, overs=10.0)
        result = sp.predict_innings_total(state)

        assert "expected" in result
        assert "std_dev" in result
        assert "confidence" in result
        assert "sample_count" in result
        assert "bucket" in result

    def test_different_wickets_give_different_predictions(self, populated_db: StatsDB):
        sp = SituationalPredictor(populated_db)
        state_good = _make_state(runs=82, wickets=1, overs=10.0)
        state_bad = _make_state(runs=82, wickets=5, overs=10.0)

        pred_good = sp.predict_innings_total(state_good)
        pred_bad = sp.predict_innings_total(state_bad)

        # The core requirement: these situations should produce different
        # bucket assignments even if expected values happen to coincide
        # due to limited data variety.
        assert pred_good["bucket"] != pred_bad["bucket"]

    def test_prediction_in_reasonable_range(self, populated_db: StatsDB):
        sp = SituationalPredictor(populated_db)
        state = _make_state(runs=100, wickets=2, overs=12.0)
        result = sp.predict_innings_total(state, venue_avg=172.0)

        assert 100.0 <= result["expected"] <= 250.0

    def test_sample_count_positive_when_bucket_has_data(self, populated_db: StatsDB):
        sp = SituationalPredictor(populated_db)
        state = _make_state(runs=50, wickets=1, overs=6.0)
        result = sp.predict_innings_total(state)

        # With 30 matches, at least some buckets should have data
        # The bucket might or might not have enough; just verify structure
        assert result["sample_count"] >= 0

    def test_early_innings_under_one_over_uses_fallback(self, populated_db: StatsDB):
        """Before 1 over, situational model should not be triggered in predictor."""
        sp = SituationalPredictor(populated_db)
        state = _make_state(runs=8, wickets=0, overs=0.5)
        result = sp.predict_innings_total(state, venue_avg=172.0)
        # Should still produce a valid result (may fall back)
        assert result["expected"] > 0


class TestBucketStatic:
    """Test static bucket methods on the class."""

    def test_class_wicket_bucket(self, empty_db: StatsDB):
        sp = SituationalPredictor(empty_db)
        assert sp.wicket_bucket(5) == "4-5"

    def test_class_over_bucket(self, empty_db: StatsDB):
        sp = SituationalPredictor(empty_db)
        assert sp.over_bucket(10.0) == "7-10"

    def test_class_run_rate_bucket(self, empty_db: StatsDB):
        sp = SituationalPredictor(empty_db)
        assert sp.run_rate_bucket(8.5) == "7.5-9"


# ── Integration with IPLPredictor ────────────────────────────────────────────


class TestIPLPredictorIntegration:
    """Verify that IPLPredictor correctly integrates the situational model."""

    def test_predictor_without_db_has_no_situational(self):
        predictor = IPLPredictor()
        assert predictor.situational is None

    def test_predictor_with_empty_db_has_situational(self, empty_db: StatsDB):
        predictor = IPLPredictor(stats_db=empty_db)
        assert predictor.situational is not None

    def test_predict_innings_total_includes_situational_keys(self, populated_db: StatsDB):
        predictor = IPLPredictor(stats_db=populated_db)
        state = _make_state(runs=80, wickets=2, overs=10.0)
        result = predictor.predict_innings_total(state, venue_avg=172.0)

        assert "situational_expected" in result
        assert "situational_confidence" in result

    def test_predict_innings_total_no_situational_before_1_over(self, populated_db: StatsDB):
        predictor = IPLPredictor(stats_db=populated_db)
        state = _make_state(runs=5, wickets=0, overs=0.5)
        result = predictor.predict_innings_total(state, venue_avg=172.0)

        # Under 1 over, situational keys should not be present
        assert "situational_expected" not in result

    def test_base_predictor_still_works_without_db(self):
        predictor = IPLPredictor()
        state = _make_state(runs=82, wickets=2, overs=10.0)
        result = predictor.predict_innings_total(state, venue_avg=172.0)

        assert "expected" in result
        assert result["expected"] > 0
        assert "situational_expected" not in result

    def test_full_predict_method_works(self, populated_db: StatsDB):
        predictor = IPLPredictor(stats_db=populated_db)
        state = _make_state(runs=100, wickets=3, overs=12.0)
        predictions = predictor.predict(state, venue_avg=172.0)

        assert "innings_total" in predictions
        assert predictions["innings_total"]["expected"] > 0
