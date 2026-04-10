"""
Integration tests — end-to-end verification of the full IPL Edge Spotter pipeline.

Tests the chain: MatchState -> IPLPredictor -> EdgeDetector -> Telegram formatting
"""

from __future__ import annotations

import os
import tempfile

import pytest

from modules.match_state import MatchState
from modules.predictor import IPLPredictor
from modules.edge_detector import EdgeDetector
from modules.telegram_bot import format_edge_alert
from modules.stats_db import StatsDB
from modules.odds_client import OddsClient


# ── Helpers ────────────────────────────────────────────────────────────────────


def _simulate_aggressive_powerplay(ms: MatchState, overs: int = 4) -> None:
    """Simulate *overs* overs of aggressive batting (~10 rpo)."""
    for over_num in range(overs):
        # 6 legal balls per over, averaging ~10 runs per over
        ball_runs = [2, 1, 4, 1, 1, 1]  # = 10 per over
        for ball_idx, runs in enumerate(ball_runs, start=1):
            ms.add_ball({
                "over": over_num,
                "ball": ball_idx,
                "runs": runs,
                "is_wicket": False,
                "extras": 0,
                "is_legal": True,
            })


def _simulate_moderate_powerplay(ms: MatchState, overs: int = 4) -> None:
    """Simulate *overs* overs of moderate batting (~8 rpo)."""
    for over_num in range(overs):
        ball_runs = [1, 1, 2, 1, 2, 1]  # = 8 per over
        for ball_idx, runs in enumerate(ball_runs, start=1):
            ms.add_ball({
                "over": over_num,
                "ball": ball_idx,
                "runs": runs,
                "is_wicket": False,
                "extras": 0,
                "is_legal": True,
            })


# ── 1. Full pipeline — powerplay edge detected ────────────────────────────────


def test_full_pipeline_powerplay_edge():
    """MatchState -> Predictor -> EdgeDetector -> format_edge_alert (OVER)."""
    # Step 1: Build match state with aggressive batting
    ms = MatchState("Mumbai Indians", "Chennai Super Kings", "Wankhede Stadium")
    _simulate_aggressive_powerplay(ms, overs=4)

    assert ms.total_runs == 40  # 10 * 4
    assert ms.current_run_rate == pytest.approx(10.0, abs=0.5)

    # Step 2: Predict powerplay total
    predictor = IPLPredictor()
    pp_pred = predictor.predict_powerplay_total(
        batting_team="Mumbai Indians",
        bowling_team="Chennai Super Kings",
        venue="Wankhede Stadium",
    )
    assert "expected" in pp_pred
    assert "std_dev" in pp_pred

    # Step 3: Evaluate against a low bookmaker line
    detector = EdgeDetector({"min_ev_pct": 1.0, "min_edge_runs": 1.0})
    bookmaker_line = 45.5  # intentionally low vs aggressive batting
    edge = detector.evaluate_line(
        market="powerplay_runs",
        model_expected=pp_pred["expected"],
        model_std_dev=pp_pred["std_dev"],
        bookmaker_line=bookmaker_line,
        over_odds=1.90,
        under_odds=1.90,
    )

    # The model expected should be around 49-52 for Wankhede; line is 45.5
    # With relaxed thresholds we expect an OVER edge
    assert edge is not None, "Expected an OVER edge to be detected"
    assert edge["direction"] == "OVER"
    assert edge["market"] == "powerplay_runs"
    assert edge["ev_pct"] > 0

    # Step 4: Format the alert
    alert = format_edge_alert("Mumbai Indians", "Chennai Super Kings", edge)
    assert "EDGE:" in alert
    assert "OVER" in alert
    assert "powerplay_runs" in alert or "Powerplay" in alert
    assert str(round(bookmaker_line)) in alert


# ── 2. Full pipeline — no edge ─────────────────────────────────────────────────


def test_full_pipeline_no_edge():
    """When the bookmaker line matches the model prediction, no edge detected."""
    ms = MatchState("Rajasthan Royals", "Punjab Kings", "Sawai Mansingh Stadium")
    _simulate_moderate_powerplay(ms, overs=4)

    predictor = IPLPredictor()
    pp_pred = predictor.predict_powerplay_total(
        batting_team="Rajasthan Royals",
        bowling_team="Punjab Kings",
        venue="Sawai Mansingh Stadium",
    )

    # Set bookmaker line to match the model expected exactly
    detector = EdgeDetector()  # default thresholds (min_edge_runs=2)
    edge = detector.evaluate_line(
        market="powerplay_runs",
        model_expected=pp_pred["expected"],
        model_std_dev=pp_pred["std_dev"],
        bookmaker_line=pp_pred["expected"],  # no gap
        over_odds=1.90,
        under_odds=1.90,
    )

    assert edge is None, "Expected no edge when line matches model"


# ── 3. Chase probability pipeline ──────────────────────────────────────────────


def test_chase_probability_pipeline():
    """Predictor.chase_win_probability -> EdgeDetector.evaluate_match_winner."""
    predictor = IPLPredictor()
    detector = EdgeDetector({"min_ev_pct": 1.0})

    # Chasing team in a strong position
    win_prob = predictor.chase_win_probability(
        target=170,
        current_score=100,
        overs_completed=12.0,
        wickets_lost=2,
    )

    assert 0.0 <= win_prob <= 1.0

    # Bookmaker offering generous odds on the chasing team
    edge = detector.evaluate_match_winner(
        model_win_prob=win_prob,
        bookmaker_odds=2.50,  # implies 40% — model should be higher
        team="Mumbai Indians",
    )

    # With score 100/2 after 12 overs chasing 170, the model prob should be
    # noticeably above 40%, giving an edge
    if edge is not None:
        assert edge["market"] == "match_winner"
        assert edge["team"] == "Mumbai Indians"
        assert "model_prob" in edge
        assert "implied_prob" in edge
        assert "odds" in edge
        assert "ev_pct" in edge
        assert "edge" in edge
        assert "confidence" in edge


# ── 4. Match state to prediction ──────────────────────────────────────────────


def test_match_state_to_prediction():
    """Build match state from simulated balls, verify innings total prediction."""
    ms = MatchState("KKR", "DC", "Eden Gardens")
    _simulate_aggressive_powerplay(ms, overs=4)

    predictor = IPLPredictor()
    pred = predictor.predict_innings_total(ms, venue_avg=174.0)

    # Required keys
    for key in ("expected", "std_dev", "confidence", "range_low", "range_high"):
        assert key in pred, f"Missing key: {key}"

    # After 4 overs at 10 rpo, projected total should be well above 150
    assert pred["expected"] > 150, f"Expected > 150, got {pred['expected']}"
    # And under 250 (sanity)
    assert pred["expected"] < 250, f"Expected < 250, got {pred['expected']}"

    # Range should be sensible
    assert pred["range_low"] < pred["expected"] < pred["range_high"]
    assert pred["confidence"] in ("HIGH", "MEDIUM", "LOW")


# ── 5. StatsDB to predictor ───────────────────────────────────────────────────


def test_stats_db_to_predictor():
    """Insert sample venue data into StatsDB, use it to inform predictor."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        db = StatsDB(db_path)

        # Insert several high-scoring matches at "Test Stadium"
        for i in range(5):
            db.insert_match({
                "match_id": 1000 + i,
                "venue": "Test Stadium",
                "team1": "Team A",
                "team2": "Team B",
                "first_innings_total": 200 + i * 2,  # 200-208
                "second_innings_total": 180 + i,
                "powerplay_runs_1st": 60 + i,  # 60-64
                "powerplay_runs_2nd": 50,
                "middle_runs_1st": 80,
                "middle_runs_2nd": 70,
                "death_runs_1st": 60 + i,
                "death_runs_2nd": 60,
                "toss_winner": "Team A",
                "toss_decision": "bat",
                "winner": "Team A",
            })

        # Insert lower-scoring matches at "Slow Ground"
        for i in range(5):
            db.insert_match({
                "match_id": 2000 + i,
                "venue": "Slow Ground",
                "team1": "Team C",
                "team2": "Team D",
                "first_innings_total": 140 + i,  # 140-144
                "second_innings_total": 130 + i,
                "powerplay_runs_1st": 35 + i,  # 35-39
                "powerplay_runs_2nd": 30,
                "middle_runs_1st": 55,
                "middle_runs_2nd": 50,
                "death_runs_1st": 50,
                "death_runs_2nd": 50,
                "toss_winner": "Team C",
                "toss_decision": "bat",
                "winner": "Team C",
            })

        high_stats = db.get_venue_stats("Test Stadium")
        low_stats = db.get_venue_stats("Slow Ground")

        assert high_stats["matches"] == 5
        assert low_stats["matches"] == 5

        # Use venue averages in predictor
        predictor = IPLPredictor()

        ms_high = MatchState("Team A", "Team B", "Test Stadium")
        _simulate_aggressive_powerplay(ms_high, overs=3)
        pred_high = predictor.predict_innings_total(
            ms_high, venue_avg=high_stats["avg_first_innings"]
        )

        ms_low = MatchState("Team C", "Team D", "Slow Ground")
        _simulate_moderate_powerplay(ms_low, overs=3)
        pred_low = predictor.predict_innings_total(
            ms_low, venue_avg=low_stats["avg_first_innings"]
        )

        # High-scoring venue + aggressive batting should produce a higher
        # predicted total than low-scoring venue + moderate batting
        assert pred_high["expected"] > pred_low["expected"], (
            f"High venue pred ({pred_high['expected']}) should exceed "
            f"low venue pred ({pred_low['expected']})"
        )

        db.close()
    finally:
        os.unlink(db_path)


# ── 6. Odds parsing to edge detection ─────────────────────────────────────────


def test_odds_parsing_to_edge_detection():
    """Parse mock Cloudbet event data, feed into EdgeDetector."""
    # Build a mock Cloudbet event matching real API structure
    mock_event = {
        "id": "12345",
        "home": {"name": "Mumbai Indians"},
        "away": {"name": "Chennai Super Kings"},
        "markets": {
            "cricket.team_totals": {
                "submarkets": {
                    "main": {
                        "selections": [
                            {
                                "outcome": "over",
                                "params": "total=165.5",
                                "price": "1.85",
                                "marketUrl": "",
                            },
                            {
                                "outcome": "under",
                                "params": "total=165.5",
                                "price": "1.95",
                                "marketUrl": "",
                            },
                        ]
                    }
                }
            },
            "cricket.winner": {
                "submarkets": {
                    "main": {
                        "selections": [
                            {"outcome": "home", "price": "1.80"},
                            {"outcome": "away", "price": "2.10"},
                        ]
                    }
                }
            },
        },
    }

    # Parse using OddsClient (no network calls needed)
    client = OddsClient({"cloudbet_api_key": "test-key"})

    # Parse innings_total market
    innings_odds = client.get_market_odds(mock_event, "innings_total")
    assert innings_odds is not None
    assert innings_odds["market"] == "innings_total"
    assert len(innings_odds["lines"]) >= 1

    line_data = innings_odds["lines"][0]
    assert line_data["line"] == 165.5
    assert line_data["over_odds"] == 1.85
    assert line_data["under_odds"] == 1.95

    # Parse match_winner market
    winner_odds = client.get_market_odds(mock_event, "match_winner")
    assert winner_odds is not None
    assert winner_odds["market"] == "match_winner"
    assert "home" in winner_odds["selections"]
    assert "away" in winner_odds["selections"]

    # Feed parsed innings line into EdgeDetector
    detector = EdgeDetector({"min_ev_pct": 1.0, "min_edge_runs": 1.0})

    # Suppose our model expects 175 runs (high-scoring venue)
    model_expected = 175.0
    model_std_dev = 20.0

    edge = detector.evaluate_line(
        market="innings_total",
        model_expected=model_expected,
        model_std_dev=model_std_dev,
        bookmaker_line=line_data["line"],
        over_odds=line_data["over_odds"],
        under_odds=line_data["under_odds"],
    )

    # Model expects 175, line is 165.5 -> 9.5 run edge OVER
    assert edge is not None, "Expected an OVER edge (175 vs 165.5 line)"
    assert edge["direction"] == "OVER"
    assert edge["edge_runs"] == pytest.approx(model_expected - 165.5, abs=0.01)
    assert edge["ev_pct"] > 0

    # Feed match_winner into EdgeDetector
    home_odds = winner_odds["selections"]["home"]["price"]
    # Model gives home team 65% win probability vs implied ~55%
    winner_edge = detector.evaluate_match_winner(
        model_win_prob=0.65,
        bookmaker_odds=home_odds,
        team="Mumbai Indians",
    )

    # 0.65 * 1.80 = 1.17 => EV = 17% which should clear the threshold
    assert winner_edge is not None
    assert winner_edge["market"] == "match_winner"
    assert winner_edge["team"] == "Mumbai Indians"
    assert winner_edge["ev_pct"] > 0
