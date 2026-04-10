"""Tests for copilot_telegram — formatted Telegram messages for match co-pilot."""

import pytest
from modules.copilot_telegram import (
    format_session_call, format_mw_call, format_over_update,
    format_book_alert, format_mw_swing, format_session_summary,
    format_pre_match_copilot, format_toss_update,
)


class TestSessionCall:
    def test_basic_yes_call(self):
        msg = format_session_call(market="6 Over", direction="YES", line=56.0, stake_per_run=300, model_prediction=59.0, home="RCB", away="SRH")
        assert "YES 56" in msg
        assert "300" in msg
        assert "59" in msg

    def test_includes_cloudbet_auto(self):
        msg = format_session_call(market="6 Over", direction="YES", line=56.0, stake_per_run=300, model_prediction=59.0, home="RCB", away="SRH", cloudbet_info="Auto-placed YES 54.5 @ 1.88 for $3.20")
        assert "Auto-placed" in msg


class TestMWCall:
    def test_lagai_call(self):
        msg = format_mw_call(team="SRH", direction="LAGAI", odds=2.30, stake=500, fair_prob=0.43, home="RCB", away="SRH")
        assert "BACK SRH" in msg
        assert "2.30" in msg
        assert "500" in msg

    def test_khai_call(self):
        msg = format_mw_call(team="RCB", direction="KHAI", odds=1.50, stake=600, fair_prob=0.60, home="RCB", away="SRH")
        # KHAI direction still shows Lagai in the signal (direction is caller's label)
        assert "RCB" in msg


class TestOverUpdate:
    def test_basic_update(self):
        msg = format_over_update(over_num=3, innings=1, batting_team="RCB", score=34, wickets=0, run_rate=11.3, projected_total=178, player_adjustment=None, mw_home_odds=1.45, mw_away_odds=2.65, home="RCB", away="SRH", positions_summary="YES 56: +3 runs ahead")
        assert "34/0" in msg
        assert "(3 ov)" in msg
        assert "11.3" in msg
        assert "Proj: 178" in msg

    def test_includes_player_context(self):
        msg = format_over_update(
            over_num=8,
            innings=1,
            batting_team="RCB",
            score=72,
            wickets=1,
            run_rate=9.0,
            projected_total=178,
            player_adjustment=-4.2,
            active_batsmen=[
                {"name": "Virat Kohli", "score": 42, "balls": 29, "sr": 145.0},
                {"name": "Rajat Patidar", "score": 18, "balls": 14, "sr": 128.6},
            ],
            active_bowler={"name": "Jasprit Bumrah", "overs": 2.0, "runs": 12, "wickets": 1, "econ": 6.0},
            mw_home_odds=1.45,
            mw_away_odds=2.65,
            home="RCB",
            away="SRH",
        )
        assert "(-4.2)" in msg
        assert "Virat Kohli 42(29)" in msg
        assert "Jasprit Bumrah" in msg


class TestBookAlert:
    def test_session_book(self):
        msg = format_book_alert(market_type="session", market_name="6 Over", action="Khai NO 64 @ Rs 300/run", guaranteed_profit=1800.0, math_breakdown="YES 56 + NO 64 @ Rs 300/run\n  Guaranteed: Rs 1800")
        assert "BOOK" in msg
        assert "NO 64" in msg

    def test_mw_book(self):
        msg = format_book_alert(market_type="match_winner", action="Khai SRH @ 1.75 for Rs 657", guaranteed_profit=157.0, math_breakdown="Wins: Rs +158 | Loses: Rs +157\n  Guaranteed: Rs 157")
        assert "BOOK" in msg
        assert "157" in msg


class TestMWSwing:
    def test_swing_alert(self):
        msg = format_mw_swing(team_moved="SRH", old_odds=2.30, new_odds=1.75, home="RCB", away="SRH", home_odds=2.05, away_odds=1.75, model_prob=0.58)
        assert "SRH" in msg
        assert "SWING" in msg
        assert "1.75" in msg


class TestSummary:
    def test_session_summary(self):
        msg = format_session_summary(cloudbet_pnl=10.28, cloudbet_bets=3, shadow_pnl=3757.0, shadow_bets=5, shadow_currency="INR", positions=[])
        assert "10.28" in msg or "10" in msg
        assert "3757" in msg or "3,757" in msg


class TestPreMatch:
    def test_pre_match_report(self):
        msg = format_pre_match_copilot(home="RCB", away="SRH", venue="Chinnaswamy", cloudbet_home_odds=1.65, cloudbet_away_odds=2.30, est_home_odds="1.58-1.60", est_away_odds="2.25-2.30", consensus_home_prob=0.59, consensus_away_prob=0.41, pp_line_est="53-54", model_pp=57.0, venue_avg_pp=52.3, venue_modifier=5.0)
        assert "RCB" in msg
        assert "SRH" in msg
        assert "Chinnaswamy" in msg

    def test_pre_match_report_formatted(self):
        """Venue averages are rounded to integers."""
        msg = format_pre_match_copilot(
            home="RCB",
            away="SRH",
            venue="Chinnaswamy",
            cloudbet_home_odds=1.65,
            cloudbet_away_odds=2.30,
            est_home_odds="1.58-1.60",
            est_away_odds="2.25-2.30",
            consensus_home_prob=0.59,
            consensus_away_prob=0.41,
            pp_line_est="53-54",
            model_pp=57.4,
            venue_avg_pp=52.6,
            venue_modifier=5.4,
        )
        assert "Model: 57" in msg
        assert "Venue avg: 53 (+5)" in msg

    def test_pre_match_report_handles_missing_venue_data(self):
        msg = format_pre_match_copilot(
            home="RCB",
            away="SRH",
            venue="Unknown",
            cloudbet_home_odds=1.65,
            cloudbet_away_odds=2.30,
            est_home_odds="1.58-1.60",
            est_away_odds="2.25-2.30",
            consensus_home_prob=0.59,
            consensus_away_prob=0.41,
            pp_line_est="53-54",
            model_pp=None,
            venue_avg_pp=None,
            venue_modifier=None,
        )
        assert "Model: -" in msg
        assert "Venue avg: -" in msg


class TestToss:
    def test_toss_update(self):
        msg = format_toss_update(winner="RCB", decision="bat first", home="RCB", away="SRH", adjustment="+2 runs batting first")
        assert "RCB" in msg
        assert "bat first" in msg
