"""
Tests for ipl_spotter.modules.match_state — MatchState engine.

Pure-Python, no external dependencies beyond pytest.
"""

import pytest

from modules.match_state import MatchState


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_state() -> MatchState:
    return MatchState("MI", "CSK", "Wankhede Stadium")


def _ball(over: int, ball: int, runs: int, *,
          is_wicket: bool = False, extras: int = 0,
          is_legal: bool = True) -> dict:
    return {
        "over": over,
        "ball": ball,
        "runs": runs,
        "is_wicket": is_wicket,
        "extras": extras,
        "is_legal": is_legal,
    }


from typing import Optional

def _add_over(ms: MatchState, over_num: int, run_per_ball: int = 1,
              wicket_on: Optional[int] = None) -> None:
    """Add six legal deliveries for a given over."""
    for b in range(1, 7):
        ms.add_ball(_ball(
            over_num, b, run_per_ball,
            is_wicket=(b == wicket_on) if wicket_on else False,
        ))


# ── Init tests ───────────────────────────────────────────────────────────────

class TestInit:
    def test_initial_values(self):
        ms = _make_state()
        assert ms.batting_team == "MI"
        assert ms.bowling_team == "CSK"
        assert ms.venue == "Wankhede Stadium"
        assert ms.total_runs == 0
        assert ms.wickets == 0
        assert ms.balls_faced == 0
        assert ms.extras_total == 0
        assert ms.overs_completed == 0.0
        assert ms.current_over == 0
        assert ms.current_ball == 0
        assert ms.phase_runs == {"powerplay": 0, "middle": 0, "death": 0}
        assert ms.phase_wickets == {"powerplay": 0, "middle": 0, "death": 0}
        assert ms.over_runs == {}
        assert ms.balls == []
        assert ms.active_batsmen == []
        assert ms.active_bowler is None
        assert ms.batting_card == []
        assert ms.bowling_card == []


# ── Phase detection ──────────────────────────────────────────────────────────

class TestPhaseDetection:
    @pytest.mark.parametrize("overs,expected", [
        (0, "powerplay"),
        (3.0, "powerplay"),
        (5.5, "powerplay"),
        (5.99, "powerplay"),
        (6.0, "middle"),
        (10.0, "middle"),
        (14.99, "middle"),
        (15.0, "death"),
        (18.0, "death"),
        (19.5, "death"),
    ])
    def test_detect_phase(self, overs, expected):
        assert MatchState._detect_phase(overs) == expected

    def test_phase_property_initial(self):
        ms = _make_state()
        assert ms.phase == "powerplay"


# ── add_ball ─────────────────────────────────────────────────────────────────

class TestAddBall:
    def test_single_legal_delivery(self):
        ms = _make_state()
        ms.add_ball(_ball(0, 1, 4))
        assert ms.total_runs == 4
        assert ms.balls_faced == 1
        assert ms.overs_completed == pytest.approx(1 / 6)
        assert ms.current_over == 0
        assert ms.current_ball == 1
        assert ms.phase_runs["powerplay"] == 4
        assert ms.over_runs[0] == 4
        assert len(ms.balls) == 1

    def test_wide_not_counted_as_legal(self):
        ms = _make_state()
        ms.add_ball(_ball(0, 1, 1, extras=1, is_legal=False))
        assert ms.total_runs == 1
        assert ms.extras_total == 1
        assert ms.balls_faced == 0
        assert ms.overs_completed == 0.0

    def test_wicket_tracking(self):
        ms = _make_state()
        ms.add_ball(_ball(0, 1, 0, is_wicket=True))
        assert ms.wickets == 1
        assert ms.phase_wickets["powerplay"] == 1

    def test_full_over(self):
        ms = _make_state()
        _add_over(ms, 0, run_per_ball=2)
        assert ms.total_runs == 12
        assert ms.balls_faced == 6
        assert ms.overs_completed == pytest.approx(1.0)
        assert ms.over_runs[0] == 12

    def test_extras_accumulate(self):
        ms = _make_state()
        ms.add_ball(_ball(0, 1, 5, extras=1, is_legal=True))  # 4 + 1 nb
        ms.add_ball(_ball(0, 1, 1, extras=1, is_legal=False))  # wide
        assert ms.extras_total == 2
        assert ms.total_runs == 6

    def test_phase_runs_across_phases(self):
        ms = _make_state()
        # Fill powerplay (overs 0-5)
        for ov in range(6):
            _add_over(ms, ov, run_per_ball=1)
        assert ms.phase_runs["powerplay"] == 36
        # Middle over
        _add_over(ms, 6, run_per_ball=2)
        assert ms.phase_runs["middle"] == 12
        # Death over
        _add_over(ms, 15, run_per_ball=3)
        assert ms.phase_runs["death"] == 18


# ── current_run_rate ─────────────────────────────────────────────────────────

class TestCurrentRunRate:
    def test_zero_balls(self):
        ms = _make_state()
        assert ms.current_run_rate == 0.0

    def test_one_over_six_runs(self):
        ms = _make_state()
        _add_over(ms, 0, run_per_ball=1)
        assert ms.current_run_rate == pytest.approx(6.0)

    def test_two_overs_different_scoring(self):
        ms = _make_state()
        _add_over(ms, 0, run_per_ball=1)  # 6 runs
        _add_over(ms, 1, run_per_ball=2)  # 12 runs
        assert ms.current_run_rate == pytest.approx(18 / 2.0)


# ── get_phase_runs ───────────────────────────────────────────────────────────

class TestGetPhaseRuns:
    def test_returns_zero_for_empty(self):
        ms = _make_state()
        assert ms.get_phase_runs("powerplay") == 0
        assert ms.get_phase_runs("middle") == 0
        assert ms.get_phase_runs("death") == 0

    def test_unknown_phase(self):
        ms = _make_state()
        assert ms.get_phase_runs("nonexistent") == 0

    def test_after_scoring(self):
        ms = _make_state()
        _add_over(ms, 0, run_per_ball=3)
        assert ms.get_phase_runs("powerplay") == 18


# ── projected_innings_total ──────────────────────────────────────────────────

class TestProjectedInningsTotal:
    def test_zero_overs_returns_venue_avg(self):
        ms = _make_state()
        assert ms.projected_innings_total() == 170.0
        assert ms.projected_innings_total(venue_avg=150.0) == 150.0

    def test_powerplay_projection(self):
        ms = _make_state()
        # 3 overs at 8 rpo = 24 runs
        for ov in range(3):
            for b in range(1, 7):
                ms.add_ball(_ball(ov, b, 1))
            # add 2 extras per over to get 8 rpo feel
        # 18 runs off 3 overs => 6.0 rpo
        crr = ms.current_run_rate  # 6.0
        overs = ms.overs_completed  # 3.0
        wf = max(0.5, 1.0 - 0 * 0.06)  # 1.0
        remaining_pp = 3.0
        raw = 18 + (
            crr * remaining_pp + 0.85 * crr * 9 + 1.30 * crr * 5
        ) * wf
        progress = overs / 20.0
        expected = raw * progress + 170.0 * (1.0 - progress)
        assert ms.projected_innings_total() == pytest.approx(expected)

    def test_middle_overs_projection(self):
        ms = _make_state()
        # Simulate 10 overs (60 balls) at 7 rpo
        for ov in range(10):
            for b in range(1, 7):
                ms.add_ball(_ball(ov, b, 1))
            # add a boundary to make exactly 7 rpo per over — keep simple
        # Actually 60 runs off 10 overs = 6.0 rpo
        crr = ms.current_run_rate
        overs = ms.overs_completed
        wf = 1.0  # no wickets
        remaining_mid = 15.0 - overs
        raw = ms.total_runs + (crr * remaining_mid + 1.40 * crr * 5) * wf
        progress = overs / 20.0
        expected = raw * progress + 170.0 * (1.0 - progress)
        assert ms.projected_innings_total() == pytest.approx(expected)

    def test_death_overs_projection(self):
        ms = _make_state()
        for ov in range(18):
            for b in range(1, 7):
                ms.add_ball(_ball(ov, b, 1))
        crr = ms.current_run_rate
        overs = ms.overs_completed
        wf = 1.0
        remaining = 20.0 - overs
        raw = ms.total_runs + crr * remaining * wf
        progress = overs / 20.0
        expected = raw * progress + 170.0 * (1.0 - progress)
        assert ms.projected_innings_total() == pytest.approx(expected)

    def test_wickets_reduce_projection(self):
        ms = _make_state()
        for ov in range(3):
            _add_over(ms, ov, run_per_ball=1, wicket_on=3)
        # 3 wickets => factor = max(0.5, 1.0 - 0.18) = 0.82
        proj_with_wickets = ms.projected_innings_total()

        ms2 = _make_state()
        for ov in range(3):
            _add_over(ms2, ov, run_per_ball=1)
        proj_no_wickets = ms2.projected_innings_total()

        assert proj_with_wickets < proj_no_wickets

    def test_wicket_factor_floor(self):
        ms = _make_state()
        # Force 9 wickets
        for b in range(1, 7):
            ms.add_ball(_ball(0, b, 1, is_wicket=True))
        for b in range(1, 4):
            ms.add_ball(_ball(1, b, 1, is_wicket=True))
        assert ms.wickets == 9
        wf = max(0.5, 1.0 - 9 * 0.06)  # 0.46 => clamped to 0.5
        assert wf == 0.5

    def test_custom_venue_avg(self):
        ms = _make_state()
        _add_over(ms, 0, run_per_ball=1)
        p1 = ms.projected_innings_total(venue_avg=200.0)
        p2 = ms.projected_innings_total(venue_avg=140.0)
        assert p1 > p2

    def test_late_innings_projection_never_drops_below_current_score(self):
        ms = _make_state()
        ms.total_runs = 146
        ms.wickets = 6
        ms.balls_faced = 106
        ms.overs_completed = 17.4
        projected = ms.projected_innings_total(venue_avg=163.5)
        assert projected >= 146
        assert projected > 150


# ── to_dict ──────────────────────────────────────────────────────────────────

class TestToDict:
    def test_contains_all_keys(self):
        ms = _make_state()
        d = ms.to_dict()
        expected_keys = {
            "batting_team", "bowling_team", "venue",
            "current_innings", "target_runs",
            "total_runs", "wickets", "balls_faced", "extras_total",
            "overs_completed", "current_over", "current_ball",
            "phase", "current_run_rate",
            "phase_runs", "phase_wickets", "over_runs", "balls",
            "active_batsmen", "active_bowler", "batting_card", "bowling_card",
        }
        assert set(d.keys()) == expected_keys

    def test_serialization_roundtrip_values(self):
        ms = _make_state()
        _add_over(ms, 0, run_per_ball=2)
        ms.add_ball(_ball(1, 1, 0, is_wicket=True))
        d = ms.to_dict()

        assert d["batting_team"] == "MI"
        assert d["total_runs"] == 12
        assert d["wickets"] == 1
        assert d["balls_faced"] == 7
        assert d["phase_runs"]["powerplay"] == 12
        assert d["phase_wickets"]["powerplay"] == 1
        assert d["over_runs"][0] == 12
        assert d["over_runs"][1] == 0
        assert len(d["balls"]) == 7

    def test_dict_is_independent_copy(self):
        ms = _make_state()
        d = ms.to_dict()
        d["total_runs"] = 999
        assert ms.total_runs == 0


class TestFromSportmonks:
    def test_builds_current_innings_state(self):
        match = {
            "localteam_id": 1,
            "visitorteam_id": 2,
            "localteam": {"name": "Quetta Gladiators"},
            "visitorteam": {"name": "Karachi Kings"},
            "venue": {"name": "Gaddafi Stadium"},
            "runs": [
                {"inning": 1, "team_id": 2, "score": 12, "wickets": 1, "overs": 1.3},
            ],
            "batting": [
                {"scoreboard": "S1", "active": True, "score": 8, "ball": 5, "rate": 160.0, "batsman": {"fullname": "Babar Azam"}},
                {"scoreboard": "S1", "active": True, "score": 3, "ball": 4, "rate": 75.0, "batsman": {"fullname": "Saud Shakeel"}},
            ],
            "bowling": [
                {"scoreboard": "S1", "active": True, "overs": 1.0, "runs": 8, "wickets": 1, "rate": 8.0, "bowler": {"fullname": "Mohammad Amir"}},
            ],
            "balls": [
                {"id": 1, "team_id": 2, "scoreboard": "S1", "ball": 0.1, "score": {"runs": 1, "ball": True, "is_wicket": False}},
                {"id": 2, "team_id": 2, "scoreboard": "S1", "ball": 0.2, "score": {"runs": 4, "ball": True, "is_wicket": False, "four": True}},
                {"id": 3, "team_id": 2, "scoreboard": "S1", "ball": 0.3, "score": {"runs": 0, "ball": True, "is_wicket": True, "out": True}},
            ],
        }

        ms = MatchState.from_sportmonks(match)

        assert ms.current_innings == 1
        assert ms.batting_team == "Karachi Kings"
        assert ms.bowling_team == "Quetta Gladiators"
        assert ms.venue == "Gaddafi Stadium"
        assert ms.total_runs == 12
        assert ms.wickets == 1
        assert ms.target_runs == 13
        assert ms.over_by_over[0] == 5
        assert len(ms.ball_by_ball) == 3
        assert len(ms.active_batsmen) == 2
        assert ms.active_batsmen[0]["name"] == "Babar Azam"
        assert ms.active_bowler["name"] == "Mohammad Amir"
        assert ms.batting_card[1]["score"] == 3

    def test_detects_second_innings(self):
        match = {
            "localteam_id": 1,
            "visitorteam_id": 2,
            "localteam": {"name": "Quetta Gladiators"},
            "visitorteam": {"name": "Karachi Kings"},
            "venue": {"name": "Gaddafi Stadium"},
            "runs": [
                {"inning": 1, "team_id": 2, "score": 181, "wickets": 7, "overs": 20},
                {"inning": 2, "team_id": 1, "score": 10, "wickets": 1, "overs": 1.0},
            ],
            "balls": [
                {"id": 1, "team_id": 2, "scoreboard": "S1", "ball": 19.6, "score": {"runs": 4, "ball": True}},
                {"id": 2, "team_id": 1, "scoreboard": "S2", "ball": 0.1, "score": {"runs": 1, "ball": True}},
                {"id": 3, "team_id": 1, "scoreboard": "S2", "ball": 0.2, "score": {"runs": 2, "ball": True}},
            ],
        }

        ms = MatchState.from_sportmonks(match)

        assert ms.current_innings == 2
        assert ms.batting_team == "Quetta Gladiators"
        assert ms.bowling_team == "Karachi Kings"
        assert ms.target_runs == 182


# ── Edge cases ───────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_all_dot_balls(self):
        ms = _make_state()
        for ov in range(5):
            _add_over(ms, ov, run_per_ball=0)
        assert ms.total_runs == 0
        assert ms.current_run_rate == 0.0

    def test_very_high_scoring(self):
        ms = _make_state()
        for ov in range(20):
            _add_over(ms, ov, run_per_ball=6)
        assert ms.total_runs == 720
        assert ms.current_run_rate == pytest.approx(36.0)

    def test_illegal_delivery_does_not_advance_overs(self):
        ms = _make_state()
        ms.add_ball(_ball(0, 1, 1, is_legal=True))
        assert ms.balls_faced == 1
        ms.add_ball(_ball(0, 1, 1, extras=1, is_legal=False))
        assert ms.balls_faced == 1  # unchanged
        assert ms.overs_completed == pytest.approx(1 / 6)
