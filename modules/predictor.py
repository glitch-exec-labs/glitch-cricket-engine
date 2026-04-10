"""
IPL Prediction Models - statistical predictions for T20 cricket matches.

v2: Data-driven venue modifiers, empirical std_dev, non-linear wickets,
wider momentum, faster EMA.

Pure Python, no external dependencies beyond sqlite3.
"""

from __future__ import annotations

import math
import sqlite3
from typing import Any

from modules.match_state import MatchState
from modules.name_matcher import NameMatcher
from modules.player_context import PlayerContext
from modules.session_markets import SESSION_PREDICTION_KEYS, session_target_over
from modules.situational_model import SituationalPredictor
from modules.stats_db import StatsDB


# ── Venue modifier computation from actual data ─────────────────────
# Keyword → all venue strings in the DB that match → weighted average.
# Replaces the old hardcoded _VENUE_MODIFIERS dict.

_VENUE_KEYWORDS: list[tuple[str, list[str]]] = [
    ("wankhede", ["wankhede"]),
    ("chinnaswamy", ["chinnaswamy"]),
    ("chepauk", ["chepauk", "chidambaram"]),
    ("eden gardens", ["eden gardens"]),
    ("feroz shah kotla", ["feroz shah kotla"]),
    ("arun jaitley", ["arun jaitley"]),
    ("rajiv gandhi", ["rajiv gandhi"]),
    ("uppal", ["uppal"]),
    ("mohali", ["mohali", "is bindra"]),
    ("is bindra", ["mohali", "is bindra"]),
    ("sawai mansingh", ["sawai mansingh", "jaipur"]),
    ("jaipur", ["sawai mansingh", "jaipur"]),
    ("narendra modi", ["narendra modi"]),
    ("ahmedabad", ["narendra modi", "ahmedabad"]),
    ("dharamsala", ["dharamsala", "dharamshala"]),
    ("brabourne", ["brabourne"]),
    ("dy patil", ["dy patil"]),
    ("lucknow", ["lucknow", "ekana"]),
    ("ekana", ["lucknow", "ekana"]),
    ("guwahati", ["guwahati"]),
    ("hyderabad", ["hyderabad", "rajiv gandhi", "uppal"]),
    ("gaddafi", ["gaddafi", "lahore"]),
    ("lahore", ["gaddafi", "lahore"]),
    ("rawalpindi", ["rawalpindi"]),
    ("national stadium", ["national stadium", "karachi"]),
    ("karachi", ["national stadium", "karachi"]),
    ("multan", ["multan"]),
]

# Hardcoded fallback — only used when the stats DB is unavailable.
_VENUE_MODIFIERS_FALLBACK: dict[str, float] = {
    "wankhede": 2.0, "chinnaswamy": 5.5, "chepauk": -6.0,
    "eden gardens": -1.5, "feroz shah kotla": -4.0,
    "arun jaitley": 12.0, "rajiv gandhi": -2.0, "uppal": -2.0,
    "mohali": -1.5, "is bindra": -1.5, "sawai mansingh": -3.0,
    "jaipur": -3.0, "narendra modi": 19.0, "ahmedabad": 19.0,
    "dharamsala": -2.0, "brabourne": 2.0, "dy patil": 0.0,
    "lucknow": 8.0, "ekana": 8.0, "guwahati": 0.0,
    "hyderabad": -2.0, "gaddafi": 10.0, "lahore": 10.0,
    "rawalpindi": 11.0, "national stadium": 9.5, "karachi": 9.5,
    "multan": 0.0,
}


def _compute_venue_modifiers_from_db(db_path: str) -> tuple[dict[str, float], float]:
    """Query ipl_stats.db and return (keyword→modifier, league_avg).

    Each keyword aggregates all matching venue rows. The modifier is
    venue_avg - league_avg.  Returns (_VENUE_MODIFIERS_FALLBACK, 167.0)
    on any error.
    """
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT venue, AVG(first_innings_total) AS avg_total, COUNT(*) AS n "
            "FROM matches WHERE first_innings_total > 50 GROUP BY venue HAVING n >= 3"
        ).fetchall()
        league_row = conn.execute(
            "SELECT AVG(first_innings_total) FROM matches WHERE first_innings_total > 50"
        ).fetchone()
        conn.close()
    except Exception:
        return dict(_VENUE_MODIFIERS_FALLBACK), 167.0

    league_avg = round(league_row[0], 1) if league_row and league_row[0] else 167.0

    # Build venue_name → (avg, n) lookup
    venue_lookup: dict[str, tuple[float, int]] = {}
    for venue_name, avg_total, n in rows:
        venue_lookup[venue_name.lower()] = (avg_total, n)

    # For each keyword, aggregate all matching venue rows
    keyword_modifiers: dict[str, float] = {}
    seen_keywords: set[str] = set()
    for keyword, match_terms in _VENUE_KEYWORDS:
        if keyword in seen_keywords:
            continue
        total_weighted = 0.0
        total_n = 0
        for venue_name, (avg, n) in venue_lookup.items():
            if any(term in venue_name for term in match_terms):
                total_weighted += avg * n
                total_n += n
        if total_n > 0:
            venue_avg = total_weighted / total_n
            keyword_modifiers[keyword] = round(venue_avg - league_avg, 1)
        # Mark all aliases as seen
        for k2, terms2 in _VENUE_KEYWORDS:
            if any(t in match_terms for t in terms2):
                seen_keywords.add(k2)

    return keyword_modifiers, league_avg


# ── Non-linear wicket impact ─────────────────────────────────────────
# 0-2 wickets = top order intact, 4-5 = middle order exposed, 7+ = collapse
_WICKET_FACTORS: list[float] = [
    1.00,  # 0 wickets
    0.98,  # 1
    0.95,  # 2
    0.90,  # 3
    0.82,  # 4
    0.72,  # 5
    0.60,  # 6
    0.48,  # 7
    0.38,  # 8
    0.30,  # 9
    0.25,  # 10
]


def _wicket_factor(wickets: int) -> float:
    """Non-linear wicket impact factor."""
    return _WICKET_FACTORS[min(wickets, 10)]


# ── Empirical std_dev by over ─────────────────────────────────────────
# Computed from historical data: actual variance of remaining runs at each over.
_STD_DEV_CHECKPOINTS: list[tuple[float, float]] = [
    (0.0, 28.0),
    (6.0, 24.0),
    (10.0, 20.0),
    (15.0, 15.0),
    (18.0, 8.0),
    (19.0, 4.0),
    (20.0, 1.0),
]


def _std_dev_at_over(overs: float) -> float:
    """Interpolate empirical std_dev for a given over."""
    if overs <= _STD_DEV_CHECKPOINTS[0][0]:
        return _STD_DEV_CHECKPOINTS[0][1]
    if overs >= _STD_DEV_CHECKPOINTS[-1][0]:
        return _STD_DEV_CHECKPOINTS[-1][1]
    for i in range(len(_STD_DEV_CHECKPOINTS) - 1):
        o1, s1 = _STD_DEV_CHECKPOINTS[i]
        o2, s2 = _STD_DEV_CHECKPOINTS[i + 1]
        if o1 <= overs <= o2:
            frac = (overs - o1) / (o2 - o1)
            return s1 + frac * (s2 - s1)
    return 20.0


# ── Brain Layer: live resource-aware adjustments ──────────────────────

def _batting_resource_multiplier(innings_state: Any) -> float:
    """Scale future run projection based on remaining batting resources.

    Strong lineup still at the crease/in shed → project higher.
    Tail exposed → project lower.  Neutral = 1.0.
    """
    if innings_state is None:
        return 1.0
    rq = getattr(innings_state, "remaining_bat_quality", 0.5)
    depth = getattr(innings_state, "batting_depth", 2)

    # Quality-based: 0.5 is neutral, >0.7 is strong, <0.3 is weak
    quality_adj = 0.85 + 0.30 * rq  # range: 0.85 (quality=0) to 1.15 (quality=1.0)

    # Depth-based: >=4 deep batters = full boost, 1 or fewer = penalty
    if depth >= 4:
        depth_adj = 1.05
    elif depth >= 2:
        depth_adj = 1.0
    elif depth == 1:
        depth_adj = 0.90
    else:
        depth_adj = 0.75  # tail fully exposed

    return quality_adj * depth_adj


def _bowling_pressure_multiplier(innings_state: Any, phase: str) -> float:
    """Scale run projection based on remaining bowling resources.

    If death specialists are bowled out → batting side scores more.
    If strong bowlers have overs left → batting side scores less.
    """
    if innings_state is None:
        return 1.0

    dbq = getattr(innings_state, "death_bowling_quality", 0.5)
    available = getattr(innings_state, "bowlers_available", [])

    if not available:
        return 1.0

    # Average in-match economy of available bowlers (THIS match, not career)
    economies = [getattr(b, "economy", 8.5) for b in available if getattr(b, "overs_remaining", 0) > 0]
    if not economies:
        return 1.0
    avg_available_econ = sum(economies) / len(economies)

    # Base reference: league average economy ~8.5
    econ_ratio = avg_available_econ / 8.5  # >1 = leaking runs, <1 = restrictive

    if phase == "death":
        # Death phase is most affected by bowling quality
        # If death specialists are exhausted (low dbq) → runs go up
        death_factor = 1.0 + (1.0 - dbq) * 0.15  # range: 1.0 (dbq=1) to 1.15 (dbq=0)
        # Weight by actual in-match economy
        econ_factor = 0.7 + 0.3 * econ_ratio  # range: 0.7 (econ=0) to 1.3+ (econ=12+)
        return death_factor * econ_factor
    elif phase == "middle":
        # Middle overs: moderate bowling impact
        return 0.8 + 0.2 * econ_ratio  # range: 0.8 to 1.2+
    else:
        # Powerplay: bowling quality matters less (field restrictions dominate)
        return 0.9 + 0.1 * econ_ratio


def _active_batter_boost(innings_state: Any) -> float:
    """Boost projection based on currently batting players' in-match form.

    If batters at the crease are on fire (SR > 150 this innings), project higher.
    If they're struggling (SR < 100), project lower.
    """
    if innings_state is None:
        return 1.0
    active = getattr(innings_state, "batters_batting", [])
    if not active:
        return 1.0

    # Use in-match SR of active batters, weighted by balls faced
    total_balls = sum(getattr(b, "balls", 0) for b in active)
    if total_balls < 6:  # not enough data yet
        return 1.0

    weighted_sr = 0.0
    for b in active:
        balls = getattr(b, "balls", 0)
        sr = getattr(b, "strike_rate", 130.0) or 130.0
        if balls > 0:
            weighted_sr += sr * (balls / total_balls)

    # Normalize around 130 SR (league average)
    # SR 160 → 1.08, SR 130 → 1.0, SR 100 → 0.92
    return 0.7 + 0.3 * (weighted_sr / 130.0)


# Competition-specific base rates (tuned from backtest data)
_COMP_RATES: dict[str, dict[str, Any]] = {
    "ipl": {
        "pp_base_1st": 46.8,
        "pp_base_2nd": 44.5,
        "pp_rr": 7.8,
        "middle_rr": 8.0,
        "death_rr": 12.2,
        "toss_bat_adj": -1.0,
        "default_venue_avg": 167.2,  # v2: actual league average from 1483 matches
    },
    "psl": {
        "pp_base_1st": 49.3,
        "pp_base_2nd": 47.0,
        "pp_rr": 8.2,
        "middle_rr": 8.8,
        "death_rr": 8.8,
        "toss_bat_adj": -1.0,
        "default_venue_avg": 172.0,
    },
}


class IPLPredictor:
    """Statistical prediction models for T20 cricket."""

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        stats_db: StatsDB | None = None,
    ) -> None:
        self.config = config or {}
        self.stats_db = stats_db
        self.competition: str = "ipl"
        self.name_matcher = NameMatcher(stats_db) if stats_db is not None else None
        self.player_context = (
            PlayerContext(stats_db, self.name_matcher)
            if stats_db is not None and self.name_matcher is not None
            else None
        )
        self.situational: SituationalPredictor | None = None
        if stats_db is not None:
            try:
                self.situational = SituationalPredictor(stats_db)
            except Exception:
                self.situational = None

        # v2.1: Scenario model (probability tree) and chase state machine
        self.scenario_model = None
        try:
            from modules.scenario_model import ScenarioModel
            self.scenario_model = ScenarioModel()
            if self.scenario_model.available:
                import logging as _l
                _l.getLogger("ipl_spotter.predictor").info("ScenarioModel loaded (%d run buckets)", len(self.scenario_model._runs))
        except Exception:
            self.scenario_model = None

        self.chase_state_machine = None
        try:
            from modules.chase_state import ChaseStateMachine
            self.chase_state_machine = ChaseStateMachine()
        except Exception:
            pass

        # v2: Load data-driven venue modifiers from ipl_stats.db
        stats_db_path = getattr(stats_db, "db_path", None) if stats_db else None
        if stats_db_path:
            self._venue_modifiers, self._league_avg = _compute_venue_modifiers_from_db(str(stats_db_path))
        else:
            self._venue_modifiers = dict(_VENUE_MODIFIERS_FALLBACK)
            self._league_avg = 167.0

        # Prediction smoothing: EMA blends new predictions with previous
        # to prevent oscillation from single-ball events.
        # v2: alpha increased from 0.35 to 0.55 for faster response
        self._prev_predictions: dict[str, float] = {}
        self._smoothing_alpha: float = float(self.config.get("prediction_smoothing_alpha", 0.55))

    def _smooth_prediction(self, key: str, new_value: float) -> float:
        """Apply exponential moving average smoothing to a prediction.

        Prevents single-ball events (boundary, wicket) from causing
        30+ run swings in projected totals.
        """
        prev = self._prev_predictions.get(key)
        if prev is None:
            # First prediction — no history to blend with
            self._prev_predictions[key] = new_value
            return new_value

        smoothed = self._smoothing_alpha * new_value + (1 - self._smoothing_alpha) * prev
        self._prev_predictions[key] = smoothed
        return smoothed

    def get_venue_modifier(self, venue: str) -> float:
        """Return the runs-above/below average for a known venue.

        v2: Uses data-driven modifiers computed from ipl_stats.db at init time.
        """
        venue_lower = venue.lower()
        for key, modifier in self._venue_modifiers.items():
            if key in venue_lower:
                return modifier
        return 0.0

    @staticmethod
    def _confidence_from_std(std_dev: float, expected: float) -> str:
        """Return HIGH / MEDIUM / LOW based on coefficient of variation."""
        if expected <= 0:
            return "LOW"
        cv = std_dev / expected
        if cv < 0.15:
            return "HIGH"
        if cv < 0.30:
            return "MEDIUM"
        return "LOW"

    def set_competition(self, competition: str) -> None:
        """Set the active competition (ipl/psl) for correct base rates."""
        self.competition = competition.lower()

    def _rates(self) -> dict[str, Any]:
        """Return the phase rates for the active competition."""
        return _COMP_RATES.get(self.competition, _COMP_RATES["ipl"])

    def predict_powerplay_total(
        self,
        batting_team: str,
        bowling_team: str,
        venue: str,
        venue_avg_pp: float | None = None,
        toss_decision: str = "bat",
        innings: int = 1,
    ) -> dict[str, Any]:
        """Predict the powerplay (overs 1-6) total for a given innings."""
        rates = self._rates()
        if venue_avg_pp is not None:
            base = venue_avg_pp
        else:
            base = rates["pp_base_1st"] if innings == 1 else rates["pp_base_2nd"]

        if innings == 1 and toss_decision == "bat":
            base += rates["toss_bat_adj"]

        venue_mod = self.get_venue_modifier(venue)
        base += venue_mod * (6.0 / 20.0)

        std_dev = 12.0
        confidence = self._confidence_from_std(std_dev, base)

        return {
            "expected": round(base, 1),
            "std_dev": std_dev,
            "confidence": confidence,
            "range_low": round(base - std_dev, 1),
            "range_high": round(base + std_dev, 1),
        }

    def predict_phase_runs(
        self,
        match_state: MatchState,
        phase: str,
        venue_avg: float | None = None,
        innings_state: Any = None,
    ) -> dict[str, Any]:
        """Predict runs for a remaining phase (middle or death).

        v2: Now factors in remaining batting quality, bowling resources,
        in-match economy, and active batter form via innings_state.
        """
        rates = self._rates()
        if phase == "middle":
            base_rr = rates["middle_rr"]
            phase_overs = 9.0
        elif phase == "death":
            base_rr = rates["death_rr"]
            phase_overs = 5.0
        else:  # powerplay
            base_rr = rates["pp_rr"]
            phase_overs = 6.0

        if venue_avg is not None:
            base_rr = venue_avg / phase_overs

        wf = _wicket_factor(match_state.wickets)
        crr = match_state.current_run_rate
        if crr > 0 and base_rr > 0:
            ratio = crr / base_rr
            momentum = 0.7 + 0.3 * ratio
            momentum = max(0.5, min(2.0, momentum))  # v2: wider range
        else:
            momentum = 1.0

        # v2 brain layer: adjust for live match resources
        bat_mult = _batting_resource_multiplier(innings_state)
        bowl_mult = _bowling_pressure_multiplier(innings_state, phase)
        batter_boost = _active_batter_boost(innings_state)

        expected = base_rr * phase_overs * wf * momentum * bat_mult * bowl_mult * batter_boost
        std_dev = expected * 0.18
        confidence = self._confidence_from_std(std_dev, expected)

        return {
            "expected": round(expected, 1),
            "std_dev": round(std_dev, 1),
            "confidence": confidence,
            "range_low": round(expected - std_dev, 1),
            "range_high": round(expected + std_dev, 1),
            "bat_resource_mult": round(bat_mult, 3),
            "bowl_pressure_mult": round(bowl_mult, 3),
            "batter_form_mult": round(batter_boost, 3),
        }

    def predict_innings_total(
        self,
        match_state: MatchState,
        venue_avg: float = 172.0,
        innings_state: Any = None,
    ) -> dict[str, Any]:
        """Predict the final innings total based on current match state.

        v2: Applies brain layer adjustments for batting/bowling resources,
        in-match bowler economy, and active batter form on top of the
        base projection.
        """
        expected = match_state.projected_innings_total(venue_avg)

        # v2 brain layer: adjust based on live resources (only after enough overs)
        if innings_state is not None and match_state.overs_completed >= 4.0:
            bat_mult = _batting_resource_multiplier(innings_state)
            bowl_mult = _bowling_pressure_multiplier(innings_state, match_state.phase or "middle")
            batter_boost = _active_batter_boost(innings_state)
            combined = bat_mult * bowl_mult * batter_boost

            import logging as _log
            _brain_logger = _log.getLogger("ipl_spotter.brain")
            if abs(combined - 1.0) > 0.03:  # only log when brain layer actually shifts prediction
                _brain_logger.info(
                    "BRAIN: %.1f ov %d/%d | bat=%.2f bowl=%.2f form=%.2f → combined=%.2fx | depth=%d dbq=%.2f",
                    match_state.overs_completed, match_state.total_runs, match_state.wickets,
                    bat_mult, bowl_mult, batter_boost, combined,
                    getattr(innings_state, "batting_depth", 0),
                    getattr(innings_state, "death_bowling_quality", 0),
                )

            # Apply to the projected FUTURE runs, not the already-scored runs
            scored = float(match_state.total_runs)
            projected_future = max(0.0, expected - scored)
            adjusted_future = projected_future * bat_mult * bowl_mult * batter_boost
            expected = scored + adjusted_future

        std_dev = _std_dev_at_over(match_state.overs_completed)
        confidence = self._confidence_from_std(std_dev, expected)

        result: dict[str, Any] = {
            "expected": round(expected, 1),
            "std_dev": round(std_dev, 1),
            "confidence": confidence,
            "range_low": round(expected - std_dev, 1),
            "range_high": round(expected + std_dev, 1),
        }

        # Blend with situational model when available
        if self.situational is not None and match_state.overs_completed >= 1.0:
            sit = self.situational.predict_innings_total(match_state, venue_avg=venue_avg)
            result["situational_expected"] = sit["expected"]
            result["situational_confidence"] = sit["confidence"]
            if sit["sample_count"] >= 10:
                blended = 0.6 * sit["expected"] + 0.4 * result["expected"]
                result["expected"] = round(blended, 1)
                result["range_low"] = round(blended - std_dev, 1)
                result["range_high"] = round(blended + std_dev, 1)

        # v2.1: Blend with scenario model (probability tree)
        if self.scenario_model is not None and self.scenario_model.available and match_state.overs_completed >= 2.0:
            try:
                scenario = self.scenario_model.project_innings_total(match_state, innings_state)
                if scenario and scenario.get("tree_depth", 0) >= 2:
                    sc_exp = scenario["expected"]
                    sc_std = scenario["std_dev"]
                    blend_w = 0.5 if scenario["confidence"] == "HIGH" else 0.4
                    current_exp = result["expected"]
                    result["expected"] = round(blend_w * sc_exp + (1.0 - blend_w) * current_exp, 1)
                    result["std_dev"] = round(blend_w * sc_std + (1.0 - blend_w) * result["std_dev"], 1)
                    result["scenario_expected"] = round(sc_exp, 1)
                    result["scenario_std"] = round(sc_std, 1)
            except Exception:
                pass

        return result

    def predict_next_over_runs(
        self,
        match_state: MatchState,
        bowler_economy: float | None = None,
        batsman_sr: float | None = None,
    ) -> dict[str, Any]:
        """Predict runs in the next over."""
        phase = match_state.phase
        if phase == "powerplay":
            base = 8.5
        elif phase == "middle":
            base = 8.0
        else:
            base = 11.5

        expected = base

        if bowler_economy is not None and bowler_economy > 0:
            expected = 0.5 * expected + 0.5 * bowler_economy

        if batsman_sr is not None and batsman_sr > 0:
            batsman_rpo = batsman_sr * 6.0 / 100.0
            expected = 0.7 * expected + 0.3 * batsman_rpo

        std_dev = 3.5
        confidence = self._confidence_from_std(std_dev, expected)

        return {
            "expected": round(expected, 1),
            "std_dev": std_dev,
            "confidence": confidence,
            "range_low": round(max(0, expected - std_dev), 1),
            "range_high": round(expected + std_dev, 1),
        }

    def chase_win_probability(
        self,
        target: int,
        current_score: int,
        overs_completed: float,
        wickets_lost: int,
        innings_state: Any = None,
    ) -> float:
        """Estimate win probability for the chasing team."""
        remaining_overs = max(0.0, 20.0 - overs_completed)
        remaining_wickets = max(0, 10 - wickets_lost)
        runs_needed = target - current_score

        if runs_needed <= 0:
            return 1.0
        if remaining_overs <= 0 or remaining_wickets <= 0:
            return 0.0

        overs_resource = remaining_overs / 20.0
        wickets_resource = remaining_wickets / 10.0
        resource_pct = overs_resource * 0.5 + wickets_resource * 0.5

        # Quality-weighted resource adjustment
        if innings_state is not None:
            # Strong remaining batters → effective resources are higher
            quality_mult = 0.7 + 0.6 * innings_state.remaining_bat_quality  # range: 0.7 - 1.3
            resource_pct *= quality_mult

            # Death bowling quality of opponent affects scoring potential
            if innings_state.death_bowling_quality > 0.7 and remaining_overs <= 5:
                resource_pct *= 0.9  # strong death bowling reduces chase resources

        expected_remaining = 172.0 * resource_pct
        required_rr = runs_needed / remaining_overs if remaining_overs > 0 else 999.0
        expected_rr = expected_remaining / remaining_overs if remaining_overs > 0 else 0.0

        # v2.1: State-dependent sigmoid steepness from chase state machine
        k = 0.5
        if self.chase_state_machine is not None:
            try:
                _fake = type("_S", (), {
                    "current_innings": 2, "target_runs": target,
                    "total_runs": current_score, "overs_completed": overs_completed,
                    "wickets": wickets_lost,
                })()
                chase_info = self.chase_state_machine.classify(_fake, innings_state)
                k = chase_info.get("k_factor", 0.5)
            except Exception:
                pass

        diff = required_rr - expected_rr
        prob = 1.0 / (1.0 + math.exp(k * diff))

        return round(max(0.0, min(1.0, prob)), 3)

    def predict_total_at_over(
        self,
        match_state: MatchState,
        target_over: float,
        venue_avg: float = 172.0,
        innings_state: Any = None,
    ) -> dict[str, Any]:
        """Project the batting side's total at a specific over milestone."""
        current_over = match_state.overs_completed
        current_score = float(match_state.total_runs)

        if target_over <= current_over:
            milestone_score = match_state.score_at_end_of_over(int(target_over))
            expected = float(milestone_score if milestone_score is not None else current_score)
            std_dev = 4.0
        else:
            remaining_runs = self._expected_runs_between(match_state, current_over, target_over, venue_avg, innings_state=innings_state)
            expected = current_score + remaining_runs
            remaining_overs = max(0.0, target_over - current_over)
            std_dev = max(4.0, remaining_overs * 2.5)

            # v2.1: Blend with scenario model for session markets
            if self.scenario_model is not None and self.scenario_model.available and remaining_overs >= 2:
                try:
                    sc = self.scenario_model.project_to_over(match_state, target_over, innings_state)
                    if sc and sc.get("tree_depth", 0) >= 2:
                        blend_w = 0.6 if sc["confidence"] == "HIGH" else 0.5
                        expected = blend_w * sc["expected"] + (1.0 - blend_w) * expected
                        std_dev = blend_w * sc["std_dev"] + (1.0 - blend_w) * std_dev
                except Exception:
                    pass

        confidence = self._confidence_from_std(std_dev, expected)
        return {
            "expected": round(expected, 1),
            "std_dev": round(std_dev, 1),
            "confidence": confidence,
            "range_low": round(max(current_score, expected - std_dev), 1),
            "range_high": round(expected + std_dev, 1),
        }

    def predict_match_winner(
        self,
        match_state: MatchState,
        home: str,
        away: str,
        venue_avg: float = 172.0,
        innings_state: Any = None,
    ) -> dict[str, Any]:
        """Estimate current home/away win probability from live state."""
        home_prob = 0.5

        if match_state.current_innings >= 2 and match_state.target_runs:
            chase_prob = self.chase_win_probability(
                target=match_state.target_runs,
                current_score=match_state.total_runs,
                overs_completed=match_state.overs_completed,
                wickets_lost=match_state.wickets,
                innings_state=innings_state,
            )
            if match_state.batting_team == home:
                home_prob = chase_prob
            else:
                home_prob = 1.0 - chase_prob
        else:
            projected = self.predict_innings_total(match_state, venue_avg=venue_avg, innings_state=innings_state)["expected"]
            baseline = venue_avg + self.get_venue_modifier(match_state.venue)
            diff = projected - baseline
            home_prob = 1.0 / (1.0 + math.exp(-diff / 12.0))
            if match_state.batting_team != home:
                home_prob = 1.0 - home_prob

        home_prob = max(0.02, min(0.98, home_prob))
        away_prob = 1.0 - home_prob
        std_dev = 0.12 if match_state.current_innings == 1 else 0.08

        return {
            "home_prob": round(home_prob, 3),
            "away_prob": round(away_prob, 3),
            "confidence": self._confidence_from_std(std_dev, max(home_prob, away_prob)),
            "std_dev": round(std_dev, 3),
        }

    def predict(
        self,
        match_state: MatchState,
        home: str | None = None,
        away: str | None = None,
        venue_avg: float = 172.0,
        venue_avg_pp: float = 49.5,
        innings_state: Any = None,
    ) -> dict[str, Any]:
        """Return the full live prediction set used by the spotter and copilot."""
        predictions: dict[str, Any] = {}

        predictions["powerplay_total"] = self.predict_total_at_over(
            match_state, target_over=6.0, venue_avg=venue_avg, innings_state=innings_state
        )
        predictions["ten_over_total"] = self.predict_total_at_over(
            match_state, target_over=10.0, venue_avg=venue_avg, innings_state=innings_state
        )
        predictions["fifteen_over_total"] = self.predict_total_at_over(
            match_state, target_over=15.0, venue_avg=venue_avg, innings_state=innings_state
        )
        predictions["innings_total"] = self.predict_innings_total(
            match_state, venue_avg=venue_avg, innings_state=innings_state
        )
        predictions["next_over"] = self.predict_next_over_runs(match_state)
        predictions["powerplay_runs"] = predictions["powerplay_total"]

        if match_state.phase != "powerplay":
            predictions["phase_middle"] = self.predict_phase_runs(match_state, phase="middle", innings_state=innings_state)
        if match_state.phase == "death":
            predictions["phase_death"] = self.predict_phase_runs(match_state, phase="death", innings_state=innings_state)

        home_team = home or match_state.batting_team
        away_team = away or match_state.bowling_team
        predictions["match_winner"] = self.predict_match_winner(
            match_state, home=home_team, away=away_team, venue_avg=venue_avg, innings_state=innings_state
        )

        if self.player_context and match_state.active_batsmen and match_state.active_bowler:
            player_adjustment = self.player_context.get_combined_adjustment(
                match_state.active_batsmen,
                match_state.active_bowler,
                venue=match_state.venue,
                overs_completed=match_state.overs_completed,
            )
            over_adjustment = player_adjustment.get("over_adjustment", 0.0)
            if "next_over" in predictions:
                base_expected = predictions["next_over"]["expected"]
                predictions["next_over"]["base_expected"] = base_expected
                predictions["next_over"]["expected"] = round(max(0.0, base_expected + over_adjustment), 1)
                predictions["next_over"]["player_adj"] = round(over_adjustment, 2)

            session_targets = {
                "powerplay_total": 6.0,
                "ten_over_total": 10.0,
                "fifteen_over_total": 15.0,
                "innings_total": 20.0,
            }
            for key, target_over in session_targets.items():
                if key not in predictions:
                    continue
                future_overs = max(0.0, target_over - match_state.overs_completed)
                session_adj = round(over_adjustment * future_overs, 2)
                base_expected = predictions[key]["expected"]
                predictions[key]["base_expected"] = base_expected
                adjusted_expected = base_expected + session_adj
                if key == "innings_total":
                    adjusted_expected = max(float(match_state.total_runs), adjusted_expected)
                predictions[key]["expected"] = round(adjusted_expected, 1)
                predictions[key]["player_adj"] = session_adj

            predictions["player_context"] = player_adjustment

        # ── Apply EMA smoothing to prevent oscillation ────────────────
        # Use match context as key prefix to avoid cross-match blending
        _smooth_key_prefix = f"{match_state.batting_team}:{match_state.current_innings}"
        _smooth_markets = ["innings_total", "powerplay_total", "ten_over_total", "fifteen_over_total"]
        for mkt in _smooth_markets:
            entry = predictions.get(mkt)
            if entry and isinstance(entry, dict) and "expected" in entry:
                raw = entry["expected"]
                key = f"{_smooth_key_prefix}:{mkt}"
                entry["expected"] = round(self._smooth_prediction(key, raw), 1)
                entry["raw_expected"] = raw  # keep raw for debugging

        return predictions

    def get_prediction_for_market(self, predictions: dict[str, Any], market_key: str) -> dict[str, Any] | None:
        """Return the prediction payload corresponding to an internal market key."""
        prediction_key = SESSION_PREDICTION_KEYS.get(market_key, market_key)
        return predictions.get(prediction_key)

    def is_completed_session_market(self, match_state: MatchState, market_key: str) -> bool:
        """Return True when a session market has already been completed in this innings."""
        target_over = session_target_over(market_key)
        return target_over is not None and match_state.overs_completed > target_over

    def _expected_runs_between(
        self,
        match_state: MatchState,
        start_over: float,
        end_over: float,
        venue_avg: float,
        innings_state: Any = None,
    ) -> float:
        crr = match_state.current_run_rate
        venue_rr = venue_avg / 20.0 if venue_avg > 0 else 8.6
        base_rr = crr if crr > 0 else venue_rr
        wicket_factor = _wicket_factor(match_state.wickets)  # v2: non-linear

        # v2 brain layer: resource-aware adjustments
        bat_mult = _batting_resource_multiplier(innings_state)
        batter_boost = _active_batter_boost(innings_state)

        future_runs = 0.0
        phase_slices = (
            ("powerplay", 0.0, 6.0, 8.25),
            ("middle", 6.0, 15.0, 8.2),
            ("death", 15.0, 20.0, 11.5),
        )
        for phase_name, phase_start, phase_end, phase_rr in phase_slices:
            overlap_start = max(start_over, phase_start)
            overlap_end = min(end_over, phase_end)
            if overlap_end <= overlap_start:
                continue
            overs = overlap_end - overlap_start
            rr = 0.65 * base_rr + 0.35 * phase_rr
            # v2: scale each phase by bowling pressure
            bowl_mult = _bowling_pressure_multiplier(innings_state, phase_name)
            future_runs += overs * rr * bowl_mult

        return future_runs * wicket_factor * bat_mult * batter_boost


class Predictor(IPLPredictor):
    """Backward-compatible wrapper used by the live debug scripts."""

    pass
