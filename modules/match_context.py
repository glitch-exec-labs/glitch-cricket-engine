"""Match context filter — reads the match before betting.

Tracks live performance ball-by-ball:
  - Current bowler: spell economy, is he leaking or tight?
  - Current batsmen: form in this innings, SR, boundaries
  - Recent overs: scoring trend (accelerating/decelerating)
  - Wicket pressure: recent collapses
  - Partnership: is a big partnership building or did one just break?

Uses all this to CONFIRM or VETO a mathematical edge.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from modules.match_state import MatchState

logger = logging.getLogger("ipl_spotter.match_context")


class LiveTracker:
    """Tracks live bowler/batsman performance from ball-by-ball data."""

    def __init__(self) -> None:
        # Per-match tracking: match_id -> tracking data
        self._match_data: Dict[int, Dict[str, Any]] = {}

    def update(self, match_id: int, state: MatchState) -> Dict[str, Any]:
        """Update tracking from current match state. Returns live analysis."""
        data = self._match_data.setdefault(match_id, {
            "last_ball_count": 0,
            "bowler_spells": {},      # bowler_name -> {runs, balls, wickets, last_over_runs}
            "batsman_form": {},       # batsman_name -> {runs, balls, fours, sixes, dots}
            "over_history": [],       # list of (over_num, runs, wickets)
            "partnership_runs": 0,
            "partnership_balls": 0,
            "last_wicket_ball": -1,
        })

        # Process new balls since last update
        new_balls = state.balls[data["last_ball_count"]:]
        for ball in new_balls:
            self._process_ball(data, ball)
        data["last_ball_count"] = len(state.balls)

        # Also use Sportmonks batting/bowling cards for more accurate data
        self._sync_from_cards(data, state)

        return self._analyze(data, state)

    def _process_ball(self, data: Dict, ball: Dict) -> None:
        """Process a single ball event."""
        runs = ball.get("runs", 0)
        is_wicket = ball.get("is_wicket", False)

        # Track partnership
        if is_wicket:
            data["partnership_runs"] = 0
            data["partnership_balls"] = 0
            data["last_wicket_ball"] = data["last_ball_count"] + 1
        else:
            data["partnership_runs"] += runs
            data["partnership_balls"] += 1

    def _sync_from_cards(self, data: Dict, state: MatchState) -> None:
        """Sync batsman/bowler form from Sportmonks cards (more accurate than ball tracking)."""
        # Current bowler performance
        if state.active_bowler:
            name = state.active_bowler.get("name", "")
            if name:
                data["bowler_spells"][name] = {
                    "overs": state.active_bowler.get("overs", 0),
                    "runs": state.active_bowler.get("runs", 0),
                    "wickets": state.active_bowler.get("wickets", 0),
                    "econ": state.active_bowler.get("econ", state.active_bowler.get("rate", 0)),
                }

        # Current batsmen form
        for bat in state.active_batsmen:
            name = bat.get("name", "")
            if name:
                data["batsman_form"][name] = {
                    "runs": bat.get("score", 0),
                    "balls": bat.get("balls", 0),
                    "sr": bat.get("sr", bat.get("rate", 0)),
                }

        # Over history from state
        data["over_history"] = [
            (ov, runs) for ov, runs in sorted(state.over_runs.items())
        ]

    def _analyze(self, data: Dict, state: MatchState) -> Dict[str, Any]:
        """Produce a live analysis summary."""
        analysis: Dict[str, Any] = {
            "bowler_pressure": "neutral",     # tight / neutral / leaking
            "bowler_name": "",
            "bowler_econ": 0.0,
            "batting_momentum": "neutral",    # accelerating / neutral / slowing
            "batsman_hot": "",                # name of batsman on fire
            "batsman_cold": "",               # name of batsman struggling
            "partnership_status": "building", # building / fresh / broken
            "recent_trend": "neutral",        # hot / neutral / cold
            "wicket_pressure": False,
        }

        # Bowler analysis — who is bowling and how
        if state.active_bowler:
            name = state.active_bowler.get("name", "")
            econ = state.active_bowler.get("econ", state.active_bowler.get("rate", 0)) or 0
            overs = state.active_bowler.get("overs", 0) or 0
            analysis["bowler_name"] = name
            analysis["bowler_econ"] = econ
            if overs >= 1:
                if econ >= 12:
                    analysis["bowler_pressure"] = "leaking"
                elif econ <= 6:
                    analysis["bowler_pressure"] = "tight"

        # Batting momentum — compare last 3 overs vs first half
        if len(data["over_history"]) >= 5:
            last_3 = sum(r for _, r in data["over_history"][-3:])
            first_half = sum(r for _, r in data["over_history"][:-3])
            first_half_overs = max(1, len(data["over_history"]) - 3)
            last_3_rr = last_3 / 3.0
            first_rr = first_half / first_half_overs

            if first_rr > 0:
                ratio = last_3_rr / first_rr
                if ratio >= 1.3:
                    analysis["batting_momentum"] = "accelerating"
                elif ratio <= 0.7:
                    analysis["batting_momentum"] = "slowing"

        # Recent trend — last 2 overs
        if len(data["over_history"]) >= 2:
            last_2 = sum(r for _, r in data["over_history"][-2:])
            if last_2 >= 22:
                analysis["recent_trend"] = "hot"
            elif last_2 <= 8:
                analysis["recent_trend"] = "cold"

        # Partnership
        if data["partnership_balls"] >= 18 and data["partnership_runs"] >= 25:
            analysis["partnership_status"] = "building"
        elif data["partnership_balls"] < 6:
            analysis["partnership_status"] = "fresh"

        # Wicket pressure — 2+ wickets in last 18 balls
        recent_balls = state.balls[-18:] if state.balls else []
        recent_wkts = sum(1 for b in recent_balls if b.get("is_wicket"))
        if recent_wkts >= 2:
            analysis["wicket_pressure"] = True

        # Batsman form — who is firing, who is struggling
        for bat in state.active_batsmen:
            name = bat.get("name", "")
            sr = bat.get("sr", bat.get("rate", 0)) or 0
            balls = bat.get("balls", 0) or 0
            runs = bat.get("score", 0) or 0
            if balls >= 10 and sr >= 160:
                analysis["batsman_hot"] = f"{name} ({runs} off {balls}, SR {sr:.0f})"
                analysis["batting_momentum"] = "accelerating"
            elif balls >= 10 and sr <= 90:
                analysis["batsman_cold"] = f"{name} ({runs} off {balls}, SR {sr:.0f})"
                if not analysis["batsman_hot"]:  # only slow if no one is hot
                    analysis["batting_momentum"] = "slowing"

        return analysis


class MatchContext:
    """Evaluates whether current match situation supports a bet."""

    def __init__(self) -> None:
        self.tracker = LiveTracker()

    def should_bet(
        self,
        edge: Dict[str, Any],
        state: MatchState,
        match_id: int = 0,
        open_bets: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[bool, str]:
        """Return (True, reason) if bet is OK, (False, reason) if we should skip."""
        market = edge.get("market", "")
        direction = edge.get("direction", "")

        # === Cross-market consistency ===
        # Don't contradict existing bets on the same match
        if open_bets:
            contradiction = self._check_contradictions(market, direction, open_bets, state)
            if contradiction:
                return False, contradiction

        # Get live analysis
        analysis = self.tracker.update(match_id, state)

        bowler = analysis["bowler_pressure"]
        bowler_name = analysis.get("bowler_name", "")
        bowler_econ = analysis.get("bowler_econ", 0)
        momentum = analysis["batting_momentum"]
        batsman_hot = analysis.get("batsman_hot", "")
        batsman_cold = analysis.get("batsman_cold", "")
        trend = analysis["recent_trend"]
        partnership = analysis["partnership_status"]
        wicket_pressure = analysis["wicket_pressure"]

        # === OVER bets ===
        if direction == "OVER":
            # Don't bet OVER when a batsman is struggling and bowler is tight
            if batsman_cold and bowler == "tight":
                return False, f"{batsman_cold} struggling + {bowler_name} bowling tight (econ {bowler_econ:.1f}) — skip OVER"

            # Don't bet OVER when wickets are falling
            if wicket_pressure:
                return False, f"Wicket pressure — 2+ wickets recently, skip OVER"

            # Don't bet OVER when scoring is slowing
            if momentum == "slowing" and trend == "cold":
                return False, f"Scoring slowing + cold last 2 overs — skip OVER"

            # Don't bet OVER when bowler is tight and new batsman at crease
            if bowler == "tight" and partnership == "fresh":
                return False, f"Tight bowler + new batsman — skip OVER"

            # Don't bet OVER with 5+ wickets down in first innings
            if state.current_innings == 1 and state.wickets >= 5:
                return False, f"5+ wickets down (1st inn) — likely collapse, skip OVER"

            # New batsman settling — wait
            if self._new_batsman_at_crease(state):
                return False, "New batsman at crease (< 6 balls) — wait"

        # === UNDER bets ===
        if direction == "UNDER":
            # Don't bet UNDER when a batsman is on fire
            if batsman_hot:
                return False, f"{batsman_hot} on fire — skip UNDER"

            # Don't bet UNDER when bowler is leaking
            if bowler == "leaking":
                return False, f"{bowler_name} leaking (econ {bowler_econ:.1f}) — skip UNDER"

            # Don't bet UNDER when batting is on fire
            if momentum == "accelerating" and trend == "hot":
                return False, f"Batting accelerating + hot last 2 overs — skip UNDER"

            # Don't bet UNDER when big partnership building
            if partnership == "building" and bowler == "leaking":
                return False, f"Big partnership + bowler leaking — skip UNDER"

        # === Death overs (17+) — too volatile for session bets ===
        if state.overs_completed >= 17.0 and market not in ("over_runs", "match_winner"):
            return False, "Death overs (17+) — too volatile for session bets"

        # === Chase feasibility ===
        if state.current_innings == 2 and state.target_runs and direction == "OVER":
            chase_msg = self._check_chase(state)
            if chase_msg:
                return False, chase_msg

        if (
            state.current_innings == 2
            and state.target_runs
            and direction == "UNDER"
            and market in ("powerplay_runs", "6_over", "10_over", "15_over", "20_over", "innings_total")
        ):
            chase_under_msg = self._check_chase_under(state, market)
            if chase_under_msg:
                return False, chase_under_msg

        # === Build confirmation reason with player names ===
        reasons = []
        if direction == "OVER":
            if batsman_hot:
                reasons.append(batsman_hot)
            elif momentum == "accelerating":
                reasons.append("batting accelerating")
            if bowler == "leaking":
                reasons.append(f"{bowler_name} leaking (econ {bowler_econ:.1f})")
            elif bowler == "neutral" and bowler_name:
                pass  # don't mention neutral bowler
            if partnership == "building":
                reasons.append("partnership building")
        elif direction == "UNDER":
            if wicket_pressure:
                reasons.append("wickets falling")
            if batsman_cold:
                reasons.append(batsman_cold)
            elif momentum == "slowing":
                reasons.append("scoring slowing")
            if bowler == "tight":
                reasons.append(f"{bowler_name} tight (econ {bowler_econ:.1f})")

        reason = ", ".join(reasons) if reasons else "edge confirmed by model"
        return True, reason

    def _check_contradictions(
        self,
        market: str,
        direction: str,
        open_bets: List[Dict[str, Any]],
        state: MatchState,
    ) -> Optional[str]:
        """Check if this bet contradicts existing open bets on the same match.

        Rules:
          - If you backed a team to WIN → don't bet UNDER on their batting sessions
          - If you bet UNDER on innings → don't back the batting team to WIN
          - If you bet OVER on 6-over → don't bet UNDER on 10-over (contradictory trend)
          - If you bet UNDER on 6-over → don't bet OVER on 10-over
        """
        # Classify existing bets
        has_mw_back = False       # backed batting team to win
        has_session_over = False   # bet OVER on any session
        has_session_under = False  # bet UNDER on any session
        has_innings_under = False
        has_innings_over = False

        for bet in open_bets:
            bet_market = bet.get("market", "")
            bet_dir = bet.get("direction", "")

            if bet_market == "match_winner":
                # For MW bets, team name is stored in 'direction' or 'team' field
                bet_team = bet.get("team", "") or bet.get("direction", "")
                if bet_team and state.batting_team:
                    # Check if backed the currently batting team
                    bt = bet_team.lower()
                    st = state.batting_team.lower()
                    if bt in st or st in bt or bt.split()[0] == st.split()[0]:
                        has_mw_back = True

            if bet_dir == "OVER":
                has_session_over = True
                if bet_market in ("innings_total", "20_over"):
                    has_innings_over = True
            elif bet_dir == "UNDER":
                has_session_under = True
                if bet_market in ("innings_total", "20_over"):
                    has_innings_under = True

        # Rule 1: Backed team to win → don't bet UNDER on their sessions
        if has_mw_back and direction == "UNDER" and market != "match_winner":
            return f"Contradicts open MW bet — backed {state.batting_team} to win, can't bet UNDER on their session"

        # Rule 2: Bet UNDER on ANY session → don't back batting team to win
        if has_session_under and market == "match_winner":
            team = state.batting_team or "batting team"
            return f"Contradicts open UNDER session bet — can't back {team} to win"

        # Rule 3: Bet OVER on early session → don't bet UNDER on later session
        if has_session_over and direction == "UNDER" and market in ("10_over", "15_over", "20_over", "innings_total"):
            return f"Contradicts open OVER session bet — can't bet UNDER on {market}"

        # Rule 4: Bet UNDER on early session → don't bet OVER on later session
        if has_session_under and direction == "OVER" and market in ("10_over", "15_over", "20_over", "innings_total"):
            return f"Contradicts open UNDER session bet — can't bet OVER on {market}"

        # Rule 5: Overall session direction vs MW
        # If multiple session bets are UNDER (bearish), don't back batting team to WIN.
        # MW direction can be a team name, HOME, AWAY, or LAGAI — check any non-empty value.
        if market == "match_winner" and direction:
            _session_mkts = {"6_over", "10_over", "15_over", "innings_total"}
            under_count = sum(1 for ob in open_bets if ob.get("direction") == "UNDER"
                            and ob.get("market", "") in _session_mkts)
            over_count = sum(1 for ob in open_bets if ob.get("direction") == "OVER"
                           and ob.get("market", "") in _session_mkts)
            if under_count >= 2 and over_count == 0:
                return f"MW contradicts bearish session position ({under_count} UNDER bets active)"

        # Rule 6: Bearish MW vs bullish sessions
        # If we have a MW bet for the BOWLING team (i.e., against the batting team),
        # don't bet OVER on the batting team's sessions — that contradicts.
        if market in ("6_over", "10_over", "15_over", "innings_total") and direction == "OVER":
            batting_team = (state.batting_team or "").lower() if state else ""
            for ob in open_bets:
                if ob.get("market") == "match_winner":
                    mw_dir = (ob.get("direction") or "").lower()
                    # Only block if the MW bet is clearly for the OTHER team
                    # (direction contains the backed team name, which differs from batting_team)
                    if mw_dir and batting_team and mw_dir not in batting_team and batting_team not in mw_dir:
                        return f"OVER contradicts MW bet backing {ob.get('direction')} (not batting team)"

        return None

    def _new_batsman_at_crease(self, state: MatchState) -> bool:
        for bat in state.active_batsmen:
            balls = bat.get("balls", 0)
            if balls is not None and balls < 6:
                return True
        return False

    def _check_chase(self, state: MatchState) -> Optional[str]:
        target = state.target_runs
        if not target:
            return None
        remaining = target - state.total_runs
        overs_left = max(0.1, 20.0 - state.overs_completed)
        required_rr = remaining / overs_left
        if state.current_run_rate > 0 and required_rr > state.current_run_rate * 2.0:
            return f"Required RR ({required_rr:.1f}) is 2x current ({state.current_run_rate:.1f}) — OVER too risky"
        return None

    def _check_chase_under(self, state: MatchState, market: str = "") -> Optional[str]:
        target = state.target_runs
        if not target:
            return None

        overs = float(state.overs_completed or 0.0)
        wickets = int(state.wickets or 0)
        remaining = target - state.total_runs
        overs_left = max(0.1, 20.0 - overs)
        required_rr = remaining / overs_left
        current_rr = float(state.current_run_rate or 0.0)

        if overs <= 1.0 and wickets == 0 and required_rr <= 10.5:
            return f"Early chase stable (RRR {required_rr:.1f}, 10 wickets in hand) — UNDER too early"

        if overs <= 6.0 and wickets <= 1 and required_rr <= 10.5:
            return f"Powerplay chase stable (RRR {required_rr:.1f}, wickets {wickets}) — UNDER too early"

        if overs <= 10.5 and wickets <= 2 and required_rr <= 10.5 and current_rr >= max(0.0, required_rr - 1.5):
            return (
                f"Chase on track (CRR {current_rr:.1f}, RRR {required_rr:.1f}, wickets {wickets}) "
                f"— UNDER too early"
            )

        return None

    def get_live_summary(self, state: MatchState, match_id: int = 0) -> str:
        """One-line summary of what's happening."""
        analysis = self.tracker.update(match_id, state)
        parts = []
        if analysis["wicket_pressure"]:
            parts.append("WICKETS FALLING")
        parts.append(f"Momentum: {analysis['batting_momentum']}")
        parts.append(f"Bowler: {analysis['bowler_pressure']}")
        parts.append(f"Trend: {analysis['recent_trend']}")
        return " | ".join(parts)
