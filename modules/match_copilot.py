"""Match Copilot — orchestrates the IPL match co-pilot experience.

Ties together model predictions, line estimation, hedge calculator,
and position book to produce actionable calls and book opportunities.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from modules.hedge_calculator import BookOpportunity, HedgeCalculator
from modules.position_book import PositionBook
from modules.session_markets import SESSION_DISPLAY, is_completed_session_market

logger = logging.getLogger("ipl_spotter.match_copilot")

# Maps market key -> (model prediction key, display name)
SESSION_MARKETS = {
    "6_over": ("powerplay_total", SESSION_DISPLAY["6_over"]),
    "10_over": ("ten_over_total", SESSION_DISPLAY["10_over"]),
    "15_over": ("fifteen_over_total", SESSION_DISPLAY["15_over"]),
    "20_over": ("innings_total", SESSION_DISPLAY["20_over"]),
}


class MatchCopilot:
    """Orchestrates model predictions, line estimation, calls, and hedging."""

    def __init__(self, config: Dict[str, Any] | None = None) -> None:
        self.config = config or {}
        self.enabled: bool = self.config.get("copilot_enabled", False)

        # Stake sizing
        self.default_stake: float = self.config.get("shadow_default_stake_inr", 500)
        self.min_stake: float = self.config.get("shadow_min_stake_inr", 200)
        self.max_stake: float = self.config.get("shadow_max_stake_inr", 1000)
        self.mw_default_stake: float = self.config.get("shadow_mw_default_stake_inr", 500)
        self.min_ev_pct: float = self.config.get("min_ev_pct", 5.0)
        self.min_ev_pct_mw: float = self.config.get("min_ev_pct_mw", self.min_ev_pct)
        self.throttle_seconds: float = self.config.get("message_throttle_seconds", 30)

        # Sub-components
        self.hedge_calculator = HedgeCalculator(config=self.config)
        self.position_book = PositionBook()

        # Tracking state
        self._sent_calls: set[str] = set()  # dedup keys
        self._last_mw_odds: Dict[int, Dict[str, float]] = {}  # match_id -> {team: odds}
        self._over_updates_sent: set[tuple[int, int]] = set()  # (match_id, over)
        self._session_direction_locks: Dict[tuple[int, int, str], str] = {}
        self._last_message_time: float = 0.0

    # ── Message Throttle ────────────────────────────────────────────

    def can_send_message(self) -> bool:
        """Return True if enough time has elapsed since the last message."""
        now = time.time()
        if now - self._last_message_time < self.throttle_seconds:
            return False
        return True

    def mark_message_sent(self) -> None:
        """Record that a message was actually sent (call after successful send)."""
        self._last_message_time = time.time()

    # ── Phase Detection ──────────────────────────────────────────────

    def _detect_phase(self, innings: int, overs: float, wickets: int) -> str:
        """Detect the current match phase."""
        if innings == 1:
            if overs <= 6.0:
                return "INNINGS_1_PP"
            elif overs <= 15.0:
                return "INNINGS_1_MIDDLE"
            else:
                return "INNINGS_1_DEATH"
        else:
            if overs <= 6.0:
                return "INNINGS_2_PP"
            else:
                return "INNINGS_2_CHASE"

    # ── Stake Sizing ─────────────────────────────────────────────────

    def _calculate_shadow_stake(self, edge_size: float, confidence: str) -> float:
        """Scale stake with edge size and confidence, clamped to bounds."""
        confidence_mult = {"LOW": 0.6, "MEDIUM": 1.0, "HIGH": 1.4}.get(confidence, 1.0)
        # Scale: default_stake * (edge / 5) * confidence_mult
        raw = self.default_stake * (edge_size / 5.0) * confidence_mult
        return max(self.min_stake, min(self.max_stake, round(raw)))

    # ── Session Call Evaluation ──────────────────────────────────────

    def _get_session_direction_lock(
        self,
        match_id: int,
        innings: int,
        market_key: str,
    ) -> Optional[str]:
        """Return the locked session direction for this match/innings/market."""
        locked = self._session_direction_locks.get((match_id, innings, market_key))
        if locked:
            return locked

        for pos in self.position_book.get_all_positions(match_id):
            if getattr(pos, "market", None) != market_key:
                continue
            if getattr(pos, "innings", None) != innings:
                continue
            return getattr(pos, "direction", None)

        return None

    # Minimum overs before a session call is valid — mirrors MIN_OVERS_BEFORE_BET in spotter.
    # Prevents garbage signals at innings start when the model has almost no data.
    SESSION_MIN_OVERS: Dict[str, float] = {
        "6_over":        2.0,
        "powerplay_runs": 2.0,
        "10_over":       5.0,
        "15_over":       6.0,
        "20_over":      10.0,
        "innings_total": 10.0,
    }

    # Maximum overs before a session shadow call is blocked — market too close to
    # closing for a shadow/book position to be meaningful.
    # Keyed by (market, innings): 1st innings allows up to 19, 2nd innings cuts at 17.
    SESSION_MAX_OVERS: Dict[tuple, float] = {
        ("20_over",      1): 19.0,
        ("innings_total", 1): 19.0,
        ("20_over",      2): 17.0,
        ("innings_total", 2): 17.0,
    }

    def evaluate_session_calls(
        self,
        match_id: int,
        model_predictions: Dict[str, Any],
        estimated_lines: Dict[str, Dict[str, float]],
        overs_completed: float,
        innings: int = 1,
    ) -> List[Dict[str, Any]]:
        """Compare model predictions to estimated lines and return calls with edge >= 3 runs."""
        calls: List[Dict[str, Any]] = []

        for market_key, (pred_key, display_name) in SESSION_MARKETS.items():
            if market_key not in estimated_lines:
                continue
            if is_completed_session_market(market_key, overs_completed):
                continue
            # Enforce minimum overs — model is too noisy before enough balls are bowled
            min_ov = self.SESSION_MIN_OVERS.get(market_key, 2.0)
            if overs_completed < min_ov:
                continue
            # Enforce maximum overs — too late to shadow-bet, market closing soon
            # Uses (market, innings) key so 1st/2nd innings have different cutoffs
            max_ov = self.SESSION_MAX_OVERS.get((market_key, innings), 999.0)
            if overs_completed >= max_ov:
                continue

            existing_open = [
                pos for pos in self.position_book.get_open_sessions(match_id, innings=innings)
                if pos.market == market_key
            ]
            if existing_open:
                continue

            pred = model_predictions.get(pred_key)
            if pred is None:
                continue

            expected = pred["expected"]
            line_yes = estimated_lines[market_key]["yes"]
            line_no = estimated_lines[market_key]["no"]
            locked_direction = self._get_session_direction_lock(match_id, innings, market_key)

            # Edge: expected vs the line we'd have to beat
            # YES direction: we need score > no_line, edge = expected - no_line
            yes_edge = expected - line_no
            # NO direction: we need score < yes_line, edge = yes_line - expected
            no_edge = line_yes - expected

            min_edge = 3.0

            if yes_edge >= min_edge:
                if locked_direction and locked_direction != "YES":
                    continue
                dedup_key = f"{match_id}:{innings}:{market_key}:YES:{line_no}"
                if dedup_key not in self._sent_calls:
                    stake = self._calculate_shadow_stake(edge_size=yes_edge, confidence="MEDIUM")
                    calls.append({
                        "market": market_key,
                        "innings": innings,
                        "direction": "YES",
                        "line": line_no,
                        "edge": round(yes_edge, 1),
                        "stake": stake,
                        "display_name": display_name,
                    })
                    self._sent_calls.add(dedup_key)
                    self._session_direction_locks[(match_id, innings, market_key)] = "YES"
                    self.position_book.add_session_call(
                        match_id, market_key, "YES", line_no, stake, innings=innings
                    )

            elif no_edge >= min_edge:
                if locked_direction and locked_direction != "NO":
                    continue
                dedup_key = f"{match_id}:{innings}:{market_key}:NO:{line_yes}"
                if dedup_key not in self._sent_calls:
                    stake = self._calculate_shadow_stake(edge_size=no_edge, confidence="MEDIUM")
                    calls.append({
                        "market": market_key,
                        "innings": innings,
                        "direction": "NO",
                        "line": line_yes,
                        "edge": round(no_edge, 1),
                        "stake": stake,
                        "display_name": display_name,
                    })
                    self._sent_calls.add(dedup_key)
                    self._session_direction_locks[(match_id, innings, market_key)] = "NO"
                    self.position_book.add_session_call(
                        match_id, market_key, "NO", line_yes, stake, innings=innings
                    )

        return calls

    # ── MW Call Evaluation ───────────────────────────────────────────

    def evaluate_mw_call(
        self,
        match_id: int,
        home: str,
        away: str,
        model_home_prob: float,
        current_home_odds: float,
        current_away_odds: float,
    ) -> Optional[Dict[str, Any]]:
        """Evaluate match-winner value. Returns call dict or None."""
        model_away_prob = 1.0 - model_home_prob

        # Fair odds
        fair_home = 1.0 / model_home_prob if model_home_prob > 0 else 999.0
        fair_away = 1.0 / model_away_prob if model_away_prob > 0 else 999.0

        # EV% for each side: (market_odds / fair_odds - 1) * 100
        home_ev = (current_home_odds / fair_home - 1) * 100
        away_ev = (current_away_odds / fair_away - 1) * 100

        best_team, best_ev, best_odds = None, 0.0, 0.0
        if home_ev >= away_ev and home_ev >= self.min_ev_pct_mw:
            best_team, best_ev, best_odds = home, home_ev, current_home_odds
        elif away_ev > home_ev and away_ev >= self.min_ev_pct_mw:
            best_team, best_ev, best_odds = away, away_ev, current_away_odds

        if best_team is None:
            return None

        existing_open = [
            pos for pos in self.position_book.get_open_mw(match_id)
            if pos.team == best_team
        ]
        if existing_open:
            return None

        dedup_key = f"{match_id}:MW:{best_team}"
        if dedup_key in self._sent_calls:
            return None

        self._sent_calls.add(dedup_key)
        stake = self.mw_default_stake
        self.position_book.add_mw_call(match_id, best_team, "LAGAI", best_odds, stake)

        return {
            "team": best_team,
            "direction": "LAGAI",
            "odds": best_odds,
            "ev_pct": round(best_ev, 1),
            "stake": stake,
        }

    # ── Book Opportunity Checking ────────────────────────────────────

    def check_book_opportunities(
        self,
        match_id: int,
        current_session_lines: Dict[str, Dict[str, float]],
        current_mw_odds: Dict[str, float],
        current_innings: int | None = None,
    ) -> List[BookOpportunity]:
        """Check all open positions for booking opportunities.

        Note: Session booking only works in Indian book (satta) markets where
        you can take opposite positions with a bookie. On Cloudbet, session
        hedging is not supported — only MW hedging works (back then lay).
        Session book alerts are skipped unless explicitly enabled.
        """
        opportunities: List[BookOpportunity] = []

        # Session positions — only for Indian book mode (not Cloudbet)
        if self.config.get("session_book_alerts", False):
            for pos in self.position_book.get_open_sessions(match_id, innings=current_innings):
                lines = current_session_lines.get(pos.market)
                if lines is None:
                    continue
                opp = self.hedge_calculator.check_session_book_opportunity(
                    entry_direction=pos.direction,
                    entry_line=pos.entry_line,
                    current_line_yes=lines["yes"],
                    current_line_no=lines["no"],
                    stake_per_run=pos.stake_per_run,
                )
                if opp is not None:
                    opportunities.append(opp)

        # MW positions — works on Cloudbet (back then lay)
        for pos in self.position_book.get_open_mw(match_id):
            cur_odds = current_mw_odds.get(pos.team)
            if cur_odds is None:
                continue
            opp = self.hedge_calculator.check_mw_book_opportunity(
                entry_direction=pos.direction,
                entry_odds=pos.odds,
                entry_stake=pos.stake,
                current_odds=cur_odds,
            )
            if opp is not None:
                opportunities.append(opp)

        return opportunities

    # ── MW Swing Detection ───────────────────────────────────────────

    def check_mw_swing(
        self,
        match_id: int,
        home: str,
        away: str,
        home_odds: float,
        away_odds: float,
    ) -> Optional[Dict[str, Any]]:
        """Detect >10% implied probability swing in MW odds."""
        key = match_id
        prev = self._last_mw_odds.get(key)

        # Store current
        self._last_mw_odds[key] = {home: home_odds, away: away_odds}

        if prev is None:
            return None

        prev_home_odds = prev.get(home)
        if prev_home_odds is None:
            return None

        # Implied prob swing
        if prev_home_odds <= 0 or home_odds <= 0:
            return None
        prev_home_prob = 1.0 / prev_home_odds
        curr_home_prob = 1.0 / home_odds
        swing = abs(curr_home_prob - prev_home_prob)

        if swing < 0.10:
            return None

        direction = "towards" if curr_home_prob > prev_home_prob else "away from"
        return {
            "home": home,
            "away": away,
            "swing_pct": round(swing * 100, 1),
            "direction": f"{direction} {home}",
            "home_odds": home_odds,
            "away_odds": away_odds,
        }

    # ── Over-by-over Update Tracking ─────────────────────────────────

    def should_send_over_update(self, match_id: int, over: int, innings: int = 1) -> bool:
        """Return True only once per completed over per match."""
        key = (match_id, innings, over)
        if key in self._over_updates_sent:
            return False
        self._over_updates_sent.add(key)
        return True

    # ── Session Line Estimation ──────────────────────────────────────

    def estimate_session_lines(
        self,
        overs_completed: float,
        current_score: int,
        cloudbet_lines: Dict[str, Any],
        model_predictions: Dict[str, Any],
    ) -> Dict[str, Dict[str, float]]:
        """Estimate session lines from Cloudbet data with model fallback.

        Cloudbet lines give yes=line-1, no=line (the line itself is the no boundary).
        Model fallback gives yes=expected-1, no=expected.
        """
        lines: Dict[str, Dict[str, float]] = {}

        for market_key, (pred_key, _) in SESSION_MARKETS.items():
            cb = cloudbet_lines.get(market_key)
            if cb is not None and "line" in cb:
                if cb.get("source") == "liveline" and cb.get("yes", 0) > 0:
                    # Ferrari Fast Line: "line" is the pre-computed NO boundary
                    # (already handles both spread and absolute formats in spotter.py).
                    # "yes" is the YES boundary (score must exceed NO to win YES bet).
                    yes_val = float(cb["yes"])
                    no_val = float(cb["line"])   # always use "line" — it's the correct NO
                    lines[market_key] = {"yes": yes_val, "no": no_val}
                else:
                    # Cloudbet: the line IS the no boundary; yes is 1 below
                    line_val = cb["line"]
                    lines[market_key] = {"yes": line_val - 1.0, "no": line_val}
            else:
                # Model fallback
                pred = model_predictions.get(pred_key)
                if pred is not None:
                    expected = pred["expected"]
                    lines[market_key] = {"yes": expected - 1.0, "no": expected}

        return lines

    # ── Match Reset ──────────────────────────────────────────────────

    def reset_match(self) -> None:
        """Clear all state for a new match."""
        self._sent_calls.clear()
        self._last_mw_odds.clear()
        self._over_updates_sent.clear()
        self._session_direction_locks.clear()
        self._last_message_time = 0.0
