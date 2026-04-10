"""Innings State — live resource model for the current innings.

This is the missing layer between MatchState (scoreboard snapshot) and
the prediction engine.  It answers questions that pure extrapolation cannot:

  - Who is left to bat? How strong is the tail?
  - Which bowlers have overs left? How many?
  - Is the remaining batting deep or fragile?
  - Does the current session direction imply a bullish or bearish script?

Built from MatchState + MatchDossier (player profiles) + live batting/bowling cards.
Updated every scan.  Consumed by predictor and contradiction logic.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("ipl_spotter.innings_state")

# Player quality tiers based on T20 career stats
# Used when we can't match a player to our DB — fall back to role-based defaults
_ROLE_DEFAULTS = {
    "Batsman":           {"bat_quality": 0.8, "bowl_quality": 0.0},
    "WK-Batsman":        {"bat_quality": 0.7, "bowl_quality": 0.0},
    "Batting Allrounder": {"bat_quality": 0.7, "bowl_quality": 0.4},
    "Bowling Allrounder": {"bat_quality": 0.4, "bowl_quality": 0.7},
    "Bowler":            {"bat_quality": 0.2, "bowl_quality": 0.8},
    "All-Rounder":       {"bat_quality": 0.5, "bowl_quality": 0.5},
}

# T20 max overs per bowler
MAX_OVERS_PER_BOWLER = 4.0


@dataclass
class BatterResource:
    """A batter's resource state in the current innings."""
    name: str
    status: str = "yet_to_bat"  # "batting", "out", "yet_to_bat"
    runs: int = 0
    balls: int = 0
    strike_rate: float = 0.0
    position: int = 0           # batting order position (1-11)
    # Quality metrics (0.0 - 1.0)
    bat_quality: float = 0.5    # derived from career stats or role
    career_sr: float = 130.0
    career_avg: float = 25.0
    role: str = ""


@dataclass
class BowlerResource:
    """A bowler's resource state in the current innings."""
    name: str
    overs_bowled: float = 0.0
    overs_remaining: float = 4.0
    runs_conceded: int = 0
    wickets: int = 0
    economy: float = 8.5
    # Quality metrics
    bowl_quality: float = 0.5
    career_econ: float = 8.5
    role: str = ""
    is_death_specialist: bool = False


@dataclass
class InningsState:
    """Complete resource picture of the current innings.

    This is what the predictor should consume instead of just
    score/overs/wickets.
    """
    # Who is batting
    batters_out: List[BatterResource] = field(default_factory=list)
    batters_batting: List[BatterResource] = field(default_factory=list)
    batters_yet_to_bat: List[BatterResource] = field(default_factory=list)

    # Who is bowling
    bowlers_used: List[BowlerResource] = field(default_factory=list)
    bowlers_available: List[BowlerResource] = field(default_factory=list)

    # Derived metrics
    remaining_bat_quality: float = 0.5   # avg quality of remaining batters
    remaining_bowl_quality: float = 0.5  # avg quality of remaining bowlers (for batting side, this is opponent)
    tail_strength: float = 0.3           # quality of batters 8-11
    batting_depth: int = 0               # how many quality batters left (quality > 0.5)
    overs_of_bowling_left: float = 0.0   # total bowler overs remaining
    death_bowling_quality: float = 0.5   # quality of bowlers likely to bowl at death

    # Script state
    innings_direction: str = ""          # "bullish" / "bearish" / "neutral"
    direction_confidence: float = 0.0

    def wickets_in_hand(self) -> int:
        return len(self.batters_batting) + len(self.batters_yet_to_bat) - 1  # -1 because need 2 to bat

    def top_order_intact(self) -> bool:
        """True if fewer than 3 top-order wickets (positions 1-5) have fallen."""
        top_out = sum(1 for b in self.batters_out if b.position <= 5)
        return top_out < 3


def build_innings_state(
    match_state: Any,
    squad: Optional[List[Dict[str, Any]]] = None,
    player_db: Any = None,
    name_matcher: Any = None,
    **_kwargs: Any,
) -> InningsState:
    """Build an InningsState from a MatchState + optional squad/player data.

    Args:
        match_state: The current MatchState object
        squad: Playing XI list from ESPN/CricData [{name, role, battingStyle, bowlingStyle}]
        player_db: StatsDB instance for career stat lookups
        name_matcher: NameMatcher instance for fuzzy ESPN→DB name resolution
    """
    state = InningsState()

    _has_nm = name_matcher is not None and hasattr(name_matcher, "match_batsman")
    _bat_card_len = len(match_state.batting_card or [])
    _bowl_card_len = len(match_state.bowling_card or [])
    _squad_len = len(squad or [])
    logger.info(
        "build_innings_state: name_matcher=%s bat_card=%d bowl_card=%d squad=%d",
        "YES" if _has_nm else "NO", _bat_card_len, _bowl_card_len, _squad_len,
    )

    # 1. Build batter list from batting card (who has batted)
    batted_names = set()
    for i, card in enumerate(match_state.batting_card or []):
        name = card.get("name", "")
        if not name:
            continue
        batted_names.add(name.lower())

        br = BatterResource(
            name=name,
            runs=card.get("score", card.get("runs", 0)) or 0,
            balls=card.get("balls", card.get("ball", 0)) or 0,
            strike_rate=float(card.get("sr", card.get("rate", 0)) or 0),
            position=i + 1,
        )

        # Enrich with career stats via name matching
        if player_db:
            _enrich_batter(br, player_db, name, name_matcher)

        # Is this batter currently at the crease?
        is_active = card.get("active", False)
        if is_active or any(
            ab.get("name", "").lower() == name.lower()
            for ab in (match_state.active_batsmen or [])
        ):
            br.status = "batting"
            state.batters_batting.append(br)
        else:
            br.status = "out"
            state.batters_out.append(br)

    # 2. Build yet-to-bat list from squad
    if squad:
        position = len(match_state.batting_card or []) + 1
        for player in squad:
            pname = player.get("name", "")
            if not pname or pname.lower() in batted_names:
                continue
            role = player.get("role", "")
            defaults = _ROLE_DEFAULTS.get(role, {"bat_quality": 0.4, "bowl_quality": 0.3})

            br = BatterResource(
                name=pname,
                status="yet_to_bat",
                position=position,
                bat_quality=defaults["bat_quality"],
                role=role,
            )

            # Enrich from player DB if available
            if player_db:
                _enrich_batter(br, player_db, pname, name_matcher)

            state.batters_yet_to_bat.append(br)
            position += 1

    # 3. Build bowler list from bowling card
    bowled_names = set()
    for card in match_state.bowling_card or []:
        name = card.get("name", "")
        if not name:
            continue
        bowled_names.add(name.lower())

        overs = float(card.get("overs", 0) or 0)
        bwr = BowlerResource(
            name=name,
            overs_bowled=overs,
            overs_remaining=max(0, MAX_OVERS_PER_BOWLER - overs),
            runs_conceded=card.get("runs", 0) or 0,
            wickets=card.get("wickets", 0) or 0,
            economy=float(card.get("econ", card.get("rate", 8.5)) or 8.5),
        )

        if player_db:
            _enrich_bowler(bwr, player_db, name, name_matcher)

        if bwr.overs_remaining > 0:
            state.bowlers_available.append(bwr)
        state.bowlers_used.append(bwr)

    # 4. Add bowlers from squad who haven't bowled yet
    if squad:
        for player in squad:
            pname = player.get("name", "")
            role = player.get("role", "")
            if not pname or pname.lower() in bowled_names:
                continue
            # Only add if player is a bowler or all-rounder
            if role and ("Bowler" in role or "Allrounder" in role or "All-Rounder" in role):
                bwr = BowlerResource(
                    name=pname,
                    overs_remaining=MAX_OVERS_PER_BOWLER,
                    role=role,
                )
                if player_db:
                    _enrich_bowler(bwr, player_db, pname, name_matcher)
                state.bowlers_available.append(bwr)

    # 5. Compute derived metrics
    _compute_derived(state, match_state)

    # Log enrichment results
    all_batters = state.batters_batting + state.batters_yet_to_bat + state.batters_out
    enriched_bat = sum(1 for b in all_batters if b.bat_quality != 0.5 and b.bat_quality != 0.4)
    all_bowlers = state.bowlers_used + [b for b in state.bowlers_available if b not in state.bowlers_used]
    enriched_bowl = sum(1 for b in all_bowlers if b.bowl_quality != 0.5)
    logger.info(
        "InningsState: %d batters (%d enriched), %d bowlers (%d enriched), depth=%d, bat_q=%.2f, dbq=%.2f",
        len(all_batters), enriched_bat, len(all_bowlers), enriched_bowl,
        state.batting_depth, state.remaining_bat_quality, state.death_bowling_quality,
    )

    return state


def _enrich_batter(br: BatterResource, player_db: Any, name: str, name_matcher: Any = None) -> None:
    """Enrich a batter with stats from the database.

    Uses NameMatcher to resolve ESPN/Sportmonks full names (e.g. "Virat Kohli")
    to the abbreviated DB names (e.g. "V Kohli") before querying.
    """
    try:
        # Try name matcher first (fuzzy match ESPN→DB names)
        db_name = name
        if name_matcher and hasattr(name_matcher, "match_batsman"):
            matched = name_matcher.match_batsman(name)
            if matched:
                db_name = matched

        stats = player_db.get_player_batting_stats(db_name)
        if stats and stats.get("innings", 0) >= 3:
            br.career_sr = stats.get("avg_strike_rate", 130.0) or 130.0
            br.career_avg = stats.get("avg_runs", 25.0) or 25.0
            br.bat_quality = min(1.0, max(0.1, (br.career_sr - 80) / 100))
            logger.debug("Enriched batter %s → %s (SR=%.0f, quality=%.2f)", name, db_name, br.career_sr, br.bat_quality)
    except Exception:
        pass


def _enrich_bowler(bwr: BowlerResource, player_db: Any, name: str, name_matcher: Any = None) -> None:
    """Enrich a bowler with stats from the database.

    Uses NameMatcher to resolve ESPN/Sportmonks full names to DB names.
    """
    try:
        db_name = name
        if name_matcher and hasattr(name_matcher, "match_bowler"):
            matched = name_matcher.match_bowler(name)
            if matched:
                db_name = matched

        stats = player_db.get_bowler_stats(db_name)
        if stats and stats.get("innings", 0) >= 3:
            bwr.career_econ = stats.get("avg_economy", 8.5) or 8.5
            bwr.bowl_quality = min(1.0, max(0.1, (12.0 - bwr.career_econ) / 8.0))
            bwr.is_death_specialist = bwr.career_econ <= 8.0
            logger.debug("Enriched bowler %s → %s (econ=%.1f, quality=%.2f)", name, db_name, bwr.career_econ, bwr.bowl_quality)
    except Exception:
        pass


def _compute_derived(state: InningsState, match_state: Any) -> None:
    """Compute derived metrics from the resource lists."""
    # Remaining batting quality
    remaining = state.batters_batting + state.batters_yet_to_bat
    if remaining:
        state.remaining_bat_quality = sum(b.bat_quality for b in remaining) / len(remaining)
        state.batting_depth = sum(1 for b in remaining if b.bat_quality > 0.5)
    else:
        state.remaining_bat_quality = 0.0
        state.batting_depth = 0

    # Tail strength (last 4 in the remaining order)
    if len(remaining) >= 4:
        tail = remaining[-4:]
        state.tail_strength = sum(b.bat_quality for b in tail) / len(tail)
    elif remaining:
        state.tail_strength = sum(b.bat_quality for b in remaining) / len(remaining)

    # Bowling overs remaining
    state.overs_of_bowling_left = sum(b.overs_remaining for b in state.bowlers_available)

    # Death bowling quality (bowlers with overs left who are death specialists)
    death_bowlers = [b for b in state.bowlers_available if b.is_death_specialist and b.overs_remaining > 0]
    if death_bowlers:
        state.death_bowling_quality = sum(b.bowl_quality for b in death_bowlers) / len(death_bowlers)
    elif state.bowlers_available:
        state.death_bowling_quality = sum(b.bowl_quality for b in state.bowlers_available) / len(state.bowlers_available)

    # Innings direction from scoring context
    overs = match_state.overs_completed or 0
    if overs > 0:
        crr = match_state.total_runs / overs
        wickets = match_state.wickets or 0
        if crr > 9.0 and wickets < 3 and state.batting_depth >= 3:
            state.innings_direction = "bullish"
            state.direction_confidence = min(0.9, 0.5 + (crr - 9.0) / 10)
        elif crr < 6.5 or wickets >= 5 or state.batting_depth <= 1:
            state.innings_direction = "bearish"
            state.direction_confidence = min(0.9, 0.5 + (7.0 - crr) / 10 + wickets * 0.05)
        else:
            state.innings_direction = "neutral"
            state.direction_confidence = 0.3
