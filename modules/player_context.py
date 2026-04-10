"""Player-aware batting and bowling adjustments for live predictions."""

from __future__ import annotations

from typing import Any, Optional

from modules.name_matcher import NameMatcher
from modules.stats_db import StatsDB


class PlayerContext:
    """Build player-based run-rate adjustments from historical batting and bowling data."""

    LEAGUE_AVG_SR = 130.0
    LEAGUE_AVG_ECON = 8.5

    def __init__(self, stats_db: StatsDB, name_matcher: NameMatcher):
        self.stats_db = stats_db
        self.name_matcher = name_matcher

    def get_batting_adjustment(self, batsman_name: str, venue: str = "") -> float:
        """Return this batter's per-over scoring adjustment versus league average."""
        profile = self._batting_profile(batsman_name, venue)
        return round(profile["adjustment"], 2)

    def get_bowling_adjustment(self, bowler_name: str, venue: str = "") -> float:
        """Return this bowler's per-over concession adjustment versus league average."""
        profile = self._bowling_profile(bowler_name, venue)
        return round(profile["adjustment"], 2)

    @staticmethod
    def get_form_multiplier(current_sr: float, career_sr: float) -> float:
        """Blend current strike-rate form into the career baseline."""
        try:
            current = float(current_sr)
            career = float(career_sr)
        except (TypeError, ValueError):
            return 1.0
        if current <= 0 or career <= 0:
            return 1.0
        blended = 0.7 * career + 0.3 * current
        return round(max(0.7, min(1.3, blended / career)), 3)

    def get_combined_adjustment(
        self,
        active_batsmen: list[dict[str, Any]],
        active_bowler: dict[str, Any],
        venue: str = "",
        overs_completed: float | None = None,
    ) -> dict[str, Any]:
        """Combine batting and bowling quality into next-over and innings adjustments."""
        result: dict[str, Any] = {
            "over_adjustment": 0.0,
            "innings_adjustment": 0.0,
            "confidence": "LOW",
        }

        matched_count = 0
        over_adjustment = 0.0
        batter_keys = ["batsman_1", "batsman_2"]

        for key, batter in zip(batter_keys, active_batsmen[:2], strict=False):
            profile = self._batting_profile(batter.get("name", ""), venue)
            current_sr = self._coerce_float(batter.get("rate", batter.get("sr")))
            balls_faced = self._coerce_float(batter.get("balls"))
            adjustment = profile["adjustment"]
            if profile["matched_name"] and current_sr is not None and balls_faced is not None and balls_faced > 10:
                adjustment *= self.get_form_multiplier(current_sr, profile["career_sr"])
            adjustment = round(adjustment, 2)
            if profile["matched_name"]:
                matched_count += 1
            over_adjustment += adjustment
            result[key] = {
                "name": profile["matched_name"] or batter.get("name", ""),
                "career_sr": round(profile["career_sr"], 1) if profile["career_sr"] is not None else None,
                "current_sr": round(current_sr, 1) if current_sr is not None else None,
                "adjustment": adjustment,
            }

        bowler_profile = self._bowling_profile(active_bowler.get("name", ""), venue)
        bowler_adjustment = round(bowler_profile["adjustment"], 2)
        if bowler_profile["matched_name"]:
            matched_count += 1
        over_adjustment += bowler_adjustment
        result["bowler"] = {
            "name": bowler_profile["matched_name"] or active_bowler.get("name", ""),
            "career_econ": round(bowler_profile["career_econ"], 2) if bowler_profile["career_econ"] is not None else None,
            "current_econ": round(self._coerce_float(active_bowler.get("rate", active_bowler.get("econ"))) or 0.0, 2),
            "adjustment": bowler_adjustment,
        }

        if matched_count >= 3:
            confidence = "HIGH"
        elif matched_count >= 2:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"
            over_adjustment = 0.0

        result["confidence"] = confidence
        result["over_adjustment"] = round(max(-4.0, min(4.0, over_adjustment)), 2)
        remaining_overs = 20.0
        if overs_completed is not None:
            remaining_overs = max(0.0, 20.0 - float(overs_completed))
        result["innings_adjustment"] = round(result["over_adjustment"] * remaining_overs, 2)
        return result

    def _batting_profile(self, sportmonks_name: str, venue: str = "") -> dict[str, Any]:
        matched_name = self.name_matcher.match_batsman(sportmonks_name)
        if not matched_name:
            return {"matched_name": None, "career_sr": 0.0, "adjustment": 0.0}

        overall = self.stats_db.get_player_batting_stats(matched_name)
        venue_stats = self.stats_db.get_player_batting_stats(matched_name, venue=venue) if venue else None
        career_sr = overall.get("avg_strike_rate") or 0.0
        if venue_stats and venue_stats.get("innings", 0) >= 5 and venue_stats.get("avg_strike_rate"):
            career_sr = 0.7 * career_sr + 0.3 * venue_stats["avg_strike_rate"]

        adjustment = ((career_sr - self.LEAGUE_AVG_SR) / 100.0) * 6.0 / 2.0 if career_sr else 0.0
        return {
            "matched_name": matched_name,
            "career_sr": career_sr,
            "adjustment": adjustment,
        }

    def _bowling_profile(self, sportmonks_name: str, venue: str = "") -> dict[str, Any]:
        matched_name = self.name_matcher.match_bowler(sportmonks_name)
        if not matched_name:
            return {"matched_name": None, "career_econ": 0.0, "adjustment": 0.0}

        overall = self.stats_db.get_bowler_stats(matched_name)
        venue_stats = self.stats_db.get_bowler_stats(matched_name, venue=venue) if venue else None
        career_econ = overall.get("avg_economy") or 0.0
        if venue_stats and venue_stats.get("innings", 0) >= 5 and venue_stats.get("avg_economy"):
            career_econ = 0.7 * career_econ + 0.3 * venue_stats["avg_economy"]

        adjustment = career_econ - self.LEAGUE_AVG_ECON if career_econ else 0.0
        return {
            "matched_name": matched_name,
            "career_econ": career_econ,
            "adjustment": adjustment,
        }

    @staticmethod
    def _coerce_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
