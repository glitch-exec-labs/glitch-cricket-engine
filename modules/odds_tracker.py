"""
IPL Edge Spotter -- odds_tracker.py
Tracks how Cloudbet odds move during a live IPL match.

Works like a price ticker: stores timestamped snapshots of odds for each market,
detects sharp movements, and formats Telegram-friendly update messages.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("ipl_spotter.odds_tracker")

# Type alias for the history key: (match_id, market_key, line_key)
HistoryKey = Tuple[str, str, str]


class OddsTracker:
    """
    Stores timestamped odds snapshots and computes movements.

    History is keyed by (match_id, market_key, line_key) where line_key
    disambiguates multiple lines within the same market (e.g. different teams
    or players in over/under markets).
    """

    def __init__(self) -> None:
        # {(match_id, market_key, line_key): [(timestamp, odds_data), ...]}
        self.history: Dict[HistoryKey, List[Tuple[datetime, dict]]] = defaultdict(list)

    # -- Recording --------------------------------------------------------------

    def record_snapshot(
        self,
        match_id: str,
        market_key: str,
        odds_data: dict,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Store a timestamped snapshot of odds for a specific market.

        Args:
            match_id: Unique match identifier.
            market_key: Market type key (e.g. "match_winner", "innings_total").
            odds_data: Parsed dict from OddsClient.
            timestamp: When the snapshot was taken; defaults to now (UTC).
        """
        ts = timestamp or datetime.now(timezone.utc)

        if odds_data.get("market") == "match_winner":
            # Single entry keyed by market
            key: HistoryKey = (str(match_id), market_key, "main")
            self.history[key].append((ts, odds_data))

        elif "lines" in odds_data:
            # Over/under markets -- one entry per line+team combo
            for line_info in odds_data["lines"]:
                line_val = line_info.get("line", 0)
                team = line_info.get("team", "")
                line_key = f"{team}:{line_val}" if team else str(line_val)
                key = (str(match_id), market_key, line_key)
                snapshot = {
                    "market": odds_data["market"],
                    "line": line_info.get("line", 0),
                    "over_odds": line_info.get("over_odds", 0.0),
                    "under_odds": line_info.get("under_odds", 0.0),
                    "team": team,
                }
                self.history[key].append((ts, snapshot))

        elif "players" in odds_data:
            for player_info in odds_data["players"]:
                player = player_info.get("player", "")
                line_val = player_info.get("line", 0)
                line_key = f"{player}:{line_val}"
                key = (str(match_id), market_key, line_key)
                snapshot = {
                    "market": odds_data["market"],
                    "player": player,
                    "line": player_info.get("line", 0),
                    "over_odds": player_info.get("over_odds", 0.0),
                    "under_odds": player_info.get("under_odds", 0.0),
                }
                self.history[key].append((ts, snapshot))

    # -- Movement analysis ------------------------------------------------------

    def get_movement(self, match_id: str, market_key: str, line_key: str = "main") -> Optional[dict]:
        """
        Compute the movement summary for a single tracked line.

        Returns dict with: opening_line, current_line, line_change,
        opening_over_odds, current_over_odds, odds_change_pct,
        snapshots_count, direction.
        """
        key: HistoryKey = (str(match_id), market_key, line_key)
        snapshots = self.history.get(key)
        if not snapshots or len(snapshots) < 1:
            return None

        first_ts, first = snapshots[0]
        last_ts, last = snapshots[-1]

        # For match_winner markets, use selection prices
        if first.get("market") == "match_winner":
            return self._match_winner_movement(snapshots)

        opening_line = first.get("line", 0.0)
        current_line = last.get("line", 0.0)
        line_change = current_line - opening_line

        opening_over = first.get("over_odds", 0.0)
        current_over = last.get("over_odds", 0.0)

        if opening_over and opening_over > 0:
            odds_change_pct = ((current_over - opening_over) / opening_over) * 100
        else:
            odds_change_pct = 0.0

        direction = _classify_direction(opening_over, current_over)

        return {
            "market_key": market_key,
            "line_key": line_key,
            "opening_line": opening_line,
            "current_line": current_line,
            "line_change": round(line_change, 2),
            "opening_over_odds": opening_over,
            "current_over_odds": current_over,
            "opening_under_odds": first.get("under_odds", 0.0),
            "current_under_odds": last.get("under_odds", 0.0),
            "odds_change_pct": round(odds_change_pct, 2),
            "snapshots_count": len(snapshots),
            "direction": direction,
        }

    def _match_winner_movement(self, snapshots: List[Tuple[datetime, dict]]) -> dict:
        """Build movement dict for match_winner market."""
        first_ts, first = snapshots[0]
        last_ts, last = snapshots[-1]

        first_sel = first.get("selections", {})
        last_sel = last.get("selections", {})

        home_open = first_sel.get("home", {}).get("price", 0.0)
        home_curr = last_sel.get("home", {}).get("price", 0.0)
        away_open = first_sel.get("away", {}).get("price", 0.0)
        away_curr = last_sel.get("away", {}).get("price", 0.0)

        home_pct = ((home_curr - home_open) / home_open * 100) if home_open else 0.0
        away_pct = ((away_curr - away_open) / away_open * 100) if away_open else 0.0

        return {
            "market_key": "match_winner",
            "line_key": "main",
            "home_opening": home_open,
            "home_current": home_curr,
            "home_change_pct": round(home_pct, 2),
            "home_direction": _classify_direction(home_open, home_curr),
            "away_opening": away_open,
            "away_current": away_curr,
            "away_change_pct": round(away_pct, 2),
            "away_direction": _classify_direction(away_open, away_curr),
            "snapshots_count": len(snapshots),
        }

    def get_all_movements(self, match_id: str) -> List[dict]:
        """
        Return movement summaries for all tracked markets in a match.
        """
        match_id_str = str(match_id)
        results: List[dict] = []
        seen_keys = set()

        for (mid, mkey, lkey) in self.history:
            if mid != match_id_str:
                continue
            if (mkey, lkey) in seen_keys:
                continue
            seen_keys.add((mkey, lkey))
            movement = self.get_movement(match_id_str, mkey, lkey)
            if movement is not None:
                results.append(movement)

        return results

    # -- Formatting -------------------------------------------------------------

    def format_odds_update(self, match_id: str, home: str, away: str) -> str:
        """
        Format a Telegram-friendly odds update message showing all market movements.
        """
        movements = self.get_all_movements(match_id)
        if not movements:
            return f"No odds data for {home} vs {away}"

        lines = [f"\U0001f4ca ODDS UPDATE \u2014 {home} vs {away}"]

        # Group by market_key
        by_market: Dict[str, List[dict]] = defaultdict(list)
        for m in movements:
            by_market[m["market_key"]].append(m)

        # Match winner first
        if "match_winner" in by_market:
            mw = by_market.pop("match_winner")[0]
            lines.append("")
            lines.append("\U0001f3cf Match Winner")

            h_arrow = _direction_arrow(mw.get("home_direction", "STABLE"))
            a_arrow = _direction_arrow(mw.get("away_direction", "STABLE"))

            lines.append(
                f"  {home}: {mw['home_opening']:.2f} \u2192 {mw['home_current']:.2f} "
                f"({h_arrow} {mw['home_direction']})"
            )
            lines.append(
                f"  {away}: {mw['away_opening']:.2f} \u2192 {mw['away_current']:.2f} "
                f"({a_arrow} {mw['away_direction']})"
            )

        # Other markets
        for mkey, mvs in sorted(by_market.items()):
            lines.append("")
            for mv in mvs:
                label = _market_label(mkey, mv.get("line_key", ""))
                lines.append(f"\U0001f4c8 {label}")

                opening_line = mv.get("opening_line", 0)
                current_line = mv.get("current_line", 0)
                line_change = mv.get("line_change", 0)

                if line_change != 0:
                    lines.append(
                        f"  Line: {opening_line} \u2192 {current_line} "
                        f"(\u2193 {line_change:+.1f})" if line_change < 0
                        else f"  Line: {opening_line} \u2192 {current_line} "
                        f"(\u2191 {line_change:+.1f})"
                    )
                else:
                    lines.append(f"  Line: {current_line} (unchanged)")

                o_open = mv.get("opening_over_odds", 0)
                o_curr = mv.get("current_over_odds", 0)
                u_open = mv.get("opening_under_odds", 0)
                u_curr = mv.get("current_under_odds", 0)

                if o_open != o_curr or u_open != u_curr:
                    lines.append(
                        f"  Over: {o_open:.2f} \u2192 {o_curr:.2f} | "
                        f"Under: {u_open:.2f} \u2192 {u_curr:.2f}"
                    )

        return "\n".join(lines)

    # -- Sharp move detection ---------------------------------------------------

    def get_sharp_moves(self, match_id: str, threshold_pct: float = 5.0) -> List[dict]:
        """
        Return markets where odds have moved more than threshold_pct since opening.
        """
        movements = self.get_all_movements(match_id)
        sharp: List[dict] = []

        for mv in movements:
            if mv.get("market_key") == "match_winner":
                # Check both home and away
                if abs(mv.get("home_change_pct", 0)) >= threshold_pct:
                    sharp.append(mv)
                elif abs(mv.get("away_change_pct", 0)) >= threshold_pct:
                    sharp.append(mv)
            else:
                if abs(mv.get("odds_change_pct", 0)) >= threshold_pct:
                    sharp.append(mv)

        return sharp

    def format_sharp_move_alert(
        self, match_id: str, home: str, away: str, move: dict
    ) -> str:
        """
        Format a Telegram alert for a sharp odds movement.
        """
        lines = [f"\u26a1 SHARP MOVE \u2014 {home} vs {away}"]

        mkey = move.get("market_key", "")
        lkey = move.get("line_key", "")
        label = _market_label(mkey, lkey)
        lines.append(f"\U0001f4ca {label}")

        if mkey == "match_winner":
            h_open = move.get("home_opening", 0)
            h_curr = move.get("home_current", 0)
            h_pct = move.get("home_change_pct", 0)
            a_open = move.get("away_opening", 0)
            a_curr = move.get("away_current", 0)
            a_pct = move.get("away_change_pct", 0)

            lines.append(
                f"\U0001f4c9 {home}: {h_open:.2f} \u2192 {h_curr:.2f} ({h_pct:+.1f}%)"
            )
            lines.append(
                f"\U0001f4c8 {away}: {a_open:.2f} \u2192 {a_curr:.2f} ({a_pct:+.1f}%)"
            )
        else:
            opening_line = move.get("opening_line", 0)
            current_line = move.get("current_line", 0)
            line_change = move.get("line_change", 0)
            o_open = move.get("opening_over_odds", 0)
            o_curr = move.get("current_over_odds", 0)
            pct = move.get("odds_change_pct", 0)

            lines.append(
                f"\U0001f4c9 Line moved: {opening_line} \u2192 {current_line} "
                f"({line_change:+.1f} runs)"
            )
            lines.append(
                f"\U0001f4b0 Over odds: {o_open:.2f} \u2192 {o_curr:.2f} ({pct:+.1f}%)"
            )

        # Count recent snapshots
        key: HistoryKey = (str(match_id), mkey, lkey)
        snapshots = self.history.get(key, [])
        count = len(snapshots)
        lines.append(f"\u23f0 {count} snapshots recorded")

        return "\n".join(lines)


# -- Module-level helpers -------------------------------------------------------


def _classify_direction(opening: float, current: float) -> str:
    """Classify odds movement direction."""
    if not opening or not current:
        return "STABLE"
    diff = current - opening
    if abs(diff) < 0.01:
        return "STABLE"
    elif diff < 0:
        return "SHORTENING"
    else:
        return "DRIFTING"


def _direction_arrow(direction: str) -> str:
    """Return an arrow character for the direction."""
    if direction == "SHORTENING":
        return "\u2193"
    elif direction == "DRIFTING":
        return "\u2191"
    return "\u2194"


def _market_label(market_key: str, line_key: str) -> str:
    """Build a human-readable label for a market + line combination."""
    pretty = {
        "match_winner": "Match Winner",
        "innings_total": "Innings Total",
        "powerplay_runs": "Powerplay Runs",
        "over_runs": "Over Runs",
        "player_runs": "Player Runs",
        "team_sixes": "Team Sixes",
        "team_fours": "Team Fours",
        "highest_over": "Highest Over",
        "first_wicket": "First Wicket",
        "player_milestone": "Player Milestone",
    }
    base = pretty.get(market_key, market_key)
    if line_key and line_key != "main":
        # e.g. "home:185.5" -> "(home)"
        parts = line_key.split(":")
        if len(parts) == 2 and parts[0]:
            base += f" ({parts[0]})"
    return base
