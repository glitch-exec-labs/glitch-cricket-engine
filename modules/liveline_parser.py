"""Live Line Telegram channel parser.

Parses Indian book live line messages into structured data:
  - Match winner rates (back/lay)
  - Session lines (6-over, 10-over, 15-over, 20-over)
  - Ball-by-ball score updates
  - Who's on strike / bowling
  - Commentary (four, six, wicket, dot)

Message format examples:
  38-41 🇸🇾 BENGALURU 🇸🇾           → MW: back 38 lay 41, team Bengaluru
  66-7 👈🏻🖥️ 6 OVER 🖥️               → 6-over session: YES 66, NO 7
  3.3 🎾 39/1                         → over.ball score/wickets
  DEVDUTT PADIKKAL ON STRIKE          → batsman
  D-Payne TO D-Padikkal 4            → bowler to batsman, runs
  FOURR!!!                            → boundary
  WICKET!!!                           → wicket
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger("ipl_spotter.liveline")


class LiveLineParser:
    """Parses live line Telegram messages into structured match data."""

    def __init__(self) -> None:
        # Current state per match
        self.state: Dict[str, Any] = {
            "score": 0,
            "wickets": 0,
            "overs": 0.0,
            "batting_team": "",
            "striker": "",
            "bowler": "",
            "mw_back": 0,
            "mw_lay": 0,
            "mw_team": "",
            "session_6": 0,
            "session_10": 0,
            "session_15": 0,
            "session_20": 0,
            "session_no_6": 0,
            "session_no_10": 0,
            "session_no_15": 0,
            "session_no_20": 0,
            "last_ball_runs": 0,
            "last_event": "",
            "messages_parsed": 0,
        }

    def parse_message(self, text: str) -> Dict[str, Any]:
        """Parse a single message from the live line channel.

        Returns dict of what was found in this message.
        """
        if not text:
            return {}

        updates: Dict[str, Any] = {}
        lines = text.strip().split("\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Try each parser — session BEFORE match_winner (both start with numbers)
            parsed = (
                self._parse_session_line(line)
                or self._parse_score_ball(line)
                or self._parse_match_winner(line)
                or self._parse_striker(line)
                or self._parse_bowler_to_batsman(line)
                or self._parse_commentary(line)
            )

            if parsed:
                updates.update(parsed)
                # Update running state
                for k, v in parsed.items():
                    if k in self.state:
                        self.state[k] = v

        self.state["messages_parsed"] += 1
        return updates

    def _parse_match_winner(self, line: str) -> Optional[Dict]:
        """Parse: '38-41 🇸🇾 BENGALURU 🇸🇾' → MW back/lay rates."""
        # Pattern: number-number <emoji> TEAM <emoji>
        match = re.match(r'^(\d+)-(\d+)\s+.*?([A-Z]{2,}(?:\s+[A-Z]+)*)', line)
        if match:
            back = int(match.group(1))
            lay = int(match.group(2))
            team = match.group(3).strip()
            # Sanity: back must be less than lay (e.g. 38-41).
            # If back >= lay it's almost certainly a session line mis-parsed.
            if back >= lay:
                return None
            # Convert to decimal odds: 38 = 1.38, 150 = 2.50
            back_decimal = 1 + back / 100.0 if back < 100 else back / 100.0
            lay_decimal = 1 + lay / 100.0 if lay < 100 else lay / 100.0
            return {
                "type": "match_winner",
                "mw_back": back,
                "mw_lay": lay,
                "mw_back_decimal": round(back_decimal, 2),
                "mw_lay_decimal": round(lay_decimal, 2),
                "mw_team": team,
            }
        return None

    def _parse_session_line(self, line: str) -> Optional[Dict]:
        """Parse: '66-7 👈🏻🖥️ 6 OVER 🖥️' → session YES/NO."""
        # Pattern: number-number ... N OVER
        match = re.match(r'^(\d+)-(\d+)\s+.*?(\d+)\s*OVER', line, re.IGNORECASE)
        if match:
            yes_line = int(match.group(1))
            no_spread = int(match.group(2))
            over_target = int(match.group(3))

            key = f"session_{over_target}"
            no_key = f"session_no_{over_target}"
            return {
                "type": "session",
                "session_over": over_target,
                "session_yes": yes_line,
                "session_no": no_spread,
                key: yes_line,
                no_key: no_spread,
            }
        return None

    def _parse_score_ball(self, line: str) -> Optional[Dict]:
        """Parse: '3.3 🎾 39/1' → over.ball score/wickets."""
        match = re.match(r'^(\d+)\.(\d+)\s+.*?(\d+)/(\d+)', line)
        if match:
            over = int(match.group(1))
            ball = int(match.group(2))
            score = int(match.group(3))
            wickets = int(match.group(4))
            return {
                "type": "score",
                "overs": float(f"{over}.{ball}"),
                "over_num": over,
                "ball_num": ball,
                "score": score,
                "wickets": wickets,
            }
        return None

    def _parse_striker(self, line: str) -> Optional[Dict]:
        """Parse: 'DEVDUTT PADIKKAL ON STRIKE' → batsman name."""
        match = re.match(r'^([A-Z][A-Z\s\-\.]+)\s+ON\s+STRIKE', line, re.IGNORECASE)
        if match:
            name = match.group(1).strip().title()
            return {
                "type": "striker",
                "striker": name,
            }
        return None

    def _parse_bowler_to_batsman(self, line: str) -> Optional[Dict]:
        """Parse: 'D-Payne TO D-Padikkal 4' → bowler, batsman, runs."""
        match = re.match(
            r'^([A-Za-z\-\.]+(?:\s+[A-Za-z\-\.]+)?)\s+TO\s+([A-Za-z\-\.]+(?:\s+[A-Za-z\-\.]+)?)\s+(\d+|W)',
            line, re.IGNORECASE,
        )
        if match:
            bowler = match.group(1).strip().title()
            batsman = match.group(2).strip().title()
            result = match.group(3)
            is_wicket = result.upper() == "W"
            runs = 0 if is_wicket else int(result)
            return {
                "type": "delivery",
                "bowler": bowler,
                "striker": batsman,
                "last_ball_runs": runs,
                "is_wicket": is_wicket,
            }
        return None

    def _parse_commentary(self, line: str) -> Optional[Dict]:
        """Parse commentary: FOURR!!!, SIX!!!, WICKET!!!, etc."""
        upper = line.upper().strip()

        if "FOUR" in upper:
            return {"type": "commentary", "last_event": "FOUR", "last_ball_runs": 4}
        if "SIX" in upper or "SIXER" in upper:
            return {"type": "commentary", "last_event": "SIX", "last_ball_runs": 6}
        if "WICKET" in upper or "OUT" in upper:
            return {"type": "commentary", "last_event": "WICKET", "is_wicket": True}
        if "DOT" in upper or "NO RUN" in upper:
            return {"type": "commentary", "last_event": "DOT", "last_ball_runs": 0}
        if "WIDE" in upper:
            return {"type": "commentary", "last_event": "WIDE"}
        if "NO BALL" in upper:
            return {"type": "commentary", "last_event": "NOBALL"}
        if "MISSFIELD" in upper:
            return {"type": "commentary", "last_event": "MISSFIELD"}

        return None

    def get_state(self) -> Dict[str, Any]:
        """Return current parsed match state."""
        return dict(self.state)

    def reset_state(self) -> None:
        """Clear all session line state — call when a new match starts.

        Prevents stale lines from a previous match (or PSL per-over data)
        from polluting session line estimates for the new match.
        """
        for key in ("session_6", "session_10", "session_15", "session_20",
                    "session_no_6", "session_no_10", "session_no_15", "session_no_20"):
            self.state[key] = 0
        self.state["mw_back"] = 0
        self.state["mw_lay"] = 0
        self.state["mw_team"] = ""

    def get_indian_book_edge(self, model_expected: float, session_over: int) -> Optional[Dict]:
        """Compare model prediction vs Indian book session line.

        Returns edge info if there's a discrepancy.
        """
        key = f"session_{session_over}"
        book_line = self.state.get(key, 0)
        if not book_line:
            return None

        edge = model_expected - book_line
        if abs(edge) < 2:
            return None

        direction = "YES" if edge > 0 else "NO"
        return {
            "session_over": session_over,
            "book_line": book_line,
            "model_expected": model_expected,
            "edge": round(edge, 1),
            "direction": direction,
        }

    def format_state_summary(self) -> str:
        """One-line summary of current state from live line."""
        s = self.state
        parts = []
        if s["score"] or s["wickets"]:
            parts.append(f"{s['score']}/{s['wickets']} ({s['overs']})")
        if s["striker"]:
            parts.append(f"Bat: {s['striker']}")
        if s["bowler"]:
            parts.append(f"Bowl: {s['bowler']}")
        if s["mw_team"]:
            parts.append(f"MW: {s['mw_team']} {s['mw_back']}-{s['mw_lay']}")
        if s["session_6"]:
            parts.append(f"6ov: {s['session_6']}")
        return " | ".join(parts) if parts else "No data"
