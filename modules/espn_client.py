"""
ESPN Cricket Client — free, no API key required.

Three jobs:
  1. Speed supplement  — live score at 1-5s TTL (faster than Sportmonks 8-15s)
                         feeds into spotter as a faster score source
  2. Player stats      — T20 career stats for IPL + PSL players (replaces CricData)
  3. Post-match data   — ball-by-ball commentary after matches for ML training

ESPN league IDs:
  IPL = 8048
  PSL = 8679

All endpoints are public, no auth, CORS open.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger("ipl_spotter.espn")

BASE_ESPN   = "https://site.api.espn.com/apis/site/v2/sports/cricket"
BASE_CORE   = "http://new.core.espnuk.org/v2/sports/cricket"
TIMEOUT     = 8

LEAGUE_IDS = {"ipl": 8048, "psl": 8679}


class ESPNClient:
    """ESPN cricket data client — free, zero auth."""

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; CricketBot/1.0)",
            "Accept": "application/json",
        })
        # Player search cache: name → athlete_id
        self._player_id_cache: Dict[str, Optional[int]] = {}
        # Player stats cache: athlete_id → stats dict
        self._player_stats_cache: Dict[int, Dict] = {}
        logger.info("ESPNClient ready (IPL=8048, PSL=8679)")

    def _get(self, url: str, params: dict = None) -> Optional[dict]:
        try:
            r = self._session.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            logger.debug("ESPN %s → %d", url, r.status_code)
        except Exception as e:
            logger.debug("ESPN fetch error: %s", e)
        return None

    # ── 1. Speed Supplement — Live Scores ────────────────────────────────────

    def get_live_scores(self, competition: str = "ipl") -> List[Dict[str, Any]]:
        """Return live match score summaries.

        Each dict has:
            espn_event_id, home, away, venue,
            innings (list of {team, score, wickets, overs, batting}),
            status ('live' | 'pre' | 'post'),
            summary  (e.g. "RCB 143/4 (14.2 ov)")
        """
        league_id = LEAGUE_IDS.get(competition, LEAGUE_IDS["ipl"])
        data = self._get(f"{BASE_ESPN}/{league_id}/scoreboard")
        if not data:
            return []

        results = []
        for event in data.get("events", []):
            state = event.get("status", {}).get("type", {}).get("state", "")
            if state not in ("in", "pre"):
                continue

            competitors = event.get("competitions", [{}])[0].get("competitors", [])
            innings_list = []
            for c in competitors:
                team = c.get("team", {}).get("displayName", "")
                score_str = c.get("score", "")     # "143/4 (14.2/20 ov)"
                is_batting = c.get("batting", False)
                runs, wickets, overs = _parse_score_str(score_str)
                innings_list.append({
                    "team": team,
                    "score": runs,
                    "wickets": wickets,
                    "overs": overs,
                    "batting": is_batting,
                    "score_str": score_str,
                })

            venue_name = (
                event.get("competitions", [{}])[0]
                    .get("venue", {})
                    .get("fullName", "")
            )
            results.append({
                "espn_event_id": event.get("id"),
                "name": event.get("name", ""),
                "home": competitors[0].get("team", {}).get("displayName", "") if competitors else "",
                "away": competitors[1].get("team", {}).get("displayName", "") if len(competitors) > 1 else "",
                "venue": venue_name,
                "innings": innings_list,
                "status": state,
                "summary": event.get("status", {}).get("type", {}).get("detail", ""),
            })

        logger.debug("ESPN live scores (%s): %d matches", competition, len(results))
        return results

    def get_live_score_for_match(
        self, home: str, away: str, competition: str = "ipl"
    ) -> Optional[Dict[str, Any]]:
        """Find the live score for a specific match by team names."""
        scores = self.get_live_scores(competition)
        h = home.lower()
        a = away.lower()
        for s in scores:
            sh = s["home"].lower()
            sa = s["away"].lower()
            # fuzzy: match on last word of team name (e.g. "Qalandars" in "Lahore Qalandars")
            if (_fuzzy_team_match(h, sh) and _fuzzy_team_match(a, sa)) or \
               (_fuzzy_team_match(h, sa) and _fuzzy_team_match(a, sh)):
                return s
        return None

    # ── 2. Pre-match squads (playing XI with roles) ───────────────────────────

    def get_squads(
        self, espn_event_id: str, competition: str = "ipl"
    ) -> Optional[Dict[str, Any]]:
        """Fetch confirmed playing XI for both teams from ESPN summary.

        Uses the 'rosters' field (actual 11 players) not 'squads' (full 20+ registered squad).

        Returns:
            home: { team_name, players: [{id, name, role, captain, keeper,
                                          batting_style, bowling_style}] }
            away: same
        """
        league_id = LEAGUE_IDS.get(competition, LEAGUE_IDS["ipl"])
        data = self._get(
            f"{BASE_ESPN}/{league_id}/summary",
            params={"event": espn_event_id},
        )
        if not data:
            return None

        # Use rosters (playing XI) — falls back to squads if rosters empty
        rosters = data.get("rosters", [])
        use_squads = False
        if not rosters or all(len(r.get("roster", [])) == 0 for r in rosters):
            rosters = data.get("squads", [])
            use_squads = True

        if not rosters:
            return None

        result = {}
        for i, roster in enumerate(rosters[:2]):
            side  = "home" if i == 0 else "away"
            team  = roster.get("team", {}).get("displayName", "")

            raw_players = roster.get("roster", []) if not use_squads else roster.get("athletes", [])
            players = []

            for entry in raw_players:
                if use_squads:
                    a = entry
                    captain = a.get("captain", False)
                    keeper  = a.get("keeper", False)
                    pos     = a.get("position", {})
                    name    = a.get("fullName", a.get("displayName", ""))
                    pid     = a.get("id", "")
                    styles  = a.get("style", [])
                else:
                    a       = entry.get("athlete", {})
                    captain = entry.get("captain", False)
                    keeper  = entry.get("keeper", False)
                    pos     = entry.get("position", a.get("position", {}))
                    name    = a.get("fullName", a.get("displayName", ""))
                    pid     = a.get("id", "")
                    styles  = a.get("style", [])

                batting_style  = next((s["shortDescription"] for s in styles if s.get("type") == "batting"), "")
                bowling_style  = next((s["shortDescription"] for s in styles if s.get("type") == "bowling"), "")

                cricinfo_id = None
                for link in a.get("links", []):
                    m = re.search(r"/player/(\d+)", link.get("href", ""))
                    if m:
                        cricinfo_id = int(m.group(1))
                        break

                players.append({
                    "id":            pid,
                    "cricinfo_id":   cricinfo_id,
                    "name":          name,
                    "role":          pos.get("name", ""),
                    "role_abbr":     pos.get("abbreviation", ""),
                    "captain":       captain,
                    "keeper":        keeper,
                    "batting_style": batting_style,
                    "bowling_style": bowling_style,
                })

            result[side] = {"team": team, "players": players}

        return result

    def get_squads_for_match(
        self, home: str, away: str, competition: str = "ipl"
    ) -> Optional[Dict[str, Any]]:
        """Find the ESPN event for home vs away and return squads."""
        scores = self.get_live_scores(competition)
        for s in scores:
            sh, sa = s["home"].lower(), s["away"].lower()
            if (_fuzzy_team_match(home.lower(), sh) and _fuzzy_team_match(away.lower(), sa)) or \
               (_fuzzy_team_match(home.lower(), sa) and _fuzzy_team_match(away.lower(), sh)):
                eid = s.get("espn_event_id")
                if eid:
                    return self.get_squads(eid, competition)
        return None

    def get_match_scorecard(
        self, espn_event_id: str, competition: str = "ipl"
    ) -> Optional[Dict[str, Any]]:
        """Fetch full scorecard for a match.

        Returns:
            batting_cards: [{name, runs, balls, fours, sixes, dismissal}, ...]
            bowling_cards: [{name, overs, wickets, runs, econ, maidens}, ...]
            toss: {winner, decision}
            venue: str
        """
        league_id = LEAGUE_IDS.get(competition, LEAGUE_IDS["ipl"])
        data = self._get(
            f"{BASE_ESPN}/{league_id}/summary",
            params={"event": espn_event_id},
        )
        if not data:
            return None

        batting_cards  = []
        bowling_cards  = []

        for card in data.get("matchcards", []):
            card_type = card.get("type", "").lower()
            entries   = card.get("batting" if "bat" in card_type else "bowling", [])

            if "bat" in card_type:
                for e in entries:
                    batting_cards.append({
                        "name":      e.get("playerName", ""),
                        "runs":      _safe_int(e.get("runs")),
                        "balls":     _safe_int(e.get("ballsFaced")),
                        "fours":     _safe_int(e.get("fours")),
                        "sixes":     _safe_int(e.get("sixes")),
                        "dismissal": e.get("dismissal", ""),
                    })
            elif "bowl" in card_type:
                for e in entries:
                    bowling_cards.append({
                        "name":    e.get("playerName", ""),
                        "overs":   _safe_float(e.get("overs")),
                        "wickets": _safe_int(e.get("wickets")),
                        "runs":    _safe_int(e.get("conceded")),
                        "econ":    _safe_float(e.get("economyRate")),
                        "maidens": _safe_int(e.get("maidens")),
                    })

        # Toss + venue from gameInfo
        game_info = data.get("gameInfo", {})
        toss_text = game_info.get("toss", "")       # "Mumbai Indians won the toss and elected to bat"
        toss = _parse_toss_text(toss_text)
        venue = game_info.get("venue", {}).get("fullName", "")

        return {
            "batting_cards": batting_cards,
            "bowling_cards": bowling_cards,
            "toss": toss,
            "venue": venue,
        }

    # ── 3. Fixtures ───────────────────────────────────────────────────────────

    def get_fixtures(self, competition: str = "psl") -> List[Dict[str, Any]]:
        """Get upcoming and live fixtures for a competition."""
        league_id = LEAGUE_IDS.get(competition, LEAGUE_IDS["ipl"])
        data = self._get(f"{BASE_ESPN}/{league_id}/scoreboard")
        if not data:
            return []

        fixtures = []
        for event in data.get("events", []):
            competitors = event.get("competitions", [{}])[0].get("competitors", [])
            home = competitors[0].get("team", {}).get("displayName", "") if competitors else ""
            away = competitors[1].get("team", {}).get("displayName", "") if len(competitors) > 1 else ""
            state = event.get("status", {}).get("type", {}).get("state", "")
            fixtures.append({
                "espn_event_id": event.get("id"),
                "name": event.get("name", ""),
                "home": home,
                "away": away,
                "date": event.get("date", ""),
                "venue": event.get("competitions", [{}])[0].get("venue", {}).get("fullName", ""),
                "status": state,
            })
        return fixtures

    # ── 4. Post-match ball-by-ball (for ML training) ──────────────────────────

    def get_post_match_plays(
        self, espn_event_id: str, competition: str = "ipl", max_balls: int = 300
    ) -> List[Dict[str, Any]]:
        """Fetch ball-by-ball play data after a match completes.

        Note: ESPN core API caches this for 3 hours — only call after match ends.
        Returns list of ball dicts with: over, ball, runs, wicket, batsman, bowler,
                                         commentary, speed_kph, boundary
        """
        league_id = LEAGUE_IDS.get(competition, LEAGUE_IDS["ipl"])
        # Get play list (just $ref links)
        plays_data = self._get(
            f"{BASE_CORE}/leagues/{league_id}/events/{espn_event_id}"
            f"/competitions/{espn_event_id}/plays",
        )
        if not plays_data:
            return []

        play_refs = plays_data.get("items", [])[:max_balls]
        balls = []

        for ref_item in play_refs:
            ref = ref_item.get("$ref", "")
            if not ref:
                continue
            ball_data = self._get(ref)
            if not ball_data:
                continue

            over_info = ball_data.get("over", {})
            innings   = ball_data.get("innings", {})
            bat       = ball_data.get("batsman", {})
            bowl      = ball_data.get("bowler", {})
            dismissal = ball_data.get("dismissal", {})

            balls.append({
                "over":        over_info.get("number", 0),
                "ball":        over_info.get("ball", 0),
                "runs":        over_info.get("runs", 0),
                "wicket":      dismissal.get("dismissal", False),
                "boundary":    ball_data.get("boundary", False),
                "wide":        over_info.get("wide", False),
                "no_ball":     over_info.get("noBall", False),
                "speed_kph":   ball_data.get("speedKPH", 0),
                "commentary":  ball_data.get("text", ""),
                "batsman":     _resolve_name(bat),
                "bowler":      _resolve_name(bowl),
                "innings_runs":    innings.get("totalRuns", 0),
                "innings_wickets": innings.get("wickets", 0),
                "innings_overs":   innings.get("balls", 0) / 6,
                "run_rate":        innings.get("runRate", 0),
            })
            time.sleep(0.05)   # gentle rate limiting

        logger.info("ESPN post-match: %d balls fetched for event %s", len(balls), espn_event_id)
        return balls


# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_score_str(s: str) -> Tuple[int, int, float]:
    """Parse '143/4 (14.2/20 ov)' → (143, 4, 14.2)."""
    try:
        runs_wkts, rest = s.split("(", 1)
        runs_wkts = runs_wkts.strip()
        if "/" in runs_wkts:
            runs, wkts = runs_wkts.split("/")
            runs, wkts = int(runs), int(wkts)
        else:
            runs, wkts = int(runs_wkts), 0
        overs_m = re.search(r"([\d.]+)\s*/?\s*\d+\s*ov", rest)
        overs = float(overs_m.group(1)) if overs_m else 0.0
        return runs, wkts, overs
    except Exception:
        return 0, 0, 0.0


def _fuzzy_team_match(name1: str, name2: str) -> bool:
    """True if any word >4 chars from name1 appears in name2."""
    for word in name1.split():
        if len(word) > 4 and word in name2:
            return True
    return False


def _parse_toss_text(text: str) -> Dict[str, str]:
    """'Mumbai Indians won the toss and elected to bat' → {winner, decision}."""
    text = text.lower()
    decision = "bat" if "bat" in text else "field" if "field" in text else ""
    winner = text.split(" won")[0].title() if " won" in text else ""
    return {"winner": winner, "decision": decision}


def _safe_int(v: Any) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _safe_float(v: Any) -> float:
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return 0.0


def _resolve_name(athlete_dict: dict) -> str:
    """Extract name from ESPN athlete dict (may just have $ref)."""
    return athlete_dict.get("fullName", athlete_dict.get("name", ""))
