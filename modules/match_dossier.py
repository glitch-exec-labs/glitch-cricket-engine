"""Pre-match dossier — built after toss when playing XI is known.

Fetches career stats for all 22 players from CricData API + our StatsDB,
then builds a match-specific intelligence report:
  - Each batsman: career SR, T20 avg, recent form, venue record
  - Each bowler: career econ, T20 wickets, death bowling stats
  - Key matchups: which bowler vs which batsman
  - Venue profile: avg scores, pace vs spin bias
  - Toss impact: bat first vs chase advantage at this venue

This dossier feeds into the predictor and context filter for smarter bets.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests

from modules.stats_db import StatsDB

try:
    from modules.espn_client import ESPNClient
except ImportError:
    ESPNClient = None

logger = logging.getLogger("ipl_spotter.match_dossier")

CRICDATA_API = "https://api.cricapi.com/v1"


class MatchDossier:
    """Builds a comprehensive player + venue dossier before the match starts."""

    def __init__(self, config: Dict[str, Any], stats_db: StatsDB):
        self.api_key = config.get("cricdata_api_key", "")
        self.stats_db = stats_db
        self.enabled = True   # ESPN is always available even without CricData key
        self._session = requests.Session()
        self._player_cache: Dict[str, Dict] = {}  # name -> stats

        # ESPN client for free T20 stats (IPL + PSL players)
        self._espn = ESPNClient() if ESPNClient is not None else None

        # Store the built dossier per match
        self.dossiers: Dict[int, Dict[str, Any]] = {}

        if self.api_key:
            logger.info("MatchDossier enabled (CricData API + ESPN)")
        else:
            logger.info("MatchDossier enabled (ESPN only — no CricData key)")

    def build(
        self,
        match_id: int,
        home: str,
        away: str,
        venue: str,
        competition: str = "IPL",
        batting_first: str = "",
        playing_xi_home: List[str] = None,
        playing_xi_away: List[str] = None,
    ) -> Dict[str, Any]:
        """Build the full match dossier. Call after toss."""
        dossier: Dict[str, Any] = {
            "match_id": match_id,
            "home": home,
            "away": away,
            "venue": venue,
            "competition": competition,
            "batting_first": batting_first,
            "venue_profile": self._build_venue_profile(venue),
            "home_players": [],
            "away_players": [],
            "key_batsmen": [],
            "key_bowlers": [],
            "matchup_notes": [],
        }

        # Build player profiles
        for name in (playing_xi_home or []):
            profile = self._build_player_profile(name, venue, competition)
            dossier["home_players"].append(profile)
            if profile.get("role") in ("Batsman", "WK-Batsman", "Batting Allrounder") or profile.get("t20_sr", 0) > 130:
                dossier["key_batsmen"].append(profile)
            if profile.get("role") in ("Bowler", "Bowling Allrounder") or profile.get("t20_econ", 0) > 0:
                dossier["key_bowlers"].append(profile)

        for name in (playing_xi_away or []):
            profile = self._build_player_profile(name, venue, competition)
            dossier["away_players"].append(profile)
            if profile.get("role") in ("Batsman", "WK-Batsman", "Batting Allrounder") or profile.get("t20_sr", 0) > 130:
                dossier["key_batsmen"].append(profile)
            if profile.get("role") in ("Bowler", "Bowling Allrounder") or profile.get("t20_econ", 0) > 0:
                dossier["key_bowlers"].append(profile)

        # Sort key players
        dossier["key_batsmen"].sort(key=lambda x: x.get("t20_sr", 0), reverse=True)
        dossier["key_bowlers"].sort(key=lambda x: x.get("t20_econ", 99))

        self.dossiers[match_id] = dossier
        logger.info(
            "Dossier built: %s vs %s — %d home, %d away players profiled",
            home, away, len(dossier["home_players"]), len(dossier["away_players"]),
        )
        return dossier

    def build_from_sportmonks(
        self,
        match_id: int,
        home: str,
        away: str,
        venue: str,
        batting_card: List[Dict],
        bowling_card: List[Dict],
        competition: str = "IPL",
        batting_first: str = "",
    ) -> Dict[str, Any]:
        """Build dossier from Sportmonks batting/bowling cards (live data)."""
        # Extract player names from cards
        all_names = set()
        for entry in batting_card:
            name = entry.get("name", "")
            if name:
                all_names.add(name)
        for entry in bowling_card:
            name = entry.get("name", "")
            if name:
                all_names.add(name)

        dossier: Dict[str, Any] = {
            "match_id": match_id,
            "home": home,
            "away": away,
            "venue": venue,
            "competition": competition,
            "batting_first": batting_first,
            "venue_profile": self._build_venue_profile(venue),
            "players": {},
            "key_batsmen": [],
            "key_bowlers": [],
        }

        for name in all_names:
            profile = self._build_player_profile(name, venue, competition)
            dossier["players"][name] = profile

        # Enrich with ESPN squads (confirmed playing XI + roles)
        if self._espn:
            try:
                comp = competition.lower()
                squads = self._espn.get_squads_for_match(home, away, comp)
                if squads:
                    for side in ("home", "away"):
                        for p in squads.get(side, {}).get("players", []):
                            pname = p["name"]
                            if pname not in dossier["players"]:
                                dossier["players"][pname] = self._build_player_profile(pname, venue, competition)
                            dossier["players"][pname]["role"]    = p.get("role", "")
                            dossier["players"][pname]["captain"] = p.get("captain", False)
                            dossier["players"][pname]["keeper"]  = p.get("keeper", False)
                    logger.info("ESPN squads enriched dossier: %s vs %s", home, away)
            except Exception:
                logger.debug("ESPN squad enrichment failed", exc_info=True)

        self.dossiers[match_id] = dossier
        logger.info("Dossier built from live cards: %s vs %s — %d players", home, away, len(all_names))
        return dossier

    def get_player_context(self, match_id: int, player_name: str) -> Optional[Dict]:
        """Get a player's dossier profile for real-time context."""
        dossier = self.dossiers.get(match_id)
        if not dossier:
            return None
        # Check players dict (from live cards)
        if "players" in dossier:
            return dossier["players"].get(player_name)
        # Check home/away lists
        for p in dossier.get("home_players", []) + dossier.get("away_players", []):
            if p.get("name") == player_name:
                return p
        return None

    def _build_player_profile(self, name: str, venue: str = "", competition: str = "IPL") -> Dict[str, Any]:
        """Build a player profile from CricData API + our StatsDB."""
        profile: Dict[str, Any] = {
            "name": name,
            "role": "",
            "batting_style": "",
            "bowling_style": "",
            "t20_runs": 0,
            "t20_avg": 0,
            "t20_sr": 0,
            "t20_innings": 0,
            "t20_econ": 0,
            "t20_wickets": 0,
            "t20_bowl_avg": 0,
            "venue_sr": 0,
            "venue_econ": 0,
            "venue_innings": 0,
            "source": "none",
        }

        # 1. Try CricData API (has format-split T20 stats)
        cricdata = self._fetch_cricdata_player(name)
        if cricdata:
            profile.update(cricdata)
            profile["source"] = "cricdata"

        # 2. Enrich from our StatsDB (IPL/PSL specific)
        db_batting = self.stats_db.get_player_batting_stats(name)
        if db_batting and db_batting.get("innings", 0) > 0:
            profile["db_sr"] = db_batting.get("avg_strike_rate", 0)
            profile["db_avg"] = db_batting.get("avg_runs", 0)
            profile["db_innings"] = db_batting.get("innings", 0)
            if profile["source"] == "none":
                profile["t20_sr"] = profile["db_sr"]
                profile["source"] = "statsdb"

        db_bowling = self.stats_db.get_bowler_stats(name)
        if db_bowling and db_bowling.get("innings", 0) > 0:
            profile["db_econ"] = db_bowling.get("avg_economy", 0)
            profile["db_wickets"] = db_bowling.get("total_wickets", 0)
            if profile["source"] == "none":
                profile["t20_econ"] = profile["db_econ"]
                profile["source"] = "statsdb"

        # 3. Venue-specific stats from our DB
        if venue:
            venue_bat = self.stats_db.get_player_batting_stats(name, venue=venue)
            if venue_bat and venue_bat.get("innings", 0) >= 3:
                profile["venue_sr"] = venue_bat.get("avg_strike_rate", 0)
                profile["venue_innings"] = venue_bat.get("innings", 0)

            venue_bowl = self.stats_db.get_bowler_stats(name, venue=venue)
            if venue_bowl and venue_bowl.get("innings", 0) >= 3:
                profile["venue_econ"] = venue_bowl.get("avg_economy", 0)

        return profile

    def _fetch_cricdata_player(self, name: str) -> Optional[Dict]:
        """Fetch player T20 stats from CricData API."""
        if not self.enabled:
            return None

        # Check cache
        if name in self._player_cache:
            return self._player_cache[name]

        try:
            # Search for player
            resp = self._session.get(
                f"{CRICDATA_API}/players",
                params={"apikey": self.api_key, "offset": 0, "search": name},
                timeout=10,
            )
            if resp.status_code != 200:
                return None

            data = resp.json()
            players = data.get("data", [])
            if not players:
                self._player_cache[name] = None
                return None

            # Get first match
            player_id = players[0].get("id")
            if not player_id:
                return None

            # Fetch full stats
            resp2 = self._session.get(
                f"{CRICDATA_API}/players_info",
                params={"apikey": self.api_key, "id": player_id},
                timeout=10,
            )
            if resp2.status_code != 200:
                return None

            pdata = resp2.json().get("data", {})
            stats = pdata.get("stats", [])

            # Parse T20 stats
            result = {
                "name": pdata.get("name", name),
                "role": pdata.get("role", ""),
                "batting_style": pdata.get("battingStyle", ""),
                "bowling_style": pdata.get("bowlingStyle", ""),
            }

            for s in stats:
                if s.get("matchtype") == "t20" and s.get("fn") == "batting":
                    stat = s.get("stat", "").strip()
                    val = s.get("value", "0").strip()
                    try:
                        if stat == "sr":
                            result["t20_sr"] = float(val)
                        elif stat == "avg":
                            result["t20_avg"] = float(val)
                        elif stat == "runs":
                            result["t20_runs"] = int(val)
                        elif stat == "inn":
                            result["t20_innings"] = int(val)
                    except (ValueError, TypeError):
                        pass

                if s.get("matchtype") == "t20" and s.get("fn") == "bowling":
                    stat = s.get("stat", "").strip()
                    val = s.get("value", "0").strip()
                    try:
                        if stat == "econ":
                            result["t20_econ"] = float(val)
                        elif stat == "wkts":
                            result["t20_wickets"] = int(val)
                        elif stat == "avg":
                            result["t20_bowl_avg"] = float(val)
                    except (ValueError, TypeError):
                        pass

            self._player_cache[name] = result
            return result

        except Exception:
            logger.debug("CricData player fetch failed for %s", name, exc_info=True)
            self._player_cache[name] = None
            return None

    def _build_venue_profile(self, venue: str) -> Dict[str, Any]:
        """Get venue stats from our DB."""
        stats = self.stats_db.get_venue_stats(venue) or {}
        return {
            "venue": venue,
            "matches": stats.get("matches", 0),
            "avg_first_innings": stats.get("avg_first_innings", 0),
            "avg_second_innings": stats.get("avg_second_innings", 0),
            "avg_powerplay_1st": stats.get("avg_powerplay_1st", 0),
        }

    def format_dossier(self, match_id: int) -> str:
        """Format dossier for Telegram."""
        d = self.dossiers.get(match_id)
        if not d:
            return "No dossier available"

        lines = [
            f"\U0001f4d1 MATCH DOSSIER: {d['home']} vs {d['away']}",
            f"   Venue: {d['venue']}",
        ]

        vp = d.get("venue_profile", {})
        if vp.get("matches"):
            lines.append(f"   Venue avg: 1st inn {vp['avg_first_innings']:.0f} | 2nd inn {vp['avg_second_innings']:.0f} ({vp['matches']} matches)")

        if d.get("batting_first"):
            lines.append(f"   Batting first: {d['batting_first']}")

        # Top batsmen
        key_bats = d.get("key_batsmen", [])[:5]
        if key_bats:
            lines.append(f"\n   Top Batsmen (T20 SR):")
            for p in key_bats:
                sr = p.get("t20_sr", 0)
                avg = p.get("t20_avg", 0)
                v_sr = p.get("venue_sr", 0)
                venue_text = f" | venue SR:{v_sr:.0f}" if v_sr else ""
                lines.append(f"   \u2022 {p['name']}: SR {sr:.0f} avg {avg:.0f}{venue_text}")

        # Top bowlers
        key_bowls = d.get("key_bowlers", [])[:5]
        if key_bowls:
            lines.append(f"\n   Top Bowlers (T20 econ):")
            for p in key_bowls:
                econ = p.get("t20_econ", 0)
                wkts = p.get("t20_wickets", 0)
                v_econ = p.get("venue_econ", 0)
                venue_text = f" | venue econ:{v_econ:.1f}" if v_econ else ""
                if econ > 0:
                    lines.append(f"   \u2022 {p['name']}: econ {econ:.1f} wkts {wkts}{venue_text}")

        return "\n".join(lines)
