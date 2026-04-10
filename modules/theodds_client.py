"""
The Odds API client — fetches consensus odds from 27+ bookmakers for IPL.

Gives us sharp fair probability for match winner by de-vigging across
Pinnacle, Betfair, DraftKings, William Hill, bet365, etc.

API docs: https://the-odds-api.com/liveapi/guides/v4/
Free tier: 500 requests/month
"""

import logging
import time
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger("ipl_spotter.theodds")

BASE_URL = "https://api.the-odds-api.com/v4"
DEFAULT_TIMEOUT = 15

SPORT_KEYS = {
    "ipl": "cricket_ipl",
    "psl": "cricket_psl",
}


class TheOddsClient:
    """Fetches consensus IPL odds from The Odds API (27+ bookmakers)."""

    def __init__(self, config: dict):
        self.api_key = config.get("theodds_api_key", "")
        self.enabled = bool(self.api_key)
        self._cache: Dict[str, Tuple[float, dict]] = {}  # event_id → (timestamp, data)
        self._cache_ttl = 120  # 2 min cache to save quota
        self.requests_remaining: Optional[int] = None

        # Connection pooling for faster repeat requests
        self._session = requests.Session()

        # Per-scan fair-prob cache to avoid redundant calls within same cycle
        self._fair_prob_cache: Dict[str, Tuple[float, Optional[dict]]] = {}
        self._fair_prob_ttl = 10  # seconds — dedups within a single scan

        if self.enabled:
            logger.info("The Odds API client enabled (IPL match winner odds)")
        else:
            logger.info("The Odds API disabled — add theodds_api_key to config")

    def get_odds(self, competition: str = "ipl") -> List[dict]:
        """Fetch match winner odds for a competition from 27+ bookmakers."""
        if not self.enabled:
            return []

        sport_key = SPORT_KEYS.get(competition, SPORT_KEYS["ipl"])
        try:
            resp = self._session.get(
                f"{BASE_URL}/sports/{sport_key}/odds/",
                params={
                    "apiKey": self.api_key,
                    "regions": "us,uk,eu,au",
                    "markets": "h2h",
                    "oddsFormat": "decimal",
                },
                timeout=10,
            )

            # Track quota
            self.requests_remaining = int(resp.headers.get("x-requests-remaining", -1))
            if self.requests_remaining >= 0:
                logger.debug("Odds API quota remaining: %d", self.requests_remaining)

            resp.raise_for_status()
            events = resp.json()

            # Cache each event
            now = time.time()
            for e in events:
                self._cache[e.get("id", "")] = (now, e)

            logger.info("Fetched %s odds: %d events", competition.upper(), len(events))
            return events

        except Exception as exc:
            logger.error("Failed to fetch %s odds: %s", competition.upper(), exc)
            return []

    def get_ipl_odds(self) -> List[dict]:
        """Backward-compatible."""
        return self.get_odds("ipl")

    def get_psl_odds(self) -> List[dict]:
        """Fetch PSL odds."""
        return self.get_odds("psl")

    def get_fair_probability(self, home_team: str, away_team: str, competition: str = "ipl") -> Optional[dict]:
        """
        Get consensus fair probability for a match by de-vigging across all bookmakers.

        Returns:
            {
                "home_team": str,
                "away_team": str,
                "home_fair_prob": float,  # 0-1
                "away_fair_prob": float,
                "home_best_odds": float,
                "away_best_odds": float,
                "home_best_book": str,
                "away_best_book": str,
                "bookmakers_count": int,
                "pinnacle_home": float or None,
                "pinnacle_away": float or None,
            }
        """
        if not self.enabled:
            return None

        # Dedup: return cached result if same teams queried within same scan cycle
        cache_key = f"{home_team.lower()}|{away_team.lower()}|{competition}"
        cached = self._fair_prob_cache.get(cache_key)
        if cached is not None:
            ts, result = cached
            if time.time() - ts < self._fair_prob_ttl:
                return result

        events = self._get_cached_or_fetch(competition)

        for event in events:
            eh = event.get("home_team", "").lower()
            ea = event.get("away_team", "").lower()
            ht = home_team.lower()
            at = away_team.lower()

            # Fuzzy match team names
            if not (_teams_match(ht, eh) and _teams_match(at, ea)):
                if not (_teams_match(ht, ea) and _teams_match(at, eh)):
                    continue

            result = self._compute_fair_probs(event)
            self._fair_prob_cache[cache_key] = (time.time(), result)
            return result

        self._fair_prob_cache[cache_key] = (time.time(), None)
        return None

    def get_best_odds(self, home_team: str, away_team: str) -> Optional[dict]:
        """Get the best available odds for each side across all bookmakers."""
        result = self.get_fair_probability(home_team, away_team)
        return result

    def _get_cached_or_fetch(self, competition: str = "ipl") -> list:
        """Return cached events if fresh, otherwise fetch new for the given competition."""
        now = time.time()
        if self._cache:
            oldest = min(ts for ts, _ in self._cache.values())
            if now - oldest < self._cache_ttl:
                return [data for _, data in self._cache.values()]
        return self.get_odds(competition)

    def _compute_fair_probs(self, event: dict) -> dict:
        """De-vig across all bookmakers to get consensus fair probability."""
        bookmakers = event.get("bookmakers", [])

        all_home_implied = []
        all_away_implied = []
        home_best = 0.0
        away_best = 0.0
        home_best_book = ""
        away_best_book = ""
        pinnacle_home = None
        pinnacle_away = None

        home_team = event.get("home_team", "")
        away_team = event.get("away_team", "")

        for bm in bookmakers:
            bm_name = bm.get("title", "")
            for mkt in bm.get("markets", []):
                if mkt.get("key") != "h2h":
                    continue

                outcomes = {o["name"]: o["price"] for o in mkt.get("outcomes", [])}
                h_price = outcomes.get(home_team, 0)
                a_price = outcomes.get(away_team, 0)

                if h_price <= 1 or a_price <= 1:
                    continue

                h_implied = 1.0 / h_price
                a_implied = 1.0 / a_price

                all_home_implied.append(h_implied)
                all_away_implied.append(a_implied)

                if h_price > home_best:
                    home_best = h_price
                    home_best_book = bm_name
                if a_price > away_best:
                    away_best = a_price
                    away_best_book = bm_name

                if bm_name.lower() == "pinnacle":
                    pinnacle_home = h_price
                    pinnacle_away = a_price

        if not all_home_implied:
            return {
                "home_team": home_team, "away_team": away_team,
                "home_fair_prob": 0.5, "away_fair_prob": 0.5,
                "home_best_odds": 0, "away_best_odds": 0,
                "home_best_book": "", "away_best_book": "",
                "bookmakers_count": 0,
                "pinnacle_home": None, "pinnacle_away": None,
            }

        # De-vig: average implied probs, then normalize to sum to 1
        avg_home = sum(all_home_implied) / len(all_home_implied)
        avg_away = sum(all_away_implied) / len(all_away_implied)
        total = avg_home + avg_away

        home_fair = round(avg_home / total, 4)
        away_fair = round(avg_away / total, 4)

        return {
            "home_team": home_team,
            "away_team": away_team,
            "home_fair_prob": home_fair,
            "away_fair_prob": away_fair,
            "home_best_odds": round(home_best, 3),
            "away_best_odds": round(away_best, 3),
            "home_best_book": home_best_book,
            "away_best_book": away_best_book,
            "bookmakers_count": len(all_home_implied),
            "pinnacle_home": pinnacle_home,
            "pinnacle_away": pinnacle_away,
        }


def _teams_match(name1: str, name2: str) -> bool:
    """Fuzzy match IPL team names."""
    # Direct containment
    if name1 in name2 or name2 in name1:
        return True

    # IPL abbreviation mappings
    aliases = {
        "rcb": ["royal challengers", "bangalore", "bengaluru"],
        "mi": ["mumbai indians", "mumbai"],
        "csk": ["chennai super kings", "chennai"],
        "kkr": ["kolkata knight riders", "kolkata"],
        "dc": ["delhi capitals", "delhi"],
        "rr": ["rajasthan royals", "rajasthan"],
        "srh": ["sunrisers hyderabad", "sunrisers", "hyderabad"],
        "pbks": ["punjab kings", "punjab", "kings xi"],
        "gt": ["gujarat titans", "gujarat"],
        "lsg": ["lucknow super giants", "lucknow"],
    }

    for abbr, names in aliases.items():
        if any(n in name1 for n in names) and any(n in name2 for n in names):
            return True
        if abbr == name1 and any(n in name2 for n in names):
            return True
        if abbr == name2 and any(n in name1 for n in names):
            return True

    # Word overlap
    words1 = set(name1.split())
    words2 = set(name2.split())
    overlap = words1 & words2
    if len(overlap) >= 1 and len(overlap) / max(len(words1), len(words2)) >= 0.4:
        return True

    return False
