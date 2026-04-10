"""
IPL Edge Spotter — cricket_client.py
Sportmonks Cricket API client for fetching live ball-by-ball IPL data.

Wraps the Sportmonks Cricket v2.0 API:
  - get_live_ipl_matches()   -> live IPL matches with balls, runs, teams
  - get_match_balls()        -> ball-by-ball data for a fixture
  - get_match_details()      -> full match data (balls, batting, bowling, runs, venue, teams)
  - get_ipl_fixtures()       -> list IPL fixtures for a season
  - get_current_season_id()  -> current IPL season ID from league endpoint
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("ipl_spotter.cricket")

DEFAULT_BASE_URL = "https://cricket.sportmonks.com/api/v2.0"
DEFAULT_TIMEOUT = 10
IPL_LEAGUE_ID = 1

LEAGUE_IDS = {
    "ipl": 1,
    "psl": 8,
}


class CricketClient:
    """
    Sportmonks Cricket API client.

    Initialised with a config dict containing:
      - sportmonks_api_key: API token for authentication
      - sportmonks_base_url: (optional) override base URL
    """

    def __init__(self, config: dict):
        self.api_key = config.get("sportmonks_api_key", "")
        if not self.api_key:
            logger.warning("sportmonks_api_key not set in config — API calls will fail")

        self.base_url = config.get("sportmonks_base_url", DEFAULT_BASE_URL).rstrip("/")
        self.timeout = DEFAULT_TIMEOUT
        self.league_id = config.get("ipl_league_id", IPL_LEAGUE_ID)

        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _request(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Make a GET request to the Sportmonks API.
        Returns the full JSON response dict, or None on failure.
        """
        url = f"{self.base_url}{path}"
        req_params = {"api_token": self.api_key}
        if params:
            req_params.update(params)

        try:
            resp = self._session.get(url, params=req_params, timeout=self.timeout)
            if resp.status_code >= 400:
                logger.error(
                    "Sportmonks API error: %s %s -> HTTP %d: %s",
                    "GET", path, resp.status_code, resp.text[:300],
                )
                return None
            return resp.json()
        except requests.RequestException as exc:
            logger.error("Sportmonks request failed: %s %s -> %s", "GET", path, exc)
            return None
        except ValueError as exc:
            logger.error("Sportmonks JSON decode error: %s %s -> %s", "GET", path, exc)
            return None

    @staticmethod
    def _extract_data(response: Optional[Dict[str, Any]]) -> Any:
        """
        Extract the 'data' field from a Sportmonks response.
        Sportmonks wraps results in {"data": ...}. This handles both
        wrapped and unwrapped formats, returning [] if missing.
        """
        if response is None:
            return []
        if isinstance(response, dict):
            return response.get("data", [])
        return response

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_live_matches(self, competition: str = "ipl") -> List[dict]:
        """Fetch live matches for a specific competition."""
        league_id = LEAGUE_IDS.get(competition, LEAGUE_IDS["ipl"])
        resp = self._request(
            "/livescores",
            params={"include": "balls.score,batting.batsman,bowling.bowler,runs,localteam,visitorteam,venue"},
        )
        all_live = self._extract_data(resp)
        if not isinstance(all_live, list):
            all_live = [all_live] if all_live else []

        matches = [m for m in all_live if m.get("league_id") == league_id]
        logger.info("Live %s matches: %d (of %d total live)", competition.upper(), len(matches), len(all_live))
        return matches

    def get_live_ipl_matches(self) -> List[dict]:
        """Backward-compatible."""
        return self.get_live_matches("ipl")

    def get_live_psl_matches(self) -> List[dict]:
        """Fetch live PSL matches."""
        return self.get_live_matches("psl")

    def get_match_balls(self, fixture_id: int) -> List[dict]:
        """
        Fetch ball-by-ball data for a specific fixture, including score details.
        Returns a list of ball dicts, or empty list on failure.
        """
        resp = self._request(
            f"/fixtures/{fixture_id}",
            params={"include": "balls.score"},
        )
        fixture = self._extract_data(resp)
        if not isinstance(fixture, dict):
            logger.warning("Unexpected fixture data type for %s", fixture_id)
            return []

        balls = fixture.get("balls", {})
        return self._extract_nested_data(balls)

    def get_match_details(self, fixture_id: int) -> Optional[dict]:
        """
        Fetch full match details: balls, batting, bowling, runs, venue, teams.
        Returns the fixture dict, or None on failure.
        """
        resp = self._request(
            f"/fixtures/{fixture_id}",
            params={"include": "balls.score,batting.batsman,bowling.bowler,runs,localteam,visitorteam,venue"},
        )
        fixture = self._extract_data(resp)
        if not isinstance(fixture, dict):
            logger.warning("Unexpected fixture data type for %s", fixture_id)
            return None
        return fixture

    def get_fixtures(self, season_id: int, competition: str = "ipl") -> List[dict]:
        """List fixtures for a season with team and venue details."""
        league_id = LEAGUE_IDS.get(competition, LEAGUE_IDS["ipl"])
        resp = self._request(
            "/fixtures",
            params={
                "filter[league_id]": league_id,
                "filter[season_id]": season_id,
                "include": "localteam,visitorteam,venue",
            },
        )
        fixtures = self._extract_data(resp)
        if not isinstance(fixtures, list):
            return []
        logger.info("%s fixtures for season %d: %d", competition.upper(), season_id, len(fixtures))
        return fixtures

    def get_ipl_fixtures(self, season_id: int) -> List[dict]:
        """Backward-compatible."""
        return self.get_fixtures(season_id, "ipl")

    def get_season_id(self, competition: str = "ipl") -> Optional[int]:
        """Get current season ID for a competition."""
        league_id = LEAGUE_IDS.get(competition, LEAGUE_IDS["ipl"])
        resp = self._request(
            f"/leagues/{league_id}",
            params={"include": ""},
        )
        league = self._extract_data(resp)
        if not isinstance(league, dict):
            logger.warning("Could not parse league data for league_id=%d", league_id)
            return None

        season_id = league.get("current_season_id") or league.get("season_id")
        if season_id is not None:
            try:
                season_id = int(season_id)
            except (TypeError, ValueError):
                logger.error("Invalid season_id value: %s", season_id)
                return None
            logger.info("Current %s season_id: %d", competition.upper(), season_id)
        return season_id

    def get_current_season_id(self) -> Optional[int]:
        """Backward-compatible."""
        return self.get_season_id("ipl")

    # ── Parsing helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _extract_nested_data(obj: Any) -> list:
        """
        Sportmonks nests included relations as {"data": [...]} objects.
        This extracts the inner list whether it's wrapped or direct.
        """
        if obj is None:
            return []
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            return obj.get("data", [])
        return []

    @staticmethod
    def parse_ball_event(ball: dict) -> dict:
        """
        Parse a raw Sportmonks ball object into a normalised dict.

        Returns dict with keys:
          over_ball, innings, batsman_id, bowler_id, is_wicket,
          batsmanout_id, runs, is_four, is_six, is_legal, score_name
        """
        score = ball.get("score") or {}
        if isinstance(score, dict) and "data" in score:
            score = score["data"]

        return {
            "over_ball": ball.get("ball"),
            "innings": ball.get("scoreboard"),  # S1, S2
            "batsman_id": ball.get("batsman_id"),
            "bowler_id": ball.get("bowler_id"),
            "is_wicket": bool(ball.get("batsmanout_id")),
            "batsmanout_id": ball.get("batsmanout_id"),
            "runs": score.get("runs", 0) if isinstance(score, dict) else 0,
            "is_four": score.get("four", False) if isinstance(score, dict) else False,
            "is_six": score.get("six", False) if isinstance(score, dict) else False,
            "is_legal": score.get("ball", True) if isinstance(score, dict) else True,
            "score_name": score.get("name", "") if isinstance(score, dict) else "",
        }

    @staticmethod
    def parse_innings_runs(runs_data: Any) -> List[dict]:
        """
        Parse Sportmonks runs data into a list of innings summaries.

        Each entry: {"inning": int, "score": int, "wickets": int, "overs": float}
        """
        if runs_data is None:
            return []
        if isinstance(runs_data, dict):
            runs_data = runs_data.get("data", [])
        if not isinstance(runs_data, list):
            return []

        return [
            {
                "inning": r.get("inning"),
                "score": r.get("score", 0),
                "wickets": r.get("wickets", 0),
                "overs": r.get("overs", 0),
            }
            for r in runs_data
        ]

    @staticmethod
    def get_current_phase(overs: float) -> str:
        """
        Determine the current match phase from the over count.
        Powerplay: 0-6, Middle: 6-15, Death: 15-20
        """
        if overs <= 6:
            return "powerplay"
        elif overs <= 15:
            return "middle"
        else:
            return "death"
