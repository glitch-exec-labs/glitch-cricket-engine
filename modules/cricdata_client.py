from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("ipl_spotter.cricdata")

DEFAULT_BASE_URL = "https://api.cricapi.com/v1"
DEFAULT_TIMEOUT = 10
_SERIES_SEARCH = {
    "ipl": ["Indian Premier League", "IPL"],
    "psl": ["Pakistan Super League", "PSL"],
}
_GENERIC_TEAM_WORDS = {
    "club",
    "cricket",
    "team",
    "super",
    "kings",
    "royals",
    "capitals",
    "united",
    "giants",
    "warriors",
}


class CricDataClient:
    """Lightweight CricData paid API client used as a supplement to Sportmonks/ESPN."""

    def __init__(self, config: dict[str, Any]):
        self.api_key = config.get("cricdata_api_key", "")
        self.base_url = config.get("cricdata_base_url", DEFAULT_BASE_URL).rstrip("/")
        self.timeout = int(config.get("cricdata_timeout", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
        self.enabled = bool(self.api_key)
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        self._series_cache: Dict[str, Optional[str]] = {}
        self._current_matches_cache: tuple[float, List[dict[str, Any]]] = (0.0, [])

        if self.enabled:
            logger.info("CricDataClient enabled")
        else:
            logger.info("CricDataClient disabled (no API key)")

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if not self.enabled:
            return None

        query = {"apikey": self.api_key}
        if params:
            query.update({k: v for k, v in params.items() if v not in (None, "")})

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            resp = self._session.get(url, params=query, timeout=self.timeout)
            if resp.status_code != 200:
                logger.debug("CricData %s -> HTTP %d: %s", endpoint, resp.status_code, resp.text[:200])
                return None
            payload = resp.json()
            if isinstance(payload, dict):
                return payload.get("data", payload)
            return payload
        except Exception:
            logger.debug("CricData request failed for %s", endpoint, exc_info=True)
            return None

    def get_current_matches(self, use_cache: bool = True) -> List[dict[str, Any]]:
        now = time.time()
        cached_at, cached_data = self._current_matches_cache
        if use_cache and cached_data and now - cached_at < 20:
            return cached_data
        data = self._get("currentMatches")
        matches = data if isinstance(data, list) else []
        self._current_matches_cache = (now, matches)
        return matches

    def get_match_info(self, match_id: str) -> Optional[dict[str, Any]]:
        data = self._get("match_info", {"id": match_id})
        return data if isinstance(data, dict) else None

    def get_match_squad(self, match_id: str) -> List[dict[str, Any]]:
        data = self._get("match_squad", {"id": match_id})
        return data if isinstance(data, list) else []

    def get_match_scorecard(self, match_id: str) -> Optional[dict[str, Any]]:
        data = self._get("match_scorecard", {"id": match_id})
        return data if isinstance(data, dict) else None

    def search_series(self, query: str) -> List[dict[str, Any]]:
        data = self._get("series", {"search": query})
        return data if isinstance(data, list) else []

    def get_series_info(self, series_id: str) -> Optional[dict[str, Any]]:
        data = self._get("series_info", {"id": series_id})
        return data if isinstance(data, dict) else None

    def get_series_matches(self, competition: str) -> List[dict[str, Any]]:
        series_id = self._resolve_series_id(competition)
        if not series_id:
            return []
        info = self.get_series_info(series_id)
        if not info:
            return []
        matches = info.get("matchList", [])
        return matches if isinstance(matches, list) else []

    def find_match(
        self,
        home: str,
        away: str,
        competition: str = "ipl",
        start_time: Any = None,
        venue: str = "",
    ) -> Optional[dict[str, Any]]:
        best_match: Optional[dict[str, Any]] = None
        best_score = -1

        for source, candidates in (
            ("current", self.get_current_matches()),
            ("series", self.get_series_matches(competition)),
        ):
            for match in candidates:
                score = self._score_match_candidate(match, home, away, start_time=start_time, venue=venue)
                if source == "current" and match.get("matchStarted"):
                    score += 20
                if score > best_score:
                    best_score = score
                    best_match = match
            if best_score >= 120:
                break

        return best_match if best_score >= 80 else None

    def _resolve_series_id(self, competition: str) -> Optional[str]:
        cached = self._series_cache.get(competition)
        if cached:
            return cached

        year = datetime.now(timezone.utc).year
        searches = []
        for term in _SERIES_SEARCH.get(competition, [competition.upper()]):
            searches.extend([f"{term} {year}", term])

        best_id = None
        best_score = -1
        for query in searches:
            for series in self.search_series(query):
                score = self._score_series_candidate(series, competition, year)
                series_id = str(series.get("id") or "")
                if not series_id or score <= best_score:
                    continue
                info = self.get_series_info(series_id)
                match_list = info.get("matchList", []) if isinstance(info, dict) else []
                if not match_list:
                    continue
                best_score = score + min(len(match_list), 100)
                best_id = series_id

        self._series_cache[competition] = best_id
        return best_id

    @staticmethod
    def _score_series_candidate(series: dict[str, Any], competition: str, year: int) -> int:
        name = str(series.get("name") or "")
        norm = _normalize_team_text(name)
        score = 0
        for term in _SERIES_SEARCH.get(competition, []):
            if _normalize_team_text(term) in norm:
                score += 50
        if str(year) in name:
            score += 25
        if str(year - 1) in name:
            score -= 10
        if series.get("matchStart") or series.get("startDate"):
            score += 5
        return score

    def _score_match_candidate(
        self,
        match: dict[str, Any],
        home: str,
        away: str,
        start_time: Any = None,
        venue: str = "",
    ) -> int:
        teams = self._extract_match_teams(match)
        if len(teams) < 2:
            return -1

        score = 0
        if _team_match(home, teams[0]) and _team_match(away, teams[1]):
            score += 100
        elif _team_match(home, teams[1]) and _team_match(away, teams[0]):
            score += 100
        else:
            return -1

        if venue:
            match_venue = str(match.get("venue") or "")
            if match_venue and _normalize_team_text(venue)[:18] in _normalize_team_text(match_venue):
                score += 10

        target_dt = _parse_datetime(start_time)
        match_dt = _parse_datetime(match.get("dateTimeGMT") or match.get("date"))
        if target_dt and match_dt:
            hours = abs((match_dt - target_dt).total_seconds()) / 3600.0
            if hours <= 8:
                score += 30
            elif hours <= 24:
                score += 15
            elif hours <= 48:
                score += 5

        status = str(match.get("status") or "").lower()
        if "abandon" in status or "cancel" in status:
            score -= 20
        return score

    @staticmethod
    def _extract_match_teams(match: dict[str, Any]) -> List[str]:
        teams = match.get("teams")
        if isinstance(teams, list) and len(teams) >= 2:
            return [str(teams[0] or ""), str(teams[1] or "")]

        team_info = match.get("teamInfo")
        if isinstance(team_info, list) and len(team_info) >= 2:
            result = []
            for team in team_info[:2]:
                if isinstance(team, dict):
                    result.append(str(team.get("name") or team.get("shortname") or ""))
            if len(result) >= 2:
                return result

        name = str(match.get("name") or "")
        if " vs " in name.lower():
            parts = re.split(r"\s+vs\s+", name, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 2:
                away = parts[1].split(",", 1)[0].strip()
                return [parts[0].strip(), away]
        return []


def _normalize_team_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _team_tokens(value: str) -> List[str]:
    return [token for token in _normalize_team_text(value).split() if token and token not in _GENERIC_TEAM_WORDS]


def _team_match(left: str, right: str) -> bool:
    left_norm = _normalize_team_text(left)
    right_norm = _normalize_team_text(right)
    if not left_norm or not right_norm:
        return False
    if left_norm == right_norm:
        return True

    left_tokens = _team_tokens(left)
    right_tokens = _team_tokens(right)
    if left_tokens and right_tokens and set(left_tokens) & set(right_tokens):
        return True

    left_words = left_norm.split()
    right_words = right_norm.split()
    if left_words and right_words and left_words[-1] == right_words[-1]:
        return True

    return any(token in right_norm for token in left_tokens if len(token) >= 4)


def _parse_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        raw = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None
