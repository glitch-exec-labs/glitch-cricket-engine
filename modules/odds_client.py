"""
Cloudbet API client for fetching live cricket odds.
"""

from __future__ import annotations

import logging
import time
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs

import requests

from modules.session_markets import session_market_key_from_to_over

logger = logging.getLogger("ipl_spotter.odds")

IPL_COMPETITION_URL = (
    "https://sports-api.cloudbet.com/pub/v2/odds/competitions/"
    "cricket-india-indian-premier-league"
)

COMPETITION_URLS = {
    "ipl": "https://sports-api.cloudbet.com/pub/v2/odds/competitions/cricket-india-indian-premier-league",
    "psl": "https://sports-api.cloudbet.com/pub/v2/odds/competitions/cricket-pakistan-pakistan-super-league",
}

DEFAULT_TIMEOUT = 12

CRICKET_MARKETS = {
    "match_winner": "cricket.winner",
    "innings_total": "cricket.team_totals",
    "powerplay_runs": "cricket.team_total_from_0_over_to_x_over",
    "over_runs": "cricket.over_team_total",
    "player_runs": "cricket.player_total",
    "team_sixes": "cricket.team_total_sixes",
    "team_fours": "cricket.team_total_fours",
    "highest_over": "cricket.team_total_in_highest_scoring_over",
    "first_wicket": "cricket.team_total_at_dismissal",
    "player_milestone": "cricket.player_to_score_milestone",
}


class OddsClient:
    """Cloudbet Sports API client."""

    def __init__(self, config: dict):
        self.api_key = config.get("cloudbet_api_key", "")
        if not self.api_key:
            logger.warning("cloudbet_api_key not set in config -- API calls will fail")

        self.timeout = DEFAULT_TIMEOUT
        self._session = requests.Session()
        self._session.headers.update({
            "X-API-Key": self.api_key,
            "Accept": "application/json",
        })

        # Events cache: competition -> (timestamp, events_list)
        self._events_cache: Dict[str, Tuple[float, List[dict]]] = {}
        self._events_cache_ttl: float = float(config.get("cloudbet_events_cache_ttl", 45))

    def get_balance(self, currency: str = "USD") -> Optional[float]:
        url = f"https://sports-api.cloudbet.com/pub/v1/account/currencies/{currency}/balance"
        data = self._get(url)
        if data and "amount" in data:
            try:
                return float(data["amount"])
            except (TypeError, ValueError):
                return None
        return None

    def get_all_balances(self) -> Dict[str, float]:
        url = "https://sports-api.cloudbet.com/pub/v1/account/currencies"
        data = self._get(url)
        if not data:
            return {}

        currencies = data.get("currencies", [])
        balances = {}
        for cur in currencies:
            bal = self.get_balance(cur)
            if bal is not None and bal > 0:
                balances[cur] = bal
        return balances

    def _get(self, url: str, params: Optional[dict] = None) -> Optional[dict]:
        try:
            resp = self._session.get(url, params=params, timeout=self.timeout)
            if resp.status_code >= 400:
                logger.error(
                    "Cloudbet API error: GET %s -> HTTP %d: %s",
                    url, resp.status_code, resp.text[:300],
                )
                return None
            return resp.json()
        except requests.RequestException as exc:
            logger.error("Cloudbet request failed: GET %s -> %s", url, exc)
            return None
        except ValueError as exc:
            logger.error("Cloudbet JSON decode error: GET %s -> %s", url, exc)
            return None

    def get_events(self, competition: str = "ipl", force: bool = False) -> List[dict]:
        now = time.time()
        cached = self._events_cache.get(competition)
        if not force and cached is not None:
            ts, events = cached
            if now - ts < self._events_cache_ttl:
                logger.debug("Cloudbet %s events from cache (%d)", competition.upper(), len(events))
                return events

        url = COMPETITION_URLS.get(competition, COMPETITION_URLS["ipl"])
        data = self._get(url, params={"limit": 40})
        if data is None:
            return cached[1] if cached else []
        events = data.get("events", [])
        if not isinstance(events, list):
            return cached[1] if cached else []
        self._events_cache[competition] = (now, events)
        logger.info("Cloudbet %s events: %d (fresh)", competition.upper(), len(events))
        return events

    def get_ipl_events(self) -> List[dict]:
        return self.get_events("ipl")

    def get_psl_events(self) -> List[dict]:
        return self.get_events("psl")

    def get_market_odds(self, event: dict, market_type: str) -> Optional[dict]:
        if market_type == "powerplay_runs":
            session_markets = self._extract_session_markets(event.get("markets", {}))
            powerplay = session_markets.get("6_over")
            if powerplay is None:
                return None
            return {
                **powerplay,
                "market": "powerplay_runs",
            }

        cloudbet_key = CRICKET_MARKETS.get(market_type)
        if cloudbet_key is None:
            logger.warning("Unknown market type: %s", market_type)
            return None

        markets = event.get("markets", {})
        market_data = markets.get(cloudbet_key)
        if market_data is None:
            return None

        return self._parse_market(market_type, cloudbet_key, market_data)

    def get_all_market_odds(self, event: dict, batting_team_side: str | None = None) -> Dict[str, dict]:
        markets = event.get("markets", {})
        result: Dict[str, dict] = {}

        for market_type, cloudbet_key in CRICKET_MARKETS.items():
            if market_type == "powerplay_runs":
                continue
            market_data = markets.get(cloudbet_key)
            if market_data is None:
                continue
            parsed = self._parse_market(market_type, cloudbet_key, market_data)
            if parsed is not None:
                result[market_type] = parsed

        result.update(self._extract_session_markets(markets, batting_team_side=batting_team_side))

        if "6_over" in result:
            result["powerplay_runs"] = {
                **result["6_over"],
                "market": "powerplay_runs",
            }

        return result

    def _parse_market(
        self, market_type: str, cloudbet_key: str, market_data: dict
    ) -> Optional[dict]:
        if market_type == "match_winner":
            return self._parse_match_winner(market_data)
        if market_type in ("player_runs", "player_milestone"):
            return self._parse_player_market(market_type, market_data)
        return self._parse_over_under_market(market_type, market_data)

    @staticmethod
    def _parse_match_winner(market_data: dict) -> Optional[dict]:
        submarkets = market_data.get("submarkets", {})
        if not submarkets:
            return None

        submarket = next(iter(submarkets.values()), None)
        if submarket is None:
            return None

        selections = submarket.get("selections", [])
        parsed_selections: Dict[str, dict] = {}
        home_odds = 0.0
        away_odds = 0.0
        home_url = ""
        away_url = ""

        for sel in selections:
            outcome = sel.get("outcome", "")
            try:
                price = float(sel.get("price", 0.0))
            except (TypeError, ValueError):
                price = 0.0

            if outcome == "home":
                parsed_selections["home"] = {"price": price}
                home_odds = price
                home_url = sel.get("marketUrl", "")
            elif outcome == "away":
                parsed_selections["away"] = {"price": price}
                away_odds = price
                away_url = sel.get("marketUrl", "")

        if not parsed_selections:
            return None

        return {
            "market": "match_winner",
            "selections": parsed_selections,
            "home_odds": home_odds,
            "away_odds": away_odds,
            "home_market_url": home_url,
            "away_market_url": away_url,
        }

    @staticmethod
    def _parse_over_under_market(market_type: str, market_data: dict) -> Optional[dict]:
        submarkets = market_data.get("submarkets", {})
        if not submarkets:
            return None

        lines: List[dict[str, Any]] = []
        for submarket in submarkets.values():
            selections = submarket.get("selections", [])
            by_line: Dict[str, dict[str, Any]] = {}
            for sel in selections:
                params = _parse_params(sel.get("params", ""))
                line_val = params.get("total", "")
                team = params.get("team", "")
                outcome = sel.get("outcome", "")
                try:
                    price = float(sel.get("price", 0.0))
                except (TypeError, ValueError):
                    price = 0.0

                to_over = _safe_int(params.get("to_over"))
                over_number = _safe_int(params.get("over"))
                key = f"{line_val}:{team}:{to_over}:{over_number}"

                if key not in by_line:
                    by_line[key] = {
                        "market": market_type,
                        "line": _safe_float(line_val),
                        "over_odds": 0.0,
                        "under_odds": 0.0,
                        "team": team,
                        "to_over": to_over,
                        "over": over_number,
                        "market_url_over": "",
                        "market_url_under": "",
                    }

                if outcome == "over":
                    by_line[key]["over_odds"] = price
                    by_line[key]["market_url_over"] = sel.get("marketUrl", "")
                elif outcome == "under":
                    by_line[key]["under_odds"] = price
                    by_line[key]["market_url_under"] = sel.get("marketUrl", "")

            lines.extend(by_line.values())

        if not lines:
            return None

        best_line = _select_balanced_line(lines)
        result = {"market": market_type, "lines": lines}
        if best_line:
            result.update(best_line)
        return result

    @staticmethod
    def _parse_player_market(market_type: str, market_data: dict) -> Optional[dict]:
        submarkets = market_data.get("submarkets", {})
        if not submarkets:
            return None

        players: List[dict] = []
        for submarket in submarkets.values():
            selections = submarket.get("selections", [])
            by_player: Dict[str, dict] = {}
            for sel in selections:
                params = _parse_params(sel.get("params", ""))
                line_val = params.get("total", params.get("milestone", ""))
                outcome = sel.get("outcome", "")
                try:
                    price = float(sel.get("price", 0.0))
                except (TypeError, ValueError):
                    price = 0.0

                market_url = sel.get("marketUrl", "")
                player_name = _extract_player_name(market_url)

                key = f"{player_name}:{line_val}"
                if key not in by_player:
                    by_player[key] = {
                        "player": player_name,
                        "line": _safe_float(line_val),
                        "over_odds": 0.0,
                        "under_odds": 0.0,
                    }

                if outcome in ("over", "yes"):
                    by_player[key]["over_odds"] = price
                elif outcome in ("under", "no"):
                    by_player[key]["under_odds"] = price

            players.extend(by_player.values())

        if not players:
            return None

        return {"market": market_type, "players": players}

    @staticmethod
    def _extract_session_markets(markets: dict[str, Any], batting_team_side: str | None = None) -> Dict[str, dict]:
        session_market = markets.get("cricket.team_total_from_0_over_to_x_over")
        if not isinstance(session_market, dict):
            return {}

        parsed = OddsClient._parse_over_under_market("powerplay_runs", session_market)
        if not parsed:
            return {}

        session_lines = [
            line for line in parsed.get("lines", [])
            if line.get("to_over")
        ]
        if batting_team_side:
            session_lines = [
                line for line in session_lines
                if line.get("team") == batting_team_side
            ]

        grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for line in session_lines:
            team = line.get("team") or ""
            to_over = _safe_int(line.get("to_over"))
            if to_over <= 0:
                continue
            grouped.setdefault((team, to_over), []).append(line)

        result: Dict[str, dict] = {}
        for (team, to_over), lines in grouped.items():
            best = _select_balanced_line(lines)
            if not best:
                continue
            best = dict(best)
            best["market"] = session_market_key_from_to_over(to_over)
            best["team"] = team
            best["to_over"] = to_over
            key = session_market_key_from_to_over(to_over)
            if key not in result or batting_team_side:
                result[key] = best
            elif team:
                result[f"{team}_{key}"] = best

        return result

    @staticmethod
    def match_cloudbet_to_sportmonks(cloudbet_event: dict, sportmonks_match: dict) -> bool:
        cb_home = _get_nested(cloudbet_event, "home", "name", default="").lower().strip()
        cb_away = _get_nested(cloudbet_event, "away", "name", default="").lower().strip()

        sm_local = sportmonks_match.get("localteam", {})
        if isinstance(sm_local, dict) and "data" in sm_local:
            sm_local = sm_local["data"]
        sm_home = (sm_local.get("name", "") if isinstance(sm_local, dict) else "").lower().strip()

        sm_visitor = sportmonks_match.get("visitorteam", {})
        if isinstance(sm_visitor, dict) and "data" in sm_visitor:
            sm_visitor = sm_visitor["data"]
        sm_away = (sm_visitor.get("name", "") if isinstance(sm_visitor, dict) else "").lower().strip()

        if not cb_home or not cb_away or not sm_home or not sm_away:
            return False

        home_ratio = _team_similarity(cb_home, sm_home)
        away_ratio = _team_similarity(cb_away, sm_away)

        return home_ratio >= 0.6 and away_ratio >= 0.6


def _parse_params(params_str: str) -> dict:
    if not params_str:
        return {}
    parsed = parse_qs(params_str, keep_blank_values=True)
    return {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}


def _safe_float(val: Any) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(val: Any) -> int:
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return 0


def _select_balanced_line(lines: List[dict[str, Any]]) -> dict[str, Any] | None:
    if not lines:
        return None

    def _score(line: dict[str, Any]) -> tuple[float, float]:
        over_odds = line.get("over_odds", 0.0) or 0.0
        under_odds = line.get("under_odds", 0.0) or 0.0
        if over_odds <= 0 or under_odds <= 0:
            return (999.0, 999.0)
        return (
            abs(over_odds - under_odds),
            abs(((over_odds + under_odds) / 2.0) - 1.85),
        )

    best = min(lines, key=_score)
    return dict(best)


def _extract_player_name(market_url: str) -> str:
    if not market_url:
        return ""

    parts = market_url.strip("/").split("/")
    for i, part in enumerate(parts):
        if part in ("player-total", "player-to-score-milestone") and i + 1 < len(parts):
            return parts[i + 1].replace("-", " ")

    if "player=" in market_url:
        params = _parse_params(market_url.split("?", 1)[1] if "?" in market_url else "")
        player = params.get("player", "")
        if player:
            return str(player).replace("-", " ")

    if len(parts) >= 2:
        candidate = parts[-2].replace("-", " ")
        if candidate and not candidate.replace(" ", "").isdigit():
            return candidate
    return ""


def _get_nested(d: dict, *keys: str, default: Any = None) -> Any:
    current = d
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
    return current


def _team_similarity(name_a: str, name_b: str) -> float:
    if name_a == name_b:
        return 1.0

    aliases = {
        "csk": "chennai super kings",
        "mi": "mumbai indians",
        "rcb": "royal challengers bengaluru",
        "kkr": "kolkata knight riders",
        "dc": "delhi capitals",
        "srh": "sunrisers hyderabad",
        "rr": "rajasthan royals",
        "pbks": "punjab kings",
        "lsg": "lucknow super giants",
        "gt": "gujarat titans",
        "royal challengers bangalore": "royal challengers bengaluru",
        "qg": "quetta gladiators",
        "kk": "karachi kings",
        "lq": "lahore qalandars",
        "iu": "islamabad united",
        "pz": "peshawar zalmi",
        "ms": "multan sultans",
    }

    resolved_a = aliases.get(name_a, name_a)
    resolved_b = aliases.get(name_b, name_b)

    if resolved_a == resolved_b:
        return 1.0

    return SequenceMatcher(None, resolved_a, resolved_b).ratio()
