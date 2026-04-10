from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("ipl_spotter.weather")

DEFAULT_BASE_URL = "https://api.weatherapi.com/v1"
DEFAULT_TIMEOUT = 8


class WeatherClient:
    """Small WeatherAPI.com wrapper for venue weather in pre-match reports."""

    def __init__(self, config: dict[str, Any]):
        self.api_key = config.get("weather_api_key", "")
        self.base_url = config.get("weather_api_base_url", DEFAULT_BASE_URL).rstrip("/")
        self.timeout = int(config.get("weather_api_timeout", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
        self.enabled = bool(self.api_key)
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

        if self.enabled:
            logger.info("WeatherClient enabled")
        else:
            logger.info("WeatherClient disabled (no API key)")

    def current(self, query: str) -> Optional[dict[str, Any]]:
        if not self.enabled or not query:
            return None

        url = f"{self.base_url}/current.json"
        try:
            resp = self._session.get(
                url,
                params={"key": self.api_key, "q": query, "aqi": "no"},
                timeout=self.timeout,
            )
            if resp.status_code != 200:
                logger.debug("Weather API %s -> HTTP %d: %s", query, resp.status_code, resp.text[:200])
                return None
            data = resp.json()
            return data if isinstance(data, dict) else None
        except Exception:
            logger.debug("Weather API request failed for %s", query, exc_info=True)
            return None

    def get_best_weather(self, venue: str) -> Optional[dict[str, Any]]:
        for query in _venue_queries(venue):
            data = self.current(query)
            if data and data.get("current"):
                return data
        return None

    def format_weather_line(self, venue: str) -> str:
        data = self.get_best_weather(venue)
        if not data:
            return ""

        current = data.get("current", {})
        condition = current.get("condition", {}) if isinstance(current, dict) else {}
        parts = []
        temp_c = current.get("temp_c")
        if temp_c is not None:
            parts.append(f"{temp_c:.0f}C")
        text = condition.get("text") if isinstance(condition, dict) else ""
        if text:
            parts.append(str(text))
        humidity = current.get("humidity")
        if humidity is not None:
            parts.append(f"humidity {int(humidity)}%")
        wind_kph = current.get("wind_kph")
        if wind_kph is not None:
            parts.append(f"wind {float(wind_kph):.0f} kph")
        return "⛅ Weather: " + " | ".join(parts) if parts else ""


def _venue_queries(venue: str) -> List[str]:
    raw = str(venue or "").strip()
    if not raw:
        return []

    candidates: List[str] = []
    for piece in raw.split(","):
        token = piece.strip()
        if token:
            candidates.append(token)

    cleaned = re.sub(r"\b(stadium|cricket|ground|international|national)\b", " ", raw, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,")
    if cleaned:
        candidates.append(cleaned)

    if "," in raw:
        last = raw.split(",")[-1].strip()
        if last:
            candidates.insert(0, last)

    seen = set()
    ordered = []
    for item in candidates:
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(item)
    return ordered
