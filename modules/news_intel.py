"""Pre-match news intelligence via Brave Search API.

Fetches latest news about an upcoming match to extract:
  - Playing XI / team changes
  - Injuries / player availability
  - Pitch report & conditions
  - Weather forecast
  - Toss result & decision
  - Expert predictions / tips

Sends a concise intel report to Telegram before the match starts.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("ipl_spotter.news_intel")

BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
BRAVE_NEWS_URL = "https://api.search.brave.com/res/v1/news/search"
DEFAULT_TIMEOUT = 10


class NewsIntel:
    """Fetches and summarizes pre-match intelligence from web news."""

    def __init__(self, config: Dict[str, Any]):
        self.api_key = config.get("brave_api_key", "")
        self.enabled = bool(self.api_key)
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": self.api_key,
        })
        # Cache to avoid re-fetching for same match
        self._cache: Dict[str, Dict[str, Any]] = {}

        if self.enabled:
            logger.info("News Intel enabled (Brave Search API)")
        else:
            logger.info("News Intel disabled — add brave_api_key to config")

    def get_pre_match_intel(
        self,
        home: str,
        away: str,
        venue: str = "",
        competition: str = "IPL",
        date: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Fetch and parse pre-match news for a given fixture.

        Returns a dict with categorized intel, or None on failure.
        """
        if not self.enabled:
            return None

        cache_key = f"{home}|{away}|{date}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Search queries — targeted for maximum signal
        queries = [
            f"{home} vs {away} {competition} {date} playing XI team news",
            f"{home} vs {away} {competition} pitch report weather",
        ]

        all_results: List[Dict[str, str]] = []
        for query in queries:
            results = self._search_news(query)
            all_results.extend(results)

        if not all_results:
            logger.info("No news results for %s vs %s", home, away)
            return None

        intel = self._extract_intel(all_results, home, away, venue)
        self._cache[cache_key] = intel
        return intel

    def _search_news(self, query: str) -> List[Dict[str, str]]:
        """Search Brave News API for recent articles."""
        try:
            resp = self._session.get(
                BRAVE_NEWS_URL,
                params={
                    "q": query,
                    "count": 8,
                    "freshness": "pd",  # past day
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if resp.status_code == 422 or resp.status_code >= 400:
                # Fallback to web search if news endpoint fails
                return self._search_web(query)

            data = resp.json()
            results = data.get("results", [])
            return [
                {
                    "title": r.get("title", ""),
                    "description": r.get("description", ""),
                    "url": r.get("url", ""),
                    "age": r.get("age", ""),
                }
                for r in results
            ]
        except Exception as exc:
            logger.debug("Brave News search failed: %s — trying web", exc)
            return self._search_web(query)

    def _search_web(self, query: str) -> List[Dict[str, str]]:
        """Fallback to Brave Web Search API."""
        try:
            resp = self._session.get(
                BRAVE_SEARCH_URL,
                params={
                    "q": query,
                    "count": 8,
                    "freshness": "pd",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if resp.status_code >= 400:
                logger.warning("Brave Web search failed: HTTP %d", resp.status_code)
                return []

            data = resp.json()
            web_results = data.get("web", {}).get("results", [])
            return [
                {
                    "title": r.get("title", ""),
                    "description": r.get("description", ""),
                    "url": r.get("url", ""),
                    "age": r.get("age", ""),
                }
                for r in web_results
            ]
        except Exception as exc:
            logger.warning("Brave Web search failed: %s", exc)
            return []

    def _extract_intel(
        self,
        results: List[Dict[str, str]],
        home: str,
        away: str,
        venue: str,
    ) -> Dict[str, Any]:
        """Parse search results into categorized intelligence."""
        intel: Dict[str, Any] = {
            "playing_xi": [],
            "injuries": [],
            "pitch_report": [],
            "weather": [],
            "toss": [],
            "expert_tips": [],
            "key_headlines": [],
            "sources": [],
        }

        for r in results:
            text = f"{r['title']} {r['description']}".lower()
            title = r["title"]
            desc = r["description"]
            combined = f"{title}. {desc}"

            # Categorize
            if any(kw in text for kw in ["playing xi", "playing 11", "team sheet", "squad",
                                          "lineup", "line-up", "team news", "changes"]):
                intel["playing_xi"].append(combined)

            if any(kw in text for kw in ["injur", "ruled out", "miss", "doubtful",
                                          "unavailable", "replaced", "rested"]):
                intel["injuries"].append(combined)

            if any(kw in text for kw in ["pitch", "wicket", "surface", "track",
                                          "curator", "spin", "pace", "bounce"]):
                intel["pitch_report"].append(combined)

            if any(kw in text for kw in ["weather", "rain", "dew", "humidity",
                                          "forecast", "temperature"]):
                intel["weather"].append(combined)

            if any(kw in text for kw in ["toss", "elected", "chose to"]):
                intel["toss"].append(combined)

            if any(kw in text for kw in ["predict", "tip", "fantasy", "captain",
                                          "pick", "dream11", "expected"]):
                intel["expert_tips"].append(combined)

            # All headlines for context
            intel["key_headlines"].append(title)
            if r.get("url"):
                intel["sources"].append(r["url"])

        # Deduplicate
        for key in intel:
            if isinstance(intel[key], list):
                intel[key] = list(dict.fromkeys(intel[key]))[:5]  # keep max 5 per category

        return intel

    def format_intel_report(
        self,
        intel: Dict[str, Any],
        home: str,
        away: str,
        venue: str = "",
    ) -> str:
        """Format intel dict into a Telegram-ready message."""
        lines = [
            f"\U0001f4f0 PRE-MATCH INTEL: {home} vs {away}",
        ]
        if venue:
            lines.append(f"   Venue: {venue}")
        lines.append("")

        # Playing XI / Team News
        if intel.get("playing_xi"):
            lines.append("\U0001f465 Team News:")
            for item in intel["playing_xi"][:3]:
                lines.append(f"   \u2022 {_truncate(item, 120)}")
            lines.append("")

        # Injuries
        if intel.get("injuries"):
            lines.append("\U0001fa79 Injuries:")
            for item in intel["injuries"][:3]:
                lines.append(f"   \u2022 {_truncate(item, 120)}")
            lines.append("")

        # Pitch Report
        if intel.get("pitch_report"):
            lines.append("\U0001f3df Pitch Report:")
            for item in intel["pitch_report"][:2]:
                lines.append(f"   \u2022 {_truncate(item, 120)}")
            lines.append("")

        # Weather
        if intel.get("weather"):
            lines.append("\u26c5 Weather:")
            for item in intel["weather"][:2]:
                lines.append(f"   \u2022 {_truncate(item, 120)}")
            lines.append("")

        # Toss
        if intel.get("toss"):
            lines.append("\U0001fa99 Toss:")
            for item in intel["toss"][:1]:
                lines.append(f"   \u2022 {_truncate(item, 120)}")
            lines.append("")

        # Expert Tips (useful for sentiment)
        if intel.get("expert_tips"):
            lines.append("\U0001f4a1 Expert Buzz:")
            for item in intel["expert_tips"][:2]:
                lines.append(f"   \u2022 {_truncate(item, 120)}")
            lines.append("")

        # If we got nothing useful
        if not any(intel.get(k) for k in ["playing_xi", "injuries", "pitch_report", "weather", "toss"]):
            lines.append("   No specific intel found — check closer to match time")

        return "\n".join(lines)


def _truncate(text: str, max_len: int = 120) -> str:
    """Truncate text to max_len, adding ellipsis if needed."""
    # Clean up HTML tags and extra whitespace
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= max_len:
        return text
    return text[:max_len - 3] + "..."
