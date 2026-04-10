"""LLM-powered pre-match intelligence using OpenAI API.

Parses raw news snippets into structured, actionable cricket betting intel.
Runs ONCE before each match — never in the live scanning loop.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("ipl_spotter.llm_intel")


SYSTEM_PROMPT = """You are a cricket betting analyst. Extract actionable intelligence from pre-match news articles for T20 cricket betting.

Return a JSON object with these fields:

{
  "playing_xi_home": {
    "confirmed": true/false,
    "changes": ["Hazlewood OUT, replaced by Dayal"],
    "impact": "Brief impact on team strength"
  },
  "playing_xi_away": {
    "confirmed": true/false,
    "changes": ["Any changes from expected XI"],
    "impact": "Brief impact"
  },
  "injuries": [
    {"player": "Name", "team": "Team", "status": "out/doubtful/recovered", "replacement": "Name or null", "impact": "How this affects predictions"}
  ],
  "pitch_report": {
    "surface": "flat/green/dry/dusty",
    "pace_friendly": true/false,
    "spin_friendly": true/false,
    "expected_first_innings": 170,
    "scoring_difficulty": "high/medium/low",
    "summary": "One line pitch summary"
  },
  "weather": {
    "temperature_c": 30,
    "humidity_pct": 65,
    "rain_risk": "none/low/medium/high",
    "dew_expected": true/false,
    "chase_advantage": true/false,
    "summary": "One line weather impact"
  },
  "toss": {
    "winner": null,
    "decision": null,
    "impact": null
  },
  "model_adjustments": {
    "first_innings_adj": 0,
    "powerplay_adj": 0,
    "death_overs_adj": 0,
    "chase_adj": 0,
    "reasoning": "Why these adjustments"
  },
  "key_insight": "The single most important piece of information for betting on this match",
  "confidence": "high/medium/low"
}

Rules:
- model_adjustments in RUNS (e.g., +5 means expect 5 more runs than default)
- Be specific about player names
- If playing XI is not confirmed, say so
- chase_advantage = true means batting second is favored
- Return ONLY valid JSON"""


class LLMIntel:
    """OpenAI-powered pre-match intelligence parser."""

    def __init__(self, config: Dict[str, Any]):
        self.api_key = config.get("openai_api_key", "")
        self.model_name = config.get("llm_model", "gpt-4o-mini")
        self.enabled = bool(self.api_key) and config.get("llm_intel_enabled", True)
        self._client = None

        if not self.enabled:
            if not self.api_key:
                logger.info("LLM Intel disabled — add openai_api_key to config")
            else:
                logger.info("LLM Intel disabled via config")
        else:
            logger.info("LLM Intel enabled (OpenAI, model=%s)", self.model_name)

    def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            from openai import OpenAI
            self._client = OpenAI(api_key=self.api_key)
            return self._client
        except Exception as exc:
            logger.warning("Failed to create OpenAI client: %s", exc)
            return None

    def parse_news_intel(
        self,
        raw_intel: Dict[str, Any],
        home: str,
        away: str,
        venue: str = "",
        competition: str = "IPL",
    ) -> Optional[Dict[str, Any]]:
        """Parse raw news snippets through GPT into structured intel."""
        if not self.enabled:
            return None

        client = self._get_client()
        if client is None:
            return None

        snippets = []
        for category in ["playing_xi", "injuries", "pitch_report", "weather", "toss", "expert_tips", "key_headlines"]:
            items = raw_intel.get(category, [])
            if items:
                snippets.append(f"\n--- {category.upper()} ---")
                for item in items[:5]:
                    snippets.append(f"  {item}")

        if not snippets:
            logger.info("No news snippets to parse for %s vs %s", home, away)
            return None

        user_prompt = (
            f"Match: {home} vs {away}\n"
            f"Competition: {competition}\n"
            f"Venue: {venue}\n\n"
            f"Raw news snippets:\n"
            + "\n".join(snippets)
            + "\n\nExtract structured betting intelligence as JSON."
        )

        try:
            response = client.chat.completions.create(
                model=self.model_name,
                temperature=0.1,
                max_tokens=2000,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
            )
            text = response.choices[0].message.content.strip()
            parsed = json.loads(text)
            logger.info("LLM intel parsed for %s vs %s (confidence: %s)",
                        home, away, parsed.get("confidence", "unknown"))
            return parsed

        except json.JSONDecodeError as exc:
            logger.warning("LLM returned invalid JSON for %s vs %s: %s", home, away, exc)
            return None
        except Exception as exc:
            logger.warning("LLM intel failed for %s vs %s: %s", home, away, exc)
            return None

    def format_llm_report(
        self,
        intel: Dict[str, Any],
        home: str,
        away: str,
    ) -> str:
        """Format LLM-parsed intel into a Telegram message."""
        lines = [
            f"\U0001f9e0 AI INTEL: {home} vs {away}",
            "",
        ]

        insight = intel.get("key_insight")
        if insight:
            lines.append(f"\u26a1 {insight}")
            lines.append("")

        for side, team in [("playing_xi_home", home), ("playing_xi_away", away)]:
            xi = intel.get(side, {})
            if not xi:
                continue
            confirmed = "\u2705" if xi.get("confirmed") else "\u2753"
            lines.append(f"{confirmed} {team}:")
            for change in (xi.get("changes") or []):
                lines.append(f"   \u2022 {change}")
            impact = xi.get("impact")
            if impact:
                lines.append(f"   \u2192 {impact}")

        injuries = intel.get("injuries", [])
        if injuries:
            lines.append("")
            lines.append("\U0001fa79 Injuries:")
            for inj in injuries[:3]:
                status = inj.get("status", "?").upper()
                lines.append(f"   \u2022 {inj.get('player', '?')} ({inj.get('team', '?')}) \u2014 {status}")
                if inj.get("impact"):
                    lines.append(f"     {inj['impact']}")

        pitch = intel.get("pitch_report", {})
        if pitch and pitch.get("summary"):
            lines.append("")
            expected = pitch.get("expected_first_innings")
            exp_text = f" | Expect: {expected}" if expected else ""
            lines.append(f"\U0001f3df Pitch: {pitch['summary']}{exp_text}")

        weather = intel.get("weather", {})
        if weather and weather.get("summary"):
            lines.append("")
            dew = " | DEW \u2192 chase advantage" if weather.get("dew_expected") else ""
            lines.append(f"\u26c5 Weather: {weather['summary']}{dew}")

        toss = intel.get("toss", {})
        if toss and toss.get("winner"):
            lines.append("")
            lines.append(f"\U0001fa99 Toss: {toss['winner']} elected to {toss.get('decision', '?')}")
            if toss.get("impact"):
                lines.append(f"   \u2192 {toss['impact']}")

        adj = intel.get("model_adjustments", {})
        adj_parts = []
        for key, label in [
            ("first_innings_adj", "1st Inn"),
            ("powerplay_adj", "PP"),
            ("death_overs_adj", "Death"),
            ("chase_adj", "Chase"),
        ]:
            val = adj.get(key, 0)
            if val and val != 0:
                adj_parts.append(f"{label}: {val:+.0f}")
        if adj_parts:
            lines.append("")
            lines.append(f"\U0001f4ca Model Adj: {' | '.join(adj_parts)}")
            if adj.get("reasoning"):
                lines.append(f"   \u2192 {adj['reasoning']}")

        confidence = intel.get("confidence", "")
        if confidence:
            icon = {"high": "\U0001f7e2", "medium": "\U0001f7e1", "low": "\U0001f534"}.get(confidence, "")
            lines.append(f"\n{icon} Confidence: {confidence.upper()}")

        return "\n".join(lines)
