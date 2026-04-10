"""
Telegram Bot — sends edge alerts and pre-match reports via Telegram.

Requires the ``python-telegram-bot`` package for actual sending;
formatting functions are pure Python with no external dependencies.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import requests

import re as _re

from modules.session_markets import SESSION_DISPLAY, market_display_name

logger = logging.getLogger(__name__)


# ── MarkdownV2 escape helper ────────────────────────────────────────────


def escape_md(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    return _re.sub(r'([_*\[\]()~`>#+\-=|{}.!\\])', r'\\\1', str(text))


# ── market display names ──────────────────────────────────────────────────

MARKET_DISPLAY: dict[str, str] = {
    "total_runs": "Total Runs",
    "powerplay_runs": SESSION_DISPLAY["powerplay_runs"],
    "6_over": SESSION_DISPLAY["6_over"],
    "10_over": SESSION_DISPLAY["10_over"],
    "12_over": SESSION_DISPLAY["12_over"],
    "15_over": SESSION_DISPLAY["15_over"],
    "20_over": SESSION_DISPLAY["20_over"],
    "innings_total": "Innings Total",
    "death_overs_runs": "Death Overs Runs",
    "first_innings_runs": "1st Innings Runs",
    "second_innings_runs": "2nd Innings Runs",
    "total_sixes": "Total Sixes",
    "total_fours": "Total Fours",
    "total_wickets": "Total Wickets",
    "match_winner": "Match Winner",
}


# ── formatting helpers ────────────────────────────────────────────────────


def _format_report_number(value: Any, default: str = "N/A") -> str:
    """Render report numbers cleanly without raw float noise."""
    if value is None:
        return default
    if isinstance(value, str):
        return value
    try:
        return f"{float(value):.0f}"
    except (TypeError, ValueError):
        return default


def format_edge_alert(
    home: str,
    away: str,
    edge: dict[str, Any],
    timestamp: str = "",
) -> str:
    """Format an edge dict into a Telegram-friendly Markdown message."""
    market = edge.get("market", "")

    if market == "match_winner":
        return _format_match_winner_alert(home, away, edge, timestamp)
    return _format_line_alert(home, away, edge, timestamp)


def _format_line_alert(
    home: str,
    away: str,
    edge: dict[str, Any],
    timestamp: str,
) -> str:
    from modules.copilot_telegram import team_tag, team_emoji, stake_advice, BAT

    market = edge["market"]
    direction = edge["direction"]
    line = edge["bookmaker_line"]
    edge_runs = edge["edge_runs"]
    ev_pct = edge.get("ev_pct", 0)

    _short = {
        "6_over": "6 Over", "powerplay_runs": "6 Over", "10_over": "10 Over",
        "12_over": "12 Over", "15_over": "15 Over", "20_over": "Innings",
        "innings_total": "Innings", "over_runs": "Next Over",
    }
    short_market = _short.get(market, market_display_name(market))

    if direction == "OVER":
        icon = "🟢"
        call = "YES"
    else:
        icon = "🔴"
        call = "NO"

    home_tag = team_tag(home)
    away_tag = team_tag(away)
    advice = stake_advice(abs(float(edge_runs)), float(ev_pct), bet_type="session", is_first_entry=True)

    lines = [
        f"{BAT} {short_market}",
        f"{'━' * 30}",
        f"{icon} {call}  {float(line):.0f} runs",
        f"{advice}",
        f"{'━' * 30}",
        f"{home_tag}  🆚  {away_tag}",
    ]

    return "\n".join(lines)


def _format_match_winner_alert(
    home: str,
    away: str,
    edge: dict[str, Any],
    timestamp: str,
) -> str:
    from modules.copilot_telegram import team_tag, team_emoji, CUP, CASH, stake_advice

    team = edge["team"]
    model_prob = edge["model_prob"]
    implied_prob = edge["implied_prob"]
    odds = edge["odds"]
    edge_pp = (model_prob - implied_prob) * 100

    backed_emoji = team_emoji(team)
    backed_tag = team_tag(team)
    home_tag = team_tag(home)
    away_tag = team_tag(away)

    s_advice = stake_advice(abs(edge_pp), abs(edge_pp), bet_type="match_winner", is_first_entry=True)

    lines = [
        f"{CUP} MATCH WINNER",
        f"{'━' * 30}",
        f"",
        f"{backed_emoji} Win {backed_tag}",
        f"@ {odds:.2f} rate",
        f"",
        f"{s_advice}",
        f"📌 1st entry — hedge when rate moves",
        f"",
        f"{'━' * 30}",
        f"{home_tag}  🆚  {away_tag}",
    ]

    return "\n".join(lines)


def format_pre_match_report(report: dict[str, Any]) -> str:
    """Format a pre-match report dict into a Telegram-friendly Markdown message."""
    home = report.get("home", "TBD")
    away = report.get("away", "TBD")
    venue = report.get("venue", "Unknown")
    venue_avg = _format_report_number(report.get("venue_avg_score", "N/A"))
    venue_avg_first = _format_report_number(report.get("venue_avg_first_innings", "N/A"))
    venue_avg_second = _format_report_number(report.get("venue_avg_second_innings", "N/A"))
    model_total = _format_report_number(report.get("model_predicted_total", "N/A"))
    model_home = _format_report_number(report.get("model_home_score", "N/A"))
    model_away = _format_report_number(report.get("model_away_score", "N/A"))
    toss_winner = report.get("toss_winner")
    toss_decision = report.get("toss_decision")
    toss_pending = bool(report.get("toss_pending"))
    toss_available = report.get("toss_available")

    lines = [
        f"\U0001f4cb *PRE-MATCH REPORT*",
        f"\U0001f3cf *{home} vs {away}*",
        f"",
        f"\U0001f3df *Venue:* {venue}",
        f"\U0001f4ca *Venue Averages:*",
        f"  \u2022 Overall: {venue_avg}",
        f"  \u2022 1st Innings: {venue_avg_first}",
        f"  \u2022 2nd Innings: {venue_avg_second}",
        f"",
        f"\U0001f52e *Model Predictions:*",
        f"  \u2022 Predicted Total: {model_total}",
        f"  \u2022 {home}: {model_home}",
        f"  \u2022 {away}: {model_away}",
    ]

    if toss_winner and toss_decision:
        lines.extend([
            "",
            f"\U0001fa99 *Toss:* {toss_winner} won \u2014 elected to {toss_decision}",
        ])
    elif toss_available is False and not toss_pending:
        lines.extend([
            "",
            "\U0001fa99 *Toss:* Data unavailable",
        ])

    return "\n".join(lines)


# ── TelegramNotifier class ────────────────────────────────────────────────


class TelegramNotifier:
    """Sends alert messages to a Telegram chat via a bot.

    Supports multi-channel routing:
      - telegram_chat_id: default channel (all messages)
      - telegram_chat_id_ipl: IPL-specific commentary
      - telegram_chat_id_psl: PSL-specific commentary
      - telegram_chat_id_signals: signals-only channel (edges, session calls, MW calls)

    Use send_alert_sync(msg, channel="ipl") to route to the IPL channel.
    Signals are always sent to the signals channel if configured.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        config = config or {}
        self.bot_token: str = config.get("telegram_bot_token", "")
        self.chat_id: str = config.get("telegram_chat_id", "")
        self.enabled: bool = bool(self.bot_token and self.chat_id)
        self._bot: Any = None  # telegram.Bot instance, set in initialize()
        self._bot_import_failed = False
        self._api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

        # Competition-specific channels
        self._channel_map: dict[str, str] = {
            "default": self.chat_id,
            "ipl": config.get("telegram_chat_id_ipl", ""),
            "psl": config.get("telegram_chat_id_psl", ""),
            "signals": config.get("telegram_chat_id_signals", ""),
        }

        if not self.enabled:
            logger.info("TelegramNotifier disabled — token or chat_id missing")

    async def initialize(self) -> None:
        """Create the underlying ``telegram.Bot`` instance.

        Requires ``python-telegram-bot`` to be installed.
        """
        if not self.enabled:
            return

        self._ensure_bot()
        if self._bot is not None:
            logger.info("TelegramNotifier initialised")

    def _ensure_bot(self) -> Any | None:
        """Lazily create the underlying ``telegram.Bot`` instance."""
        if not self.enabled or self._bot is not None or self._bot_import_failed:
            return self._bot

        try:
            from telegram import Bot  # type: ignore[import-untyped]

            self._bot = Bot(token=self.bot_token)
        except ImportError:
            self._bot_import_failed = True
            logger.warning(
                "python-telegram-bot not installed — using direct Telegram API fallback"
            )
        except Exception:
            self._bot_import_failed = True
            logger.exception("Failed to initialise Telegram bot client — using API fallback")

        return self._bot

    async def _send_with_bot(self, message: str, parse_mode: str | None) -> bool:
        bot = self._ensure_bot()
        if bot is None:
            return False

        try:
            kwargs = {
                "chat_id": self.chat_id,
                "text": message,
            }
            if parse_mode:
                kwargs["parse_mode"] = parse_mode
            await bot.send_message(**kwargs)
            return True
        except Exception:
            self._bot = None
            logger.exception(
                "Failed to send Telegram alert via telegram.Bot — switching to API fallback"
            )
            return False

    def _send_via_http(self, message: str, parse_mode: str | None) -> bool:
        return self._send_via_http_to(self.chat_id, message, parse_mode)

    async def send_alert(
        self,
        message: str,
        parse_mode: str | None = None,
    ) -> bool:
        """Send a Telegram alert, retrying without Markdown if needed."""
        if not self.enabled:
            logger.debug("Telegram alert skipped (not enabled)")
            return False

        sent = await self._send_with_bot(message, parse_mode)
        if not sent:
            sent = await asyncio.to_thread(self._send_via_http, message, parse_mode)

        if not sent and parse_mode:
            logger.info("Retrying Telegram alert without parse_mode")
            sent = await self._send_with_bot(message, None)
            if not sent:
                sent = await asyncio.to_thread(self._send_via_http, message, None)

        if sent:
            logger.debug("Telegram alert sent")
        else:
            logger.error("Telegram alert failed after retries")

        return sent

    def _resolve_chat_id(self, channel: str | None) -> str:
        """Return the chat_id for a given channel, falling back to default."""
        if channel:
            cid = self._channel_map.get(channel, "")
            if cid:
                return cid
        return self.chat_id

    def send_alert_sync(
        self,
        message: str,
        parse_mode: str | None = None,
        channel: str | None = None,
        is_signal: bool = False,
    ) -> bool:
        """Synchronous send with optional channel routing.

        Args:
            message: Text to send.
            parse_mode: Telegram parse mode.
            channel: Route to a specific channel ("ipl", "psl", "signals", or None for default).
            is_signal: If True, also sends to the signals channel (if configured).
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            loop.create_task(self.send_alert(message, parse_mode=parse_mode))
            return True

        if not self.enabled:
            logger.debug("Telegram alert skipped (not enabled)")
            return False

        target_chat_id = self._resolve_chat_id(channel)
        sent = self._send_via_http_to(target_chat_id, message, parse_mode)
        if not sent and parse_mode:
            logger.info("Retrying Telegram alert without parse_mode")
            sent = self._send_via_http_to(target_chat_id, message, None)

        # Also send to signals channel if this is a trading signal
        if is_signal:
            signals_id = self._channel_map.get("signals", "")
            if signals_id and signals_id != target_chat_id:
                self._send_via_http_to(signals_id, message, parse_mode)

        if sent:
            logger.debug("Telegram alert sent to %s", channel or "default")
        else:
            logger.error("Telegram alert failed after retries")

        return sent

    def _send_via_http_to(self, chat_id: str, message: str, parse_mode: str | None) -> bool:
        """Send to a specific chat_id via HTTP API."""
        payload = {
            "chat_id": chat_id,
            "text": message,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode

        try:
            response = requests.post(self._api_url, json=payload, timeout=20)
        except Exception:
            logger.exception("Failed to send Telegram alert via HTTP API")
            return False

        if response.ok:
            return True

        logger.warning(
            "Telegram API send failed (%s): %s",
            response.status_code,
            response.text[:300],
        )
        return False
