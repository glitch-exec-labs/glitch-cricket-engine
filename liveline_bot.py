"""
Live Line Bot — standalone process for Indian book live line signals.

Reads FERRARI FAST LINE™ Telegram channel in real time using Telethon userbot.
Parses every message and forwards formatted updates + signals to your Telegram.

Runs completely independently from spotter.py (Cloudbet bot).

Start:   sudo systemctl start ipl-liveline.service
Logs:    tail -f logs/liveline.err.log
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

# ── Logging setup ────────────────────────────────────────────────────────────

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stderr),
    ],
)
logger = logging.getLogger("liveline_bot")

# ── Imports ───────────────────────────────────────────────────────────────────

from telethon import TelegramClient, events
from modules.liveline_parser import LiveLineParser


# ── Config ────────────────────────────────────────────────────────────────────

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "ipl_spotter_config.json")

def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return json.load(f)


# ── Telegram sender ───────────────────────────────────────────────────────────

import httpx

class TelegramSender:
    """Sends messages to Telegram via Bot API."""

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self._base = f"https://api.telegram.org/bot{token}"

    def send(self, text: str) -> bool:
        try:
            r = httpx.post(
                f"{self._base}/sendMessage",
                json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"},
                timeout=10,
            )
            return r.status_code == 200
        except Exception as e:
            logger.warning("Telegram send failed: %s", e)
            return False


# ── Message formatter ─────────────────────────────────────────────────────────

class LiveLineFormatter:
    """Formats parsed live line data into clean Telegram messages."""

    def __init__(self, throttle_seconds: int = 20):
        self._last_mw_send: float = 0
        self._last_session: Dict[int, float] = {}   # over → last sent time
        self._last_score_over: float = -1.0          # last over we sent score for
        self._sent_dedup: Set[tuple] = set()
        self._throttle = throttle_seconds

    def format_mw(self, update: dict) -> Optional[str]:
        """Rate update: '38-41 BENGALURU' → one line per over."""
        now = time.time()
        if now - self._last_mw_send < self._throttle:
            return None                              # throttle MW spam
        self._last_mw_send = now

        team = update.get("mw_team", "")
        back = update.get("mw_back", 0)
        lay = update.get("mw_lay", 0)
        back_d = update.get("mw_back_decimal", 0)
        lay_d = update.get("mw_lay_decimal", 0)

        if not team:
            return None

        # Favourite vs underdog indicator
        fav = "🏆 FAV" if back < 100 else "💸 DOG"
        spread = lay - back
        return (
            f"💰 <b>MW Rates</b>\n"
            f"{fav} {team}\n"
            f"Back: {back} ({back_d:.2f})  |  Lay: {lay} ({lay_d:.2f})\n"
            f"Spread: {spread} pts"
        )

    def format_session(self, update: dict) -> Optional[str]:
        """Session line update: '66-7 6 OVER' → YES/NO with context."""
        session_over = update.get("session_over", 0)
        yes = update.get("session_yes", 0)
        no_spread = update.get("session_no", 0)

        if not session_over or not yes:
            return None

        now = time.time()
        last = self._last_session.get(session_over, 0)
        if now - last < 30:                          # don't spam same session
            return None
        self._last_session[session_over] = now

        no_line = yes - no_spread
        phase_name = {
            6: "PowerPlay (1–6)",
            10: "Middle-1 (1–10)",
            15: "Middle-2 (1–15)",
            20: "Full Innings",
        }.get(session_over, f"{session_over}-Over")

        return (
            f"📋 <b>{phase_name} Session Line</b>\n"
            f"🟢 YES ≥ {yes}  |  🔴 NO ≤ {no_line}\n"
            f"(spread {no_spread} pts)"
        )

    def format_score(self, update: dict, parser_state: dict) -> Optional[str]:
        """Ball-by-ball score line — send once per completed over."""
        over_num = update.get("over_num", 0)
        ball_num = update.get("ball_num", 0)
        score = update.get("score", 0)
        wickets = update.get("wickets", 0)
        overs = update.get("overs", 0.0)

        # Only send at end of each over (ball 6) or wicket ball
        if ball_num != 6:
            return None

        dedup = ("score_over", over_num)
        if dedup in self._sent_dedup:
            return None
        self._sent_dedup.add(dedup)

        striker = parser_state.get("striker", "")
        bowler = parser_state.get("bowler", "")
        mw_team = parser_state.get("mw_team", "")
        mw_back = parser_state.get("mw_back", 0)
        mw_lay = parser_state.get("mw_lay", 0)

        rpo = round(score / overs, 2) if overs > 0 else 0

        parts = [
            f"🏏 <b>End of Over {over_num}</b>",
            f"Score: {score}/{wickets}  ({overs} ov, {rpo:.1f} RPO)",
        ]
        if striker:
            parts.append(f"Bat: {striker}")
        if bowler:
            parts.append(f"Bowl: {bowler}")
        if mw_team and mw_back:
            parts.append(f"MW: {mw_team} {mw_back}-{mw_lay}")

        return "\n".join(parts)

    def format_delivery(self, update: dict) -> Optional[str]:
        """Per-delivery line for ball-by-ball commentary."""
        bowler = update.get("bowler", "")
        batsman = update.get("striker", "")
        runs = update.get("last_ball_runs", 0)
        is_wicket = update.get("is_wicket", False)

        if not bowler or not batsman:
            return None

        if is_wicket:
            return f"🔴 WICKET! {bowler} → {batsman}"
        if runs == 4:
            return f"🟢 FOUR! {bowler} → {batsman}"
        if runs == 6:
            return f"🟣 SIX! {bowler} → {batsman}"
        # Dot / 1-3 runs — no message (too noisy)
        return None

    def format_commentary(self, update: dict) -> Optional[str]:
        """Commentary events: FOUR, SIX, WICKET, DOT."""
        event = update.get("last_event", "")
        if event == "FOUR":
            return "🟢 <b>FOUR!</b>"
        if event == "SIX":
            return "🟣 <b>SIX!</b>"
        if event == "WICKET":
            return "🔴 <b>WICKET!</b>"
        return None


# ── Main bot ──────────────────────────────────────────────────────────────────

class LiveLineBot:
    """Standalone live line bot — reads Telegram channel, sends formatted updates."""

    def __init__(self, config: dict):
        self.config = config
        self.api_id = config["telegram_api_id"]
        self.api_hash = config["telegram_api_hash"]
        self.channel = config["liveline_channel"]

        bot_token = config["telegram_bot_token"]
        chat_id = config["telegram_chat_id"]
        self.telegram = TelegramSender(bot_token, chat_id)

        self.parser = LiveLineParser()
        self.formatter = LiveLineFormatter(
            throttle_seconds=config.get("message_throttle_seconds", 20)
        )

        self.client = TelegramClient(
            "data/liveline_session",
            self.api_id,
            self.api_hash,
        )

    async def run(self) -> None:
        logger.info("Connecting to Telegram...")
        await self.client.start()
        logger.info("Connected ✓")

        entity = await self.client.get_entity(self.channel)
        logger.info("Listening to: %s (id=%s)", getattr(entity, "title", self.channel), entity.id)

        # Send startup ping
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")
        self.telegram.send(
            f"📡 <b>Live Line Bot started</b>\n"
            f"Channel: {getattr(entity, 'title', self.channel)}\n"
            f"Time: {now}"
        )

        @self.client.on(events.NewMessage(chats=entity))
        async def handler(event):
            text = event.raw_text
            if not text:
                return
            try:
                self._handle_message(text)
            except Exception:
                logger.debug("Handle error", exc_info=True)

        logger.info("Listening for messages...")
        await self.client.run_until_disconnected()

    def _handle_message(self, text: str) -> None:
        """Parse a raw message and dispatch formatted Telegram output."""
        update = self.parser.parse_message(text)
        if not update:
            return

        update_type = update.get("type", "")
        state = self.parser.get_state()

        msg: Optional[str] = None

        if update_type == "match_winner":
            msg = self.formatter.format_mw(update)

        elif update_type == "session":
            msg = self.formatter.format_session(update)

        elif update_type == "score":
            msg = self.formatter.format_score(update, state)

        elif update_type == "delivery":
            msg = self.formatter.format_delivery(update)

        elif update_type == "commentary":
            msg = self.formatter.format_commentary(update)

        if msg:
            logger.info("Sending: %s", msg[:80])
            self.telegram.send(msg)


def main() -> None:
    config = load_config()

    bot = LiveLineBot(config)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    _stop_event = asyncio.Event()

    def _shutdown(sig, frame):
        logger.info("Shutdown signal received — disconnecting cleanly...")
        # Schedule graceful disconnect so the session file is not corrupted.
        # loop.stop() alone would kill the loop mid-coroutine.
        async def _do_disconnect():
            try:
                await bot.client.disconnect()
            except Exception:
                pass
            _stop_event.set()

        loop.call_soon_threadsafe(loop.create_task, _do_disconnect())

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    async def _main_with_stop():
        task = loop.create_task(bot.run())
        await _stop_event.wait()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    try:
        loop.run_until_complete(_main_with_stop())
    except Exception:
        logger.exception("LiveLine bot crashed")
        sys.exit(1)
    finally:
        loop.close()


if __name__ == "__main__":
    main()
