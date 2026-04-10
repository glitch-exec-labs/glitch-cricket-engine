"""Live Line Telegram channel listener using Telethon userbot.

Connects to Telegram as a user (not bot) to read messages from
live line channels that don't allow bots. Parses messages and
feeds them into the LiveLineParser for real-time Indian book data.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any, Callable, Dict, Optional

from telethon import TelegramClient, events

from modules.liveline_parser import LiveLineParser

logger = logging.getLogger("ipl_spotter.liveline_listener")


class LiveLineListener:
    """Listens to a Telegram channel via userbot and parses live line data."""

    def __init__(self, config: Dict[str, Any]):
        self.api_id = config.get("telegram_api_id", 0)
        self.api_hash = config.get("telegram_api_hash", "")
        self.source_channel = config.get("liveline_channel", "")
        self.enabled = bool(self.api_id and self.api_hash and self.source_channel)

        self.parser = LiveLineParser()
        self.client: Optional[TelegramClient] = None
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._callbacks: list[Callable] = []

        if not self.enabled:
            logger.info("LiveLine listener disabled — set telegram_api_id, telegram_api_hash, liveline_channel")
        else:
            logger.info("LiveLine listener configured for channel: %s", self.source_channel)

    def on_update(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback for parsed live line updates."""
        self._callbacks.append(callback)

    def start(self) -> None:
        """Start the listener in a background thread."""
        if not self.enabled:
            return

        self._thread = threading.Thread(target=self._run, daemon=True, name="liveline")
        self._thread.start()
        logger.info("LiveLine listener started")

    def _run(self) -> None:
        """Run the Telethon client in its own event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        # Note: loop= was removed in Telethon 1.24+.
        # asyncio.set_event_loop(self._loop) is already called above so
        # Telethon will pick up the correct loop automatically.
        self.client = TelegramClient(
            "data/liveline_session",
            self.api_id,
            self.api_hash,
        )

        self._loop.run_until_complete(self._start_listening())

    async def _start_listening(self) -> None:
        """Connect and listen for messages."""
        try:
            await self.client.start()
            logger.info("LiveLine: Telegram userbot connected")

            # Resolve the channel
            try:
                entity = await self.client.get_entity(self.source_channel)
                logger.info("LiveLine: Listening to '%s' (id=%s)", getattr(entity, 'title', self.source_channel), entity.id)
            except Exception as exc:
                logger.error("LiveLine: Could not resolve channel '%s': %s", self.source_channel, exc)
                return

            @self.client.on(events.NewMessage(chats=entity))
            async def handler(event):
                text = event.raw_text
                if not text:
                    return

                try:
                    parsed = self.parser.parse_message(text)
                    if parsed:
                        # Add raw text for debugging
                        parsed["_raw"] = text[:200]
                        logger.debug("LiveLine parsed: %s", parsed)

                        # Notify callbacks
                        for cb in self._callbacks:
                            try:
                                cb(parsed)
                            except Exception:
                                logger.exception("LiveLine callback error")
                except Exception:
                    logger.debug("LiveLine parse error for: %s", text[:100], exc_info=True)

            logger.info("LiveLine: Listening for messages...")
            await self.client.run_until_disconnected()

        except Exception:
            logger.exception("LiveLine: Connection failed")

    def get_state(self) -> Dict[str, Any]:
        """Get current parsed state from the live line."""
        return self.parser.get_state()

    def get_parser(self) -> LiveLineParser:
        """Get the parser instance for edge checking."""
        return self.parser

    def stop(self) -> None:
        """Stop the listener."""
        if self.client and self._loop:
            self._loop.call_soon_threadsafe(self.client.disconnect)
        logger.info("LiveLine listener stopped")
