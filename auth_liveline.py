"""One-time auth for the live line Telegram listener.

Run this once: python3 auth_liveline.py
It will ask for your phone number and OTP, then save the session.
After that, the bot can read the live line channel automatically.
"""

import asyncio
import os
import sys
from telethon import TelegramClient

# NOTE (2026-04-17): credentials were previously hardcoded here and leaked
# publicly. They have been removed. When reactivating this project, create a
# fresh Telegram app at https://my.telegram.org/apps and export:
#   TELEGRAM_API_ID, TELEGRAM_API_HASH, LIVELINE_CHANNEL
# The old hardcoded values must be considered compromised — do NOT reuse them.
API_ID = int(os.environ["TELEGRAM_API_ID"]) if os.environ.get("TELEGRAM_API_ID") else None
API_HASH = os.environ.get("TELEGRAM_API_HASH")
CHANNEL = os.environ.get("LIVELINE_CHANNEL")

if not (API_ID and API_HASH and CHANNEL):
    sys.exit("Missing env vars: TELEGRAM_API_ID, TELEGRAM_API_HASH, LIVELINE_CHANNEL")

async def main():
    client = TelegramClient("data/liveline_session", API_ID, API_HASH)

    print("=== Live Line Auth ===")
    print("This will ask for your phone number and OTP code.")
    print("Only needed once — session is saved for future use.")
    print()

    await client.start()

    me = await client.get_me()
    print(f"\nLogged in as: {me.first_name} {me.last_name or ''}")

    # Test channel access
    try:
        entity = await client.get_entity(CHANNEL)
        print(f"Channel access OK: {entity.title}")

        messages = await client.get_messages(entity, limit=3)
        print(f"\nLast {len(messages)} messages:")
        for msg in reversed(messages):
            print(f"  {msg.text[:80] if msg.text else '(no text)'}")

        print(f"\nChannel ID: {entity.id}")
        print("\nAuth complete! The bot can now read this channel.")

    except Exception as e:
        print(f"\nChannel error: {e}")
        print("Make sure you've joined the channel first!")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
