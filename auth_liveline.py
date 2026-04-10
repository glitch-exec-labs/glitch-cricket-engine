"""One-time auth for the live line Telegram listener.

Run this once: python3 auth_liveline.py
It will ask for your phone number and OTP, then save the session.
After that, the bot can read the live line channel automatically.
"""

import asyncio
from telethon import TelegramClient

API_ID = 35087526
API_HASH = "4fa10426d6e5836677ac1a51145d5c57"
CHANNEL = "https://t.me/+Su8-m-kbsbUdpcPM"

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
