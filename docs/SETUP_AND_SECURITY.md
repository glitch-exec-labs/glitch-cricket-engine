# Setup and Security

## First-Time Setup

1. Create a fresh virtual environment.
2. Install dependencies.
3. Copy `ipl_spotter_config.example.json` to `ipl_spotter_config.json`.
4. Add your own provider credentials.
5. Start `spotter.py` and `liveline_bot.py` separately.

## Do Not Commit

Never commit:
- `ipl_spotter_config.json`
- `.env` files
- runtime databases in `data/`
- logs in `logs/`
- local virtualenvs

## Secret Rotation Checklist

If this project was ever run with real keys in plaintext config, rotate at least:
- Sportmonks API key
- Cloudbet API key(s)
- Telegram bot token
- OpenAI API key
- Brave API key
- CricData API key
- Weather API key
- TheOdds API key
- Telegram API ID / hash if they were tied to a production account

## GitHub Publishing Checklist

Before pushing publicly:
- confirm `.gitignore` excludes local state and secrets
- confirm only `ipl_spotter_config.example.json` is tracked
- confirm no live `.db`, `.log`, `.session`, or `.env` files are staged
- confirm API keys are blank in shared config examples
