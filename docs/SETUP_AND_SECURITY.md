# Setup and Security

## First-Time Setup

1. Create a fresh virtual environment.
2. Install dependencies from `requirements.txt`.
3. Copy `ipl_spotter_config.example.json` to `ipl_spotter_config.json`.
4. Add your own provider credentials.
5. Start `spotter.py` and `liveline_bot.py` separately.

## Public Repository Safety Rules

Never commit:
- `ipl_spotter_config.json`
- `.env` files
- runtime databases in `data/`
- logs in `logs/`
- local virtualenvs
- model artifacts or training outputs in local runtime folders
- private inspection helpers containing environment-specific values

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
- confirm no live `.db`, `.log`, `.session`, `.pid`, or `.env` files are staged
- confirm API keys are blank in shared config examples
- confirm docs do not contain pasted provider credentials

## Windows / Alternate Host Deployments

If you deploy outside the main Linux server, use the sanitized setup templates in this repo and do not copy old production config files into a public working tree.
