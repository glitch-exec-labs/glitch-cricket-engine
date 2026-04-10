# IPL Edge Spotter — Windows VM Deployment & Verification

This document is intentionally sanitized for public release.

## Setup Instructions

### 1. Copy the bot to the Windows VM

Copy the entire repository to a working directory such as:

```text
C:\Users\Administrator\.openclaw\workspace\ipl_bot\
```

### 2. Install Python dependencies

```cmd
cd C:\Users\Administrator\.openclaw\workspace\ipl_bot
pip install -r requirements.txt
```

### 3. Prepare the config file

Use the public example file as your template:

```cmd
copy ipl_spotter_config.example.json ipl_spotter_config.json
```

Fill in your own values for:
- `sportmonks_api_key`
- `cloudbet_api_key`
- `telegram_bot_token`
- `telegram_chat_id`
- `theodds_api_key`

Do not paste real production credentials into shared screenshots or public docs.

### 4. Run the bot

```cmd
cd C:\Users\Administrator\.openclaw\workspace\ipl_bot
python spotter.py
```

## Verification Checklist

### Check 1: Python & Dependencies
```cmd
python --version
python -c "import requests; print('requests OK')"
python -c "import flask; print('flask OK')"
```

### Check 2: Config loads
```cmd
cd C:\Users\Administrator\.openclaw\workspace\ipl_bot
python -c "from config import load_config; c = load_config(); print(f'Keys: {len(c)}'); print(f'Currency: {c.get(\"default_currency\")}')"
```

### Check 3: Cloudbet connectivity
```cmd
python -c "from modules.odds_client import OddsClient; from config import load_config; c = load_config(); client = OddsClient(c); print(client.get_balance('USD'))"
```

### Check 4: Sportmonks connectivity
```cmd
python -c "from modules.cricket_client import CricketClient; from config import load_config; c = load_config(); client = CricketClient(c); print(client.get_current_season_id())"
```

### Check 5: Optional odds/reference providers
```cmd
python -c "from modules.theodds_client import TheOddsClient; from config import load_config; c = load_config(); client = TheOddsClient(c); print(client.enabled)"
```

## Public Safety Note

This document uses placeholders only. Keep real credentials in local config files that are ignored by git.
