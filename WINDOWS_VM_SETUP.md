# IPL Edge Spotter — Windows VM Deployment & Verification

## Setup Instructions

### 1. Copy the bot to the Windows VM

Copy the entire `ipl_bot` folder to:
```
C:\Users\Administrator\.openclaw\workspace\ipl_bot\
```

### 2. Install Python dependencies

```cmd
cd C:\Users\Administrator\.openclaw\workspace\ipl_bot
pip install requests python-telegram-bot flask python-dotenv
```

### 3. Verify the config file

Open `ipl_spotter_config.json` and confirm all keys are present:
```json
{
  "sportmonks_api_key": "DUTV96Mrw0h07B64Y80htZF05SdSO4Yd6YCkIj7OwtjmZWImp6yXPLUbmfte",
  "cloudbet_api_key": "<JWT token>",
  "telegram_bot_token": "8608752619:AAGyTmp_lIJ6PMH5Q5-bjNRfMD0YY9wCp5A",
  "telegram_chat_id": "6280075826",
  "theodds_api_key": "7d105f155c4071475ce03aa04a78078b",
  "default_currency": "USD",
  "max_position_size_usd": 10.0,
  "bankroll_usd": 100.0
}
```

### 4. Run the bot

```cmd
cd C:\Users\Administrator\.openclaw\workspace\ipl_bot
python spotter.py
```

---

## Verification Checklist

Run these checks IN ORDER to confirm everything works:

### Check 1: Python & Dependencies
```cmd
python --version
python -c "import requests; print('requests OK')"
python -c "import flask; print('flask OK')"
```
Expected: Python 3.9+ and no import errors.

### Check 2: Config loads
```cmd
cd C:\Users\Administrator\.openclaw\workspace\ipl_bot
python -c "from config import load_config; c = load_config(); print(f'Keys: {len(c)}'); print(f'Currency: {c.get(\"default_currency\")}')"
```
Expected: Keys: ~20, Currency: USD

### Check 3: Cloudbet API + Balance
```cmd
python -c "
from modules.odds_client import OddsClient
from config import load_config
c = load_config()
client = OddsClient(c)
bal = client.get_balance('USD')
print(f'Cloudbet USD Balance: ${bal}')
events = client.get_ipl_events()
print(f'IPL events on Cloudbet: {len(events)}')
for e in events:
    if e.get('type') == 'EVENT_TYPE_EVENT':
        h = (e.get('home') or {}).get('name', '?')
        a = (e.get('away') or {}).get('name', '?')
        print(f'  Match: {h} vs {a}')
"
```
Expected: Balance shown (should be ~$88), IPL events listed.

### Check 4: Sportmonks API
```cmd
python -c "
from modules.cricket_client import CricketClient
from config import load_config
c = load_config()
client = CricketClient(c)
sid = client.get_current_season_id()
print(f'IPL season ID: {sid}')
matches = client.get_live_ipl_matches()
print(f'Live IPL matches: {len(matches)}')
fixtures = client.get_ipl_fixtures(sid)
print(f'IPL fixtures this season: {len(fixtures)}')
"
```
Expected: Season ID returned, fixtures listed (live matches only during match time).

### Check 5: The Odds API
```cmd
python -c "
from modules.theodds_client import TheOddsClient
from config import load_config
c = load_config()
client = TheOddsClient(c)
events = client.get_ipl_odds()
print(f'IPL events with odds: {len(events)}')
for e in events[:3]:
    print(f'  {e.get(\"home_team\")} vs {e.get(\"away_team\")}')
    print(f'    Bookmakers: {len(e.get(\"bookmakers\", []))}')
fair = client.get_fair_probability(events[0]['home_team'], events[0]['away_team']) if events else None
if fair:
    print(f'  Fair prob: {fair[\"home_team\"]} {fair[\"home_fair_prob\"]*100:.1f}% vs {fair[\"away_team\"]} {fair[\"away_fair_prob\"]*100:.1f}%')
    print(f'  From {fair[\"bookmakers_count\"]} bookmakers')
print(f'Quota remaining: {client.requests_remaining}')
"
```
Expected: IPL events with 25+ bookmakers, fair probabilities shown.

### Check 6: Historical Stats DB
```cmd
python -c "
from modules.stats_db import StatsDB
db = StatsDB('data/ipl_stats.db')
for venue in ['Wankhede Stadium', 'M.Chinnaswamy Stadium', 'MA Chidambaram Stadium']:
    s = db.get_venue_stats(venue)
    print(f'{venue}: {s[\"matches\"]} matches, avg: {s[\"avg_first_innings\"]:.0f}')
for p in ['V Kohli', 'MS Dhoni', 'RG Sharma']:
    s = db.get_player_batting_stats(p)
    print(f'{p}: {s[\"innings\"]} inn, avg {s[\"avg_runs\"]:.0f}, SR {s[\"avg_strike_rate\"]:.0f}')
db.close()
"
```
Expected: Venue stats (1000+ total matches), player stats with reasonable averages.

### Check 7: Telegram
```cmd
python -c "
from modules.telegram_bot import TelegramNotifier
from config import load_config
c = load_config()
t = TelegramNotifier(c)
print(f'Telegram enabled: {t.enabled}')
t.send_alert_sync('🏏 IPL Bot test from Windows VM — all systems go!')
print('Message sent — check Telegram')
"
```
Expected: "Telegram enabled: True" and message appears on your phone.

### Check 8: Risk Manager
```cmd
python -c "
from modules.risk_manager import RiskManager
from config import load_config
c = load_config()
rm = RiskManager(c)
print(f'Bankroll: ${rm.bankroll_usd}')
print(f'Max stake: ${rm.max_position_size_usd}')
print(f'Max open bets: {rm.max_open_bets}')
stake = rm.calculate_stake(ev_pct=10.0, odds=1.85)
print(f'Kelly stake for 10% EV @ 1.85: ${stake:.2f}')
can, reason = rm.can_place_bet(10.0, 1.85, 'test', 0)
print(f'Can place bet: {can} ({reason})')
"
```
Expected: Bankroll $100, stake ~$2-3 for 10% EV.

### Check 9: Bet Executor (dry run)
```cmd
python -c "
from modules.bet_executor import BetExecutor
from config import load_config
c = load_config()
be = BetExecutor(c)
print(f'BetExecutor ready')
print(f'Currency: {be.currency}')
print(f'Open bets: {len(be.open_bets)}')
status = be.get_status()
print(f'Status: {status}')
"
```
Expected: BetExecutor ready, currency USD, 0 open bets.

### Check 10: Full Tests
```cmd
cd C:\Users\Administrator\.openclaw\workspace\ipl_bot
pip install pytest
python -m pytest tests/ -q
```
Expected: 406 passed.

### Check 11: Start the Bot
```cmd
python spotter.py
```
Expected output:
```
╔══════════════════════════════════════════════════════════╗
║        🏏 IPL Edge Spotter v1.0.0                        ║
║        ⚡ MODE: LIVE BETTING                              ║
╠══════════════════════════════════════════════════════════╣
║  Cloudbet Balance: $88.85    (USD)                       ║
║  Max stake: $10.00 | Kelly: 25%                          ║
...
```

If no live IPL match, it will say "No live IPL matches — waiting..." every few minutes. The first match is RCB vs SRH on March 28 at 7:30 PM IST.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `ModuleNotFoundError` | Run from `ipl_bot/` directory, not parent |
| `No live IPL matches` | Normal when no match is on. Bot auto-detects live matches |
| `Cloudbet Balance: N/A` | Check cloudbet_api_key in config |
| `Telegram not sending` | Verify telegram_bot_token and telegram_chat_id |
| `406 tests not all passing` | Run `pip install requests flask python-telegram-bot` |
