# Match Co-Pilot Design

**Date:** 2026-03-26
**Status:** Approved

## Problem

The IPL bot currently auto-detects edges and places bets on Cloudbet, but the user also bets manually on an Indian book (10wicket.com) which has different mechanics. The bot needs to:

1. Auto-bet on Cloudbet (back only, decimal odds)
2. Guide the user on 10wicket via Telegram (khai-lagai for match winner, session lines for runs)
3. Tell the user when to book profit (lay off positions)
4. Provide over-by-over match updates

## Two Betting Systems

### Cloudbet (Auto)
- Decimal odds, back only
- Match winner, innings total, PP runs, over runs, player runs, sixes, fours
- Max stake $10, Kelly-sized
- Bot places bets automatically, no user interaction

### 10wicket / Indian Book (Manual Guide)
- **Match Winner**: Khai-Lagai format. e.g. SRH 1.60-1.62. Lagai = back, Khai = lay. User can both back and lay.
- **Sessions**: Line moves with run rate, payout at even money per run. e.g. "6 Over Session: 55-56" means YES 56 / NO 55 at Rs/run.
- User stakes vary Rs 200-1000 per bet
- Bot sends Telegram calls, assumes user follows them, tracks shadow portfolio

## Session Mechanics (Indian Book)

Sessions are NOT over/under at fixed odds. The line moves live:

```
Over 3: RCB 30/0 (RR 10.0)  → 6 Over Session: 55-56
Over 4: RCB 49/0 (RR 12.25) → 6 Over Session: 62-63
```

Payout = (actual - line) x stake_per_run if YES, or (line - actual) x stake_per_run if NO.

### Session Booking Formula
```
Entry:  YES line_entry @ Rs X/run
Exit:   NO line_exit @ Rs X/run  (when line has moved up)
Profit: (line_exit - line_entry) x Rs X = GUARANTEED
```

Example: YES 56 then NO 64 at Rs 200/run = (64-56) x 200 = Rs 1600 guaranteed.

### Match Winner Booking Formula (Khai-Lagai)
```
Entry:  Lagai (back) Team A @ back_odds for Rs stake
Exit:   Khai (lay) Team A @ lay_odds for Rs (stake x back_odds / lay_odds)

If Team A wins:  +stake x (back_odds - 1) - exit_stake x (lay_odds - 1)
If Team A loses: -stake + exit_stake
Guaranteed:      stake x (back_odds - lay_odds) / lay_odds
```

Example: Lagai SRH @ 2.30 for Rs 500, later Khai SRH @ 1.75 for Rs 657.
Guaranteed = 500 x (2.30 - 1.75) / 1.75 = Rs 157.

## New Modules

### 1. MatchCopilot (match_copilot.py)

Orchestrates the entire match experience. Sits between spotter's scan loop and all output.

**Match phases:**
- PRE_MATCH: pre-match report with model projections vs book lines
- TOSS: toss result, initial calls
- INNINGS_1_PP (overs 1-6): PP session tracking, over-by-over updates
- INNINGS_1_MIDDLE (overs 7-15): middle overs sessions, match winner shifts
- INNINGS_1_DEATH (overs 16-20): death overs, innings total convergence
- INNINGS_BREAK: target set, 2nd innings preview
- INNINGS_2_PP: chase begins, session + MW tracking
- INNINGS_2_MIDDLE: required rate tracking, MW swings
- INNINGS_2_CHASE (overs 16-20): endgame, book remaining positions
- POST_MATCH: final P&L summary

**Per-over duties:**
- Send over summary (runs, wickets, RR, projections)
- Check if any shadow position can be booked
- Check for new session/MW calls
- Update match winner probability

### 2. PositionBook (position_book.py)

Tracks two separate books:

```python
@dataclass
class SessionPosition:
    market: str           # "6_over", "10_over", "20_over", "player_runs_kohli"
    direction: str        # "YES" or "NO"
    entry_line: float     # 56.0
    stake_per_run: float  # 200.0 (INR)
    timestamp: datetime
    status: str           # "OPEN", "BOOKED", "SETTLED"
    exit_line: float | None      # 64.0 when booked
    booked_profit: float | None  # (64-56)*200 = 1600

@dataclass
class MatchWinnerPosition:
    team: str             # "SRH"
    direction: str        # "LAGAI" (back) or "KHAI" (lay)
    odds: float           # 2.30
    stake: float          # 500.0 (INR)
    timestamp: datetime
    status: str           # "OPEN", "BOOKED", "SETTLED"
    exit_odds: float | None
    exit_stake: float | None
    booked_profit: float | None

class PositionBook:
    cloudbet_positions: list[LiveBet]       # real, from BetExecutor
    shadow_sessions: list[SessionPosition]  # assumed 10wicket
    shadow_mw: list[MatchWinnerPosition]    # assumed 10wicket

    def add_session_call(market, direction, line, stake_per_run) -> SessionPosition
    def add_mw_call(team, direction, odds, stake) -> MatchWinnerPosition
    def book_session(position, exit_line) -> float  # returns guaranteed profit
    def book_mw(position, exit_odds) -> float       # returns guaranteed profit
    def settle_all(actual_results: dict)            # mark WON/LOST after match
    def get_open_positions() -> list
    def get_session_pnl() -> float
    def get_mw_pnl() -> float
    def get_total_pnl() -> dict  # {cloudbet_usd, shadow_inr, combined_approx_usd}
```

### 3. HedgeCalculator (hedge_calculator.py)

Determines when to send BOOK alerts.

```python
class HedgeCalculator:
    def check_session_book(position: SessionPosition, current_line: float) -> BookOpportunity | None
        # Trigger if (current_line - entry_line) >= 4 runs for YES
        # Or if (entry_line - current_line) >= 4 runs for NO
        # And guaranteed_profit >= min_profit_inr (default 100)

    def check_mw_book(position: MatchWinnerPosition, current_odds: float) -> BookOpportunity | None
        # Trigger if odds moved 0.20+ in favor
        # And guaranteed_profit >= min_profit_inr (default 100)

    def calculate_session_book(entry_line, exit_line, stake_per_run) -> dict
        # Returns: {profit_guaranteed, action, exit_line, exit_stake_per_run}

    def calculate_mw_book(entry_odds, exit_odds, entry_stake) -> dict
        # Returns: {profit_guaranteed, exit_stake, if_wins, if_loses, action}

@dataclass
class BookOpportunity:
    position: SessionPosition | MatchWinnerPosition
    action: str           # "Khai NO 64 @ Rs 200/run" or "Khai SRH @ 1.75 for Rs 657"
    guaranteed_profit: float
    math_breakdown: str   # human-readable P&L explanation
```

### 4. CopilotTelegram (copilot_telegram.py)

Extends TelegramNotifier with co-pilot message formatting.

**Message types and emojis:**

| Emoji | Type | Method |
|-------|------|--------|
| `format_pre_match()` | Pre-match report | |
| `format_toss()` | Toss result + adjustments | |
| `format_session_call()` | Session bet recommendation | |
| `format_mw_call()` | Match winner recommendation | |
| `format_auto_bet()` | Cloudbet auto-bet confirmation | |
| `format_over_update()` | Over-by-over compact update | |
| `format_book_alert()` | HEDGE/BOOK profit alert | |
| `format_mw_swing()` | Match winner probability swing | |
| `format_match_commentary()` | Situational analysis | |
| `format_session_summary()` | End-of-match P&L | |

**Throttling:** Max 1 message per 20 seconds to avoid Telegram spam. Over updates batched if multiple overs pass in one scan.

## Integration With Existing Spotter

The MatchCopilot hooks into `_process_match()` in spotter.py:

```
_process_match():
    ... existing ball processing ...
    ... existing predictions ...
    ... existing edge detection + auto-bet on Cloudbet ...

    # NEW: Co-pilot logic
    copilot.on_new_balls(match_state, predictions)
    copilot.check_session_calls(model_predictions, current_session_lines)
    copilot.check_mw_calls(model_win_prob, current_mw_odds)
    copilot.check_book_opportunities(current_lines, current_odds)
    copilot.send_over_update_if_due(match_state)
```

## Session Line Estimation

10wicket lines aren't available via API. The bot estimates them from:
1. Cloudbet over/under lines (available via API) as baseline
2. Current run rate extrapolation
3. Model predictions

The Telegram call includes "10wicket line should be around X" so the user can verify before acting.

## Config Additions

```json
{
    "copilot_enabled": true,
    "shadow_currency": "INR",
    "shadow_min_stake_inr": 200,
    "shadow_max_stake_inr": 1000,
    "shadow_default_stake_inr": 500,
    "shadow_mw_default_stake_inr": 500,
    "hedge_min_profit_inr": 100,
    "hedge_session_min_runs": 4,
    "hedge_mw_min_odds_move": 0.20,
    "over_by_over_updates": true,
    "match_winner_tracking": true,
    "win_prob_swing_threshold_pct": 10,
    "message_throttle_seconds": 20
}
```

## Example Full Match Telegram Flow

```
--- PRE-MATCH ---
 PRE-MATCH: RCB vs SRH @ Chinnaswamy
   Cloudbet: RCB 1.65 | SRH 2.30
   10wicket: RCB ~1.58-1.60 | SRH ~2.25-2.30
   Consensus (27 books): RCB 59% | SRH 41%
   6 Over Line (est): 53-54 | Model: 57
   Venue avg PP: 52.3 | Chinnaswamy modifier: +5

--- TOSS ---
 TOSS: RCB win, bat first
   Adjust: +2 (batting first at Chinnaswamy)
   Updated 6 Over est: 55-56 | Model: 59

--- SESSION CALL ---
 SESSION: Lagai YES 56 (6 Over) @ Rs 300/run
   Model: 59 | Line 56 = 3 runs underpriced
   [Cloudbet: Auto-placed YES 54.5 @ 1.88 for $3.20]

--- MATCH WINNER CALL ---
 MW: Lagai SRH @ 2.30 for Rs 500
   Model: SRH 43% = fair 2.33, slight value on underdog

--- OVER UPDATES ---
 Over 3 | RCB 34/0 | RR 11.3 | Proj PP: 62
   Kohli 22*(14) | Faf 10*(4)
   6 Over line est: 59-60
   MW: RCB 1.45 | SRH 2.65
   Your YES 56: +3 runs ahead, HOLD
   Your SRH lagai: odds drifting, HOLD

 Over 5 | RCB 52/1 | Kohli OUT c&b
   6 Over line est: 62-63
   MW: RCB 1.55 | SRH 2.40
    BOOK SESSION: Khai NO 62 @ Rs 300/run
    Locked: (62-56) x Rs 300 = Rs 1800 GUARANTEED

 Over 6 DONE | PP: 65 runs
   YES 56 SETTLED: +(65-56) x 300 = +Rs 2700
   NO 62 SETTLED: -(65-62) x 300 = -Rs 900
   Net: Rs 1800 (booked)
   [Cloudbet YES 54.5: WON +$2.82]

--- MIDDLE OVERS ---
 Over 12 | RCB 82/4 | Collapse!
   MW: RCB 2.00-2.05 | SRH 1.75-1.80
    MW SWING: SRH 2.30 -> 1.75 (your lagai in profit)
    BOOK MW: Khai SRH @ 1.75 for Rs 657
    Locked: Rs 157 GUARANTEED
     SRH wins: +650 - 492 = +Rs 158
     SRH loses: -500 + 657 = +Rs 157

--- DEATH OVERS ---
 SESSION: Lagai YES 155 (20 Over) @ Rs 200/run
   Model: 162 | Line 155 = 7 runs underpriced after collapse recovery

 Over 19 | RCB 158/7
   20 Over line est: 164-165
    BOOK: Khai NO 164 @ Rs 200/run
    Locked: (164-155) x Rs 200 = Rs 1800

--- INNINGS BREAK ---
 TARGET: RCB 168 all out
   20 Over session: Booked Rs 1800
   [Cloudbet UNDER 171.5: WON +$3.77]
   Chase preview: SRH need 169 @ 8.45/over
   MW: SRH 1.55

--- 2ND INNINGS + POST MATCH ---
(similar flow continues)

--- MATCH END ---
 SESSION P&L:
   CLOUDBET (auto):
     YES PP 54.5 @ 1.88: WON +$2.82
     UNDER 171.5 @ 1.92: WON +$3.77
     SRH MW @ 1.82: WON +$3.69
     Total: +$10.28

   10WICKET (shadow):
     6 Over YES 56: Booked Rs 1800
     MW SRH: Booked Rs 157
     20 Over YES 155: Booked Rs 1800
     Total: Rs 3757 (~$45)

   COMBINED: ~$55.28
```

## Files to Create/Modify

### New files:
- `modules/match_copilot.py` — main copilot orchestrator
- `modules/position_book.py` — dual portfolio tracker
- `modules/hedge_calculator.py` — booking math engine
- `modules/copilot_telegram.py` — formatted Telegram messages
- `tests/test_match_copilot.py`
- `tests/test_position_book.py`
- `tests/test_hedge_calculator.py`
- `tests/test_copilot_telegram.py`

### Modified files:
- `spotter.py` — integrate copilot into _process_match()
- `ipl_spotter_config.json` — add copilot config keys
- `config.py` — load new config keys with defaults
