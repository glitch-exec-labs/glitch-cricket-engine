"""Pure formatting functions for Telegram co-pilot messages. No Telegram dependency.

Design: Clean premium-channel signals for cricket betting.
  🟢 YES = OVER / BACK
  🔴 NO  = UNDER / LAY
"""

from typing import Any, Dict, List, Optional

from modules.shared_core import decimal_to_probability

# ── Signal & cricket emoji ────────────────────────────────────────────
YES  = "🟢"         # session YES / OVER / BACK
NO   = "🔴"         # session NO / UNDER / LAY
WARN = "⚠️"
BOOK = "💰"
INFO = "ℹ️"
FOUR = "4️⃣"         # boundary four
SIX  = "6️⃣"         # maximum six
WIKT = "🎯"         # wicket
BAT  = "🏏"         # batting
BALL = "⚾"         # bowling/ball
STAD = "🏟️"         # stadium/venue
CUP  = "🏆"         # trophy/win
FIRE = "🔥"         # hot streak / strong performance
ICE  = "🧊"         # cold / under pressure
CASH = "💵"         # stake/money
CHART = "📊"        # stats
TIME = "⏰"         # time / overs
TOSS = "🪙"         # coin toss

# ── Team branding ─────────────────────────────────────────────────────
# "name_fragment" → (display_tag, team_emoji)

_IPL_TEAMS: dict[str, tuple[str, str]] = {
    "mumbai":       ("Ⓜ️𝐈  𝗠𝘂𝗺𝗯𝗮𝗶 𝗜𝗻𝗱𝗶𝗮𝗻𝘀",            "🔵"),
    "chennai":      ("🦁 𝗖𝗵𝗲𝗻𝗻𝗮𝗶 𝗦𝘂𝗽𝗲𝗿 𝗞𝗶𝗻𝗴𝘀",         "💛"),
    "kolkata":      ("🪖 𝗞𝗼𝗹𝗸𝗮𝘁𝗮 𝗞𝗻𝗶𝗴𝗵𝘁 𝗥𝗶𝗱𝗲𝗿𝘀",       "💜"),
    "bangalore":    ("👑 𝗥𝗼𝘆𝗮𝗹 𝗖𝗵𝗮𝗹𝗹𝗲𝗻𝗴𝗲𝗿𝘀",           "🔴"),
    "bengaluru":    ("👑 𝗥𝗼𝘆𝗮𝗹 𝗖𝗵𝗮𝗹𝗹𝗲𝗻𝗴𝗲𝗿𝘀",           "🔴"),
    "delhi":        ("🦅 𝗗𝗲𝗹𝗵𝗶 𝗖𝗮𝗽𝗶𝘁𝗮𝗹𝘀",              "🔷"),
    "rajasthan":    ("💎 𝗥𝗮𝗷𝗮𝘀𝘁𝗵𝗮𝗻 𝗥𝗼𝘆𝗮𝗹𝘀",            "💗"),
    "punjab":       ("🗡️ 𝗣𝘂𝗻𝗷𝗮𝗯 𝗞𝗶𝗻𝗴𝘀",                "❤️"),
    "hyderabad":    ("🌅 𝗦𝘂𝗻𝗿𝗶𝘀𝗲𝗿𝘀 𝗛𝘆𝗱𝗲𝗿𝗮𝗯𝗮𝗱",         "🧡"),
    "sunrisers":    ("🌅 𝗦𝘂𝗻𝗿𝗶𝘀𝗲𝗿𝘀 𝗛𝘆𝗱𝗲𝗿𝗮𝗯𝗮𝗱",         "🧡"),
    "lucknow":      ("🐺 𝗟𝘂𝗰𝗸𝗻𝗼𝘄 𝗦𝘂𝗽𝗲𝗿 𝗚𝗶𝗮𝗻𝘁𝘀",        "🩵"),
    "gujarat":      ("⚡ 𝗚𝘂𝗷𝗮𝗿𝗮𝘁 𝗧𝗶𝘁𝗮𝗻𝘀",              "🪨"),
}

_PSL_TEAMS: dict[str, tuple[str, str]] = {
    "lahore":       ("⭐ 𝗟𝗮𝗵𝗼𝗿𝗲 𝗤𝗮𝗹𝗮𝗻𝗱𝗮𝗿𝘀",            "🟢"),
    "karachi":      ("🔵 𝗞𝗮𝗿𝗮𝗰𝗵𝗶 𝗞𝗶𝗻𝗴𝘀",               "🔵"),
    "multan":       ("🟩 𝗠𝘂𝗹𝘁𝗮𝗻 𝗦𝘂𝗹𝘁𝗮𝗻𝘀",              "💚"),
    "islamabad":    ("🔺 𝗜𝘀𝗹𝗮𝗺𝗮𝗯𝗮𝗱 𝗨𝗻𝗶𝘁𝗲𝗱",            "🔴"),
    "peshawar":     ("🟡 𝗣𝗲𝘀𝗵𝗮𝘄𝗮𝗿 𝗭𝗮𝗹𝗺𝗶",              "💛"),
    "quetta":       ("🟣 𝗤𝘂𝗲𝘁𝘁𝗮 𝗚𝗹𝗮𝗱𝗶𝗮𝘁𝗼𝗿𝘀",           "💜"),
    "rawalpindi":   ("⚪ 𝗥𝗮𝘄𝗮𝗹𝗽𝗶𝗻𝗱𝗶",                  "⚪"),
    "kingsmen":             ("👑 𝗛𝘆𝗱𝗲𝗿𝗮𝗯𝗮𝗱 𝗞𝗶𝗻𝗴𝘀𝗺𝗲𝗻",  "🟠"),
    "hyderabad kingsmen":   ("👑 𝗛𝘆𝗱𝗲𝗿𝗮𝗯𝗮𝗱 𝗞𝗶𝗻𝗴𝘀𝗺𝗲𝗻",  "🟠"),
}

# PSL first so PSL-specific fragments (e.g. "kingsmen") are checked before
# generic IPL fragments (e.g. "hyderabad") that could match PSL teams too.
_ALL_TEAMS = {**_PSL_TEAMS, **_IPL_TEAMS}

# ── Per-match entry tracking ──────────────────────────────────────────
# Tracks how many signals have been sent per match per bet type.
# Used to determine 1st/2nd/3rd entry stake advice.
# Key: (match_context, bet_type) → count
_match_entry_counts: dict[str, int] = {}


def get_entry_number(match_key: str, bet_type: str = "session") -> int:
    """Get the current entry number for this match+type and increment."""
    key = f"{match_key}:{bet_type}"
    count = _match_entry_counts.get(key, 0) + 1
    _match_entry_counts[key] = count
    return count


def reset_match_entries(match_key: str) -> None:
    """Reset entry counts for a match (call at match start or innings change)."""
    keys_to_remove = [k for k in _match_entry_counts if k.startswith(match_key)]
    for k in keys_to_remove:
        del _match_entry_counts[k]


def _best_team_match(name: str) -> tuple[str, str] | None:
    """Find the best (longest fragment) team match to avoid IPL/PSL collisions."""
    lower = name.lower()
    best_frag = ""
    best_entry = None
    for fragment, entry in _ALL_TEAMS.items():
        if fragment in lower and len(fragment) > len(best_frag):
            best_frag = fragment
            best_entry = entry
    return best_entry


def team_tag(name: str) -> str:
    """Return the branded team tag for a team name, or the name itself."""
    entry = _best_team_match(name)
    return entry[0] if entry else name


def team_emoji(name: str) -> str:
    """Return just the color emoji for a team."""
    entry = _best_team_match(name)
    return entry[1] if entry else "⚪"


def stake_advice(
    edge_runs: float,
    ev_pct: float = 0.0,
    bet_type: str = "session",
    is_first_entry: bool = True,
) -> str:
    """Return a stake % recommendation based on bet type and edge.

    Capital model: each match = 100% capital per player.
      - 1st session entry: 15-20% (lower risk, test the waters)
      - Follow-up sessions: 10-15% (already have position)
      - 1st match winner entry: 30-40% (MW is the core trade, hedge later)
      - Follow-up MW: 15-20% (adding to position)
    """
    if bet_type == "match_winner":
        if is_first_entry:
            if abs(edge_runs) >= 10 or ev_pct >= 15:
                return f"{CASH} Stake: 40% of match capital"
            else:
                return f"{CASH} Stake: 30% of match capital"
        else:
            if abs(edge_runs) >= 10 or ev_pct >= 15:
                return f"{CASH} Stake: 20% of match capital"
            else:
                return f"{CASH} Stake: 15% of match capital"
    else:
        # Session bets
        if is_first_entry:
            if abs(edge_runs) >= 12 or ev_pct >= 18:
                return f"{CASH} Stake: 20% of match capital"
            else:
                return f"{CASH} Stake: 15% of match capital"
        else:
            if abs(edge_runs) >= 12 or ev_pct >= 18:
                return f"{CASH} Stake: 15% of match capital"
            else:
                return f"{CASH} Stake: 10% of match capital"


def session_status_note(
    market: str,
    current_score: int,
    target_line: float,
    overs: float,
) -> str:
    """If a session target is already passed, note that clients are up.

    Example: bet was 6-over YES 42, score is already 45 at 5.2 overs →
    "✅ Target passed! Players ~10% up"
    """
    # Determine target over for this market
    _target_overs = {
        "6-over": 6.0, "6 Over": 6.0, "10-over": 10.0, "10 Over": 10.0,
        "15-over": 15.0, "15 Over": 15.0, "Innings": 20.0, "20-over": 20.0,
    }
    target_over = _target_overs.get(market, 20.0)

    if overs < target_over and current_score >= target_line:
        return "✅ Target passed! Players ~10% up"
    return ""


def _fmt(value: object) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.0f}"
    except (TypeError, ValueError):
        return str(value)


def _phase_label(overs: float) -> str:
    if overs <= 6:
        return "Powerplay"
    elif overs <= 15:
        return "Middle Overs"
    else:
        return "Death Overs"


def _momentum_arrow(run_rate: float, projected: float, target: Optional[int] = None) -> str:
    """Give a quick momentum read."""
    if target and target > 0:
        required_rr = 0
        # chase context would go here
        return ""
    if run_rate >= 10:
        return "Batting on fire"
    elif run_rate >= 8:
        return "Good scoring rate"
    elif run_rate >= 6.5:
        return "Steady innings"
    else:
        return "Under pressure"


# ── BALL-BY-BALL COMMENTARY ────────────────────────────────────────────

def format_ball_commentary(
    ball: Dict[str, Any],
    batting_team: str,
    score: int,
    wickets: int,
    overs_display: str,
    run_rate: float,
    active_batsmen: Optional[List[dict]] = None,
    active_bowler: Optional[dict] = None,
    projected_total: int = 0,
    target: int | None = None,
    innings: int = 1,
    home: str = "",
    away: str = "",
    signals: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Ball-by-ball live commentary — one message per delivery."""
    runs = ball.get("runs", 0)
    is_wicket = ball.get("is_wicket", False)
    extras = ball.get("extras", 0)

    # Ball description
    if is_wicket:
        ball_text = "\u274c WICKET!"
    elif runs == 6:
        ball_text = "\U0001f525 SIX!"
    elif runs == 4:
        ball_text = "\U0001f4a5 FOUR!"
    elif runs == 0 and extras == 0:
        ball_text = "\u25aa\ufe0f Dot ball"
    elif extras > 0:
        ball_text = f"\u25ab\ufe0f {runs} runs (+ extras)"
    else:
        ball_text = f"\u25ab\ufe0f {runs} run{'s' if runs != 1 else ''}"

    # Score line
    lines = [
        f"{overs_display} {ball_text}",
        f"   {batting_team} {score}/{wickets} | RR: {run_rate:.1f} | Proj: {projected_total}",
    ]

    # Batsmen (show on wickets, boundaries, or every 6 balls)
    ball_num = ball.get("ball", 0)
    show_batsmen = is_wicket or runs >= 4 or ball_num == 1
    if show_batsmen and active_batsmen:
        bat_parts = []
        for b in (active_batsmen or [])[:2]:
            name = b.get("name", "").strip()
            if not name:
                continue
            r = _fmt(b.get("score", 0))
            bl = _fmt(b.get("balls", 0))
            bat_parts.append(f"{name} {r}({bl})")
        if bat_parts:
            lines.append(f"   {' & '.join(bat_parts)}")

    # Bowler on wicket
    if is_wicket and active_bowler and active_bowler.get("name"):
        w = _fmt(active_bowler.get("wickets", 0))
        lines.append(f"   Bowl: {active_bowler['name']} ({w} wkts)")

    # Chase context
    if target and innings >= 2:
        over_num = int(float(overs_display.split(".")[0]) if "." in str(overs_display) else overs_display.split(" ")[0] if " " in str(overs_display) else overs_display)
        remaining = target - score
        overs_left = max(0.1, 20 - over_num)
        req_rr = remaining / overs_left
        lines.append(f"   Need {remaining} off {20 - over_num} ov (RRR: {req_rr:.1f})")

    # Embedded signals
    if signals:
        lines.append("")
        for sig in signals:
            lines.append(_format_signal(sig))

    return "\n".join(lines)


def format_over_summary(
    over_num: int,
    innings: int,
    batting_team: str,
    score: int,
    wickets: int,
    run_rate: float,
    projected_total: int,
    over_runs: int,
    over_wickets: int = 0,
    home: str = "",
    away: str = "",
    mw_home_odds: float = 0.0,
    mw_away_odds: float = 0.0,
    positions_summary: str = "",
    player_adjustment: float | None = None,
) -> str:
    """End-of-over summary line — compact recap."""
    phase = _phase_label(over_num)
    wkt_text = f" ({over_wickets}W)" if over_wickets > 0 else ""

    proj_text = f"Proj: {projected_total}"
    if player_adjustment is not None:
        try:
            proj_text += f" ({float(player_adjustment):+.1f})"
        except (TypeError, ValueError):
            pass

    lines = [
        f"\u2500\u2500\u2500 End of Over {over_num} \u2500\u2500\u2500",
        f"   {batting_team} {score}/{wickets} | {over_runs} off the over{wkt_text}",
        f"   RR: {run_rate:.1f} | {proj_text} | {phase}",
    ]

    if mw_home_odds > 0 or mw_away_odds > 0:
        lines.append(f"   MW: {home} @ {mw_home_odds:.2f} | {away} @ {mw_away_odds:.2f}")

    if positions_summary:
        lines.append(f"   Book: {positions_summary}")

    return "\n".join(lines)


# ── LIVE OVER COMMENTARY (legacy — still used if ball_by_ball disabled) ──

def format_over_update(
    over_num: int,
    innings: int,
    batting_team: str,
    score: int,
    wickets: int,
    run_rate: float,
    projected_total: int,
    player_adjustment: float | None = None,
    active_batsmen: Optional[List[dict]] = None,
    active_bowler: Optional[dict] = None,
    mw_home_odds: float = 0.0,
    mw_away_odds: float = 0.0,
    home: str = "",
    away: str = "",
    positions_summary: str = "",
    signals: Optional[List[Dict[str, Any]]] = None,
    last_over_runs: int | None = None,
    target: int | None = None,
) -> str:
    """Live commentary update after each over — the main feed message."""
    phase = _phase_label(over_num)
    momentum = _momentum_arrow(run_rate, projected_total, target)

    # Header line
    lines = [
        f"\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500",
        f"\U0001f3cf  {batting_team} {score}/{wickets} ({over_num} ov)",
    ]

    # Last over context
    if last_over_runs is not None:
        lines.append(f"   Last over: {last_over_runs} runs")

    # Batsmen at crease
    bat_parts = []
    for b in (active_batsmen or [])[:2]:
        name = b.get("name", "").strip()
        if not name:
            continue
        runs = _fmt(b.get("score", 0))
        balls = _fmt(b.get("balls", 0))
        sr = b.get("sr", b.get("rate"))
        sr_text = f" SR:{_fmt(sr)}" if sr is not None else ""
        bat_parts.append(f"{name} {runs}({balls}){sr_text}")
    if bat_parts:
        lines.append(f"   {' & '.join(bat_parts)}")

    # Bowler
    if active_bowler and active_bowler.get("name"):
        overs = active_bowler.get("overs")
        ov_text = f"{float(overs):.1f}" if overs is not None else "?"
        econ = active_bowler.get("econ", active_bowler.get("rate"))
        econ_text = f" econ:{_fmt(econ)}" if econ is not None else ""
        lines.append(
            f"   Bowl: {active_bowler['name']} "
            f"{_fmt(active_bowler.get('runs', 0))}/{_fmt(active_bowler.get('wickets', 0))} "
            f"({ov_text} ov){econ_text}"
        )

    # Run rate + projection
    proj_text = f"Proj: {projected_total}"
    if player_adjustment is not None:
        try:
            proj_text += f" ({float(player_adjustment):+.1f})"
        except (TypeError, ValueError):
            pass

    lines.append(f"   RR: {run_rate:.1f} | {proj_text} | {phase}")

    # Chase context
    if target and innings >= 2:
        remaining = target - score
        overs_left = max(0.1, 20 - over_num)
        req_rr = remaining / overs_left
        lines.append(f"   Need {remaining} off {20 - over_num} ov (RRR: {req_rr:.1f})")

    # Momentum read
    if momentum:
        lines.append(f"   {momentum}")

    # MW odds
    if mw_home_odds > 0 or mw_away_odds > 0:
        lines.append(f"   MW: {home} @ {mw_home_odds:.2f} | {away} @ {mw_away_odds:.2f}")

    # Open positions status
    if positions_summary:
        lines.append(f"   Book: {positions_summary}")

    # Embedded signals (the key part!)
    if signals:
        lines.append("")
        for sig in signals:
            lines.append(_format_signal(sig))

    return "\n".join(lines)


def _format_signal(sig: Dict[str, Any]) -> str:
    """Format a single embedded signal line."""
    sig_type = sig.get("type", "")
    direction = sig.get("direction", "")

    if direction in ("YES", "OVER", "LAGAI"):
        icon = YES
    elif direction in ("NO", "UNDER", "KHAI"):
        icon = NO
    else:
        icon = WARN

    if sig_type == "session":
        market = sig.get("market", "")
        line = sig.get("line", 0)
        edge = sig.get("edge", 0)
        model = sig.get("model", 0)
        stake = sig.get("stake", 0)
        return (
            f"{icon} SIGNAL: {direction} {int(line)} ({market})\n"
            f"   Model: {int(model)} | Edge: {edge:+.1f} runs | Rs {stake}/run"
        )

    elif sig_type == "mw":
        team = sig.get("team", "")
        odds = sig.get("odds", 0)
        ev_pct = sig.get("ev_pct", 0)
        fair_prob = sig.get("fair_prob", 0)
        implied = decimal_to_probability(odds) if odds > 1.0 else 0
        stake = sig.get("stake", 0)
        return (
            f"{icon} SIGNAL: Lagai {team} @ {odds:.2f}\n"
            f"   Model: {fair_prob:.0%} vs Market: {implied:.0%} | EV: +{ev_pct:.0f}% | Rs {stake}"
        )

    elif sig_type == "book":
        action = sig.get("action", "")
        profit = sig.get("profit", 0)
        return (
            f"{BOOK} BOOK NOW: {action}\n"
            f"   Guaranteed: Rs {profit:,.0f}"
        )

    elif sig_type == "mw_swing":
        detail = sig.get("detail", "")
        return f"{WARN} MW SWING: {detail}"

    elif sig_type == "speed_edge":
        detail = sig.get("detail", "")
        action = sig.get("action", "")
        return (
            f"{icon} SPEED EDGE: {detail}\n"
            f"   {action}"
        )

    else:
        return f"{WARN} {sig.get('text', '')}"


# ── STANDALONE SIGNAL (when no over update is pending) ────────────────

def format_session_call(
    market: str,
    direction: str,
    line: float,
    stake_per_run: int,
    model_prediction: float,
    home: str,
    away: str,
    cloudbet_info: Optional[str] = None,
) -> str:
    """Standalone session signal (sent between overs if needed).
    Deprecated: prefer format_session_bundle for multi-session messages."""
    icon = YES if direction == "YES" else NO
    edge = model_prediction - line
    msg = (
        f"{icon} SESSION: {direction} {int(line)} ({market})\n"
        f"{home} vs {away}\n"
        f"\n"
        f"   Our model says: {int(model_prediction)} runs\n"
        f"   Market line: {int(line)}\n"
        f"   Edge: {edge:+.0f} runs\n"
        f"\n"
        f"   Action: {direction} {int(line)} @ Rs {stake_per_run}/run"
    )
    if cloudbet_info:
        msg += f"\n   Cloudbet: {cloudbet_info}"
    return msg


def _enforce_session_consistency(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop signals that contradict each other within the same bundle.

    Rule: all session signals in one bundle must agree on direction.
    The longest-horizon market (Innings > 15-over > 10-over > 6-over) is
    the anchor.  Any shorter session that disagrees is dropped.

    If 6-over says NO but 10-over and Innings say YES, the 6-over NO is
    removed — clients should not see contradictory calls.
    """
    if len(signals) <= 1:
        return signals

    _rank = {
        "6-over": 0, "Powerplay": 0, "6 Over": 0, "6 Over Runs": 0,
        "6 Over Runs (Powerplay)": 0,
        "10-over": 1, "10 Over": 1, "10 Over Runs": 1,
        "15-over": 2, "15 Over": 2, "15 Over Runs": 2,
        "20-over": 3, "Innings": 3, "Innings Total": 3,
    }

    # Find the longest-horizon signal as the anchor
    sorted_by_rank = sorted(signals, key=lambda s: _rank.get(s.get("market", ""), -1), reverse=True)
    anchor = sorted_by_rank[0]
    anchor_dir = anchor.get("direction", "")

    if not anchor_dir:
        return signals

    consistent = []
    for sig in signals:
        if sig.get("direction", "") == anchor_dir:
            consistent.append(sig)
        # else: silently dropped — contradicts the anchor

    return consistent


def format_session_bundle(
    signals: List[Dict[str, Any]],
    batting_team: str,
    bowling_team: str,
    score: int,
    wickets: int,
    overs: float,
    innings: int = 1,
    target: int = 0,
    first_innings_total: int = 0,
) -> str:
    """Premium session signal bundle with YES/NO calls and stake advice."""
    # Enforce consistency — drop contradictory signals before showing to clients
    signals = _enforce_session_consistency(signals)

    if not signals:
        return ""

    order = {"6-over": 0, "10-over": 1, "15-over": 2, "20-over": 3,
             "Powerplay": 0, "10 Over Runs": 1, "15 Over Runs": 2,
             "Innings Total": 3, "6 Over Runs": 0}

    sorted_sigs = sorted(signals, key=lambda s: order.get(s.get("market", ""), 9))

    bat_tag = team_tag(batting_team)
    bowl_tag = team_tag(bowling_team)
    bat_emoji = team_emoji(batting_team)
    inn_label = "1st Inn" if innings == 1 else "2nd Inn (Chase)"

    # Short market labels
    _short_market = {
        "6-over": "6 Over", "10-over": "10 Over", "15-over": "15 Over",
        "20-over": "Innings", "Powerplay": "6 Over", "10 Over Runs": "10 Over",
        "15 Over Runs": "15 Over", "Innings Total": "Innings",
        "6 Over Runs": "6 Over", "6 Over Runs (Powerplay)": "6 Over",
    }

    lines = [
        f"{bat_emoji} {bat_tag} — {inn_label}",
        f"{BAT} Score: {score}/{wickets} ({overs:.1f} ov)",
        f"vs {bowl_tag}",
    ]

    # Add chase context for 2nd innings
    if innings >= 2 and target > 0:
        runs_needed = max(0, target - score)
        overs_left = max(0.1, 20.0 - overs)
        rrr = runs_needed / overs_left
        lines.append(f"🎯 Target: {target} | Need {runs_needed} off {overs_left:.1f} ov (RRR {rrr:.1f})")
    elif innings >= 2 and first_innings_total > 0:
        lines.append(f"🎯 1st Inn: {first_innings_total} | Target: {first_innings_total + 1}")

    lines.append(f"{'━' * 30}")

    for sig in sorted_sigs:
        direction = sig.get("direction", "")
        market    = sig.get("market", "")
        line      = sig.get("line", 0)
        model     = sig.get("model", 0)
        edge      = model - line
        ev_pct    = sig.get("ev_pct", 0)

        short = _short_market.get(market, market)

        if direction == "YES":
            icon = YES
            call = "YES"
        else:
            icon = NO
            call = "NO"

        _match_key = f"{batting_team}:{innings}"
        _entry_num = get_entry_number(_match_key, "session")
        _is_first = _entry_num == 1
        advice = stake_advice(abs(edge), ev_pct, bet_type="session", is_first_entry=_is_first)

        # Check if target is already passed (session won early)
        status = session_status_note(short, score, line, overs)

        lines.append(f"{icon} {short}  {call} {int(line)}")
        lines.append(f"   {advice}")
        if status:
            lines.append(f"   {status}")

    lines.append(f"{'━' * 30}")
    return "\n".join(lines)


def format_mw_call(
    team: str,
    direction: str,
    odds: float,
    stake: int,
    fair_prob: float,
    home: str,
    away: str,
) -> str:
    """Premium match winner signal — clean, actionable, hedge-aware."""
    implied = decimal_to_probability(odds) if odds > 1.0 else 0
    edge_pp = (fair_prob - implied) * 100

    backed_tag = team_tag(team)
    backed_emoji = team_emoji(team)
    home_tag = team_tag(home)
    away_tag = team_tag(away)

    _match_key = f"{home}:{away}"
    _entry_num = get_entry_number(_match_key, "match_winner")
    _is_first = _entry_num == 1
    s_advice = stake_advice(abs(edge_pp), abs(edge_pp), bet_type="match_winner", is_first_entry=_is_first)

    # Entry number hint
    if _is_first:
        entry_hint = "1st entry — hedge when rate moves"
    else:
        entry_hint = f"Entry #{_entry_num} — manage position"

    return (
        f"{CUP} MATCH WINNER\n"
        f"{'━' * 30}\n"
        f"\n"
        f"{backed_emoji} Win {backed_tag}\n"
        f"@ {odds:.2f} rate\n"
        f"\n"
        f"{s_advice}\n"
        f"📌 {entry_hint}\n"
        f"\n"
        f"{'━' * 30}\n"
        f"{home_tag}  🆚  {away_tag}"
    )


def format_book_alert(
    market_type: str,
    action: str,
    guaranteed_profit: float,
    math_breakdown: str,
    market_name: str = "",
) -> str:
    """Book opportunity alert."""
    label = market_name if market_name else market_type
    return (
        f"{BOOK} BOOK OPPORTUNITY\n"
        f"{'━' * 28}\n"
        f"{label}: {action}\n"
        f"Guaranteed: Rs {guaranteed_profit:,.0f}\n"
        f"{'━' * 28}"
    )


def format_mw_swing(
    team_moved: str,
    old_odds: float,
    new_odds: float,
    home: str,
    away: str,
    home_odds: float,
    away_odds: float,
    model_prob: float,
) -> str:
    """MW odds movement alert."""
    moved_emoji = team_emoji(team_moved)
    home_tag = team_tag(home)
    away_tag = team_tag(away)
    arrow = "📈" if new_odds < old_odds else "📉"
    return (
        f"{arrow} ODDS MOVE — {moved_emoji} {team_tag(team_moved)}\n"
        f"   {old_odds:.2f} → {new_odds:.2f}\n"
        f"   {home_tag} @ {home_odds:.2f}  |  {away_tag} @ {away_odds:.2f}"
    )


# ── PRE-MATCH & TOSS ─────────────────────────────────────────────────

def format_pre_match_copilot(
    home: str,
    away: str,
    venue: str,
    cloudbet_home_odds: float,
    cloudbet_away_odds: float,
    est_home_odds: str,
    est_away_odds: str,
    consensus_home_prob: float,
    consensus_away_prob: float,
    pp_line_est: str,
    model_pp: float,
    venue_avg_pp: float,
    venue_modifier: float,
) -> str:
    mod_text = f" ({float(venue_modifier):+.0f})" if venue_modifier else ""
    return (
        f"\U0001f3cf PRE-MATCH: {home} vs {away}\n"
        f"   Venue: {venue}\n"
        f"\n"
        f"   MW: {home} {cloudbet_home_odds} | {away} {cloudbet_away_odds}\n"
        f"   Model: {home} {est_home_odds} | {away} {est_away_odds}\n"
        f"   Consensus: {home} {consensus_home_prob:.0%} | {away} {consensus_away_prob:.0%}\n"
        f"\n"
        f"   PP Line: {pp_line_est} | Model: {_fmt(model_pp)}\n"
        f"   Venue avg: {_fmt(venue_avg_pp)}{mod_text}"
    )


def format_toss_update(
    winner: str,
    decision: str,
    home: str,
    away: str,
    adjustment: str = "",
) -> str:
    msg = f"\U0001fa99 TOSS: {winner} elected to {decision} ({home} vs {away})"
    if adjustment:
        msg += f"\n   {adjustment}"
    return msg


# ── MATCH SUMMARY ─────────────────────────────────────────────────────

def format_session_summary(
    cloudbet_pnl: float,
    cloudbet_bets: int,
    shadow_pnl: float,
    shadow_bets: int,
    shadow_currency: str = "INR",
    positions: Optional[List[str]] = None,
) -> str:
    positions = positions or []
    msg = (
        f"\U0001f3c1 MATCH OVER\n"
        f"\n"
        f"   Cloudbet: ${cloudbet_pnl:+.2f} ({cloudbet_bets} bets)\n"
        f"   Shadow: {shadow_currency} {shadow_pnl:+,.0f} ({shadow_bets} bets)"
    )
    if positions:
        msg += "\n\n   " + "\n   ".join(positions)
    return msg
