"""
PSL Pre-Match Context.

Since there is no Indian book live line channel for PSL (unlike IPL),
this module compensates by providing a rich pre-match dossier:

  - Venue profiles: pitch type, avg scores, spin/pace/dew tendencies
  - Team key players: top bat/bowl per franchise (stable across season)
  - Betting biases: which venues/matchups lean OVER or UNDER
  - Head-to-head records from our StatsDB

All of this is sent to Telegram before first ball so you can trade
the Indian book manually with context even without a live line feed.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from modules.stats_db import StatsDB

logger = logging.getLogger("ipl_spotter.psl_context")


# ── PSL Venue Profiles ────────────────────────────────────────────────────────
# Based on PSL historical averages + known pitch characteristics

PSL_VENUES: Dict[str, Dict[str, Any]] = {
    "gaddafi": {
        "name": "Gaddafi Stadium, Lahore",
        "pitch": "Spin-friendly, slower surface",
        "avg_1st": 177,
        "avg_pp": 52,
        "avg_death": 43,
        "dew": "Significant after over 10 — chasing team benefits",
        "bat_first_win_pct": 42,   # chasers win more due to dew
        "pace_vs_spin": "Spin dominant in middle overs",
        "lean": "UNDER in 1st innings, OVER in 2nd (dew)",
        "note": "Dew makes chasing easy. Back UNDER on 1st innings session lines. Death bowling suffers in 2nd innings.",
    },
    "rawalpindi": {
        "name": "Rawalpindi Cricket Stadium",
        "pitch": "Flat belter, pace-friendly",
        "avg_1st": 194,
        "avg_pp": 58,
        "avg_death": 42,
        "dew": "Moderate",
        "bat_first_win_pct": 55,
        "pace_vs_spin": "Pace gets carry, spinners can be expensive",
        "lean": "OVER early — powerplay average 58+ here",
        "note": "Highest-scoring PSL venue. Lean OVER on PP and innings total. 190+ is par.",
    },
    "national stadium karachi": {
        "name": "National Stadium, Karachi",
        "pitch": "Hard and bouncy, good for pace",
        "avg_1st": 177,
        "avg_pp": 51,
        "avg_death": 45,
        "dew": "Less dew than Lahore",
        "bat_first_win_pct": 50,
        "pace_vs_spin": "Pace dominant early, evens out later",
        "lean": "Neutral — depends on toss",
        "note": "Balanced venue. Hard surface means edges carry to slip. Wickets early possible.",
    },
    "multan": {
        "name": "Multan Cricket Stadium",
        "pitch": "Slower, spin-friendly",
        "avg_1st": 172,
        "avg_pp": 48,
        "avg_death": 40,
        "dew": "Heavy dew in evening games",
        "bat_first_win_pct": 40,   # heavy dew means chasers dominate
        "pace_vs_spin": "Spin very effective here",
        "lean": "UNDER 1st innings — slow pitch + dew means batting 2nd huge advantage",
        "note": "Slowest PSL venue. 165-170 is a defendable 1st innings score. Strong dew advantage for chasing team.",
    },
    "abu dhabi": {
        "name": "Sheikh Zayed Stadium, Abu Dhabi",
        "pitch": "Slow, low bounce",
        "avg_1st": 162,
        "avg_pp": 44,
        "avg_death": 52,
        "dew": "Minimal (desert climate)",
        "bat_first_win_pct": 52,
        "pace_vs_spin": "Spinners dominate middle overs",
        "lean": "UNDER — this is a 155-165 venue, not a belter",
        "note": "Neutral venue used for PSL. Lower scores than Pakistan venues. Death batters can accelerate well on true surface.",
    },
    "dubai": {
        "name": "Dubai International Cricket Stadium",
        "pitch": "Good for batting once set",
        "avg_1st": 166,
        "avg_pp": 46,
        "avg_death": 52,
        "dew": "Minimal",
        "bat_first_win_pct": 50,
        "pace_vs_spin": "Balanced",
        "lean": "Slight UNDER — 165-175 is par",
        "note": "Neutral venue. Consistent surface. Death overs see acceleration. No strong lean either way.",
    },
}


def get_venue_profile(venue: str) -> Optional[Dict[str, Any]]:
    """Match a venue string to the PSL venue profile."""
    v = venue.lower()
    for key, profile in PSL_VENUES.items():
        if key in v:
            return profile
    return None


# ── PSL Team Profiles ─────────────────────────────────────────────────────────
# Current 2025 PSL season key players per franchise

PSL_TEAMS: Dict[str, Dict[str, Any]] = {
    "lahore qalandars": {
        "short": "LAH",
        "key_batsmen": ["Fakhar Zaman", "Abdullah Shafique", "Sikandar Raza", "David Wiese"],
        "key_bowlers": ["Shaheen Shah Afridi", "Haris Rauf", "Zaman Khan", "Rashid Khan"],
        "captain": "Shaheen Shah Afridi",
        "strengths": "World-class pace attack. Shaheen + Haris in PP is devastating.",
        "weakness": "Middle order can collapse if top 3 fail early.",
        "powerplay_style": "Aggressive — Fakhar targets the powerplay",
        "death_style": "Strong death bowling (Haris Rauf specialist)",
    },
    "multan sultans": {
        "short": "MUL",
        "key_batsmen": ["Mohammad Rizwan", "Shan Masood", "Rilee Rossouw", "Tim David"],
        "key_bowlers": ["Ihsanullah", "Usama Mir", "Abbas Afridi", "David Willey"],
        "captain": "Mohammad Rizwan",
        "strengths": "Rizwan is PSL's most consistent run scorer. Strong batting depth.",
        "weakness": "Pace bowling can be expensive at high-scoring venues.",
        "powerplay_style": "Conservative — Rizwan anchors, others attack",
        "death_style": "Tim David/Rossouw can explode in death",
    },
    "islamabad united": {
        "short": "ISL",
        "key_batsmen": ["Paul Stirling", "Alex Hales", "Shadab Khan", "Azam Khan"],
        "key_bowlers": ["Naseem Shah", "Mohammad Wasim Jr", "Shadab Khan", "Faheem Ashraf"],
        "captain": "Shadab Khan",
        "strengths": "Explosive opening pair (Hales/Stirling). Shadab is x-factor.",
        "weakness": "Dependent on openers — middle order inconsistent.",
        "powerplay_style": "Very aggressive — Hales & Stirling go hard from ball 1",
        "death_style": "Azam Khan can hit big in death",
    },
    "peshawar zalmi": {
        "short": "PES",
        "key_batsmen": ["Babar Azam", "Tom Kohler-Cadmore", "Rovman Powell", "Saim Ayub"],
        "key_bowlers": ["Mohammad Amir", "Wahab Riaz", "Luke Wood", "Salman Irshad"],
        "captain": "Babar Azam",
        "strengths": "Babar Azam — best PSL batsman. Strong top order.",
        "weakness": "Bowling attack lacks variety. Can be expensive on flat tracks.",
        "powerplay_style": "Solid — Babar builds a platform",
        "death_style": "Powell targets death overs",
    },
    "karachi kings": {
        "short": "KAR",
        "key_batsmen": ["James Vince", "Joe Clarke", "Shoaib Malik", "Imad Wasim"],
        "key_bowlers": ["Mohammad Amir", "Imad Wasim", "Mir Hamza", "Chris Jordan"],
        "captain": "Imad Wasim",
        "strengths": "Home advantage at National Stadium. Experience in pressure games.",
        "weakness": "Inconsistent — can underperform badly away from home.",
        "powerplay_style": "Balanced",
        "death_style": "Experienced death bowlers (Jordan, Amir)",
    },
    "quetta gladiators": {
        "short": "QUE",
        "key_batsmen": ["Jason Roy", "Will Smeed", "Sarfaraz Ahmed", "Mohammad Nabi"],
        "key_bowlers": ["Naseem Shah", "Mohammad Hasnain", "Abrar Ahmed", "Nabi"],
        "captain": "Sarfaraz Ahmed",
        "strengths": "Jason Roy provides PP explosion. Hasnain pace is raw and fast.",
        "weakness": "Middle order can be fragile if Roy fails.",
        "powerplay_style": "Roy goes hard — powerplay or bust",
        "death_style": "Nabi useful at death with economy",
    },
    "hyderabad kingsmen": {
        "short": "HYD",
        "key_batsmen": ["Usman Khan", "Tayyab Tahir", "Tom Banton", "Shoaib Malik"],
        "key_bowlers": ["Sohail Khan", "Waqas Maqsood", "Qais Ahmad", "Faisal Akram"],
        "captain": "Shoaib Malik",
        "strengths": "New franchise with young talent. Aggressive approach.",
        "weakness": "Less experienced unit — can be inconsistent under pressure.",
        "powerplay_style": "Attacking — backs big hitters to score early",
        "death_style": "Rely on hitting rather than bowling at death",
    },
}


def get_team_profile(team_name: str) -> Optional[Dict[str, Any]]:
    """Fuzzy match a team name to the PSL team profile.

    Uses word-set intersection so "Karachi Kings" never matches "Hyderabad Kingsmen"
    (the word "kings" is not in "kingsmen" as a standalone token).
    Priority: exact full match → word set overlap → short code.
    """
    t = team_name.lower().strip()
    t_words = set(t.split())

    # Pass 1 — exact full key match
    if t in PSL_TEAMS:
        profile = PSL_TEAMS[t]
        return {**profile, "team": t.title()}

    # Pass 2 — word-set overlap (each word must match exactly as a token)
    best_key: Optional[str] = None
    best_score = 0
    for key, profile in PSL_TEAMS.items():
        key_words = set(key.split())
        # Only count meaningful words (len > 3) to avoid matching "the", "of", etc.
        significant = {w for w in key_words if len(w) > 3}
        overlap = significant & t_words
        if overlap and len(overlap) > best_score:
            best_score = len(overlap)
            best_key = key

    if best_key:
        profile = PSL_TEAMS[best_key]
        return {**profile, "team": best_key.title()}

    # Pass 3 — short code (e.g. "HYD", "KAR")
    for key, profile in PSL_TEAMS.items():
        if profile["short"].lower() == t or profile["short"].lower() in t_words:
            return {**profile, "team": key.title()}

    return None


# ── H2H from StatsDB ─────────────────────────────────────────────────────────

def get_h2h_record(stats_db: StatsDB, team1: str, team2: str) -> Dict[str, Any]:
    """Get head-to-head record between two PSL teams from StatsDB."""
    try:
        import sqlite3
        conn = sqlite3.connect(stats_db.db_path)

        # Normalize names for fuzzy match
        t1 = team1.split()[-1].lower()   # e.g. "Lahore Qalandars" → "qalandars"
        t2 = team2.split()[-1].lower()

        rows = conn.execute("""
            SELECT
                SUM(CASE WHEN LOWER(winner) LIKE ? THEN 1 ELSE 0 END) as t1_wins,
                SUM(CASE WHEN LOWER(winner) LIKE ? THEN 1 ELSE 0 END) as t2_wins,
                COUNT(*) as total,
                ROUND(AVG(first_innings_total), 0) as avg_1st,
                ROUND(AVG(second_innings_total), 0) as avg_2nd
            FROM matches
            WHERE match_id > 1200000
              AND ((LOWER(team1) LIKE ? AND LOWER(team2) LIKE ?)
                OR (LOWER(team1) LIKE ? AND LOWER(team2) LIKE ?))
        """, (
            f"%{t1}%", f"%{t2}%",
            f"%{t1}%", f"%{t2}%",
            f"%{t2}%", f"%{t1}%",
        )).fetchone()
        conn.close()

        if rows and rows[2] and rows[2] > 0:
            return {
                "t1_wins": rows[0] or 0,
                "t2_wins": rows[1] or 0,
                "total": rows[2],
                "avg_1st_innings": rows[3] or 0,
                "avg_2nd_innings": rows[4] or 0,
            }
    except Exception:
        logger.debug("H2H lookup failed", exc_info=True)
    return {}


# ── Main context builder ──────────────────────────────────────────────────────

class PSLContext:
    """Builds comprehensive pre-match context for PSL when no live line is available."""

    def __init__(self, stats_db: StatsDB):
        self.stats_db = stats_db

    def build(self, match_id: int, home: str, away: str, venue: str) -> Dict[str, Any]:
        """Build the full PSL pre-match context."""
        venue_profile  = get_venue_profile(venue) or {}
        home_profile   = get_team_profile(home) or {}
        away_profile   = get_team_profile(away) or {}
        h2h            = get_h2h_record(self.stats_db, home, away)

        # Venue stats from DB (may have data from PSL neutral-venue games)
        db_venue = self.stats_db.get_venue_stats(venue) or {}
        avg_1st  = db_venue.get("avg_first_innings") or venue_profile.get("avg_1st", 175)

        return {
            "match_id":     match_id,
            "home":         home,
            "away":         away,
            "venue":        venue,
            "venue_profile": venue_profile,
            "home_profile":  home_profile,
            "away_profile":  away_profile,
            "h2h":           h2h,
            "model_avg":     avg_1st,
        }

    def format_telegram(self, ctx: Dict[str, Any]) -> str:
        """Format PSL pre-match context for Telegram."""
        home = ctx["home"]
        away = ctx["away"]
        venue = ctx["venue"]
        vp = ctx["venue_profile"]
        hp = ctx["home_profile"]
        ap = ctx["away_profile"]
        h2h = ctx["h2h"]

        lines = [
            f"🏏 <b>PSL PRE-MATCH CONTEXT</b>",
            f"<b>{home} vs {away}</b>",
            f"📍 {venue}",
            f"⚠️ No live line available — use this for manual trading\n",
        ]

        # Venue profile
        if vp:
            lines.append(f"🏟 <b>Venue: {vp.get('name', venue)}</b>")
            lines.append(f"   Pitch: {vp.get('pitch', '')}")
            lines.append(f"   Avg 1st inn: {vp.get('avg_1st', '?')} | PP: {vp.get('avg_pp', '?')} | Death: {vp.get('avg_death', '?')}")
            lines.append(f"   Dew: {vp.get('dew', 'Unknown')}")
            lines.append(f"   Bat-first wins: {vp.get('bat_first_win_pct', '?')}%")
            lines.append(f"   📊 Lean: {vp.get('lean', '')}")
            lines.append(f"   💡 {vp.get('note', '')}\n")
        else:
            avg = ctx.get("model_avg", 175)
            lines.append(f"🏟 <b>Venue: {venue}</b>")
            lines.append(f"   DB avg 1st innings: {avg:.0f}\n")

        # Home team
        if hp:
            lines.append(f"🔵 <b>{home}</b> ({hp.get('short', '')})")
            bats  = ", ".join(hp.get("key_batsmen", [])[:3])
            bowls = ", ".join(hp.get("key_bowlers", [])[:3])
            lines.append(f"   Bat: {bats}")
            lines.append(f"   Bowl: {bowls}")
            lines.append(f"   PP style: {hp.get('powerplay_style', '')}")
            lines.append(f"   ⚡ {hp.get('strengths', '')}\n")

        # Away team
        if ap:
            lines.append(f"🔴 <b>{away}</b> ({ap.get('short', '')})")
            bats  = ", ".join(ap.get("key_batsmen", [])[:3])
            bowls = ", ".join(ap.get("key_bowlers", [])[:3])
            lines.append(f"   Bat: {bats}")
            lines.append(f"   Bowl: {bowls}")
            lines.append(f"   PP style: {ap.get('powerplay_style', '')}")
            lines.append(f"   ⚡ {ap.get('strengths', '')}\n")

        # H2H
        if h2h.get("total", 0) >= 3:
            lines.append(f"📊 <b>H2H ({h2h['total']} matches)</b>")
            lines.append(f"   {home}: {h2h.get('t1_wins', 0)} wins | {away}: {h2h.get('t2_wins', 0)} wins")
            lines.append(f"   Avg scores: 1st {h2h.get('avg_1st_innings', 0):.0f} | 2nd {h2h.get('avg_2nd_innings', 0):.0f}\n")

        # Trading guide
        lines.append(f"💰 <b>Trading Guide (no live line)</b>")
        if vp:
            lines.append(f"   {vp.get('lean', 'No clear lean')}")
            if "dew" in vp.get("dew", "").lower() and "significant" in vp.get("dew", "").lower():
                lines.append(f"   🌊 DEW ALERT: back UNDER on 1st innings — bowling gets harder later")
        lines.append(f"   Watch: Sportmonks model predictions + Cloudbet line vs XGBoost")

        return "\n".join(lines)
