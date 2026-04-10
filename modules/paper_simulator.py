"""Paper Simulator — $100K virtual bankroll simulation engine.

Captures every signal the bot generates (sessions + match winner), places
virtual bets using Kelly sizing, and self-settles from actual match outcomes.
Runs entirely in its own SQLite DB — no interference with real betting.

All settlement is from live score data, not Cloudbet API.
"""
from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from modules.shared_core import StakingRecommendation, recommend_stake_from_edge

logger = logging.getLogger("ipl_spotter.paper_sim")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_bets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reference_id TEXT UNIQUE NOT NULL,
    match_id INTEGER NOT NULL,
    home TEXT,
    away TEXT,
    venue TEXT,
    competition TEXT,
    innings INTEGER,
    market TEXT NOT NULL,
    direction TEXT NOT NULL,
    line REAL,
    odds REAL NOT NULL,
    stake REAL NOT NULL,
    ev_pct REAL,
    edge_runs REAL,
    model_expected REAL,
    confidence TEXT,
    trigger TEXT,
    overs_at_entry REAL,
    score_at_entry INTEGER,
    wickets_at_entry INTEGER,
    placed_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN',
    result TEXT,
    actual_value REAL,
    pnl REAL DEFAULT 0.0,
    settled_at TEXT,
    settle_source TEXT
);

CREATE TABLE IF NOT EXISTS paper_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    event TEXT NOT NULL,
    bankroll REAL NOT NULL,
    details TEXT
);

CREATE TABLE IF NOT EXISTS paper_daily (
    date TEXT PRIMARY KEY,
    bets_placed INTEGER DEFAULT 0,
    bets_won INTEGER DEFAULT 0,
    bets_lost INTEGER DEFAULT 0,
    pnl REAL DEFAULT 0.0,
    bankroll_eod REAL DEFAULT 0.0
);
"""

# Kelly sizing parameters for paper sim
FRACTIONAL_KELLY = 0.25
MAX_POSITION_PCT = 0.03       # max 3% of bankroll per bet
MAX_POSITION_USD = 5000.0     # hard cap per bet
MIN_STAKE_USD = 50.0          # minimum bet size
MIN_EV_PCT = 3.0              # minimum EV to place

MARKET_MULTIPLIERS = {
    "10_over": 1.5,
    "15_over": 1.5,
    "20_over": 1.3,
    "innings_total": 1.3,
    "6_over": 1.0,
    "powerplay_runs": 1.0,
    "over_runs": 0.5,
    "match_winner": 1.0,
}


class PaperSimulator:
    """Virtual bankroll paper trading simulator."""

    def __init__(
        self,
        bankroll: float = 100_000.0,
        db_path: str = "data/paper_sim.db",
        max_open_per_match: int = 8,
    ) -> None:
        self.initial_bankroll = bankroll
        self.db_path = db_path
        self.max_open_per_match = max_open_per_match

        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

        # Load current bankroll from ledger or use initial
        self.bankroll = self._load_bankroll()
        self._open_bets: Dict[str, dict] = {}
        self._restore_open()

        logger.info(
            "PaperSimulator ready — bankroll=$%.2f, %d open bets, db=%s",
            self.bankroll, len(self._open_bets), db_path,
        )

    def _load_bankroll(self) -> float:
        row = self._conn.execute(
            "SELECT bankroll FROM paper_ledger ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row:
            return float(row["bankroll"])
        # First run — log initial bankroll
        self._log_event("INIT", self.initial_bankroll, {"initial": self.initial_bankroll})
        return self.initial_bankroll

    def _restore_open(self) -> None:
        rows = self._conn.execute(
            "SELECT * FROM paper_bets WHERE status = 'OPEN'"
        ).fetchall()
        for r in rows:
            self._open_bets[r["reference_id"]] = dict(r)

    def _log_event(self, event: str, bankroll: float, details: Any = None) -> None:
        self._conn.execute(
            "INSERT INTO paper_ledger (ts, event, bankroll, details) VALUES (?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), event, bankroll,
             json.dumps(details, default=str) if details else None),
        )
        self._conn.commit()

    # ── Stake sizing ─────────────────────────────────────────────────

    def build_staking_recommendation(self, ev_pct: float, odds: float, market: str = "") -> StakingRecommendation:
        market_multiplier = MARKET_MULTIPLIERS.get(market, 1.0)
        if ev_pct < MIN_EV_PCT or odds <= 1.0 or self.bankroll <= 0.0:
            return StakingRecommendation(
                stake=0.0,
                kelly_fraction=0.0,
                recommended_fraction=0.0,
                bankroll=max(self.bankroll, 0.0),
                edge_percent=ev_pct,
                decimal_odds=odds,
                market_multiplier=market_multiplier,
                capped=False,
                min_stake_met=False,
            )

        return recommend_stake_from_edge(
            edge_percent=ev_pct,
            decimal_odds=odds,
            bankroll=self.bankroll,
            fraction=FRACTIONAL_KELLY,
            market_multiplier=market_multiplier,
            max_bankroll_fraction=MAX_POSITION_PCT,
            max_stake=MAX_POSITION_USD,
            min_stake=MIN_STAKE_USD,
        )

    def calculate_stake(self, ev_pct: float, odds: float, market: str = "") -> float:
        recommendation = self.build_staking_recommendation(ev_pct, odds, market)
        return recommendation.stake

    # ── Bet placement ────────────────────────────────────────────────

    def place_bet(
        self,
        match_id: int,
        home: str,
        away: str,
        venue: str,
        competition: str,
        innings: int,
        market: str,
        direction: str,
        line: float,
        odds: float,
        ev_pct: float,
        edge_runs: float = 0.0,
        model_expected: float = 0.0,
        confidence: str = "",
        trigger: str = "PAPER_SIM",
        overs: float = 0.0,
        score: int = 0,
        wickets: int = 0,
    ) -> Optional[str]:
        """Place a paper bet. Returns reference_id or None if rejected."""

        # Check max open per match
        match_open = sum(
            1 for b in self._open_bets.values()
            if b.get("match_id") == match_id
        )
        if match_open >= self.max_open_per_match:
            return None

        # Dedup: don't bet same market/direction/innings twice
        for b in self._open_bets.values():
            if (b.get("match_id") == match_id
                    and b.get("market") == market
                    and b.get("direction") == direction
                    and b.get("innings") == innings):
                return None

        stake = self.calculate_stake(ev_pct, odds, market)
        if stake <= 0:
            return None

        ref_id = str(uuid.uuid4())[:12]
        now = datetime.now(timezone.utc).isoformat()

        bet = {
            "reference_id": ref_id,
            "match_id": match_id,
            "home": home,
            "away": away,
            "venue": venue,
            "competition": competition,
            "innings": innings,
            "market": market,
            "direction": direction,
            "line": line,
            "odds": odds,
            "stake": stake,
            "ev_pct": round(ev_pct, 1),
            "edge_runs": round(edge_runs, 1),
            "model_expected": round(model_expected, 1),
            "confidence": confidence,
            "trigger": trigger,
            "overs_at_entry": round(overs, 1),
            "score_at_entry": score,
            "wickets_at_entry": wickets,
            "placed_at": now,
            "status": "OPEN",
        }

        cols = [k for k in bet.keys()]
        placeholders = ", ".join(["?"] * len(cols))
        self._conn.execute(
            f"INSERT INTO paper_bets ({', '.join(cols)}) VALUES ({placeholders})",
            tuple(bet[c] for c in cols),
        )
        self._conn.commit()
        self._open_bets[ref_id] = bet

        logger.info(
            "PAPER BET: %s %s %s line=%.1f odds=%.2f stake=$%.0f ev=%.1f%% [%s vs %s]",
            market, direction, line, odds, stake, ev_pct, home, away,
        )
        self._log_event("BET_PLACED", self.bankroll, {
            "ref": ref_id, "market": market, "direction": direction,
            "line": line, "odds": odds, "stake": stake,
        })
        return ref_id

    # ── Settlement ───────────────────────────────────────────────────

    def settle_match(self, match_id: int, actuals: Dict[str, Any]) -> int:
        """Settle all open paper bets for a completed match.

        actuals format:
            {
                "6_over": 48,        # actual runs at 6 overs
                "10_over": 82,       # actual runs at 10 overs
                "15_over": 128,      # actual runs at 15 overs
                "innings_total": 183, # final innings total
                "20_over": 183,
                "match_winner": "Chennai Super Kings",
                "inn1_total": 185,   # 1st innings total (for 2nd inn bets)
                "inn2_total": 172,   # 2nd innings total
            }
        """
        bets_to_settle = [
            b for b in self._open_bets.values()
            if b.get("match_id") == match_id
        ]

        if not bets_to_settle:
            return 0

        settled = 0
        now = datetime.now(timezone.utc).isoformat()

        for bet in bets_to_settle:
            market = bet["market"]
            direction = bet["direction"]
            line = float(bet.get("line") or 0)
            odds = float(bet["odds"])
            stake = float(bet["stake"])
            innings = bet.get("innings", 1)

            # Determine actual value for this bet
            actual = None
            if market == "match_winner":
                actual = actuals.get("match_winner")
            else:
                # Session markets: use innings-specific actual if available
                inn_key = f"inn{innings}_{market}" if f"inn{innings}_{market}" in actuals else market
                actual = actuals.get(inn_key, actuals.get(market))

            if actual is None:
                continue

            # Determine result
            if market == "match_winner":
                # direction is team name for MW bets
                won = (str(actual).lower() == str(direction).lower())
            else:
                actual_val = float(actual)
                if direction in ("OVER", "YES"):
                    won = actual_val > line
                elif direction in ("UNDER", "NO"):
                    won = actual_val < line
                else:
                    continue

            # Compute PnL
            if won:
                pnl = stake * (odds - 1)
                result = "WIN"
            else:
                pnl = -stake
                result = "LOSS"

            pnl = round(pnl, 2)
            self.bankroll += pnl

            # Update DB
            self._conn.execute(
                """UPDATE paper_bets
                   SET status=?, result=?, actual_value=?, pnl=?, settled_at=?, settle_source=?
                   WHERE reference_id=?""",
                (result, result, actual if not isinstance(actual, str) else None,
                 pnl, now, "match_complete", bet["reference_id"]),
            )

            # Remove from open
            self._open_bets.pop(bet["reference_id"], None)
            settled += 1

            logger.info(
                "PAPER SETTLED: %s %s %s line=%.1f actual=%s → %s pnl=$%.0f bankroll=$%.0f",
                market, direction, bet.get("home", ""), line, actual, result, pnl, self.bankroll,
            )

        self._conn.commit()
        self._log_event("MATCH_SETTLED", self.bankroll, {
            "match_id": match_id, "settled": settled,
        })

        # Update daily stats
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._conn.execute(
            """INSERT INTO paper_daily (date, bets_placed, bets_won, bets_lost, pnl, bankroll_eod)
               VALUES (?, 0, 0, 0, 0, ?)
               ON CONFLICT(date) DO UPDATE SET bankroll_eod=?""",
            (today, self.bankroll, self.bankroll),
        )
        self._conn.commit()

        return settled

    # ── Reporting ────────────────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        """Return current simulation status."""
        total = self._conn.execute("SELECT COUNT(*) FROM paper_bets").fetchone()[0]
        wins = self._conn.execute("SELECT COUNT(*) FROM paper_bets WHERE result='WIN'").fetchone()[0]
        losses = self._conn.execute("SELECT COUNT(*) FROM paper_bets WHERE result='LOSS'").fetchone()[0]
        open_count = len(self._open_bets)
        total_pnl = self._conn.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM paper_bets WHERE result IS NOT NULL"
        ).fetchone()[0]
        total_staked = self._conn.execute(
            "SELECT COALESCE(SUM(stake), 0) FROM paper_bets WHERE result IS NOT NULL"
        ).fetchone()[0]

        return {
            "bankroll": round(self.bankroll, 2),
            "initial_bankroll": self.initial_bankroll,
            "total_bets": total,
            "open": open_count,
            "wins": wins,
            "losses": losses,
            "win_pct": round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0,
            "total_pnl": round(total_pnl, 2),
            "total_staked": round(total_staked, 2),
            "roi_pct": round(total_pnl / total_staked * 100, 1) if total_staked > 0 else 0,
        }

    def format_dashboard(self) -> str:
        """Telegram-ready dashboard."""
        s = self.get_status()
        lines = [
            "📊 PAPER SIM DASHBOARD",
            f"Bankroll: ${s['bankroll']:,.0f} (started ${s['initial_bankroll']:,.0f})",
            f"Bets: {s['total_bets']} total ({s['open']} open)",
            f"Record: {s['wins']}W-{s['losses']}L ({s['win_pct']:.1f}%)",
            f"PnL: ${s['total_pnl']:+,.0f}  ROI: {s['roi_pct']:+.1f}%",
        ]

        # Per-market breakdown
        rows = self._conn.execute(
            """SELECT market, COUNT(*) as n,
                      SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                      COALESCE(SUM(pnl), 0) as pnl
               FROM paper_bets WHERE result IS NOT NULL
               GROUP BY market ORDER BY pnl DESC"""
        ).fetchall()
        if rows:
            lines.append("")
            for r in rows:
                w = r["wins"]
                n = r["n"]
                lines.append(f"  {r['market']:<16} {n}bets {w}W ({w/n*100:.0f}%) ${r['pnl']:+,.0f}")

        return "\n".join(lines)
