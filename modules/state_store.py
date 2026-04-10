"""State persistence layer — SQLite-backed store for bot state.

Persists open/closed bets, shadow positions, and arbitrary key-value state
so the bot can recover gracefully after restarts.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from modules.bet_executor import LiveBet

logger = logging.getLogger("ipl_spotter.state_store")

_OPEN_BETS_SCHEMA = """
CREATE TABLE IF NOT EXISTS open_bets (
    reference_id TEXT PRIMARY KEY,
    event_id TEXT,
    home_team TEXT,
    away_team TEXT,
    innings INTEGER,
    market TEXT,
    market_url TEXT,
    direction TEXT,
    line REAL,
    price REAL,
    stake_usd REAL,
    ev_pct REAL,
    trigger TEXT,
    paper BOOLEAN,
    status TEXT,
    placed_at TEXT,
    pnl REAL,
    cashout_eligible BOOLEAN DEFAULT 0,
    cashout_available BOOLEAN DEFAULT 0,
    cashout_price REAL,
    min_bet REAL DEFAULT 0
)
"""

_CLOSED_BETS_SCHEMA = """
CREATE TABLE IF NOT EXISTS closed_bets (
    reference_id TEXT PRIMARY KEY,
    event_id TEXT,
    home_team TEXT,
    away_team TEXT,
    innings INTEGER,
    market TEXT,
    market_url TEXT,
    direction TEXT,
    line REAL,
    price REAL,
    stake_usd REAL,
    ev_pct REAL,
    trigger TEXT,
    paper BOOLEAN,
    status TEXT,
    placed_at TEXT,
    pnl REAL,
    settled_at TEXT,
    cashout_eligible BOOLEAN DEFAULT 0
)
"""

_SHADOW_POSITIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS shadow_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id TEXT,
    market TEXT,
    direction TEXT,
    entry_line REAL,
    stake REAL,
    innings INTEGER,
    team TEXT,
    odds REAL,
    position_type TEXT,
    status TEXT,
    created_at TEXT
)
"""

_BOT_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_state (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT
)
"""

_BET_TRACKING_SCHEMA = """
CREATE TABLE IF NOT EXISTS bet_tracking (
    reference_id TEXT PRIMARY KEY,
    match_id INTEGER,
    innings INTEGER,
    home_team TEXT,
    away_team TEXT,
    market TEXT,
    direction TEXT,
    target_line REAL,
    target_over REAL,
    stake_usd REAL,
    stake_pct REAL,
    odds REAL,
    ev_pct REAL,
    trigger TEXT,
    status TEXT,
    result TEXT,
    pnl REAL,
    bankroll_at_bet REAL,
    score_at_bet TEXT,
    score_at_settle TEXT,
    score_snapshots TEXT,
    placed_at TEXT,
    settled_at TEXT,
    streak_at_bet INTEGER DEFAULT 0,
    market_streak_at_bet INTEGER DEFAULT 0
)
"""


def _bet_to_row(bet: LiveBet) -> tuple:
    """Convert a LiveBet to a tuple matching the open_bets column order."""
    return (
        bet.reference_id,
        bet.event_id,
        bet.home_team,
        bet.away_team,
        bet.innings,
        bet.market,
        bet.market_url,
        bet.direction,
        bet.line,
        bet.price,
        bet.stake_usd,
        bet.ev_pct,
        bet.trigger,
        bet.paper,
        bet.status,
        bet.placed_at.isoformat() if isinstance(bet.placed_at, datetime) else str(bet.placed_at),
        bet.pnl,
    )


def _row_to_bet(row: sqlite3.Row | tuple, *, has_settled_at: bool = False) -> LiveBet:
    """Reconstruct a LiveBet from a database row."""
    placed_at_str = row[15] if not isinstance(row, dict) else row["placed_at"]
    placed_at = datetime.fromisoformat(placed_at_str)
    if placed_at.tzinfo is None:
        placed_at = placed_at.replace(tzinfo=timezone.utc)

    settled_at = None
    if has_settled_at:
        settled_at_str = row[17] if not isinstance(row, dict) else row["settled_at"]
        if settled_at_str:
            settled_at = datetime.fromisoformat(settled_at_str)
            if settled_at.tzinfo is None:
                settled_at = settled_at.replace(tzinfo=timezone.utc)

    # Cashout fields — positions differ between open_bets and closed_bets
    if has_settled_at:
        # closed_bets: cols 0-16 original, 17 settled_at, 18 cashout_eligible
        cashout_eligible = bool(row[18]) if len(row) > 18 else False
        cashout_available = False
        cashout_price = None
        min_bet = 0.0
    else:
        # open_bets: cols 0-16 original, 17-20 cashout fields
        cashout_eligible = bool(row[17]) if len(row) > 17 else False
        cashout_available = bool(row[18]) if len(row) > 18 else False
        cashout_price = float(row[19]) if len(row) > 19 and row[19] is not None else None
        min_bet = float(row[20]) if len(row) > 20 and row[20] is not None else 0.0

    return LiveBet(
        reference_id=row[0],
        event_id=row[1],
        home_team=row[2],
        away_team=row[3],
        innings=row[4],
        market=row[5],
        market_url=row[6],
        direction=row[7],
        line=row[8],
        price=row[9],
        stake_usd=row[10],
        ev_pct=row[11],
        trigger=row[12],
        paper=bool(row[13]),
        status=row[14],
        placed_at=placed_at,
        pnl=row[16],
        settled_at=settled_at,
        cashout_eligible=cashout_eligible,
        cashout_available=cashout_available,
        cashout_price=cashout_price,
        min_bet=min_bet,
    )


class StateStore:
    """SQLite-backed persistence for bot state."""

    def __init__(self, db_path: str = "data/bot_state.db") -> None:
        self.db_path = db_path
        # Ensure parent directory exists
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()   # serialise all writes across threads
        self._create_tables()

    def _create_tables(self) -> None:
        cur = self._conn.cursor()
        cur.execute(_OPEN_BETS_SCHEMA)
        cur.execute(_CLOSED_BETS_SCHEMA)
        cur.execute(_SHADOW_POSITIONS_SCHEMA)
        cur.execute(_BOT_STATE_SCHEMA)
        cur.execute(_BET_TRACKING_SCHEMA)
        self._conn.commit()
        self._migrate_tables()

    def _migrate_tables(self) -> None:
        """Add new columns to existing tables without dropping data."""
        migrations = [
            "ALTER TABLE open_bets ADD COLUMN cashout_eligible BOOLEAN DEFAULT 0",
            "ALTER TABLE open_bets ADD COLUMN cashout_available BOOLEAN DEFAULT 0",
            "ALTER TABLE open_bets ADD COLUMN cashout_price REAL",
            "ALTER TABLE open_bets ADD COLUMN min_bet REAL DEFAULT 0",
            "ALTER TABLE closed_bets ADD COLUMN cashout_eligible BOOLEAN DEFAULT 0",
        ]
        cur = self._conn.cursor()
        for sql in migrations:
            try:
                cur.execute(sql)
            except sqlite3.OperationalError:
                pass  # column already exists
        self._conn.commit()

    # ── Open Bets ─────────────────────────────────────────────────────

    def save_open_bet(self, bet: LiveBet) -> None:
        """Insert or replace an open bet."""
        row = _bet_to_row(bet)
        cashout_row = row + (
            getattr(bet, 'cashout_eligible', False),
            getattr(bet, 'cashout_available', False),
            getattr(bet, 'cashout_price', None),
            getattr(bet, 'min_bet', 0.0),
        )
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO open_bets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                cashout_row,
            )
            self._conn.commit()

    def remove_open_bet(self, reference_id: str) -> None:
        """Delete an open bet by reference_id."""
        with self._lock:
            self._conn.execute(
                "DELETE FROM open_bets WHERE reference_id = ?", (reference_id,)
            )
            self._conn.commit()

    def load_open_bets(self) -> Dict[str, LiveBet]:
        """Load all open bets into a dict keyed by reference_id."""
        cur = self._conn.execute("SELECT * FROM open_bets")
        rows = cur.fetchall()
        result: Dict[str, LiveBet] = {}
        for row in rows:
            bet = _row_to_bet(row, has_settled_at=False)
            result[bet.reference_id] = bet
        return result

    # ── Closed Bets ───────────────────────────────────────────────────

    def save_closed_bet(self, bet: LiveBet) -> None:
        """Insert or replace a closed (settled) bet."""
        row = _bet_to_row(bet)
        settled_at_str = (
            bet.settled_at.isoformat()
            if isinstance(bet.settled_at, datetime)
            else str(bet.settled_at) if bet.settled_at else None
        )
        cashout_eligible = getattr(bet, 'cashout_eligible', False)
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO closed_bets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                row + (settled_at_str, cashout_eligible),
            )
            self._conn.commit()

    def load_closed_bets(self, limit: int = 100) -> List[LiveBet]:
        """Load the most recent closed bets (newest first)."""
        cur = self._conn.execute(
            "SELECT * FROM closed_bets ORDER BY datetime(settled_at) DESC LIMIT ?",
            (limit,),
        )
        rows = cur.fetchall()
        return [_row_to_bet(row, has_settled_at=True) for row in rows]

    # ── Key-Value State ───────────────────────────────────────────────

    def save_state(self, key: str, value: Any) -> None:
        """Save an arbitrary JSON-serializable value."""
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO bot_state (key, value, updated_at) VALUES (?, ?, ?)",
                (key, json.dumps(value), now),
            )
            self._conn.commit()

    def load_state(self, key: str) -> Any:
        """Load a value by key. Returns None if not found."""
        cur = self._conn.execute(
            "SELECT value FROM bot_state WHERE key = ?", (key,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    # ── Daily PnL convenience ─────────────────────────────────────────

    def save_daily_pnl(self, pnl: float) -> None:
        """Persist the current daily P&L."""
        self.save_state("daily_pnl", pnl)

    def load_daily_pnl(self) -> float:
        """Load the persisted daily P&L (default 0.0)."""
        val = self.load_state("daily_pnl")
        if val is None:
            return 0.0
        return float(val)

    # ── Bet Tracking (smart staking history) ─────────────────────────

    def save_bet_tracking(self, record: dict) -> None:
        """Save a bet tracking record (full lifecycle with live snapshots)."""
        cols = (
            "reference_id", "match_id", "innings", "home_team", "away_team",
            "market", "direction", "target_line", "target_over",
            "stake_usd", "stake_pct", "odds", "ev_pct", "trigger",
            "status", "result", "pnl", "bankroll_at_bet",
            "score_at_bet", "score_at_settle", "score_snapshots",
            "placed_at", "settled_at", "streak_at_bet", "market_streak_at_bet",
        )
        vals = tuple(record.get(c) for c in cols)
        placeholders = ",".join(["?"] * len(cols))
        col_names = ",".join(cols)
        with self._lock:
            self._conn.execute(
                f"INSERT OR REPLACE INTO bet_tracking ({col_names}) VALUES ({placeholders})",
                vals,
            )
            self._conn.commit()

    def update_bet_tracking(self, reference_id: str, updates: dict) -> None:
        """Update specific fields on a bet tracking record."""
        if not updates:
            return
        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        vals = list(updates.values()) + [reference_id]
        with self._lock:
            self._conn.execute(
                f"UPDATE bet_tracking SET {set_clause} WHERE reference_id = ?",
                vals,
            )
            self._conn.commit()

    def load_bet_tracking_history(self, limit: int = 100) -> list[dict]:
        """Load recent bet tracking records for analysis."""
        cur = self._conn.execute(
            "SELECT * FROM bet_tracking ORDER BY placed_at DESC LIMIT ?",
            (limit,),
        )
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def get_bet_tracking_stats(self, days: int = 7) -> dict:
        """Aggregate stats from bet tracking history."""
        cur = self._conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result IN ('WIN','HALF_WIN','WON') THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result IN ('LOSS','HALF_LOSS','LOST') THEN 1 ELSE 0 END) as losses,
                COALESCE(SUM(pnl), 0) as total_pnl,
                COALESCE(AVG(stake_pct), 0) as avg_stake_pct,
                market,
                direction
            FROM bet_tracking
            WHERE placed_at >= datetime('now', ?)
            AND result IS NOT NULL AND result != 'PENDING'
            GROUP BY market, direction
        """, (f"-{days} days",))
        results = []
        for row in cur.fetchall():
            results.append({
                "total": row[0], "wins": row[1], "losses": row[2],
                "pnl": row[3], "avg_stake_pct": row[4],
                "market": row[5], "direction": row[6],
            })
        return {"by_market": results}

    # ── Cleanup ───────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
