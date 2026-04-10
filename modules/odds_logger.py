"""Logs every Cloudbet odds snapshot to SQLite for model fine-tuning.

Captures: timestamp, match, market, line, over/under odds, model prediction.
This gives us real market data to backtest against instead of synthetic lines.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger("ipl_spotter.odds_logger")


class OddsLogger:
    """Persists every odds snapshot + model prediction for later analysis."""

    def __init__(self, db_path: str = "data/odds_history.db"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        with self._lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self._create_tables()
        logger.info("OddsLogger ready: %s", db_path)

    def _create_tables(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS odds_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                match_id INTEGER,
                home TEXT,
                away TEXT,
                venue TEXT,
                competition TEXT,
                innings INTEGER,
                overs REAL,
                score INTEGER,
                wickets INTEGER,
                market TEXT,
                line REAL,
                over_odds REAL,
                under_odds REAL,
                model_expected REAL,
                model_std_dev REAL,
                edge_runs REAL,
                player_adj REAL,
                situational_expected REAL,
                raw_data TEXT
            );

            CREATE TABLE IF NOT EXISTS mw_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                match_id INTEGER,
                home TEXT,
                away TEXT,
                competition TEXT,
                innings INTEGER,
                overs REAL,
                score INTEGER,
                wickets INTEGER,
                home_odds REAL,
                away_odds REAL,
                model_home_prob REAL,
                raw_data TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_odds_match ON odds_snapshots(match_id, market);
            CREATE INDEX IF NOT EXISTS idx_mw_match ON mw_snapshots(match_id);
        """)
        self.conn.commit()

    def log_odds(
        self,
        match_id: int,
        home: str,
        away: str,
        venue: str,
        competition: str,
        innings: int,
        overs: float,
        score: int,
        wickets: int,
        cloudbet_odds: Dict[str, Any],
        predictions: Dict[str, Any],
    ) -> None:
        """Log all market odds + model predictions for this scan."""
        now = datetime.now(timezone.utc).isoformat()

        with self._lock:
            for market_key in ["6_over", "10_over", "15_over", "20_over", "innings_total", "powerplay_runs", "over_runs"]:
                mkt = cloudbet_odds.get(market_key)
                if not mkt or "line" not in mkt:
                    continue

                pred = predictions.get(market_key, {})
                if not pred:
                    pred_map = {
                        "6_over": "powerplay_total",
                        "10_over": "ten_over_total",
                        "15_over": "fifteen_over_total",
                        "20_over": "innings_total",
                        "powerplay_runs": "powerplay_total",
                        "over_runs": "next_over",
                    }
                    pred = predictions.get(pred_map.get(market_key, ""), {})

                try:
                    self.conn.execute(
                        """INSERT INTO odds_snapshots
                           (timestamp, match_id, home, away, venue, competition,
                            innings, overs, score, wickets,
                            market, line, over_odds, under_odds,
                            model_expected, model_std_dev, edge_runs,
                            player_adj, situational_expected, raw_data)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            now, match_id, home, away, venue, competition,
                            innings, overs, score, wickets,
                            market_key,
                            mkt.get("line"),
                            mkt.get("over_odds", mkt.get("yes_odds")),
                            mkt.get("under_odds", mkt.get("no_odds")),
                            pred.get("expected"),
                            pred.get("std_dev"),
                            (pred.get("expected", 0) or 0) - (mkt.get("line", 0) or 0) if pred.get("expected") else None,
                            pred.get("player_adj"),
                            pred.get("situational_expected"),
                            json.dumps(mkt)[:500],
                        ),
                    )
                except Exception:
                    logger.debug("Failed to log odds for %s", market_key, exc_info=True)

            mw = cloudbet_odds.get("match_winner")
            if mw:
                mw_pred = predictions.get("match_winner", {})
                try:
                    self.conn.execute(
                        """INSERT INTO mw_snapshots
                           (timestamp, match_id, home, away, competition,
                            innings, overs, score, wickets,
                            home_odds, away_odds, model_home_prob, raw_data)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            now, match_id, home, away, competition,
                            innings, overs, score, wickets,
                            mw.get("home_odds"),
                            mw.get("away_odds"),
                            mw_pred.get("home_prob"),
                            json.dumps(mw)[:500],
                        ),
                    )
                except Exception:
                    logger.debug("Failed to log MW odds", exc_info=True)

            self.conn.commit()

    def get_stats(self) -> Dict[str, int]:
        """Quick stats for logging."""
        with self._lock:
            odds_count = self.conn.execute("SELECT COUNT(*) FROM odds_snapshots").fetchone()[0]
            mw_count = self.conn.execute("SELECT COUNT(*) FROM mw_snapshots").fetchone()[0]
        return {"odds_snapshots": odds_count, "mw_snapshots": mw_count}

    def close(self) -> None:
        with self._lock:
            self.conn.close()
