"""
Match Recorder — comprehensive per-scan data capture for simulation replay.

Stores every scan cycle's full state to SQLite so matches can be replayed
with the exact data the model saw at each point in time.

Tables:
  ball_log          — ball-by-ball scorecard (runs, extras, wickets)
  scan_snapshots    — full prediction + odds + Ferrari state per scan
  signal_log        — every signal generated (sent AND suppressed)
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from modules.shared_core import SignalPayload, StakingRecommendation

logger = logging.getLogger("ipl_spotter.match_recorder")


class MatchRecorder:
    """Captures everything needed to replay a match offline."""

    def __init__(self, db_path: str = "data/match_replay.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()
        # Track last ball to avoid duplicate ball_log inserts
        self._last_ball: Dict[int, str] = {}  # match_id -> "innings:overs:score:wickets"
        logger.info("MatchRecorder ready: %s", db_path)

    def _create_tables(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS ball_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                match_id INTEGER NOT NULL,
                competition TEXT,
                home TEXT,
                away TEXT,
                venue TEXT,
                innings INTEGER,
                over_num REAL,
                score INTEGER,
                wickets INTEGER,
                batting_team TEXT,
                bowling_team TEXT,
                run_rate REAL,
                required_rate REAL,
                target INTEGER,
                last_over_runs INTEGER,
                striker TEXT,
                striker_runs INTEGER,
                striker_balls INTEGER,
                striker_sr REAL,
                bowler TEXT,
                bowler_overs REAL,
                bowler_runs INTEGER,
                bowler_wickets INTEGER,
                bowler_econ REAL
            );

            CREATE TABLE IF NOT EXISTS scan_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                match_id INTEGER NOT NULL,
                competition TEXT,
                innings INTEGER,
                overs REAL,
                score INTEGER,
                wickets INTEGER,
                batting_team TEXT,
                predictions_json TEXT,
                cloudbet_odds_json TEXT,
                ferrari_state_json TEXT,
                ml_override_json TEXT
            );

            CREATE TABLE IF NOT EXISTS signal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                match_id INTEGER NOT NULL,
                competition TEXT,
                innings INTEGER,
                overs REAL,
                score INTEGER,
                wickets INTEGER,
                signal_type TEXT,
                market TEXT,
                direction TEXT,
                line REAL,
                odds REAL,
                model_expected REAL,
                model_std_dev REAL,
                ev_pct REAL,
                edge_runs REAL,
                confidence TEXT,
                action TEXT,
                suppression_reason TEXT,
                bet_ref TEXT,
                stake REAL
            );

            CREATE INDEX IF NOT EXISTS idx_ball_match ON ball_log(match_id, innings, over_num);
            CREATE INDEX IF NOT EXISTS idx_scan_match ON scan_snapshots(match_id, innings, overs);
            CREATE INDEX IF NOT EXISTS idx_signal_match ON signal_log(match_id, market);
        """)
        self.conn.commit()

    # ── Ball-by-ball ──────────────────────────────────────────────────────────

    def record_ball(
        self,
        match_id: int,
        state: Any,
        home: str,
        away: str,
        competition: str = "ipl",
    ) -> None:
        """Record a ball-by-ball snapshot from MatchState."""
        ball_key = f"{state.current_innings}:{state.overs_completed}:{state.total_runs}:{state.wickets}"
        if self._last_ball.get(match_id) == ball_key:
            return  # duplicate
        self._last_ball[match_id] = ball_key

        now = datetime.now(timezone.utc).isoformat()

        # Extract player info from innings_state if available
        striker = getattr(state, "striker", "") or ""
        striker_runs = getattr(state, "striker_runs", 0) or 0
        striker_balls = getattr(state, "striker_balls", 0) or 0
        striker_sr = round(striker_runs / striker_balls * 100, 1) if striker_balls else 0
        bowler = getattr(state, "bowler", "") or ""
        bowler_overs = getattr(state, "bowler_overs", 0) or 0
        bowler_runs = getattr(state, "bowler_runs_conceded", 0) or 0
        bowler_wickets = getattr(state, "bowler_wickets", 0) or 0
        bowler_econ = round(bowler_runs / bowler_overs, 1) if bowler_overs else 0

        try:
            self.conn.execute(
                """INSERT INTO ball_log
                   (timestamp, match_id, competition, home, away, venue,
                    innings, over_num, score, wickets,
                    batting_team, bowling_team, run_rate, required_rate, target,
                    last_over_runs, striker, striker_runs, striker_balls, striker_sr,
                    bowler, bowler_overs, bowler_runs, bowler_wickets, bowler_econ)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    now, match_id, competition, home, away,
                    getattr(state, "venue", ""),
                    state.current_innings, state.overs_completed,
                    state.total_runs, state.wickets,
                    getattr(state, "batting_team", ""),
                    getattr(state, "bowling_team", ""),
                    round(state.total_runs / max(state.overs_completed, 0.1), 2),
                    getattr(state, "required_run_rate", 0) or 0,
                    getattr(state, "target_runs", 0) or 0,
                    getattr(state, "last_over_runs", 0) or 0,
                    striker, striker_runs, striker_balls, striker_sr,
                    bowler, bowler_overs, bowler_runs, bowler_wickets, bowler_econ,
                ),
            )
            self.conn.commit()
        except Exception:
            logger.debug("ball_log insert failed", exc_info=True)

    # ── Scan snapshot ─────────────────────────────────────────────────────────

    def record_scan(
        self,
        match_id: int,
        state: Any,
        predictions: Dict[str, Any],
        cloudbet_odds: Optional[Dict[str, Any]],
        ferrari_state: Optional[Dict[str, Any]] = None,
        ml_override: Optional[Dict[str, Any]] = None,
        competition: str = "ipl",
    ) -> None:
        """Record the full state of one scan cycle."""
        now = datetime.now(timezone.utc).isoformat()

        def _safe_json(obj: Any) -> Optional[str]:
            if obj is None:
                return None
            try:
                return json.dumps(obj, default=str)
            except Exception:
                return None

        try:
            self.conn.execute(
                """INSERT INTO scan_snapshots
                   (timestamp, match_id, competition, innings, overs, score, wickets,
                    batting_team, predictions_json, cloudbet_odds_json,
                    ferrari_state_json, ml_override_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    now, match_id, competition,
                    state.current_innings, state.overs_completed,
                    state.total_runs, state.wickets,
                    getattr(state, "batting_team", ""),
                    _safe_json(predictions),
                    _safe_json(cloudbet_odds),
                    _safe_json(ferrari_state),
                    _safe_json(ml_override),
                ),
            )
            self.conn.commit()
        except Exception:
            logger.debug("scan_snapshot insert failed", exc_info=True)

    # ── Signal log ────────────────────────────────────────────────────────────

    def record_signal(
        self,
        match_id: int,
        state: Any,
        signal_type: str,
        market: str,
        direction: str,
        line: float = 0,
        odds: float = 0,
        model_expected: float = 0,
        model_std_dev: float = 0,
        ev_pct: float = 0,
        edge_runs: float = 0,
        confidence: str = "",
        action: str = "SENT",
        suppression_reason: str = "",
        bet_ref: str = "",
        stake: float = 0,
        competition: str = "ipl",
    ) -> None:
        """
        Record any signal decision.

        action: SENT, AUTOBET, SUPPRESSED, VETOED, SKIPPED
        """
        now = datetime.now(timezone.utc).isoformat()
        try:
            payload = SignalPayload(
                dedupe_key=f"{match_id}|{state.current_innings}|{market}|{direction}|{line}",
                market_type=market,
                selection=direction,
                line=line,
                edge_pct=ev_pct / 100.0 if ev_pct else 0.0,
                stake_recommendation=StakingRecommendation(
                    stake=stake,
                    kelly_fraction=0.0,
                    recommended_fraction=0.0,
                    bankroll=0.0,
                    edge_percent=ev_pct,
                    decimal_odds=odds if odds > 0 else 0.0,
                    market_multiplier=1.0,
                    capped=False,
                    min_stake_met=stake > 0,
                ),
                metadata={
                    "match_id": match_id,
                    "competition": competition,
                    "innings": state.current_innings,
                    "overs": state.overs_completed,
                    "score": state.total_runs,
                    "wickets": state.wickets,
                    "signal_type": signal_type,
                    "model_expected": model_expected,
                    "model_std_dev": model_std_dev,
                    "edge_runs": edge_runs,
                    "confidence": confidence,
                    "action": action,
                    "suppression_reason": suppression_reason,
                    "bet_ref": bet_ref,
                },
            ).to_dict()
            self.conn.execute(
                """INSERT INTO signal_log
                   (timestamp, match_id, competition, innings, overs, score, wickets,
                    signal_type, market, direction, line, odds,
                    model_expected, model_std_dev, ev_pct, edge_runs, confidence,
                    action, suppression_reason, bet_ref, stake)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    now, match_id, competition,
                    state.current_innings, state.overs_completed,
                    state.total_runs, state.wickets,
                    payload["signal_type"], payload["market_type"], payload["selection"], payload["line"], odds,
                    payload["model_expected"], payload["model_std_dev"], ev_pct, payload["edge_runs"], payload["confidence"],
                    payload["action"], payload["suppression_reason"], payload["bet_ref"], payload["stake_amount"],
                ),
            )
            self.conn.commit()
        except Exception:
            logger.debug("signal_log insert failed", exc_info=True)

    def get_stats(self) -> Dict[str, int]:
        """Quick row counts for health check."""
        balls = self.conn.execute("SELECT COUNT(*) FROM ball_log").fetchone()[0]
        scans = self.conn.execute("SELECT COUNT(*) FROM scan_snapshots").fetchone()[0]
        signals = self.conn.execute("SELECT COUNT(*) FROM signal_log").fetchone()[0]
        return {"balls": balls, "scans": scans, "signals": signals}

    def close(self) -> None:
        self.conn.close()
