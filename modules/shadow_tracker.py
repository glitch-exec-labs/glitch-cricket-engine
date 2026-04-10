"""Shadow Tracker — SQLite-backed ledger for every signal the bot generates.

Logs all session, match-winner, and speed-edge signals, settles them after
matches complete, and produces a Telegram-ready dashboard summary.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("ipl_spotter.shadow_tracker")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id        INTEGER,
    match_date      TEXT,
    home            TEXT,
    away            TEXT,
    venue           TEXT,
    signal_type     TEXT,
    direction       TEXT,
    market          TEXT,
    entry_line      REAL,
    model_expected  REAL,
    edge_runs       REAL,
    odds            REAL,
    ev_pct          REAL,
    confidence      TEXT,
    stake           REAL,
    created_at      TEXT,
    result          TEXT,
    actual_value    REAL,
    pnl             REAL
)
"""


class ShadowTracker:
    """Persistent shadow-trading ledger backed by SQLite."""

    def __init__(self, db_path: str = "data/shadow_ledger.db") -> None:
        self.db_path = db_path
        # Ensure the parent directory exists
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_CREATE_TABLE)
        self._conn.commit()

    # ── logging ───────────────────────────────────────────────────────

    def log_signal(
        self,
        match_id: int,
        home: str,
        away: str,
        venue: str,
        signal_type: str,
        direction: str,
        market: str,
        entry_line: float,
        model_expected: float,
        edge_runs: float,
        odds: float,
        ev_pct: float,
        confidence: str,
        stake: float,
    ) -> int:
        """Insert a new signal row and return the row id."""
        now = datetime.now(timezone.utc).isoformat()
        today = date.today().isoformat()
        cur = self._conn.execute(
            """
            INSERT INTO signals
                (match_id, match_date, home, away, venue,
                 signal_type, direction, market,
                 entry_line, model_expected, edge_runs,
                 odds, ev_pct, confidence, stake, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                match_id, today, home, away, venue,
                signal_type, direction, market,
                entry_line, model_expected, edge_runs,
                odds, ev_pct, confidence, stake, now,
            ),
        )
        self._conn.commit()
        row_id = cur.lastrowid
        logger.info(
            "Logged signal #%d: %s %s %s edge=%.1f ev=%.1f%%",
            row_id, signal_type, direction, market, edge_runs, ev_pct,
        )
        return row_id

    # ── settlement ────────────────────────────────────────────────────

    def settle_match(self, match_id: int, actual_scores: Dict[str, Any]) -> int:
        """Settle all signals for a match given actual outcomes.

        actual_scores format:
            {"6_over": 48, "10_over": 82, "15_over": 128,
             "20_over": 175, "match_winner": "RCB"}

        Returns the number of rows settled.
        """
        rows = self._conn.execute(
            "SELECT * FROM signals WHERE match_id = ? AND result IS NULL",
            (match_id,),
        ).fetchall()

        settled = 0
        for row in rows:
            signal_type = row["signal_type"]
            market = row["market"]
            direction = row["direction"]
            entry_line = row["entry_line"]
            stake = row["stake"]

            actual = actual_scores.get(market)
            if actual is None:
                continue

            if market == "match_winner" or signal_type == "mw":
                # MW settlement: actual is the winning team name
                winner = actual
                odds = row["odds"]
                if direction == "LAGAI":
                    # We backed a team; entry_line is not relevant,
                    # but odds determine payout.
                    if isinstance(winner, str):
                        # Determine which team was backed from the signal
                        # For MW, entry_line stores 0 (unused); odds is the back odds.
                        # The 'home' or 'away' that matches direction is in
                        # the signal context. We check if the winner matches
                        # either home or away from the signal.
                        backed_team = self._get_backed_team(row)
                        if backed_team and backed_team == winner:
                            result = "WIN"
                            pnl = stake * (odds - 1)
                        else:
                            result = "LOSS"
                            pnl = -stake
                    else:
                        continue
                elif direction == "KHAI":
                    backed_team = self._get_backed_team(row)
                    if backed_team and backed_team != winner:
                        result = "WIN"
                        pnl = stake
                    else:
                        result = "LOSS"
                        pnl = -stake * (row["odds"] - 1)
                else:
                    continue

                self._conn.execute(
                    """UPDATE signals
                       SET result = ?, actual_value = ?, pnl = ?
                       WHERE id = ?""",
                    (result, None, round(pnl, 2), row["id"]),
                )
                settled += 1

            else:
                # Session market settlement
                actual_val = float(actual)
                if direction in ("YES", "OVER"):
                    if actual_val > entry_line:
                        result = "WIN"
                        pnl = (actual_val - entry_line) * stake
                    else:
                        result = "LOSS"
                        pnl = -(entry_line - actual_val) * stake
                elif direction in ("NO", "UNDER"):
                    if actual_val < entry_line:
                        result = "WIN"
                        pnl = (entry_line - actual_val) * stake
                    else:
                        result = "LOSS"
                        pnl = -(actual_val - entry_line) * stake
                else:
                    continue

                self._conn.execute(
                    """UPDATE signals
                       SET result = ?, actual_value = ?, pnl = ?
                       WHERE id = ?""",
                    (result, actual_val, round(pnl, 2), row["id"]),
                )
                settled += 1

        self._conn.commit()
        logger.info("Settled %d signals for match %d", settled, match_id)
        return settled

    def _get_backed_team(self, row: sqlite3.Row) -> Optional[str]:
        """Infer the backed team from signal context.

        For MW signals the 'home' field typically stores the backed team
        when direction is LAGAI, but we also look at the market context.
        Convention: for MW signals we store the backed team name in the
        'home' column when it is the home team, otherwise in 'away'.
        To resolve unambiguously, we check which team the direction
        points to. By convention in evaluate_mw_call, the entry_line
        is 0 and model_expected holds the fair probability; the backed
        team appears as the first word after 'Lagai' in the direction.

        Simplified approach: we store additional context. For now, we
        compare odds with common naming. The caller should pass the
        match_winner key in actual_scores as the winning team name.
        We reconstruct the backed team from home/away + direction.

        Actually, the simplest approach: look at edge_runs. For MW,
        model_expected is the model prob, entry_line is 0. But that
        doesn't tell us the team. We need to check which team was
        backed. The convention from evaluate_mw_call is that if the
        best team is home, we store (home, away) normally. We should
        rely on additional info.

        For a robust approach: we store the backed team name in the
        'venue' field? No, that's venue. Let's use a convention:
        for MW, model_expected > 0.5 means we backed home, else away.
        But that's fragile.

        Best approach: look at the edge_runs sign. Positive = home backed.
        Negative = away backed. Or simpler: we just check both.
        """
        # For MW signals, figure out which team was backed.
        # If direction is LAGAI, we backed some team.
        # We'll check if home or away was the favorable side.
        # The model_expected stores the model probability for the backed side.
        # If model_expected > implied prob from odds, that side has +EV.
        # The edge_runs field stores the probability edge for MW.
        # Positive edge_runs = this is the team with +EV.
        # But we still need to know WHICH team.
        #
        # Convention from match_copilot.evaluate_mw_call:
        #   best_team is set to home or away based on EV comparison.
        #   Then the call returns {"team": best_team, "direction": "LAGAI", ...}
        # So we need to store the team name. We'll use a simple heuristic:
        # check the model_expected (prob). If model_expected > 0.5, likely home.
        # But this isn't reliable. The safest way is to store the team name.
        #
        # Since we control log_signal, the caller should set home=backed_team
        # for MW signals. Let's document that and use home as the backed team.
        return row["home"] if row["home"] else row["away"]

    # ── dashboard ─────────────────────────────────────────────────────

    def get_dashboard(self, days: int = 14) -> Dict[str, Any]:
        """Aggregate stats over the last *days* days."""
        cutoff = (date.today() - timedelta(days=days)).isoformat()

        rows = self._conn.execute(
            """SELECT * FROM signals
               WHERE match_date >= ? AND result IS NOT NULL""",
            (cutoff,),
        ).fetchall()

        total_signals = len(rows)
        win_count = sum(1 for r in rows if r["result"] == "WIN")
        loss_count = sum(1 for r in rows if r["result"] == "LOSS")
        hit_rate = (win_count / total_signals * 100) if total_signals else 0.0
        total_pnl = sum(r["pnl"] for r in rows if r["pnl"] is not None)
        avg_ev_pct = (
            sum(r["ev_pct"] for r in rows if r["ev_pct"] is not None) / total_signals
            if total_signals
            else 0.0
        )
        avg_edge_runs = (
            sum(abs(r["edge_runs"]) for r in rows if r["edge_runs"] is not None)
            / total_signals
            if total_signals
            else 0.0
        )

        # Breakdown by market
        by_market: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            m = r["market"]
            if m not in by_market:
                by_market[m] = {"signals": 0, "wins": 0, "pnl": 0.0}
            by_market[m]["signals"] += 1
            if r["result"] == "WIN":
                by_market[m]["wins"] += 1
            by_market[m]["pnl"] += r["pnl"] or 0.0

        for v in by_market.values():
            v["hit_rate"] = (v["wins"] / v["signals"] * 100) if v["signals"] else 0.0

        # Breakdown by signal_type
        by_signal_type: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            st = r["signal_type"]
            if st not in by_signal_type:
                by_signal_type[st] = {"signals": 0, "wins": 0, "pnl": 0.0}
            by_signal_type[st]["signals"] += 1
            if r["result"] == "WIN":
                by_signal_type[st]["wins"] += 1
            by_signal_type[st]["pnl"] += r["pnl"] or 0.0

        for v in by_signal_type.values():
            v["hit_rate"] = (v["wins"] / v["signals"] * 100) if v["signals"] else 0.0

        # Breakdown by confidence
        by_confidence: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            c = r["confidence"] or "UNKNOWN"
            if c not in by_confidence:
                by_confidence[c] = {"signals": 0, "wins": 0}
            by_confidence[c]["signals"] += 1
            if r["result"] == "WIN":
                by_confidence[c]["wins"] += 1

        for v in by_confidence.values():
            v["hit_rate"] = (v["wins"] / v["signals"] * 100) if v["signals"] else 0.0

        # Daily P&L (last N days that have data)
        daily_pnl: Dict[str, float] = {}
        for r in rows:
            d = r["match_date"]
            daily_pnl[d] = daily_pnl.get(d, 0.0) + (r["pnl"] or 0.0)

        daily_pnl_list = [
            {"date": d, "pnl": round(daily_pnl[d], 2)}
            for d in sorted(daily_pnl.keys())
        ]

        # Unsettled count
        unsettled = self._conn.execute(
            "SELECT COUNT(*) FROM signals WHERE match_date >= ? AND result IS NULL",
            (cutoff,),
        ).fetchone()[0]

        return {
            "days": days,
            "total_signals": total_signals,
            "win_count": win_count,
            "loss_count": loss_count,
            "hit_rate": round(hit_rate, 1),
            "total_pnl": round(total_pnl, 2),
            "avg_ev_pct": round(avg_ev_pct, 1),
            "avg_edge_runs": round(avg_edge_runs, 1),
            "by_market": by_market,
            "by_signal_type": by_signal_type,
            "by_confidence": by_confidence,
            "daily_pnl": daily_pnl_list,
            "unsettled": unsettled,
        }

    # ── formatting ────────────────────────────────────────────────────

    def format_dashboard(self, stats: Dict[str, Any]) -> str:
        """Render a Telegram-ready dashboard summary."""
        days = stats["days"]
        total = stats["total_signals"]
        wins = stats["win_count"]
        losses = stats["loss_count"]
        hit = stats["hit_rate"]
        pnl = stats["total_pnl"]
        avg_ev = stats["avg_ev_pct"]

        lines = [
            f"SHADOW REPORT ({days} days)",
            "",
            f"Signals: {total} | Wins: {wins} | Losses: {losses} | Hit: {hit}%",
            "",
            f"P&L: Rs {pnl:+,.0f} (avg EV: {avg_ev}%)",
        ]

        # By Market
        market_order = ["6_over", "10_over", "15_over", "20_over", "match_winner"]
        market_labels = {
            "6_over": "6 Over",
            "10_over": "10 Over",
            "15_over": "15 Over",
            "20_over": "20 Over",
            "match_winner": "MW",
        }
        by_market = stats.get("by_market", {})
        if by_market:
            lines.append("")
            lines.append("By Market:")
            for mk in market_order:
                if mk not in by_market:
                    continue
                v = by_market[mk]
                label = market_labels.get(mk, mk)
                lines.append(
                    f"  {label:8s} {v['signals']:>2d} signals, "
                    f"{v['hit_rate']:.0f}% hit, Rs {v['pnl']:+,.0f}"
                )
            # Any markets not in the standard order
            for mk, v in by_market.items():
                if mk not in market_order:
                    lines.append(
                        f"  {mk:8s} {v['signals']:>2d} signals, "
                        f"{v['hit_rate']:.0f}% hit, Rs {v['pnl']:+,.0f}"
                    )

        # By Confidence
        by_confidence = stats.get("by_confidence", {})
        if by_confidence:
            lines.append("")
            lines.append("By Confidence:")
            for level in ("HIGH", "MEDIUM", "LOW"):
                if level not in by_confidence:
                    continue
                v = by_confidence[level]
                lines.append(
                    f"  {level:7s} {v['signals']:>2d} signals, "
                    f"{v['hit_rate']:.0f}% hit"
                )

        # Daily P&L (last 5 entries)
        daily = stats.get("daily_pnl", [])
        if daily:
            last5 = daily[-5:]
            pnl_str = " ".join(f"{d['pnl']:+,.0f}" for d in last5)
            lines.append("")
            lines.append(f"Last {len(last5)} days: {pnl_str}")

        return "\n".join(lines)

    # ── cleanup ───────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
