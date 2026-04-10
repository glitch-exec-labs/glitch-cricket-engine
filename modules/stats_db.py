"""
IPL Edge Spotter — stats_db.py
SQLite database for IPL historical statistics.

Stores match-level totals, player batting innings, and bowler innings
to support venue/player/bowler lookups for pre-match and in-play analysis.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, Iterable, Optional

logger = logging.getLogger("ipl_spotter.stats_db")

_SCHEMA_MATCHES = """
CREATE TABLE IF NOT EXISTS matches (
    match_id      INTEGER PRIMARY KEY,
    venue         TEXT,
    team1         TEXT,
    team2         TEXT,
    first_innings_total   INTEGER,
    second_innings_total  INTEGER,
    powerplay_runs_1st    INTEGER,
    powerplay_runs_2nd    INTEGER,
    middle_runs_1st       INTEGER,
    middle_runs_2nd       INTEGER,
    death_runs_1st        INTEGER,
    death_runs_2nd        INTEGER,
    toss_winner           TEXT,
    toss_decision         TEXT,
    winner                TEXT
)
"""

_SCHEMA_PLAYER_INNINGS = """
CREATE TABLE IF NOT EXISTS player_innings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id    INTEGER,
    player      TEXT,
    team        TEXT,
    runs        INTEGER,
    balls       INTEGER,
    fours       INTEGER,
    sixes       INTEGER,
    venue       TEXT,
    phase       TEXT,
    opposition  TEXT
)
"""

_SCHEMA_BOWLER_INNINGS = """
CREATE TABLE IF NOT EXISTS bowler_innings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id        INTEGER,
    player          TEXT,
    team            TEXT,
    overs           REAL,
    runs_conceded   INTEGER,
    wickets         INTEGER,
    venue           TEXT,
    phase           TEXT,
    opposition      TEXT
)
"""


class StatsDB:
    """
    SQLite-backed store for IPL historical statistics.

    Usage:
        db = StatsDB("/path/to/stats.db")
        db.insert_match({...})
        venue = db.get_venue_stats("Wankhede Stadium")
        db.close()
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        logger.info("StatsDB opened: %s", db_path)

    # ── Schema ────────────────────────────────────────────────────────────────

    def _create_tables(self) -> None:
        cur = self.conn.cursor()
        cur.execute(_SCHEMA_MATCHES)
        cur.execute(_SCHEMA_PLAYER_INNINGS)
        cur.execute(_SCHEMA_BOWLER_INNINGS)
        self.conn.commit()
        logger.debug("StatsDB tables ensured")

    # ── Inserts ───────────────────────────────────────────────────────────────

    def insert_match(self, data: dict) -> None:
        """INSERT OR REPLACE a match record."""
        sql = """
        INSERT OR REPLACE INTO matches (
            match_id, venue, team1, team2,
            first_innings_total, second_innings_total,
            powerplay_runs_1st, powerplay_runs_2nd,
            middle_runs_1st, middle_runs_2nd,
            death_runs_1st, death_runs_2nd,
            toss_winner, toss_decision, winner
        ) VALUES (
            :match_id, :venue, :team1, :team2,
            :first_innings_total, :second_innings_total,
            :powerplay_runs_1st, :powerplay_runs_2nd,
            :middle_runs_1st, :middle_runs_2nd,
            :death_runs_1st, :death_runs_2nd,
            :toss_winner, :toss_decision, :winner
        )
        """
        self.conn.execute(sql, data)
        self.conn.commit()
        logger.debug("Inserted match %s", data.get("match_id"))

    def insert_player_innings(self, data: dict) -> None:
        """INSERT a player batting innings record."""
        sql = """
        INSERT INTO player_innings (
            match_id, player, team, runs, balls, fours, sixes,
            venue, phase, opposition
        ) VALUES (
            :match_id, :player, :team, :runs, :balls, :fours, :sixes,
            :venue, :phase, :opposition
        )
        """
        self.conn.execute(sql, data)
        self.conn.commit()
        logger.debug("Inserted player innings: %s (match %s)", data.get("player"), data.get("match_id"))

    def insert_bowler_innings(self, data: dict) -> None:
        """INSERT a bowler innings record."""
        sql = """
        INSERT INTO bowler_innings (
            match_id, player, team, overs, runs_conceded, wickets,
            venue, phase, opposition
        ) VALUES (
            :match_id, :player, :team, :overs, :runs_conceded, :wickets,
            :venue, :phase, :opposition
        )
        """
        self.conn.execute(sql, data)
        self.conn.commit()
        logger.debug("Inserted bowler innings: %s (match %s)", data.get("player"), data.get("match_id"))

    # ── Queries ───────────────────────────────────────────────────────────────

    def get_venue_stats(self, venue: str) -> Dict[str, Any]:
        """
        Aggregate stats for a venue.

        Returns dict with: matches, avg_first_innings, avg_second_innings,
        avg_powerplay_1st, avg_middle_1st, avg_death_1st.

        Uses exact match first, then fuzzy LIKE match if no exact hit
        (handles "Barsapara Cricket Stadium" vs "Barsapara Cricket Stadium, Guwahati").
        """
        _EMPTY = {
            "matches": 0,
            "avg_first_innings": None,
            "avg_second_innings": None,
            "avg_powerplay_1st": None,
            "avg_middle_1st": None,
            "avg_death_1st": None,
        }
        sql = """
        SELECT
            COUNT(*)                        AS matches,
            AVG(first_innings_total)        AS avg_first_innings,
            AVG(second_innings_total)       AS avg_second_innings,
            AVG(powerplay_runs_1st)         AS avg_powerplay_1st,
            AVG(middle_runs_1st)            AS avg_middle_1st,
            AVG(death_runs_1st)             AS avg_death_1st
        FROM matches
        WHERE venue = ?
        """
        row = self.conn.execute(sql, (venue,)).fetchone()
        if row is not None and row["matches"] > 0:
            return dict(row)

        # Fuzzy fallback: try LIKE match on the core venue name
        # "Barsapara Cricket Stadium" matches "Barsapara Cricket Stadium, Guwahati"
        if venue:
            like_sql = sql.replace("WHERE venue = ?", "WHERE venue LIKE ?")
            row = self.conn.execute(like_sql, (f"%{venue}%",)).fetchone()
            if row is not None and row["matches"] > 0:
                return dict(row)
            # Also try the reverse: DB name might be shorter
            # Get first significant word (skip "The", short words)
            words = [w for w in venue.split() if len(w) > 3]
            if words:
                row = self.conn.execute(like_sql, (f"%{words[0]}%",)).fetchone()
                if row is not None and row["matches"] > 0:
                    return dict(row)

        return _EMPTY

    def get_head_to_head(self, team1: str, team2: str, limit: int = 10) -> Dict[str, Any]:
        """Get head-to-head record between two teams.

        Uses LIKE matching to handle name variations
        (e.g. 'Chennai' matches 'Chennai Super Kings').
        """
        sql = """
        SELECT team1, team2, first_innings_total, second_innings_total,
               winner, venue, toss_winner, toss_decision
        FROM matches
        WHERE (team1 LIKE ? AND team2 LIKE ?)
           OR (team1 LIKE ? AND team2 LIKE ?)
        ORDER BY rowid DESC
        LIMIT ?
        """
        # Use first significant word for fuzzy matching
        t1_like = f"%{team1.split()[0]}%" if team1 else "%"
        t2_like = f"%{team2.split()[0]}%" if team2 else "%"
        rows = self.conn.execute(sql, (t1_like, t2_like, t2_like, t1_like, limit)).fetchall()

        if not rows:
            return {"matches": 0}

        t1_wins = 0
        t2_wins = 0
        t1_scores = []
        t2_scores = []
        toss_bat_first_wins = 0
        results = []

        for r in rows:
            r_team1, r_team2, inn1, inn2, winner, venue, toss_w, toss_d = r
            # Figure out which team is which
            if team1.split()[0].lower() in r_team1.lower():
                this_t1, this_t2 = r_team1, r_team2
                t1_score, t2_score = inn1 or 0, inn2 or 0
            else:
                this_t1, this_t2 = r_team2, r_team1
                t1_score, t2_score = inn2 or 0, inn1 or 0

            t1_scores.append(t1_score)
            t2_scores.append(t2_score)

            if winner and team1.split()[0].lower() in winner.lower():
                t1_wins += 1
                results.append("W")
            elif winner:
                t2_wins += 1
                results.append("L")
            else:
                results.append("D")

        avg_t1 = sum(t1_scores) / len(t1_scores) if t1_scores else 0
        avg_t2 = sum(t2_scores) / len(t2_scores) if t2_scores else 0

        return {
            "matches": len(rows),
            "team1": team1,
            "team2": team2,
            "team1_wins": t1_wins,
            "team2_wins": t2_wins,
            "team1_avg_score": round(avg_t1, 1),
            "team2_avg_score": round(avg_t2, 1),
            "last_results": results,  # from team1's perspective
        }

    def get_player_batting_stats(
        self,
        player: str,
        venue: Optional[str] = None,
        opposition: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Aggregate batting stats for a player, optionally filtered by venue/opposition.

        Returns dict with: innings, avg_runs, avg_strike_rate, total_runs.
        """
        conditions = ["player = ?"]
        params: list = [player]

        if venue is not None:
            conditions.append("venue = ?")
            params.append(venue)
        if opposition is not None:
            conditions.append("opposition = ?")
            params.append(opposition)

        where = " AND ".join(conditions)

        sql = f"""
        SELECT
            COUNT(*)            AS innings,
            AVG(runs)           AS avg_runs,
            SUM(runs)           AS total_runs,
            SUM(balls)          AS total_balls
        FROM player_innings
        WHERE {where}
        """
        row = self.conn.execute(sql, params).fetchone()
        if row is None or row["innings"] == 0:
            return {
                "innings": 0,
                "avg_runs": None,
                "avg_strike_rate": None,
                "total_runs": 0,
            }

        total_balls = row["total_balls"] or 0
        avg_sr = (row["total_runs"] / total_balls * 100) if total_balls > 0 else None

        return {
            "innings": row["innings"],
            "avg_runs": row["avg_runs"],
            "avg_strike_rate": avg_sr,
            "total_runs": row["total_runs"],
        }

    def get_bowler_stats(
        self,
        player: str,
        venue: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Aggregate bowling stats for a player, optionally filtered by venue.

        Returns dict with: innings, avg_economy, avg_wickets.
        """
        conditions = ["player = ?"]
        params: list = [player]

        if venue is not None:
            conditions.append("venue = ?")
            params.append(venue)

        where = " AND ".join(conditions)

        sql = f"""
        SELECT
            COUNT(*)                AS innings,
            AVG(wickets)            AS avg_wickets,
            SUM(runs_conceded)      AS total_runs,
            SUM(overs)              AS total_overs
        FROM bowler_innings
        WHERE {where}
        """
        row = self.conn.execute(sql, params).fetchone()
        if row is None or row["innings"] == 0:
            return {
                "innings": 0,
                "avg_economy": None,
                "avg_wickets": None,
            }

        total_overs = row["total_overs"] or 0
        avg_economy = (row["total_runs"] / total_overs) if total_overs > 0 else None

        return {
            "innings": row["innings"],
            "avg_economy": avg_economy,
            "avg_wickets": row["avg_wickets"],
        }

    def get_player_name_counts(self, kind: str = "batting") -> Dict[str, int]:
        """Return a name -> innings count map for batting or bowling records."""
        table = "player_innings" if kind == "batting" else "bowler_innings"
        sql = f"""
        SELECT player, COUNT(*) AS innings
        FROM {table}
        WHERE player IS NOT NULL AND TRIM(player) != ''
        GROUP BY player
        """
        rows = self.conn.execute(sql).fetchall()
        return {
            row["player"]: int(row["innings"])
            for row in rows
            if row["player"]
        }

    def delete_player_innings_for_matches(self, match_ids: Iterable[int | str]) -> int:
        """Delete batting rows for the provided match ids and return rows affected."""
        ids = [int(mid) for mid in match_ids]
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        cur = self.conn.execute(
            f"DELETE FROM player_innings WHERE match_id IN ({placeholders})",
            ids,
        )
        self.conn.commit()
        return cur.rowcount or 0

    def delete_bowler_innings_for_matches(self, match_ids: Iterable[int | str]) -> int:
        """Delete bowling rows for the provided match ids and return rows affected."""
        ids = [int(mid) for mid in match_ids]
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        cur = self.conn.execute(
            f"DELETE FROM bowler_innings WHERE match_id IN ({placeholders})",
            ids,
        )
        self.conn.commit()
        return cur.rowcount or 0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()
        logger.info("StatsDB closed: %s", self.db_path)
