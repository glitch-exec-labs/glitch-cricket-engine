"""Series Database — per-series per-season accumulating match/player stats.

Unlike ipl_stats.db (historical bulk load), this database builds LIVE during
a tournament season.  After each match completes, the bot writes the final
scorecard here.  This gives the prediction engine series-specific context
(team form, player form, venue trends) that improves as the season progresses.

Database file: data/series_{competition}_{year}.db
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("ipl_spotter.series_db")

_SERIES_MATCHES_SCHEMA = """
CREATE TABLE IF NOT EXISTS series_matches (
    fixture_id INTEGER PRIMARY KEY,
    match_number INTEGER,
    home TEXT,
    away TEXT,
    venue TEXT,
    date TEXT,
    toss_winner TEXT,
    toss_decision TEXT,
    inn1_total INTEGER,
    inn1_wickets INTEGER,
    inn2_total INTEGER,
    inn2_wickets INTEGER,
    winner TEXT,
    pp_runs_inn1 INTEGER,
    pp_runs_inn2 INTEGER,
    middle_runs_inn1 INTEGER,
    middle_runs_inn2 INTEGER,
    death_runs_inn1 INTEGER,
    death_runs_inn2 INTEGER,
    created_at TEXT
)
"""

_SERIES_BATTING_SCHEMA = """
CREATE TABLE IF NOT EXISTS series_batting (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_id INTEGER,
    innings INTEGER,
    player TEXT,
    team TEXT,
    runs INTEGER DEFAULT 0,
    balls INTEGER DEFAULT 0,
    fours INTEGER DEFAULT 0,
    sixes INTEGER DEFAULT 0,
    strike_rate REAL DEFAULT 0,
    position INTEGER DEFAULT 0,
    FOREIGN KEY (fixture_id) REFERENCES series_matches(fixture_id)
)
"""

_SERIES_BOWLING_SCHEMA = """
CREATE TABLE IF NOT EXISTS series_bowling (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_id INTEGER,
    innings INTEGER,
    player TEXT,
    team TEXT,
    overs REAL DEFAULT 0,
    runs_conceded INTEGER DEFAULT 0,
    wickets INTEGER DEFAULT 0,
    economy REAL DEFAULT 0,
    FOREIGN KEY (fixture_id) REFERENCES series_matches(fixture_id)
)
"""

_STANDINGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS standings (
    team TEXT PRIMARY KEY,
    played INTEGER DEFAULT 0,
    won INTEGER DEFAULT 0,
    lost INTEGER DEFAULT 0,
    no_result INTEGER DEFAULT 0,
    nrr REAL DEFAULT 0,
    points INTEGER DEFAULT 0,
    updated_at TEXT
)
"""


class SeriesDB:
    """Per-series per-season SQLite database that grows during the tournament."""

    def __init__(self, competition: str = "ipl", year: int | None = None) -> None:
        if year is None:
            year = datetime.now(timezone.utc).year
        self.competition = competition
        self.year = year
        db_dir = "data"
        os.makedirs(db_dir, exist_ok=True)
        self.db_path = os.path.join(db_dir, f"series_{competition}_{year}.db")
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        self._create_tables()
        logger.info("SeriesDB opened: %s (%d matches on record)", self.db_path, self.match_count())

    def _create_tables(self) -> None:
        cur = self._conn.cursor()
        cur.execute(_SERIES_MATCHES_SCHEMA)
        cur.execute(_SERIES_BATTING_SCHEMA)
        cur.execute(_SERIES_BOWLING_SCHEMA)
        cur.execute(_STANDINGS_SCHEMA)
        self._conn.commit()

    # ── Recording matches ─────────────────────────────────────────────

    def record_match(
        self,
        fixture_id: int,
        match_number: int,
        home: str,
        away: str,
        venue: str,
        date: str,
        toss_winner: str,
        toss_decision: str,
        inn1_total: int,
        inn1_wickets: int,
        inn2_total: int,
        inn2_wickets: int,
        winner: str,
        phase_runs: Optional[Dict[str, int]] = None,
        batting_cards: Optional[List[Dict]] = None,
        bowling_cards: Optional[List[Dict]] = None,
    ) -> None:
        """Record a completed match with full scorecard."""
        phase = phase_runs or {}
        now = datetime.now(timezone.utc).isoformat()

        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO series_matches
                   (fixture_id, match_number, home, away, venue, date,
                    toss_winner, toss_decision,
                    inn1_total, inn1_wickets, inn2_total, inn2_wickets, winner,
                    pp_runs_inn1, pp_runs_inn2,
                    middle_runs_inn1, middle_runs_inn2,
                    death_runs_inn1, death_runs_inn2, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (fixture_id, match_number, home, away, venue, date,
                 toss_winner, toss_decision,
                 inn1_total, inn1_wickets, inn2_total, inn2_wickets, winner,
                 phase.get("pp_inn1", 0), phase.get("pp_inn2", 0),
                 phase.get("mid_inn1", 0), phase.get("mid_inn2", 0),
                 phase.get("death_inn1", 0), phase.get("death_inn2", 0), now),
            )

            # Batting cards
            if batting_cards:
                self._conn.execute("DELETE FROM series_batting WHERE fixture_id = ?", (fixture_id,))
                for card in batting_cards:
                    self._conn.execute(
                        """INSERT INTO series_batting
                           (fixture_id, innings, player, team, runs, balls, fours, sixes, strike_rate, position)
                           VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        (fixture_id, card.get("innings", 1), card.get("player", ""),
                         card.get("team", ""), card.get("runs", 0), card.get("balls", 0),
                         card.get("fours", 0), card.get("sixes", 0),
                         card.get("strike_rate", 0.0), card.get("position", 0)),
                    )

            # Bowling cards
            if bowling_cards:
                self._conn.execute("DELETE FROM series_bowling WHERE fixture_id = ?", (fixture_id,))
                for card in bowling_cards:
                    self._conn.execute(
                        """INSERT INTO series_bowling
                           (fixture_id, innings, player, team, overs, runs_conceded, wickets, economy)
                           VALUES (?,?,?,?,?,?,?,?)""",
                        (fixture_id, card.get("innings", 1), card.get("player", ""),
                         card.get("team", ""), card.get("overs", 0.0), card.get("runs_conceded", 0),
                         card.get("wickets", 0), card.get("economy", 0.0)),
                    )

            self._conn.commit()

        logger.info(
            "SeriesDB: recorded match %d — %s vs %s (%d/%d vs %d/%d) winner=%s",
            fixture_id, home, away, inn1_total, inn1_wickets, inn2_total, inn2_wickets, winner,
        )

    def update_standings(self, team: str, won: bool, nrr_delta: float = 0.0) -> None:
        """Update team standings after a match."""
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            existing = self._conn.execute(
                "SELECT played, won, lost, nrr, points FROM standings WHERE team = ?", (team,)
            ).fetchone()
            if existing:
                played, w, l, nrr, pts = existing
                played += 1
                if won:
                    w += 1
                    pts += 2
                else:
                    l += 1
                nrr += nrr_delta
                self._conn.execute(
                    "UPDATE standings SET played=?, won=?, lost=?, nrr=?, points=?, updated_at=? WHERE team=?",
                    (played, w, l, round(nrr, 3), pts, now, team),
                )
            else:
                self._conn.execute(
                    "INSERT INTO standings (team, played, won, lost, nrr, points, updated_at) VALUES (?,1,?,?,?,?,?)",
                    (team, 1 if won else 0, 0 if won else 1, round(nrr_delta, 3), 2 if won else 0, now),
                )
            self._conn.commit()

    # ── Queries ───────────────────────────────────────────────────────

    def match_count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) FROM series_matches").fetchone()
        return row[0] if row else 0

    def has_match(self, fixture_id: int) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM series_matches WHERE fixture_id = ?", (fixture_id,)
        ).fetchone()
        return row is not None

    def get_team_form(self, team: str, last_n: int = 5) -> Dict[str, Any]:
        """Get recent form for a team in this series."""
        rows = self._conn.execute(
            """SELECT home, away, inn1_total, inn2_total, winner, venue
               FROM series_matches
               WHERE home = ? OR away = ?
               ORDER BY date DESC LIMIT ?""",
            (team, team, last_n),
        ).fetchall()
        if not rows:
            return {"matches": 0}

        wins = sum(1 for r in rows if r[4] == team)
        scores = []
        for r in rows:
            if r[0] == team:  # home
                scores.append(r[2] or 0)  # inn1_total
            else:
                scores.append(r[3] or 0)  # inn2_total
        avg_score = sum(scores) / len(scores) if scores else 0

        return {
            "matches": len(rows),
            "wins": wins,
            "losses": len(rows) - wins,
            "avg_score": round(avg_score, 1),
            "last_results": ["W" if r[4] == team else "L" for r in rows],
        }

    def get_player_series_stats(self, player: str) -> Dict[str, Any]:
        """Get batting + bowling stats for a player in this series."""
        bat_rows = self._conn.execute(
            """SELECT SUM(runs), SUM(balls), SUM(fours), SUM(sixes), COUNT(*)
               FROM series_batting WHERE player = ?""",
            (player,),
        ).fetchone()

        bowl_rows = self._conn.execute(
            """SELECT SUM(overs), SUM(runs_conceded), SUM(wickets), COUNT(*)
               FROM series_bowling WHERE player = ?""",
            (player,),
        ).fetchone()

        result: Dict[str, Any] = {"player": player}
        if bat_rows and bat_rows[4] > 0:
            total_runs, total_balls, fours, sixes, innings = bat_rows
            result["batting"] = {
                "innings": innings,
                "runs": total_runs or 0,
                "balls": total_balls or 0,
                "fours": fours or 0,
                "sixes": sixes or 0,
                "avg": round((total_runs or 0) / max(1, innings), 1),
                "sr": round((total_runs or 0) / max(1, total_balls or 1) * 100, 1),
            }
        if bowl_rows and bowl_rows[3] > 0:
            overs, runs, wickets, innings = bowl_rows
            result["bowling"] = {
                "innings": innings,
                "overs": overs or 0,
                "wickets": wickets or 0,
                "runs": runs or 0,
                "economy": round((runs or 0) / max(0.1, overs or 0.1), 1),
            }
        return result

    def get_venue_series_stats(self, venue: str) -> Dict[str, Any]:
        """Get venue averages from matches in this series."""
        rows = self._conn.execute(
            """SELECT AVG(inn1_total), AVG(inn2_total),
                      AVG(pp_runs_inn1), AVG(pp_runs_inn2),
                      AVG(death_runs_inn1), AVG(death_runs_inn2),
                      COUNT(*)
               FROM series_matches WHERE venue = ?""",
            (venue,),
        ).fetchone()
        if not rows or rows[6] == 0:
            return {"matches": 0}
        return {
            "matches": rows[6],
            "avg_inn1": round(rows[0] or 0, 1),
            "avg_inn2": round(rows[1] or 0, 1),
            "avg_pp_inn1": round(rows[2] or 0, 1),
            "avg_pp_inn2": round(rows[3] or 0, 1),
            "avg_death_inn1": round(rows[4] or 0, 1),
            "avg_death_inn2": round(rows[5] or 0, 1),
        }

    def get_head_to_head(self, team1: str, team2: str) -> Dict[str, Any]:
        """Get head-to-head record between two teams in this series."""
        rows = self._conn.execute(
            """SELECT winner, inn1_total, inn2_total, home, away
               FROM series_matches
               WHERE (home = ? AND away = ?) OR (home = ? AND away = ?)""",
            (team1, team2, team2, team1),
        ).fetchall()
        if not rows:
            return {"matches": 0}
        t1_wins = sum(1 for r in rows if r[0] == team1)
        t2_wins = sum(1 for r in rows if r[0] == team2)
        return {
            "matches": len(rows),
            f"{team1}_wins": t1_wins,
            f"{team2}_wins": t2_wins,
        }

    def get_standings(self) -> List[Dict[str, Any]]:
        """Get current series standings sorted by points then NRR."""
        rows = self._conn.execute(
            "SELECT team, played, won, lost, no_result, nrr, points FROM standings ORDER BY points DESC, nrr DESC"
        ).fetchall()
        return [
            {"team": r[0], "played": r[1], "won": r[2], "lost": r[3],
             "no_result": r[4], "nrr": r[5], "points": r[6]}
            for r in rows
        ]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
