"""
Tests for ipl_spotter.modules.stats_db — StatsDB.

Uses temporary DB files so no persistent state is needed.
"""

import os
import tempfile

import pytest

from modules.stats_db import StatsDB


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_db() -> StatsDB:
    """Create a StatsDB backed by a temporary file."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return StatsDB(path)


def _sample_match(match_id: int = 1, venue: str = "Wankhede Stadium") -> dict:
    return {
        "match_id": match_id,
        "venue": venue,
        "team1": "MI",
        "team2": "CSK",
        "first_innings_total": 180,
        "second_innings_total": 160,
        "powerplay_runs_1st": 55,
        "powerplay_runs_2nd": 45,
        "middle_runs_1st": 70,
        "middle_runs_2nd": 60,
        "death_runs_1st": 55,
        "death_runs_2nd": 55,
        "toss_winner": "MI",
        "toss_decision": "bat",
        "winner": "MI",
    }


def _sample_batting(match_id: int = 1, player: str = "Rohit Sharma",
                     venue: str = "Wankhede Stadium") -> dict:
    return {
        "match_id": match_id,
        "player": player,
        "team": "MI",
        "runs": 45,
        "balls": 30,
        "fours": 4,
        "sixes": 2,
        "venue": venue,
        "phase": "powerplay",
        "opposition": "CSK",
    }


def _sample_bowling(match_id: int = 1, player: str = "Jasprit Bumrah",
                     venue: str = "Wankhede Stadium") -> dict:
    return {
        "match_id": match_id,
        "player": player,
        "team": "MI",
        "overs": 4.0,
        "runs_conceded": 28,
        "wickets": 3,
        "venue": venue,
        "phase": "death",
        "opposition": "CSK",
    }


# ── Initialization ───────────────────────────────────────────────────────────

class TestInit:
    def test_creates_tables(self):
        db = _make_db()
        cur = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = sorted(row["name"] for row in cur.fetchall())
        assert "bowler_innings" in tables
        assert "matches" in tables
        assert "player_innings" in tables
        db.close()

    def test_row_factory_set(self):
        import sqlite3
        db = _make_db()
        assert db.conn.row_factory is sqlite3.Row
        db.close()

    def test_close(self):
        db = _make_db()
        db.close()
        # Connection should be closed; executing should raise
        with pytest.raises(Exception):
            db.conn.execute("SELECT 1")


# ── insert_match ─────────────────────────────────────────────────────────────

class TestInsertMatch:
    def test_insert_and_retrieve(self):
        db = _make_db()
        db.insert_match(_sample_match())
        row = db.conn.execute("SELECT * FROM matches WHERE match_id = 1").fetchone()
        assert row is not None
        assert row["venue"] == "Wankhede Stadium"
        assert row["first_innings_total"] == 180
        db.close()

    def test_insert_or_replace(self):
        db = _make_db()
        db.insert_match(_sample_match())
        # Update winner
        updated = _sample_match()
        updated["winner"] = "CSK"
        db.insert_match(updated)
        row = db.conn.execute("SELECT * FROM matches WHERE match_id = 1").fetchone()
        assert row["winner"] == "CSK"
        count = db.conn.execute("SELECT COUNT(*) AS c FROM matches").fetchone()["c"]
        assert count == 1
        db.close()


# ── insert_player_innings ────────────────────────────────────────────────────

class TestInsertPlayerInnings:
    def test_insert_and_retrieve(self):
        db = _make_db()
        db.insert_player_innings(_sample_batting())
        row = db.conn.execute("SELECT * FROM player_innings WHERE player = 'Rohit Sharma'").fetchone()
        assert row is not None
        assert row["runs"] == 45
        assert row["balls"] == 30
        db.close()

    def test_autoincrement_id(self):
        db = _make_db()
        db.insert_player_innings(_sample_batting())
        db.insert_player_innings(_sample_batting(match_id=2))
        rows = db.conn.execute("SELECT id FROM player_innings ORDER BY id").fetchall()
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[1]["id"] == 2
        db.close()


# ── insert_bowler_innings ────────────────────────────────────────────────────

class TestInsertBowlerInnings:
    def test_insert_and_retrieve(self):
        db = _make_db()
        db.insert_bowler_innings(_sample_bowling())
        row = db.conn.execute("SELECT * FROM bowler_innings WHERE player = 'Jasprit Bumrah'").fetchone()
        assert row is not None
        assert row["wickets"] == 3
        assert row["overs"] == 4.0
        db.close()


# ── get_venue_stats ──────────────────────────────────────────────────────────

class TestGetVenueStats:
    def test_no_matches(self):
        db = _make_db()
        stats = db.get_venue_stats("Eden Gardens")
        assert stats["matches"] == 0
        assert stats["avg_first_innings"] is None
        db.close()

    def test_single_match(self):
        db = _make_db()
        db.insert_match(_sample_match())
        stats = db.get_venue_stats("Wankhede Stadium")
        assert stats["matches"] == 1
        assert stats["avg_first_innings"] == 180
        assert stats["avg_second_innings"] == 160
        assert stats["avg_powerplay_1st"] == 55
        assert stats["avg_middle_1st"] == 70
        assert stats["avg_death_1st"] == 55
        db.close()

    def test_multiple_matches_averaged(self):
        db = _make_db()
        m1 = _sample_match(match_id=1)
        m1["first_innings_total"] = 200
        m2 = _sample_match(match_id=2)
        m2["first_innings_total"] = 160
        db.insert_match(m1)
        db.insert_match(m2)
        stats = db.get_venue_stats("Wankhede Stadium")
        assert stats["matches"] == 2
        assert stats["avg_first_innings"] == 180.0
        db.close()

    def test_filters_by_venue(self):
        db = _make_db()
        db.insert_match(_sample_match(match_id=1, venue="Wankhede Stadium"))
        db.insert_match(_sample_match(match_id=2, venue="Chinnaswamy"))
        stats = db.get_venue_stats("Wankhede Stadium")
        assert stats["matches"] == 1
        db.close()


# ── get_player_batting_stats ─────────────────────────────────────────────────

class TestGetPlayerBattingStats:
    def test_no_innings(self):
        db = _make_db()
        stats = db.get_player_batting_stats("Unknown Player")
        assert stats["innings"] == 0
        assert stats["avg_runs"] is None
        assert stats["total_runs"] == 0
        db.close()

    def test_single_innings(self):
        db = _make_db()
        db.insert_player_innings(_sample_batting())
        stats = db.get_player_batting_stats("Rohit Sharma")
        assert stats["innings"] == 1
        assert stats["avg_runs"] == 45
        assert stats["total_runs"] == 45
        assert stats["avg_strike_rate"] == 150.0  # 45/30 * 100
        db.close()

    def test_multiple_innings(self):
        db = _make_db()
        inn1 = _sample_batting(match_id=1)
        inn1["runs"] = 40
        inn1["balls"] = 25
        inn2 = _sample_batting(match_id=2)
        inn2["runs"] = 60
        inn2["balls"] = 35
        db.insert_player_innings(inn1)
        db.insert_player_innings(inn2)
        stats = db.get_player_batting_stats("Rohit Sharma")
        assert stats["innings"] == 2
        assert stats["total_runs"] == 100
        assert stats["avg_runs"] == 50.0
        # SR = 100 / 60 * 100 = 166.67
        assert round(stats["avg_strike_rate"], 2) == 166.67
        db.close()

    def test_filter_by_venue(self):
        db = _make_db()
        db.insert_player_innings(_sample_batting(match_id=1, venue="Wankhede Stadium"))
        db.insert_player_innings(_sample_batting(match_id=2, venue="Chinnaswamy"))
        stats = db.get_player_batting_stats("Rohit Sharma", venue="Wankhede Stadium")
        assert stats["innings"] == 1
        db.close()

    def test_filter_by_opposition(self):
        db = _make_db()
        inn1 = _sample_batting(match_id=1)
        inn1["opposition"] = "CSK"
        inn2 = _sample_batting(match_id=2)
        inn2["opposition"] = "RCB"
        db.insert_player_innings(inn1)
        db.insert_player_innings(inn2)
        stats = db.get_player_batting_stats("Rohit Sharma", opposition="CSK")
        assert stats["innings"] == 1
        db.close()

    def test_filter_by_venue_and_opposition(self):
        db = _make_db()
        inn1 = _sample_batting(match_id=1)
        inn2 = _sample_batting(match_id=2, venue="Chinnaswamy")
        db.insert_player_innings(inn1)
        db.insert_player_innings(inn2)
        stats = db.get_player_batting_stats(
            "Rohit Sharma", venue="Wankhede Stadium", opposition="CSK"
        )
        assert stats["innings"] == 1
        db.close()


# ── get_bowler_stats ─────────────────────────────────────────────────────────

class TestGetBowlerStats:
    def test_no_innings(self):
        db = _make_db()
        stats = db.get_bowler_stats("Unknown Bowler")
        assert stats["innings"] == 0
        assert stats["avg_economy"] is None
        assert stats["avg_wickets"] is None
        db.close()

    def test_single_innings(self):
        db = _make_db()
        db.insert_bowler_innings(_sample_bowling())
        stats = db.get_bowler_stats("Jasprit Bumrah")
        assert stats["innings"] == 1
        assert stats["avg_wickets"] == 3.0
        assert stats["avg_economy"] == 7.0  # 28 / 4
        db.close()

    def test_multiple_innings(self):
        db = _make_db()
        b1 = _sample_bowling(match_id=1)
        b1["runs_conceded"] = 24
        b1["overs"] = 4.0
        b1["wickets"] = 2
        b2 = _sample_bowling(match_id=2)
        b2["runs_conceded"] = 36
        b2["overs"] = 4.0
        b2["wickets"] = 4
        db.insert_bowler_innings(b1)
        db.insert_bowler_innings(b2)
        stats = db.get_bowler_stats("Jasprit Bumrah")
        assert stats["innings"] == 2
        assert stats["avg_wickets"] == 3.0
        assert stats["avg_economy"] == 7.5  # 60 / 8
        db.close()

    def test_filter_by_venue(self):
        db = _make_db()
        db.insert_bowler_innings(_sample_bowling(match_id=1, venue="Wankhede Stadium"))
        db.insert_bowler_innings(_sample_bowling(match_id=2, venue="Chinnaswamy"))
        stats = db.get_bowler_stats("Jasprit Bumrah", venue="Wankhede Stadium")
        assert stats["innings"] == 1
        db.close()
