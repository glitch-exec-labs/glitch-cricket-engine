"""Tests for the SQLite state persistence layer."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

from modules.bet_executor import BetExecutor, LiveBet
from modules.state_store import StateStore


# ── Helpers ───────────────────────────────────────────────────────────────

def _make_bet(
    ref: str = "ref-001",
    status: str = "ACCEPTED",
    paper: bool = True,
    pnl: float = 0.0,
    settled_at: datetime | None = None,
) -> LiveBet:
    return LiveBet(
        reference_id=ref,
        event_id="evt-123",
        home_team="MI",
        away_team="CSK",
        innings=1,
        market="innings_total",
        market_url="cricket.team_totals/over?team=home&total=185.5",
        direction="OVER",
        line=185.5,
        price=1.85,
        stake_usd=0.15,
        ev_pct=8.5,
        trigger="MODEL_EDGE",
        paper=paper,
        status=status,
        placed_at=datetime(2026, 3, 27, 12, 0, 0, tzinfo=timezone.utc),
        pnl=pnl,
        settled_at=settled_at,
    )


# ── StateStore unit tests ────────────────────────────────────────────────


class TestOpenBetsRoundTrip:
    def test_save_and_load(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        bet = _make_bet()
        store.save_open_bet(bet)

        loaded = store.load_open_bets()
        assert len(loaded) == 1
        assert "ref-001" in loaded

        b = loaded["ref-001"]
        assert b.reference_id == bet.reference_id
        assert b.event_id == bet.event_id
        assert b.home_team == "MI"
        assert b.away_team == "CSK"
        assert b.innings == 1
        assert b.market == "innings_total"
        assert b.direction == "OVER"
        assert b.line == 185.5
        assert b.price == 1.85
        assert b.stake_usd == 0.15
        assert b.ev_pct == 8.5
        assert b.trigger == "MODEL_EDGE"
        assert b.paper is True
        assert b.status == "ACCEPTED"
        assert b.pnl == 0.0

        store.close()

    def test_remove_open_bet(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        store.save_open_bet(_make_bet("a"))
        store.save_open_bet(_make_bet("b"))
        assert len(store.load_open_bets()) == 2

        store.remove_open_bet("a")
        loaded = store.load_open_bets()
        assert len(loaded) == 1
        assert "b" in loaded

        store.close()

    def test_upsert_replaces(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        bet = _make_bet()
        store.save_open_bet(bet)

        bet.status = "PENDING"
        store.save_open_bet(bet)

        loaded = store.load_open_bets()
        assert len(loaded) == 1
        assert loaded["ref-001"].status == "PENDING"

        store.close()


class TestClosedBets:
    def test_save_and_load(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        bet = _make_bet(
            status="WON",
            pnl=0.1275,
            settled_at=datetime(2026, 3, 27, 13, 0, 0, tzinfo=timezone.utc),
        )
        store.save_closed_bet(bet)

        loaded = store.load_closed_bets()
        assert len(loaded) == 1
        b = loaded[0]
        assert b.status == "WON"
        assert b.pnl == pytest.approx(0.1275)
        assert b.settled_at is not None
        assert b.settled_at.year == 2026

        store.close()

    def test_settlement_persistence(self, tmp_path):
        """Full cycle: open -> settle -> verify open is gone, closed exists."""
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        bet = _make_bet()
        store.save_open_bet(bet)
        assert len(store.load_open_bets()) == 1

        # Settle
        bet.status = "WON"
        bet.pnl = 0.1275
        bet.settled_at = datetime(2026, 3, 27, 13, 0, 0, tzinfo=timezone.utc)

        store.remove_open_bet(bet.reference_id)
        store.save_closed_bet(bet)

        assert len(store.load_open_bets()) == 0
        closed = store.load_closed_bets()
        assert len(closed) == 1
        assert closed[0].status == "WON"

        store.close()

    def test_load_limit(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        for i in range(10):
            bet = _make_bet(
                ref=f"ref-{i:03d}",
                status="WON",
                pnl=0.01 * i,
                settled_at=datetime(2026, 3, 27, 12, i, 0, tzinfo=timezone.utc),
            )
            store.save_closed_bet(bet)

        loaded = store.load_closed_bets(limit=3)
        assert len(loaded) == 3

        store.close()


class TestKeyValueState:
    def test_save_and_load_string(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        store.save_state("greeting", "hello")
        assert store.load_state("greeting") == "hello"
        store.close()

    def test_save_and_load_dict(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        data = {"alerts_sent": ["a", "b"], "count": 42}
        store.save_state("spotter_state", data)
        loaded = store.load_state("spotter_state")
        assert loaded == data
        store.close()

    def test_load_missing_key(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        assert store.load_state("nonexistent") is None
        store.close()

    def test_overwrite(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        store.save_state("k", 1)
        store.save_state("k", 2)
        assert store.load_state("k") == 2
        store.close()


class TestDailyPnl:
    def test_save_and_load(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        store.save_daily_pnl(-0.25)
        assert store.load_daily_pnl() == pytest.approx(-0.25)
        store.close()

    def test_default_zero(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        assert store.load_daily_pnl() == 0.0
        store.close()


# ── BetExecutor integration ──────────────────────────────────────────────


class TestBetExecutorIntegration:
    def test_paper_bet_persists(self, tmp_path):
        """Placing a paper bet should persist it in the store."""
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        executor = BetExecutor(
            config={"cloudbet_api_key": "fake"},
            paper_mode=True,
            state_store=store,
        )

        bet = executor.place_bet(
            event_id="evt-1",
            market_url="cricket.innings_total/over?total=185.5",
            price=1.85,
            stake=0.10,
            market="innings_total",
            direction="OVER",
            line=185.5,
            home="MI",
            away="CSK",
            ev_pct=8.0,
            trigger="MODEL_EDGE",
        )

        assert bet is not None

        # Verify it was persisted
        loaded = store.load_open_bets()
        assert len(loaded) == 1
        assert bet.reference_id in loaded

        store.close()

    def test_restore_on_init(self, tmp_path):
        """BetExecutor should restore open bets from store on init."""
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        # Pre-populate the store
        bet = _make_bet()
        store.save_open_bet(bet)
        store.save_daily_pnl(0.05)

        executor = BetExecutor(
            config={"cloudbet_api_key": "fake"},
            paper_mode=True,
            state_store=store,
        )

        assert len(executor.open_bets) == 1
        assert "ref-001" in executor.open_bets
        assert executor.daily_pnl == pytest.approx(0.05)

        store.close()

    def test_backward_compatible_no_store(self):
        """BetExecutor with no state_store should work exactly as before."""
        executor = BetExecutor(
            config={"cloudbet_api_key": "fake"},
            paper_mode=True,
        )
        assert executor.state_store is None
        assert len(executor.open_bets) == 0

        bet = executor.place_bet(
            event_id="evt-1",
            market_url="cricket.innings_total/over?total=185.5",
            price=1.85,
            stake=0.10,
            market="innings_total",
            direction="OVER",
            line=185.5,
            home="MI",
            away="CSK",
            ev_pct=8.0,
            trigger="MODEL_EDGE",
        )

        assert bet is not None
        assert len(executor.open_bets) == 1

    def test_multiple_bets_persistence(self, tmp_path):
        """Multiple bets placed should all be persisted."""
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        executor = BetExecutor(
            config={"cloudbet_api_key": "fake"},
            paper_mode=True,
            state_store=store,
        )

        markets = ["innings_total", "powerplay_runs", "over_runs"]
        for i in range(3):
            executor.place_bet(
                event_id=f"evt-{i}",
                market_url=f"cricket.{markets[i]}/over?total={180 + i}.5",
                price=1.85,
                stake=0.10,
                market=markets[i],
                direction="OVER",
                line=180 + i + 0.5,
                home="MI",
                away="CSK",
                ev_pct=8.0,
                trigger="MODEL_EDGE",
            )

        loaded = store.load_open_bets()
        assert len(loaded) == 3

        store.close()


class TestTableCreation:
    def test_tables_exist(self, tmp_path):
        db = os.path.join(tmp_path, "test.db")
        store = StateStore(db_path=db)

        import sqlite3
        conn = sqlite3.connect(db)
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = sorted(row[0] for row in cur.fetchall())
        conn.close()

        assert "bot_state" in tables
        assert "closed_bets" in tables
        assert "open_bets" in tables
        assert "shadow_positions" in tables

        store.close()

    def test_creates_parent_directory(self, tmp_path):
        db = os.path.join(tmp_path, "subdir", "nested", "test.db")
        store = StateStore(db_path=db)
        assert os.path.exists(db)
        store.close()
