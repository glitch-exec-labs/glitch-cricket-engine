"""Tests for shadow_tracker — SQLite-backed signal ledger and dashboard."""

import os
import pytest
from modules.shadow_tracker import ShadowTracker


@pytest.fixture
def tracker(tmp_path):
    db_path = str(tmp_path / "test_ledger.db")
    t = ShadowTracker(db_path=db_path)
    yield t
    t.close()


class TestLogSignal:
    def test_basic_insert(self, tracker):
        row_id = tracker.log_signal(
            match_id=101, home="CSK", away="MI", venue="Chepauk",
            signal_type="session", direction="YES", market="6_over",
            entry_line=45.0, model_expected=50.0, edge_runs=5.0,
            odds=1.9, ev_pct=8.5, confidence="MEDIUM", stake=500,
        )
        assert row_id == 1

    def test_multiple_inserts(self, tracker):
        for i in range(3):
            tracker.log_signal(
                match_id=101, home="CSK", away="MI", venue="Chepauk",
                signal_type="session", direction="YES", market="6_over",
                entry_line=45.0 + i, model_expected=50.0, edge_runs=5.0 - i,
                odds=1.9, ev_pct=8.0, confidence="HIGH", stake=500,
            )
        rows = tracker._conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        assert rows == 3

    def test_fields_stored_correctly(self, tracker):
        tracker.log_signal(
            match_id=202, home="RCB", away="KKR", venue="Chinnaswamy",
            signal_type="mw", direction="LAGAI", market="match_winner",
            entry_line=0, model_expected=0.55, edge_runs=0.08,
            odds=2.10, ev_pct=12.3, confidence="HIGH", stake=500,
        )
        row = tracker._conn.execute("SELECT * FROM signals WHERE id = 1").fetchone()
        assert row["match_id"] == 202
        assert row["home"] == "RCB"
        assert row["signal_type"] == "mw"
        assert row["confidence"] == "HIGH"
        assert row["result"] is None
        assert row["pnl"] is None


class TestSettleMatch:
    def _insert_session_signals(self, tracker, match_id=101):
        """Insert a set of session signals for settlement testing."""
        tracker.log_signal(
            match_id=match_id, home="CSK", away="MI", venue="Chepauk",
            signal_type="session", direction="YES", market="6_over",
            entry_line=45.0, model_expected=50.0, edge_runs=5.0,
            odds=1.9, ev_pct=8.0, confidence="MEDIUM", stake=100,
        )
        tracker.log_signal(
            match_id=match_id, home="CSK", away="MI", venue="Chepauk",
            signal_type="session", direction="NO", market="20_over",
            entry_line=170.0, model_expected=162.0, edge_runs=8.0,
            odds=1.85, ev_pct=7.0, confidence="HIGH", stake=200,
        )

    def test_session_settlement_win(self, tracker):
        self._insert_session_signals(tracker)
        actual = {"6_over": 52, "20_over": 160}
        settled = tracker.settle_match(101, actual)
        assert settled == 2

        rows = tracker._conn.execute(
            "SELECT * FROM signals WHERE match_id = 101 ORDER BY id"
        ).fetchall()
        # 6_over: YES at 45, actual 52 -> WIN, pnl = (52-45)*100 = 700
        assert rows[0]["result"] == "WIN"
        assert rows[0]["pnl"] == 700.0
        assert rows[0]["actual_value"] == 52.0

        # 20_over: NO at 170, actual 160 -> WIN, pnl = (170-160)*200 = 2000
        assert rows[1]["result"] == "WIN"
        assert rows[1]["pnl"] == 2000.0

    def test_session_settlement_loss(self, tracker):
        tracker.log_signal(
            match_id=101, home="CSK", away="MI", venue="Chepauk",
            signal_type="session", direction="YES", market="6_over",
            entry_line=45.0, model_expected=50.0, edge_runs=5.0,
            odds=1.9, ev_pct=8.0, confidence="MEDIUM", stake=100,
        )
        actual = {"6_over": 40}
        tracker.settle_match(101, actual)

        row = tracker._conn.execute("SELECT * FROM signals WHERE id = 1").fetchone()
        # YES at 45, actual 40 -> LOSS, pnl = -(45-40)*100 = -500
        assert row["result"] == "LOSS"
        assert row["pnl"] == -500.0

    def test_mw_settlement_win(self, tracker):
        tracker.log_signal(
            match_id=201, home="RCB", away="KKR", venue="Chinnaswamy",
            signal_type="mw", direction="LAGAI", market="match_winner",
            entry_line=0, model_expected=0.55, edge_runs=0.08,
            odds=2.10, ev_pct=12.0, confidence="HIGH", stake=500,
        )
        actual = {"match_winner": "RCB"}
        settled = tracker.settle_match(201, actual)
        assert settled == 1

        row = tracker._conn.execute("SELECT * FROM signals WHERE id = 1").fetchone()
        # LAGAI RCB @ 2.10, stake 500, RCB wins -> WIN, pnl = 500*(2.10-1) = 550
        assert row["result"] == "WIN"
        assert row["pnl"] == 550.0

    def test_mw_settlement_loss(self, tracker):
        tracker.log_signal(
            match_id=201, home="RCB", away="KKR", venue="Chinnaswamy",
            signal_type="mw", direction="LAGAI", market="match_winner",
            entry_line=0, model_expected=0.55, edge_runs=0.08,
            odds=2.10, ev_pct=12.0, confidence="HIGH", stake=500,
        )
        actual = {"match_winner": "KKR"}
        tracker.settle_match(201, actual)

        row = tracker._conn.execute("SELECT * FROM signals WHERE id = 1").fetchone()
        assert row["result"] == "LOSS"
        assert row["pnl"] == -500.0

    def test_no_double_settlement(self, tracker):
        self._insert_session_signals(tracker)
        actual = {"6_over": 52, "20_over": 160}
        settled1 = tracker.settle_match(101, actual)
        settled2 = tracker.settle_match(101, actual)
        assert settled1 == 2
        assert settled2 == 0  # already settled

    def test_partial_settlement(self, tracker):
        self._insert_session_signals(tracker)
        # Only settle one market
        actual = {"6_over": 52}
        settled = tracker.settle_match(101, actual)
        assert settled == 1

        unsettled = tracker._conn.execute(
            "SELECT COUNT(*) FROM signals WHERE match_id = 101 AND result IS NULL"
        ).fetchone()[0]
        assert unsettled == 1


class TestGetDashboard:
    def _populate(self, tracker, match_id=101, market="6_over",
                  direction="YES", entry_line=45.0, actual=52.0,
                  confidence="MEDIUM", signal_type="session", stake=100,
                  ev_pct=8.0, edge_runs=5.0, odds=1.9):
        tracker.log_signal(
            match_id=match_id, home="CSK", away="MI", venue="Chepauk",
            signal_type=signal_type, direction=direction, market=market,
            entry_line=entry_line, model_expected=entry_line + edge_runs,
            edge_runs=edge_runs,
            odds=odds, ev_pct=ev_pct, confidence=confidence, stake=stake,
        )
        scores = {}
        if market == "match_winner":
            scores[market] = actual
        else:
            scores[market] = actual
        tracker.settle_match(match_id, scores)

    def test_basic_dashboard(self, tracker):
        self._populate(tracker, match_id=1, market="6_over",
                       direction="YES", entry_line=45, actual=52,
                       confidence="HIGH")
        self._populate(tracker, match_id=2, market="10_over",
                       direction="NO", entry_line=90, actual=95,
                       confidence="MEDIUM")

        stats = tracker.get_dashboard(days=14)
        assert stats["total_signals"] == 2
        assert stats["win_count"] == 1
        assert stats["loss_count"] == 1
        assert stats["hit_rate"] == 50.0

    def test_dashboard_by_market(self, tracker):
        self._populate(tracker, match_id=1, market="6_over",
                       direction="YES", entry_line=45, actual=50)
        self._populate(tracker, match_id=2, market="6_over",
                       direction="YES", entry_line=45, actual=50)
        self._populate(tracker, match_id=3, market="20_over",
                       direction="YES", entry_line=160, actual=170)

        stats = tracker.get_dashboard(days=14)
        assert "6_over" in stats["by_market"]
        assert stats["by_market"]["6_over"]["signals"] == 2
        assert stats["by_market"]["20_over"]["signals"] == 1

    def test_dashboard_by_confidence(self, tracker):
        self._populate(tracker, match_id=1, confidence="HIGH",
                       direction="YES", entry_line=45, actual=50)
        self._populate(tracker, match_id=2, confidence="LOW",
                       direction="YES", entry_line=45, actual=40)

        stats = tracker.get_dashboard(days=14)
        assert stats["by_confidence"]["HIGH"]["signals"] == 1
        assert stats["by_confidence"]["LOW"]["signals"] == 1

    def test_daily_pnl(self, tracker):
        self._populate(tracker, match_id=1, direction="YES",
                       entry_line=45, actual=50, stake=100)
        self._populate(tracker, match_id=2, direction="YES",
                       entry_line=45, actual=40, stake=100)

        stats = tracker.get_dashboard(days=14)
        # Both logged on the same day
        assert len(stats["daily_pnl"]) == 1
        # First: (50-45)*100 = 500, Second: -(45-40)*100 = -500 => net 0
        assert stats["daily_pnl"][0]["pnl"] == 0.0

    def test_empty_dashboard(self, tracker):
        stats = tracker.get_dashboard(days=14)
        assert stats["total_signals"] == 0
        assert stats["hit_rate"] == 0.0
        assert stats["total_pnl"] == 0.0


class TestFormatDashboard:
    def test_format_output(self, tracker):
        # Populate with some data
        signals_data = [
            (1, "6_over", "YES", 45, 52, "HIGH", 100, 8.0, 5.0),
            (2, "6_over", "YES", 45, 50, "MEDIUM", 100, 7.0, 4.0),
            (3, "10_over", "NO", 90, 95, "LOW", 100, 6.0, 3.0),
            (4, "20_over", "YES", 160, 170, "HIGH", 200, 9.0, 6.0),
        ]
        for mid, market, direction, entry, actual, conf, stake, ev, edge in signals_data:
            tracker.log_signal(
                match_id=mid, home="CSK", away="MI", venue="Chepauk",
                signal_type="session", direction=direction, market=market,
                entry_line=entry, model_expected=entry + edge,
                edge_runs=edge, odds=1.9, ev_pct=ev,
                confidence=conf, stake=stake,
            )
            scores = {market: actual}
            tracker.settle_match(mid, scores)

        stats = tracker.get_dashboard(days=14)
        output = tracker.format_dashboard(stats)

        assert "SHADOW REPORT (14 days)" in output
        assert "Signals: 4" in output
        assert "By Market:" in output
        assert "By Confidence:" in output
        assert "HIGH" in output
        assert "MEDIUM" in output
        assert "Last" in output

    def test_format_empty(self, tracker):
        stats = tracker.get_dashboard(days=14)
        output = tracker.format_dashboard(stats)
        assert "SHADOW REPORT" in output
        assert "Signals: 0" in output

    def test_format_contains_pnl(self, tracker):
        tracker.log_signal(
            match_id=1, home="CSK", away="MI", venue="Chepauk",
            signal_type="session", direction="YES", market="6_over",
            entry_line=45, model_expected=50, edge_runs=5,
            odds=1.9, ev_pct=8.0, confidence="HIGH", stake=100,
        )
        tracker.settle_match(1, {"6_over": 55})
        stats = tracker.get_dashboard(days=14)
        output = tracker.format_dashboard(stats)
        assert "P&L: Rs" in output


class TestMWSettlementEdgeCases:
    def test_khai_direction_win(self, tracker):
        tracker.log_signal(
            match_id=301, home="SRH", away="DC", venue="Hyderabad",
            signal_type="mw", direction="KHAI", market="match_winner",
            entry_line=0, model_expected=0.4, edge_runs=-0.05,
            odds=1.80, ev_pct=6.0, confidence="LOW", stake=500,
        )
        # SRH is the home/backed team for KHAI. KHAI wins if SRH loses.
        tracker.settle_match(301, {"match_winner": "DC"})
        row = tracker._conn.execute("SELECT * FROM signals WHERE id = 1").fetchone()
        assert row["result"] == "WIN"
        assert row["pnl"] == 500.0

    def test_khai_direction_loss(self, tracker):
        tracker.log_signal(
            match_id=301, home="SRH", away="DC", venue="Hyderabad",
            signal_type="mw", direction="KHAI", market="match_winner",
            entry_line=0, model_expected=0.4, edge_runs=-0.05,
            odds=1.80, ev_pct=6.0, confidence="LOW", stake=500,
        )
        tracker.settle_match(301, {"match_winner": "SRH"})
        row = tracker._conn.execute("SELECT * FROM signals WHERE id = 1").fetchone()
        assert row["result"] == "LOSS"
        assert row["pnl"] == -400.0  # -500 * (1.80 - 1)


class TestDatabasePersistence:
    def test_data_persists_across_instances(self, tmp_path):
        db_path = str(tmp_path / "persist.db")
        t1 = ShadowTracker(db_path=db_path)
        t1.log_signal(
            match_id=1, home="CSK", away="MI", venue="Chepauk",
            signal_type="session", direction="YES", market="6_over",
            entry_line=45, model_expected=50, edge_runs=5,
            odds=1.9, ev_pct=8.0, confidence="HIGH", stake=100,
        )
        t1.close()

        t2 = ShadowTracker(db_path=db_path)
        count = t2._conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        assert count == 1
        t2.close()
