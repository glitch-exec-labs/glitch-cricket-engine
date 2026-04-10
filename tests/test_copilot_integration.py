"""Integration test: simulate a mini-match through the copilot pipeline."""

import pytest
from modules.match_copilot import MatchCopilot
from modules.copilot_telegram import format_session_call, format_book_alert


def test_full_session_lifecycle():
    """Simulate: call YES 56, line moves to 64, book it, settle."""
    copilot = MatchCopilot(config={
        "copilot_enabled": True, "shadow_default_stake_inr": 300,
        "shadow_min_stake_inr": 200, "shadow_max_stake_inr": 1000,
        "hedge_min_profit_inr": 100, "hedge_session_min_runs": 4,
        "hedge_mw_min_odds_move": 0.20, "min_ev_pct": 5.0,
        "message_throttle_seconds": 0, "session_book_alerts": True,
    })
    # 1. Edge found (evaluate_session_calls adds positions internally)
    calls = copilot.evaluate_session_calls(
        match_id=1,
        model_predictions={"powerplay_total": {"expected": 62.0, "std_dev": 8.0}},
        estimated_lines={"6_over": {"yes": 55.0, "no": 56.0}},
        overs_completed=1.0)
    assert len(calls) == 1
    assert calls[0]["direction"] == "YES"

    # 2. Position was created internally
    open_sessions = copilot.position_book.get_open_sessions(1)
    assert len(open_sessions) == 1
    pos = open_sessions[0]
    assert pos.status == "OPEN"
    assert pos.entry_line == 56.0

    # 3. Line moves — book
    books = copilot.check_book_opportunities(1, {"6_over": {"yes": 63.0, "no": 64.0}}, {})
    assert len(books) == 1
    profit = copilot.position_book.book_session(pos, exit_line=64.0)
    assert profit == (64 - 56) * pos.stake_per_run

    # 4. Settle
    copilot.position_book.settle_session(pos, actual_total=68.0)
    assert pos.status == "SETTLED"
    assert pos.pnl == profit

    # 5. Messages format
    msg = format_session_call(market="6 Over", direction="YES", line=56.0, stake_per_run=pos.stake_per_run, model_prediction=62.0, home="RCB", away="SRH")
    assert "YES 56" in msg
    book_msg = format_book_alert(market_type="session", market_name="6 Over", action="Khai NO 64", guaranteed_profit=profit, math_breakdown=f"Rs {profit:.0f}")
    assert "BOOK" in book_msg


def test_full_mw_lifecycle():
    """Simulate: lagai SRH @ 2.30, odds shorten to 1.75, book it."""
    copilot = MatchCopilot(config={
        "copilot_enabled": True, "shadow_mw_default_stake_inr": 500,
        "shadow_min_stake_inr": 200, "shadow_max_stake_inr": 1000,
        "hedge_min_profit_inr": 100, "hedge_mw_min_odds_move": 0.20,
        "min_ev_pct": 5.0, "message_throttle_seconds": 0,
    })
    # evaluate_mw_call adds position internally
    mw = copilot.evaluate_mw_call(1, "RCB", "SRH", model_home_prob=0.50, current_home_odds=1.65, current_away_odds=2.30)
    assert mw is not None
    assert mw["team"] == "SRH"

    # Position was created internally
    open_mw = copilot.position_book.get_open_mw(1)
    assert len(open_mw) == 1
    pos = open_mw[0]

    books = copilot.check_book_opportunities(1, {}, {"SRH": 1.75})
    assert len(books) == 1
    copilot.position_book.book_mw(pos, exit_odds=1.75)
    assert pos.status == "BOOKED"
    copilot.position_book.settle_mw(pos, team_won=True)
    assert pos.pnl == pos.booked_profit
