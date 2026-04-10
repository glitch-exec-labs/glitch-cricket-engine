#!/usr/bin/env python3
"""
Test script to reproduce the EXACT contradiction bugs from the
Multan vs Islamabad match on 2026-03-28.

Bug 1: OVER 94.5 on 10_over placed at 16:28, then restart at 16:30 wiped
        open_bets from memory, allowing UNDER 84.5 on 10_over at 16:35.

Bug 2: MW bet on Multan placed at 16:42:45 was NOT blocked despite having
        UNDER 84.5 on 10_over already open (contradiction check only checks
        innings_total/20_over, not session markets like 10_over).

Bug 3: Locks are set BEFORE context veto, so a vetoed bet still locks the
        market direction, incorrectly blocking valid edges later.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from modules.match_state import MatchState
from modules.match_context import MatchContext

def test_bug1_restart_wipes_open_bets():
    """
    After restart, open_bets is empty. The contradiction checker
    sees no existing bets and lets the opposite direction through.
    """
    print("=" * 60)
    print("BUG 1: Restart wipes open_bets from memory")
    print("=" * 60)

    state = MatchState("Multan Sultans", "Islamabad United", "Gaddafi Stadium")
    state.current_innings = 2
    state.overs_completed = 5.2
    state.total_runs = 47
    state.wickets = 1

    ctx = MatchContext()

    # SCENARIO A: With the pre-restart OVER bet in open_bets
    open_bets_with_over = [
        {"market": "10_over", "direction": "OVER", "line": 94.5, "innings": 2},
    ]
    edge_under = {"market": "10_over", "direction": "UNDER", "ev_pct": 58.5}

    ok, reason = ctx.should_bet(edge_under, state, match_id=69477, open_bets=open_bets_with_over)
    print(f"\n  With OVER in open_bets, try UNDER:")
    print(f"    Result: {'ALLOWED' if ok else 'BLOCKED'} — {reason}")
    assert not ok, "UNDER should be BLOCKED when OVER exists!"
    print("    PASS: Correctly blocked")

    # SCENARIO B: After restart — open_bets is empty (the actual bug)
    open_bets_empty = []
    ok, reason = ctx.should_bet(edge_under, state, match_id=69477, open_bets=open_bets_empty)
    print(f"\n  After restart (empty open_bets), try UNDER:")
    print(f"    Result: {'ALLOWED' if ok else 'BLOCKED'} — {reason}")
    if ok:
        print("    *** BUG CONFIRMED: UNDER was ALLOWED because restart wiped open_bets ***")
    else:
        print("    Fixed: UNDER correctly blocked even after restart")


def test_bug2_mw_vs_session_under():
    """
    _check_contradictions Rule 2 only blocks MW when has_innings_under is True.
    But has_innings_under only checks innings_total and 20_over.
    A 10_over UNDER bet does NOT set has_innings_under, so MW is allowed.
    """
    print("\n" + "=" * 60)
    print("BUG 2: MW bet allowed despite UNDER on 10_over session")
    print("=" * 60)

    state = MatchState("Multan Sultans", "Islamabad United", "Gaddafi Stadium")
    state.current_innings = 2
    state.overs_completed = 7.2
    state.total_runs = 61
    state.wickets = 1

    ctx = MatchContext()

    # We have UNDER on 10_over already open
    open_bets = [
        {"market": "10_over", "direction": "UNDER", "line": 84.5, "innings": 2},
    ]

    # Now try to place MW bet backing Multan (the batting team)
    edge_mw = {"market": "match_winner", "direction": "Multan Sultans", "ev_pct": 25.6}

    ok, reason = ctx.should_bet(edge_mw, state, match_id=69477, open_bets=open_bets)
    print(f"\n  With 10_over UNDER open, try MW BACK Multan:")
    print(f"    Result: {'ALLOWED' if ok else 'BLOCKED'} — {reason}")
    if ok:
        print("    *** BUG CONFIRMED: MW was ALLOWED despite contradicting UNDER on Multan's session ***")
        print("    Root cause: Rule 2 only checks innings_total/20_over, not 10_over/15_over/6_over")
    else:
        print("    Fixed: MW correctly blocked")


def test_bug2_reverse_mw_then_under():
    """
    Rule 1 DOES catch MW -> UNDER on session. But only if the MW bet's
    team matches state.batting_team.
    """
    print("\n" + "=" * 60)
    print("BUG 2 reverse: UNDER blocked when MW already placed")
    print("=" * 60)

    state = MatchState("Multan Sultans", "Islamabad United", "Gaddafi Stadium")
    state.current_innings = 2
    state.overs_completed = 7.5
    state.total_runs = 62
    state.wickets = 1

    ctx = MatchContext()

    # MW bet backing Multan (the batting team) already placed
    open_bets = [
        {"market": "match_winner", "direction": "Multan Sultans", "innings": 2},
    ]

    # Try UNDER on 10_over (Multan's session)
    edge_under = {"market": "10_over", "direction": "UNDER", "ev_pct": 30.0}

    ok, reason = ctx.should_bet(edge_under, state, match_id=69477, open_bets=open_bets)
    print(f"\n  With MW Multan open, try UNDER on 10_over:")
    print(f"    Result: {'ALLOWED' if ok else 'BLOCKED'} — {reason}")
    if not ok:
        print("    PASS: Rule 1 works correctly (MW -> UNDER blocked)")
    else:
        print("    *** BUG: UNDER should be blocked when MW exists ***")


def test_bug3_lock_before_veto():
    """
    In _send_edge_alert, lock_market() is called at line 1691 BEFORE
    should_bet() context check at line 1732. If the bet is vetoed by context,
    the lock remains, incorrectly blocking future valid edges in the
    opposite direction.

    This is a code flow issue in spotter.py, not testable with MatchContext
    alone, but we document it here.
    """
    print("\n" + "=" * 60)
    print("BUG 3: Lock set BEFORE context veto")
    print("=" * 60)
    print("""
    In spotter.py _send_edge_alert():
      Line 1691: self.edge_detector.lock_market(match_id, market, direction, innings)
      Line 1732: context_ok, context_reason = self.match_context.should_bet(...)
      Line 1735: if not context_ok: return  # bet vetoed, BUT LOCK REMAINS

    Timeline from logs:
      16:30:52 - 10_over UNDER edge found, lock set to UNDER, then VETOED
      16:31:01 - 10_over OVER edge SKIPPED because "locked to UNDER"

    The OVER was a valid edge that was incorrectly blocked by a lock
    from a bet that was never placed.
    """)


def test_bug4_livbet_no_team_field():
    """
    LiveBet dataclass has no 'team' field. When open_bets_for_match is built
    (spotter.py line 1730), it does getattr(bet, "team", ""), which always
    returns "" because LiveBet has no team attribute.

    For MW bets, the team name is stored in bet.direction (e.g. "Multan Sultans").
    The contradiction checker DOES handle this via:
        bet_team = bet.get("team", "") or bet.get("direction", "")

    But this only works if 'direction' is actually the team name, not "OVER"/"UNDER".
    For MW bets placed by _send_edge_alert, direction IS set to team name (line 1679).
    So this part actually works, but the open_bets dict still has team="" which is
    misleading and could cause issues if anyone relies on it.
    """
    print("\n" + "=" * 60)
    print("BUG 4: LiveBet has no 'team' field (partial bug)")
    print("=" * 60)

    state = MatchState("Multan Sultans", "Islamabad United", "Gaddafi Stadium")
    state.current_innings = 2

    ctx = MatchContext()

    # Simulate how open_bets_for_match is built from LiveBet
    # For MW bets, bet.direction = "Multan Sultans" (set at line 1679)
    # But bet has no .team, so getattr returns ""
    open_bets = [
        {"market": "match_winner", "direction": "Multan Sultans", "team": "", "innings": 2},
    ]

    edge_under = {"market": "10_over", "direction": "UNDER", "ev_pct": 30.0}
    ok, reason = ctx.should_bet(edge_under, state, match_id=69477, open_bets=open_bets)
    print(f"\n  MW bet with team='', direction='Multan Sultans', try UNDER:")
    print(f"    Result: {'ALLOWED' if ok else 'BLOCKED'} — {reason}")
    if not ok:
        print("    PASS: Works because fallback to direction field")
    else:
        print("    *** BUG: Should be blocked ***")


def test_bug5_multi_executor_independent_open_bets():
    """
    MultiBetExecutor.has_open_bet checks ALL accounts, BUT
    MultiBetExecutor.open_bets (property) only returns primary's open_bets.

    When open_bets_for_match is built in spotter.py (line 1723), it iterates
    self.bet_executor.open_bets.values() — which is the PRIMARY's open_bets only.

    If a bet was placed on the client account but not the primary (e.g. primary
    was blocked by something), the contradiction check won't see it.
    """
    print("\n" + "=" * 60)
    print("BUG 5: open_bets_for_match only checks primary account")
    print("=" * 60)
    print("""
    spotter.py line 1722-1731:
        for bet in self.bet_executor.open_bets.values():
            # ^ This is MultiBetExecutor.open_bets which delegates to primary only

    multi_executor.py line 70-71:
        @property
        def open_bets(self) -> Dict[str, LiveBet]:
            return self.primary.open_bets  # <-- ONLY PRIMARY

    If client has a bet that primary doesn't (e.g. primary rejected, client accepted),
    the contradiction check won't see it.
    """)


if __name__ == "__main__":
    test_bug1_restart_wipes_open_bets()
    test_bug2_mw_vs_session_under()
    test_bug2_reverse_mw_then_under()
    test_bug3_lock_before_veto()
    test_bug4_livbet_no_team_field()
    test_bug5_multi_executor_independent_open_bets()

    print("\n" + "=" * 60)
    print("SUMMARY OF ALL GAPS")
    print("=" * 60)
    print("""
    BUG 1 (CRITICAL): open_bets are in-memory only. No state_store is
        passed to BetExecutor. Every restart wipes all knowledge of
        existing bets. This is the PRIMARY cause of contradictory bets.
        The OVER 94.5 at 16:28 was forgotten after restart at 16:30,
        allowing UNDER 84.5 at 16:35.

    BUG 2 (HIGH): _check_contradictions Rule 2 only checks innings_total
        and 20_over for the "UNDER on innings -> don't back MW" rule.
        Session markets (6_over, 10_over, 15_over) are ignored. So you
        can have UNDER on 10_over AND MW BACK batting team simultaneously.

    BUG 3 (MEDIUM): lock_market() is called BEFORE should_bet() context
        check. A vetoed bet still locks the direction, incorrectly
        blocking valid edges in the opposite direction later.

    BUG 4 (LOW): LiveBet has no 'team' field, so open_bets_for_match
        always has team="". Works by accident because direction contains
        the team name for MW bets, but fragile.

    BUG 5 (MEDIUM): open_bets_for_match iterates primary.open_bets only,
        not all accounts. If only client has a bet, contradiction check
        misses it.
    """)
