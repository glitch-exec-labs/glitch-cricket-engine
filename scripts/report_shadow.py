#!/usr/bin/env python3
"""Shadow performance dashboard — report from shadow_ledger.db.

Run after match days to see how v2 signals would have performed.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

SHADOW_DB = ROOT / "data" / "shadow_ledger.db"


def main() -> None:
    if not SHADOW_DB.exists():
        print("No shadow_ledger.db found — no signals logged yet.")
        sys.exit(0)

    conn = sqlite3.connect(str(SHADOW_DB))
    conn.row_factory = sqlite3.Row

    total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    settled = conn.execute("SELECT COUNT(*) FROM signals WHERE result IS NOT NULL").fetchone()[0]
    pending = total - settled

    print("=" * 60)
    print("  SHADOW TRACKER DASHBOARD")
    print("=" * 60)
    print(f"\n  Total signals: {total}  (settled: {settled}, pending: {pending})")

    if settled == 0:
        print("\n  No settled signals yet — waiting for matches to complete.")
        conn.close()
        return

    rows = conn.execute(
        "SELECT * FROM signals WHERE result IS NOT NULL ORDER BY created_at"
    ).fetchall()

    wins = sum(1 for r in rows if r["result"] == "WIN")
    losses = sum(1 for r in rows if r["result"] == "LOSS")
    total_pnl = sum(float(r["pnl"] or 0) for r in rows)
    total_staked = sum(float(r["stake"] or 0) for r in rows)

    print(f"  Wins: {wins}  Losses: {losses}  Win%: {wins/settled*100:.1f}%")
    print(f"  Total PnL: {total_pnl:+.0f}  Staked: {total_staked:.0f}  ROI: {total_pnl/total_staked*100:+.1f}%" if total_staked else "")

    # By market
    print(f"\n  {'Market':<18} {'N':>4} {'W':>4} {'Win%':>6} {'PnL':>8}")
    print("  " + "-" * 44)
    by_market = defaultdict(list)
    for r in rows:
        by_market[r["market"]].append(r)
    for mkt in sorted(by_market.keys()):
        ss = by_market[mkt]
        n = len(ss)
        w = sum(1 for s in ss if s["result"] == "WIN")
        pnl = sum(float(s["pnl"] or 0) for s in ss)
        print(f"  {mkt:<18} {n:>4} {w:>4} {w/n*100:>5.1f}% {pnl:>+7.0f}")

    # By direction
    print(f"\n  {'Direction':<18} {'N':>4} {'W':>4} {'Win%':>6} {'PnL':>8}")
    print("  " + "-" * 44)
    by_dir = defaultdict(list)
    for r in rows:
        by_dir[r["direction"]].append(r)
    for d in sorted(by_dir.keys()):
        ss = by_dir[d]
        n = len(ss)
        w = sum(1 for s in ss if s["result"] == "WIN")
        pnl = sum(float(s["pnl"] or 0) for s in ss)
        print(f"  {d:<18} {n:>4} {w:>4} {w/n*100:>5.1f}% {pnl:>+7.0f}")

    # Model accuracy
    errors = []
    for r in rows:
        if r["model_expected"] and r["actual_value"]:
            errors.append(abs(float(r["model_expected"]) - float(r["actual_value"])))
    if errors:
        print(f"\n  Model accuracy: avg |expected - actual| = {sum(errors)/len(errors):.1f}")

    print("\n" + "=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
