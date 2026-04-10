"""
Edge Detector — compares model predictions against bookmaker lines
to identify mispriced markets.

Pure Python, no external dependencies.
"""

from __future__ import annotations

import math
from typing import Any, Optional


class EdgeDetector:
    """Identifies edges between model predictions and bookmaker lines."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        config = config or {}
        self.min_ev_pct: float = config.get("min_ev_pct", 5.0)
        self.min_ev_pct_mw: float = config.get("min_ev_pct_mw", self.min_ev_pct)
        self.min_edge_runs: float = config.get("min_edge_runs", 2.0)
        self.locked_markets: dict[tuple[int, int, str], str] = {}

    # ── public API ───────────────────────────────────────────────────────

    def evaluate_line(
        self,
        market: str,
        model_expected: float,
        model_std_dev: float,
        bookmaker_line: float,
        over_odds: float,
        under_odds: float,
    ) -> Optional[dict]:
        """Evaluate a runs-based line market for positive expected value.

        Returns a dict describing the edge, or *None* if the edge is too
        small or the EV% is below threshold.
        """
        edge_runs = model_expected - bookmaker_line

        if abs(edge_runs) < self.min_edge_runs:
            return None

        # P(X > line) where X ~ N(model_expected, model_std_dev)
        if model_std_dev <= 0:
            return None
        prob_over = 1.0 - self._normal_cdf(
            (bookmaker_line - model_expected) / model_std_dev
        )
        prob_under = 1.0 - prob_over

        if edge_runs > 0:
            direction = "OVER"
            probability = prob_over
            odds = over_odds
        else:
            direction = "UNDER"
            probability = prob_under
            odds = under_odds

        fair_odds = 1.0 / probability if probability > 0 else float("inf")
        ev_pct = ((odds / fair_odds) - 1.0) * 100.0

        if ev_pct < self.min_ev_pct:
            return None

        confidence = self._edge_confidence(edge_runs, model_std_dev, ev_pct)

        return {
            "market": market,
            "direction": direction,
            "bookmaker_line": bookmaker_line,
            "model_expected": model_expected,
            "edge_runs": edge_runs,
            "model_prob": round(probability, 4),
            "odds": odds,
            "fair_odds": round(fair_odds, 4),
            "ev_pct": round(ev_pct, 2),
            "confidence": confidence,
        }

    def evaluate_match_winner(
        self,
        model_win_prob: float,
        bookmaker_odds: float,
        team: str,
    ) -> Optional[dict]:
        """Evaluate a match-winner market for positive expected value."""
        if bookmaker_odds <= 0:
            return None
        implied_prob = 1.0 / bookmaker_odds
        ev_pct = (bookmaker_odds * model_win_prob - 1.0) * 100.0

        if ev_pct < self.min_ev_pct_mw:
            return None

        edge = model_win_prob - implied_prob
        confidence = self._winner_confidence(ev_pct, edge)

        return {
            "market": "match_winner",
            "team": team,
            "model_prob": round(model_win_prob, 4),
            "implied_prob": round(implied_prob, 4),
            "odds": bookmaker_odds,
            "ev_pct": round(ev_pct, 2),
            "edge": round(edge, 4),
            "confidence": confidence,
        }

    def lock_market(
        self,
        match_id: int,
        market_key: str,
        direction: str,
        innings: int,
    ) -> None:
        """Lock a market direction for the rest of the innings."""
        self.locked_markets[(match_id, innings, market_key)] = direction

    def get_locked_direction(
        self,
        match_id: int,
        market_key: str,
        innings: int,
    ) -> str | None:
        """Return the locked direction for a market/innings if present."""
        return self.locked_markets.get((match_id, innings, market_key))

    def is_market_locked(
        self,
        match_id: int,
        market_key: str,
        innings: int,
        proposed_direction: str | None = None,
    ) -> bool:
        """Return True if a market is locked for this innings."""
        locked_direction = self.get_locked_direction(match_id, market_key, innings)
        if locked_direction is None:
            return False
        if proposed_direction is None:
            return True
        return locked_direction != proposed_direction

    def clear_locks(self, match_id: int, innings: int | None = None) -> None:
        """Clear locks for a match, or for a specific innings only."""
        to_delete = []
        for key in self.locked_markets:
            key_match_id, key_innings, _market = key
            if key_match_id != match_id:
                continue
            if innings is not None and key_innings != innings:
                continue
            to_delete.append(key)
        for key in to_delete:
            del self.locked_markets[key]

    # ── confidence helpers ───────────────────────────────────────────────

    def _edge_confidence(
        self, edge_runs: float, std_dev: float, ev_pct: float
    ) -> str:
        """Classify confidence for a runs-based line edge."""
        z = abs(edge_runs) / std_dev if std_dev > 0 else 0.0
        if z > 0.5 and ev_pct > 12:
            return "HIGH"
        if z > 0.25 and ev_pct > 6:
            return "MEDIUM"
        return "LOW"

    @staticmethod
    def _winner_confidence(ev_pct: float, edge: float) -> str:
        """Classify confidence for a match-winner edge."""
        if ev_pct > 15 and edge > 0.10:
            return "HIGH"
        if ev_pct > 8 and edge > 0.05:
            return "MEDIUM"
        return "LOW"

    # ── math helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _normal_cdf(x: float) -> float:
        """Standard normal cumulative distribution function using math.erf."""
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
