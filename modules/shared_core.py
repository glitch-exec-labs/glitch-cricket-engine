from __future__ import annotations

import os
import sys
from pathlib import Path


def _candidate_core_paths() -> list[Path]:
    candidates: list[Path] = []
    env_value = os.getenv("GLITCH_BETTING_CORE_SRC")
    if env_value:
        candidates.append(Path(env_value).expanduser())
    candidates.append(Path(__file__).resolve().parents[2] / "glitch-betting-core" / "src")
    return candidates


for candidate in _candidate_core_paths():
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))


try:
    from glitch_betting_core.odds import decimal_to_probability
    from glitch_betting_core.staking import kelly_fraction_from_edge
except ImportError:
    def decimal_to_probability(decimal_odds: float) -> float:
        if decimal_odds <= 1.0:
            raise ValueError("decimal_odds must be greater than 1.0")
        return 1.0 / decimal_odds

    def kelly_fraction_from_edge(edge_percent: float, decimal_odds: float, fraction: float = 1.0) -> float:
        if decimal_odds <= 1.0:
            raise ValueError("decimal_odds must be greater than 1.0")
        if fraction <= 0.0:
            raise ValueError("fraction must be positive")
        edge = edge_percent / 100.0
        return max(0.0, (edge / (decimal_odds - 1.0)) * fraction)
