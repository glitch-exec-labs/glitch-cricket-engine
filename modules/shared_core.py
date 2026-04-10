from __future__ import annotations

import os
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class StakingRecommendation:
    stake: float
    kelly_fraction: float
    recommended_fraction: float
    bankroll: float
    edge_percent: float
    decimal_odds: float
    market_multiplier: float = 1.0
    capped: bool = False
    min_stake_met: bool = True


@dataclass(frozen=True)
class SignalPayload:
    dedupe_key: str
    market_type: str
    selection: str
    line: float | None
    edge_pct: float
    stake_recommendation: StakingRecommendation
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = dict(self.metadata)
        payload.update(
            {
                "dedupe_key": self.dedupe_key,
                "market_type": self.market_type,
                "selection": self.selection,
                "line": self.line,
                "edge_pct": self.edge_pct,
                "stake_amount": self.stake_recommendation.stake,
                "stake_recommendation": asdict(self.stake_recommendation),
            }
        )
        return payload


@dataclass(frozen=True)
class ExecutionPayload:
    stake_currency: str
    stake_recommendation: StakingRecommendation
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = dict(self.metadata)
        payload.update(
            {
                "stake_amount": self.stake_recommendation.stake,
                "stake_currency": self.stake_currency,
                "stake_recommendation": asdict(self.stake_recommendation),
            }
        )
        return payload


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
    from glitch_betting_core.staking import kelly_fraction_from_edge, recommend_stake_from_edge
    from glitch_betting_core.types import ExecutionPayload, SignalPayload, StakingRecommendation
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

    def recommend_stake_from_edge(
        edge_percent: float,
        decimal_odds: float,
        bankroll: float,
        fraction: float = 1.0,
        market_multiplier: float = 1.0,
        max_bankroll_fraction: float | None = None,
        max_stake: float | None = None,
        min_stake: float = 0.0,
    ) -> StakingRecommendation:
        if bankroll <= 0.0:
            raise ValueError("bankroll must be positive")
        if market_multiplier <= 0.0:
            raise ValueError("market_multiplier must be positive")
        if min_stake < 0.0:
            raise ValueError("min_stake must be non-negative")
        if max_bankroll_fraction is not None and max_bankroll_fraction <= 0.0:
            raise ValueError("max_bankroll_fraction must be positive")
        if max_stake is not None and max_stake <= 0.0:
            raise ValueError("max_stake must be positive")

        base_kelly_fraction = kelly_fraction_from_edge(edge_percent, decimal_odds, fraction)
        stake = bankroll * base_kelly_fraction * market_multiplier
        capped = False

        if max_bankroll_fraction is not None:
            bankroll_cap = bankroll * max_bankroll_fraction
            if stake > bankroll_cap:
                stake = bankroll_cap
                capped = True

        if max_stake is not None and stake > max_stake:
            stake = max_stake
            capped = True

        min_stake_met = stake >= min_stake
        if not min_stake_met:
            stake = 0.0

        rounded_stake = round(stake, 2)
        recommended_fraction = rounded_stake / bankroll if bankroll > 0.0 else 0.0
        return StakingRecommendation(
            stake=rounded_stake,
            kelly_fraction=base_kelly_fraction,
            recommended_fraction=recommended_fraction,
            bankroll=bankroll,
            edge_percent=edge_percent,
            decimal_odds=decimal_odds,
            market_multiplier=market_multiplier,
            capped=capped,
            min_stake_met=min_stake_met,
        )
