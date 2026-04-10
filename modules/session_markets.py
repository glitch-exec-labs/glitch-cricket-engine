"""Shared helpers for cricket session-market labels and over cutoffs."""

from __future__ import annotations

from urllib.parse import parse_qs

SESSION_OVERS: dict[str, int] = {
    "powerplay_runs": 6,
    "6_over": 6,
    "10_over": 10,
    "12_over": 12,
    "15_over": 15,
    "20_over": 20,
}

SESSION_PREDICTION_KEYS: dict[str, str] = {
    "powerplay_runs": "powerplay_total",
    "6_over": "powerplay_total",
    "10_over": "ten_over_total",
    "15_over": "fifteen_over_total",
    "20_over": "innings_total",
}

SESSION_DISPLAY: dict[str, str] = {
    "total_runs": "Total Runs",
    "innings_total": "Innings Total",
    "match_winner": "Match Winner",
    "powerplay_runs": "6 Over Runs (Powerplay)",
    "6_over": "6 Over Runs (Powerplay)",
    "10_over": "10 Over Runs",
    "12_over": "12 Over Runs",
    "15_over": "15 Over Runs",
    "20_over": "Innings Total",
}


def session_market_key_from_to_over(to_over: int) -> str:
    """Convert a Cloudbet ``to_over`` value into our internal market key."""
    return f"{to_over}_over"


def session_target_over(market_key: str) -> int | None:
    """Return the session cutoff over for a market, if this is a session market."""
    return SESSION_OVERS.get(market_key)


def is_completed_session_market(market_key: str, overs_completed: float) -> bool:
    """Return True if a session market is already settled by the current over."""
    target = session_target_over(market_key)
    return target is not None and overs_completed > target


def market_display_name(market_key: str) -> str:
    """Return a user-facing label for a market key."""
    return SESSION_DISPLAY.get(market_key, market_key)


def session_market_from_url(market_url: str, market_key: str = "") -> tuple[str | None, int | None]:
    """Infer the internal session market key and target over from a Cloudbet URL."""
    if market_key in SESSION_OVERS:
        return market_key, SESSION_OVERS[market_key]

    query = market_url.split("?", 1)[1] if "?" in market_url else ""
    params = parse_qs(query)
    raw_to_over = params.get("to_over", [None])[0]
    try:
        to_over = int(float(raw_to_over)) if raw_to_over is not None else None
    except (TypeError, ValueError):
        to_over = None

    if to_over is None:
        return None, None

    return session_market_key_from_to_over(to_over), to_over
