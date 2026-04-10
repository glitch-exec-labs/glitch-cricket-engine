"""Series registry — maps competition identifiers to SeriesProfile instances.

Supports Cloudbet competition paths, Sportmonks league slugs, config file
competition names, and short aliases.  Adding a new series requires only:
  1. Create series/<name>.py with a SeriesProfile instance.
  2. Register it here with _register(<profile>, *keys).
"""

from __future__ import annotations

import logging
from typing import Optional

from series.profile import SeriesProfile
from series.ipl import IPL_PROFILE
from series.psl import PSL_PROFILE
from series.default import DEFAULT_PROFILE

logger = logging.getLogger("ipl_spotter.series")

_REGISTRY: dict[str, SeriesProfile] = {}


def _register(profile: SeriesProfile, *keys: str) -> None:
    """Register a profile under one or more lookup keys (case-insensitive)."""
    for k in keys:
        _REGISTRY[k.lower().strip()] = profile


# ── IPL ───────────────────────────────────────────────────────────────
_register(
    IPL_PROFILE,
    "ipl",
    "indian-premier-league",
    "cricket-india-indian-premier-league",       # Cloudbet competition path
)

# ── PSL ───────────────────────────────────────────────────────────────
_register(
    PSL_PROFILE,
    "psl",
    "pakistan-super-league",
    "cricket-pakistan-pakistan-super-league",
)

# To add BBL later:
# from series.bbl import BBL_PROFILE
# _register(BBL_PROFILE, "bbl", "big-bash-league",
#           "cricket-australia-big-bash-league")


def get_profile(
    competition: Optional[str] = None,
    cloudbet_competition_id: Optional[str] = None,
    sportmonks_league_id: Optional[int] = None,
) -> SeriesProfile:
    """Resolve a SeriesProfile from any available identifier.

    Tries each key in order; returns DEFAULT_PROFILE if nothing matches.
    """
    for raw_key in (competition, cloudbet_competition_id, str(sportmonks_league_id or "")):
        if raw_key:
            profile = _REGISTRY.get(raw_key.lower().strip())
            if profile:
                return profile

    logger.warning(
        "No series profile for competition=%s cloudbet=%s sportmonks=%s — using DEFAULT",
        competition, cloudbet_competition_id, sportmonks_league_id,
    )
    return DEFAULT_PROFILE
