"""Map Sportmonks full player names to the abbreviated names stored in StatsDB."""

from __future__ import annotations

from difflib import SequenceMatcher
from typing import Optional

from modules.stats_db import StatsDB


class NameMatcher:
    """Fuzzy name matcher for batting and bowling player lookups."""

    # Manual overrides for known problem names that fuzzy matching can't resolve.
    # Maps normalised ESPN/Sportmonks name → DB name.
    _MANUAL_ALIASES: dict[str, str] = {
        "wanindu hasaranga": "PVD Hasaranga",
        "hasaranga de silva": "PVD Hasaranga",
        "pvd hasaranga": "PVD Hasaranga",
        "shahnawaz dahani": "Shahnawaz Dhani",
        "anrich nortje": "A Nortje",
        "lungi ngidi": "L Ngidi",
        "alzarri joseph": "AS Joseph",
        "dewald brevis": "D Brevis",
        "gerald coetzee": "G Coetzee",
        "tristan stubbs": "T Stubbs",
        "marco jansen": "M Jansen",
        "fazalhaq farooqi": "Fazalhaq Farooqi",
        "rashid khan": "Rashid Khan",
        "mujeeb ur rahman": "Mujeeb Ur Rahman",
        "rahmanullah gurbaz": "Rahmanullah Gurbaz",
        "jake fraser-mcgurk": "J Fraser-McGurk",
        "phil salt": "PD Salt",
        "will jacks": "WG Jacks",
        "sam curran": "SM Curran",
        "liam livingstone": "LS Livingstone",
        "mitchell starc": "MA Starc",
        "rinku singh": "Rinku Singh",
        "yashasvi jaiswal": "YBK Jaiswal",
        "sanju samson": "SV Samson",
        "rishabh pant": "RR Pant",
    }

    def __init__(self, stats_db: StatsDB):
        self.stats_db = stats_db
        self._batting_counts = stats_db.get_player_name_counts("batting")
        self._bowling_counts = stats_db.get_player_name_counts("bowling")
        self._batting_names = set(self._batting_counts)
        self._bowling_names = set(self._bowling_counts)
        self._cache: dict[tuple[str, str], Optional[str]] = {}

    def match_batsman(self, sportmonks_name: str) -> Optional[str]:
        """Convert a Sportmonks batsman name to the DB representation."""
        return self._match("batting", sportmonks_name, self._batting_names, self._batting_counts)

    def match_bowler(self, sportmonks_name: str) -> Optional[str]:
        """Convert a Sportmonks bowler name to the DB representation."""
        return self._match("bowling", sportmonks_name, self._bowling_names, self._bowling_counts)

    def _match(
        self,
        kind: str,
        sportmonks_name: str,
        candidates: set[str],
        counts: dict[str, int],
    ) -> Optional[str]:
        key = (kind, self._normalise_name(sportmonks_name))
        if key in self._cache:
            return self._cache[key]

        result = self._fuzzy_match(sportmonks_name, candidates, counts)
        self._cache[key] = result
        return result

    def _fuzzy_match(
        self,
        full_name: str,
        candidates: set[str],
        counts: dict[str, int],
    ) -> Optional[str]:
        normalised = self._normalise_name(full_name)
        if not normalised:
            return None

        # Check manual aliases first
        alias = self._MANUAL_ALIASES.get(normalised)
        if alias and alias in candidates:
            return alias

        exact = next(
            (candidate for candidate in candidates if self._normalise_name(candidate) == normalised),
            None,
        )
        if exact:
            return exact

        initial_form = self._initial_form(full_name)
        exact_initial = None
        if initial_form:
            exact_initial = next(
                (
                    candidate
                    for candidate in candidates
                    if self._normalise_name(candidate) == self._normalise_name(initial_form)
                ),
                None,
            )

        surname_family = self._surname_family(full_name, candidates)
        if surname_family:
            initial_matches = [
                candidate
                for candidate in surname_family
                if self._candidate_initials(candidate).startswith(self._first_initial(full_name))
            ]
            if len(initial_matches) == 1:
                candidate = initial_matches[0]
                if self._is_plausible_given_name_match(full_name, candidate):
                    return candidate
                return None
            if len(initial_matches) > 1:
                best = max(
                    initial_matches,
                    key=lambda candidate: (
                        counts.get(candidate, 0),
                        self._similarity(full_name, candidate),
                    ),
                )
                if self._is_plausible_given_name_match(full_name, best):
                    return best
                return None
            if exact_initial and self._is_plausible_given_name_match(full_name, exact_initial):
                return exact_initial
            if len(surname_family) == 1 and self._is_plausible_given_name_match(full_name, surname_family[0]):
                return surname_family[0]

        if exact_initial:
            return exact_initial

        fallback_candidates = candidates
        tokens = self._tokens(full_name)
        if len(tokens) >= 2:
            surname = tokens[-1]
            surname_matches = {
                candidate
                for candidate in candidates
                if self._tokens(candidate) and self._tokens(candidate)[-1] == surname
            }
            if surname_matches:
                fallback_candidates = surname_matches
            else:
                return None

        close = sorted(
            fallback_candidates,
            key=lambda candidate: (
                self._similarity(full_name, candidate),
                counts.get(candidate, 0),
            ),
            reverse=True,
        )
        if close:
            best = close[0]
            if self._similarity(full_name, best) >= 0.55:
                return best

        return None

    @staticmethod
    def _normalise_name(name: str) -> str:
        return " ".join(str(name or "").replace(".", " ").split()).strip().lower()

    @classmethod
    def _tokens(cls, name: str) -> list[str]:
        return cls._normalise_name(name).split()

    @classmethod
    def _first_initial(cls, name: str) -> str:
        tokens = cls._tokens(name)
        return tokens[0][:1] if tokens else ""

    @classmethod
    def _initial_form(cls, full_name: str) -> str:
        tokens = cls._tokens(full_name)
        if len(tokens) < 2:
            return ""
        return f"{tokens[0][0].upper()} {' '.join(token.title() for token in tokens[1:])}"

    @classmethod
    def _candidate_initials(cls, candidate: str) -> str:
        tokens = cls._tokens(candidate)
        if len(tokens) < 2:
            return tokens[0][:1] if tokens else ""
        return tokens[0]

    @classmethod
    def _is_plausible_given_name_match(cls, full_name: str, candidate: str) -> bool:
        full_tokens = cls._tokens(full_name)
        candidate_tokens = cls._tokens(candidate)
        if len(full_tokens) < 2 or len(candidate_tokens) < 2:
            return True

        if full_tokens[-1] != candidate_tokens[-1]:
            return False

        full_first = full_tokens[0]
        cand_first = candidate_tokens[0]
        if not full_first or not cand_first:
            return False
        if full_first[0] != cand_first[0]:
            return False

        # Accept abbreviated DB forms like "RG Sharma" when initial agrees.
        if len(cand_first) <= 2 or len(full_first) <= 1:
            return True

        return SequenceMatcher(None, full_first, cand_first).ratio() >= 0.72

    @classmethod
    def _surname_family(cls, full_name: str, candidates: set[str]) -> list[str]:
        tokens = cls._tokens(full_name)
        if len(tokens) < 2:
            return []
        surname = tokens[-1]
        family = [
            candidate
            for candidate in candidates
            if cls._tokens(candidate) and cls._tokens(candidate)[-1] == surname
        ]
        return family

    @classmethod
    def _similarity(cls, full_name: str, candidate: str) -> float:
        return SequenceMatcher(
            None,
            cls._normalise_name(full_name),
            cls._normalise_name(candidate),
        ).ratio()
