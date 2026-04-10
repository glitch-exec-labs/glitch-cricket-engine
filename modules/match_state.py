"""
Match State Engine - tracks ball-by-ball state of a T20 innings.

Pure Python, no external dependencies.
"""

from __future__ import annotations

from typing import Any


class MatchState:
    """Tracks the evolving state of a single T20 innings."""

    def __init__(self, batting_team: str, bowling_team: str, venue: str) -> None:
        self.batting_team = batting_team
        self.bowling_team = bowling_team
        self.venue = venue
        self.current_innings: int = 1
        self.target_runs: int | None = None
        self.active_batsmen: list[dict[str, Any]] = []
        self.active_bowler: dict[str, Any] | None = None
        self.batting_card: list[dict[str, Any]] = []
        self.bowling_card: list[dict[str, Any]] = []

        # Core counters
        self.total_runs: int = 0
        self.wickets: int = 0
        self.balls_faced: int = 0  # legal deliveries only
        self.extras_total: int = 0

        # Over tracking
        self.overs_completed: float = 0.0
        self.current_over: int = 0
        self.current_ball: int = 0

        # Phase breakdowns
        self.phase_runs: dict[str, int] = {
            "powerplay": 0,
            "middle": 0,
            "death": 0,
        }
        self.phase_wickets: dict[str, int] = {
            "powerplay": 0,
            "middle": 0,
            "death": 0,
        }

        # Per-over runs
        self.over_runs: dict[int, int] = {}

        # Full ball log
        self.balls: list[dict[str, Any]] = []

    @property
    def phase(self) -> str:
        """Return the current match phase based on overs completed."""
        return self._detect_phase(self.overs_completed)

    @property
    def current_run_rate(self) -> float:
        """Runs per over so far. Returns 0.0 if no legal balls faced."""
        if self.balls_faced == 0:
            return 0.0
        return self.total_runs / (self.balls_faced / 6.0)

    @property
    def over_by_over(self) -> dict[int, int]:
        """Backward-compatible alias for over-level scoring."""
        return dict(self.over_runs)

    @property
    def ball_by_ball(self) -> list[dict[str, Any]]:
        """Backward-compatible alias for the normalised ball log."""
        return list(self.balls)

    def add_ball(self, ball: dict[str, Any]) -> None:
        """Process a single ball event and update all tracking fields."""
        over_num: int = ball["over"]
        ball_num: int = ball["ball"]
        runs: int = ball["runs"]
        is_wicket: bool = ball.get("is_wicket", False)
        extras: int = ball.get("extras", 0)
        is_legal: bool = ball.get("is_legal", True)

        # Update runs
        self.total_runs += runs
        self.extras_total += extras

        # Over-level tracking
        self.current_over = over_num
        self.current_ball = ball_num
        self.over_runs.setdefault(over_num, 0)
        self.over_runs[over_num] += runs

        # Legal-delivery counters
        if is_legal:
            self.balls_faced += 1
            self.overs_completed = self.balls_faced / 6.0

        # Phase tracking
        phase = self._detect_phase(over_num)
        self.phase_runs[phase] += runs

        if is_wicket:
            self.wickets += 1
            self.phase_wickets[phase] += 1

        self.balls.append(ball)

    def get_phase_runs(self, phase: str) -> int:
        """Return accumulated runs for the given phase."""
        return self.phase_runs.get(phase, 0)

    def score_at_end_of_over(self, target_over: int) -> int | None:
        """Return the batting total at the end of a completed over milestone."""
        if target_over <= 0:
            return 0

        if self.overs_completed < target_over:
            return None

        if not self.over_runs:
            return self.total_runs

        return sum(
            runs for over_index, runs in self.over_runs.items()
            if over_index < target_over
        )

    def projected_innings_total(self, venue_avg: float = 170.0) -> float:
        """Phase-aware innings projection blended with venue average."""
        crr = self.current_run_rate
        overs = self.overs_completed
        progress = overs / 20.0

        if overs == 0:
            return venue_avg

        wicket_factor = max(0.5, 1.0 - self.wickets * 0.06)
        phase = self.phase

        if phase == "powerplay":
            remaining_pp_overs = max(0.0, 6.0 - overs)
            future_runs = (
                crr * remaining_pp_overs
                + 0.85 * crr * 9
                + 1.30 * crr * 5
            )
        elif phase == "middle":
            remaining_mid_overs = max(0.0, 15.0 - overs)
            future_runs = crr * remaining_mid_overs + 1.40 * crr * 5
        else:
            remaining_overs = max(0.0, 20.0 - overs)
            future_runs = crr * remaining_overs

        projected = self.total_runs + future_runs * wicket_factor
        blended = projected * progress + venue_avg * (1.0 - progress)
        return max(float(self.total_runs), blended)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the full match state to a plain dict."""
        return {
            "batting_team": self.batting_team,
            "bowling_team": self.bowling_team,
            "venue": self.venue,
            "current_innings": self.current_innings,
            "target_runs": self.target_runs,
            "total_runs": self.total_runs,
            "wickets": self.wickets,
            "balls_faced": self.balls_faced,
            "extras_total": self.extras_total,
            "overs_completed": self.overs_completed,
            "current_over": self.current_over,
            "current_ball": self.current_ball,
            "phase": self.phase,
            "current_run_rate": self.current_run_rate,
            "phase_runs": dict(self.phase_runs),
            "phase_wickets": dict(self.phase_wickets),
            "over_runs": dict(self.over_runs),
            "balls": list(self.balls),
            "active_batsmen": list(self.active_batsmen),
            "active_bowler": dict(self.active_bowler) if self.active_bowler else None,
            "batting_card": list(self.batting_card),
            "bowling_card": list(self.bowling_card),
        }

    @classmethod
    def from_sportmonks(cls, match: dict[str, Any]) -> "MatchState":
        """Build current-innings state from a Sportmonks live fixture payload."""
        localteam = match.get("localteam") or {}
        visitorteam = match.get("visitorteam") or {}
        localteam_id = match.get("localteam_id")
        visitorteam_id = match.get("visitorteam_id")

        runs_data = cls._normalise_runs_data(match.get("runs"))
        balls_data = match.get("balls") or []
        if isinstance(balls_data, dict):
            balls_data = balls_data.get("data", [])
        if not isinstance(balls_data, list):
            balls_data = []

        current_innings = cls._detect_current_innings(balls_data, runs_data)
        current_team_id = cls._detect_current_team_id(balls_data, runs_data, current_innings)

        batting_team = cls._team_name_for_id(
            current_team_id, localteam_id, localteam, visitorteam_id, visitorteam
        )
        bowling_team = cls._opponent_name_for_id(
            current_team_id, localteam_id, localteam, visitorteam_id, visitorteam
        )

        venue_data = match.get("venue") or {}
        if isinstance(venue_data, dict) and "data" in venue_data:
            venue_data = venue_data["data"]
        venue = venue_data.get("name", "Unknown") if isinstance(venue_data, dict) else "Unknown"

        state = cls(batting_team=batting_team, bowling_team=bowling_team, venue=venue)
        state.current_innings = current_innings
        state.batting_card = cls._build_batting_card(match.get("batting"), current_innings)
        state.bowling_card = cls._build_bowling_card(match.get("bowling"), current_innings)
        # Active batsmen: striker (active=True) first, then non-striker
        # Sportmonks only marks striker as active=True
        # Non-striker: last entry in batting card that is NOT the striker and NOT dismissed
        # Dismissed batsmen have balls > 0 and score can be 0, but they appear earlier in the card
        # The non-striker is typically the last non-active entry (most recently came in)
        striker = [e for e in state.batting_card if e.get("active")]
        wickets = state.wickets
        total_batsmen = len(state.batting_card)
        # If N batsmen in card and W wickets, the last (N - W) should be not-out
        # But to be safe, take the last non-active entry only if there are more batsmen than wickets
        not_out_candidates = [e for e in state.batting_card if not e.get("active")]
        if not_out_candidates and total_batsmen > wickets + 1:
            non_striker = not_out_candidates[-1:]
        else:
            non_striker = []
        state.active_batsmen = (striker + non_striker)[:2]
        active_bowlers = [
            entry for entry in state.bowling_card
            if entry.get("active")
        ]
        if active_bowlers:
            state.active_bowler = active_bowlers[0]
        elif state.bowling_card:
            state.active_bowler = state.bowling_card[0]

        first_innings = next((r for r in runs_data if r.get("inning") == 1), None)
        if first_innings and first_innings.get("score") is not None:
            try:
                state.target_runs = int(first_innings["score"]) + 1
            except (TypeError, ValueError):
                state.target_runs = None

        for raw_ball in balls_data:
            innings = cls._parse_scoreboard(raw_ball.get("scoreboard"))
            if innings != current_innings:
                continue

            over_num, ball_num = cls._parse_ball_number(raw_ball.get("ball"))
            score = raw_ball.get("score") or {}
            if isinstance(score, dict) and "data" in score:
                score = score["data"]
            if not isinstance(score, dict):
                score = {}

            extras = 0
            for extra_key in ("bye", "leg_bye", "noball", "noball_runs"):
                try:
                    extras += int(score.get(extra_key, 0) or 0)
                except (TypeError, ValueError):
                    continue

            runs = score.get("runs", 0) or 0
            try:
                runs = int(runs)
            except (TypeError, ValueError):
                runs = 0

            is_wicket = bool(
                raw_ball.get("batsmanout_id")
                or score.get("is_wicket")
                or score.get("out")
            )

            state.add_ball({
                "over": over_num,
                "ball": ball_num if ball_num > 0 else 1,
                "runs": runs,
                "is_wicket": is_wicket,
                "extras": extras,
                "is_legal": not bool(score.get("noball_runs") or score.get("wide_runs")),
                "source_id": raw_ball.get("id"),
                "innings": current_innings,
                "team_id": raw_ball.get("team_id"),
            })

        summary = next((r for r in runs_data if r.get("inning") == current_innings), None)
        if summary is None and runs_data:
            summary = runs_data[-1]

        if summary:
            try:
                state.total_runs = int(summary.get("score", state.total_runs) or state.total_runs)
            except (TypeError, ValueError):
                pass
            try:
                state.wickets = int(summary.get("wickets", state.wickets) or state.wickets)
            except (TypeError, ValueError):
                pass
            try:
                overs_float = float(summary.get("overs", state.overs_completed) or state.overs_completed)
                state.overs_completed = overs_float
                completed_overs = int(overs_float)
                current_ball = int(round((overs_float - completed_overs) * 10))
                state.current_over = completed_overs
                state.current_ball = current_ball
                state.balls_faced = completed_overs * 6 + current_ball
            except (TypeError, ValueError):
                pass

        return state

    @classmethod
    def _build_batting_card(
        cls,
        batting_data: Any,
        current_innings: int,
    ) -> list[dict[str, Any]]:
        card: list[dict[str, Any]] = []
        for entry in cls._normalise_card_data(batting_data):
            if cls._parse_scoreboard(entry.get("scoreboard")) != current_innings:
                continue

            batsman = entry.get("batsman") or {}
            if isinstance(batsman, dict) and "data" in batsman:
                batsman = batsman["data"]

            card.append({
                "name": (
                    batsman.get("fullname")
                    or batsman.get("name")
                    or entry.get("fullname")
                    or entry.get("player")
                    or ""
                ),
                "score": cls._safe_int(entry.get("score")),
                "balls": cls._safe_int(entry.get("ball")),
                "sr": cls._safe_float(entry.get("rate")),
                "rate": cls._safe_float(entry.get("rate")),
                "active": bool(entry.get("active")),
                "scoreboard": entry.get("scoreboard"),
            })
        return card

    @classmethod
    def _build_bowling_card(
        cls,
        bowling_data: Any,
        current_innings: int,
    ) -> list[dict[str, Any]]:
        card: list[dict[str, Any]] = []
        for entry in cls._normalise_card_data(bowling_data):
            if cls._parse_scoreboard(entry.get("scoreboard")) != current_innings:
                continue

            bowler = entry.get("bowler") or {}
            if isinstance(bowler, dict) and "data" in bowler:
                bowler = bowler["data"]

            card.append({
                "name": (
                    bowler.get("fullname")
                    or bowler.get("name")
                    or entry.get("fullname")
                    or entry.get("player")
                    or ""
                ),
                "overs": cls._safe_float(entry.get("overs")),
                "runs": cls._safe_int(entry.get("runs")),
                "wickets": cls._safe_int(entry.get("wickets")),
                "econ": cls._safe_float(entry.get("rate")),
                "rate": cls._safe_float(entry.get("rate")),
                "active": bool(entry.get("active")),
                "scoreboard": entry.get("scoreboard"),
            })
        return card

    @staticmethod
    def _detect_phase(overs: float) -> str:
        """Classify an over value into a match phase."""
        if overs < 6:
            return "powerplay"
        if overs < 15:
            return "middle"
        return "death"

    @staticmethod
    def _normalise_runs_data(runs_data: Any) -> list[dict[str, Any]]:
        if runs_data is None:
            return []
        if isinstance(runs_data, dict):
            runs_data = runs_data.get("data", [])
        if not isinstance(runs_data, list):
            return []
        return [run for run in runs_data if isinstance(run, dict)]

    @staticmethod
    def _parse_scoreboard(scoreboard: Any) -> int:
        if scoreboard is None:
            return 1
        scoreboard_str = str(scoreboard).strip().upper()
        if scoreboard_str.startswith("S") and scoreboard_str[1:].isdigit():
            return int(scoreboard_str[1:])
        try:
            return int(float(scoreboard_str))
        except (TypeError, ValueError):
            return 1

    @classmethod
    def _detect_current_innings(
        cls, balls_data: list[dict[str, Any]], runs_data: list[dict[str, Any]]
    ) -> int:
        if balls_data:
            return cls._parse_scoreboard(balls_data[-1].get("scoreboard"))
        innings_numbers = [cls._parse_scoreboard(r.get("inning")) for r in runs_data]
        return max(innings_numbers) if innings_numbers else 1

    @classmethod
    def _detect_current_team_id(
        cls,
        balls_data: list[dict[str, Any]],
        runs_data: list[dict[str, Any]],
        current_innings: int,
    ) -> Any:
        for raw_ball in reversed(balls_data):
            if cls._parse_scoreboard(raw_ball.get("scoreboard")) == current_innings:
                team_id = raw_ball.get("team_id")
                if team_id is not None:
                    return team_id

        for summary in runs_data:
            if cls._parse_scoreboard(summary.get("inning")) == current_innings:
                team_id = summary.get("team_id")
                if team_id is not None:
                    return team_id

        return None

    @staticmethod
    def _team_name(team_data: Any) -> str:
        if isinstance(team_data, dict) and "data" in team_data:
            team_data = team_data["data"]
        if isinstance(team_data, dict):
            return team_data.get("name", "?")
        return "?"

    @classmethod
    def _team_name_for_id(
        cls,
        team_id: Any,
        localteam_id: Any,
        localteam: Any,
        visitorteam_id: Any,
        visitorteam: Any,
    ) -> str:
        if team_id == localteam_id:
            return cls._team_name(localteam)
        if team_id == visitorteam_id:
            return cls._team_name(visitorteam)
        return cls._team_name(localteam)

    @classmethod
    def _opponent_name_for_id(
        cls,
        team_id: Any,
        localteam_id: Any,
        localteam: Any,
        visitorteam_id: Any,
        visitorteam: Any,
    ) -> str:
        if team_id == localteam_id:
            return cls._team_name(visitorteam)
        if team_id == visitorteam_id:
            return cls._team_name(localteam)
        return cls._team_name(visitorteam)

    @staticmethod
    def _parse_ball_number(ball_value: Any) -> tuple[int, int]:
        try:
            over_ball = float(ball_value)
            over_num = int(over_ball)
            ball_num = int(round((over_ball - over_num) * 10))
            return over_num, ball_num if ball_num > 0 else 1
        except (TypeError, ValueError):
            return 0, 1

    @staticmethod
    def _normalise_card_data(card_data: Any) -> list[dict[str, Any]]:
        if isinstance(card_data, dict):
            card_data = card_data.get("data", [])
        if not isinstance(card_data, list):
            return []
        return [entry for entry in card_data if isinstance(entry, dict)]

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
