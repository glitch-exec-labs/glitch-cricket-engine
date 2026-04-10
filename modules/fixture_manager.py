"""Fixture Manager — schedule-aware match lifecycle for premium channels.

Handles:
  - Fixture fetching from Sportmonks (using existing cricket_client methods)
  - Daily schedule messages to IPL/PSL Telegram channels
  - Pre-toss ground reports (venue stats + team H2H)
  - Post-toss playing XI reports (player stats from DB + ESPN)
  - Scan window gating (only poll live feed when a match is near)
  - Toss detection by polling the fixture for toss_won_team_id
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

try:
    from modules.cricdata_client import CricDataClient
except ImportError:
    CricDataClient = None

try:
    from modules.news_intel import NewsIntel
except ImportError:
    NewsIntel = None

try:
    from modules.weather_client import WeatherClient
except ImportError:
    WeatherClient = None

logger = logging.getLogger("ipl_spotter.fixture_mgr")

# IST = UTC+5:30, PKT = UTC+5:00
_TZ_OFFSETS = {"ipl": timedelta(hours=5, minutes=30), "psl": timedelta(hours=5)}
_TZ_NAMES = {"ipl": "IST", "psl": "PKT"}


class FixtureManager:
    """Schedule-driven pre-match lifecycle manager."""

    def __init__(
        self,
        config: dict,
        cricket_client: Any,
        odds_client: Any,
        telegram: Any,
        state_store: Any,
        stats_db: Any = None,
        series_dbs: Optional[Dict[str, Any]] = None,
        espn_client: Any = None,
        match_dossier: Any = None,
    ) -> None:
        self.config = config
        self.cricket = cricket_client
        self.odds = odds_client
        self.telegram = telegram
        self.store = state_store
        self.stats_db = stats_db
        self.series_dbs = series_dbs or {}
        self.espn = espn_client
        self.dossier = match_dossier
        self._cricdata = CricDataClient(config) if CricDataClient is not None else None
        self._weather = WeatherClient(config) if WeatherClient is not None else None
        self._news_intel = None
        if NewsIntel is not None and config.get("brave_api_key"):
            try:
                self._news_intel = NewsIntel(config)
            except Exception:
                logger.debug("NewsIntel unavailable for fixture manager", exc_info=True)

        self.competitions: List[str] = config.get("competitions", ["ipl"])
        self._refresh_hours: int = config.get("fixture_refresh_hours", 6)
        self._alert_minutes_before: int = config.get("fixture_alert_minutes_before", 90)
        self._toss_watch_minutes: int = config.get("fixture_toss_watch_minutes", 15)
        self._scan_window_minutes: int = config.get("fixture_scan_window_minutes", 30)
        self._daily_hour_utc: int = config.get("fixture_daily_schedule_hour_utc", 3)

        # In-memory fixture cache: competition → list of fixture dicts
        self._fixtures: Dict[str, List[dict]] = {}
        # Alert tracking: fixture_id → set of sent alert types
        self._alerts_sent: Dict[int, set] = {}

        # Timing
        self._last_refresh: float = 0.0
        self._last_daily_sent: Dict[str, str] = {}  # comp → date string
        self._scan_interval = config.get("scan_interval_seconds", 8)
        self._initialized = False

        # Restore cached fixtures from state_store
        self._restore_from_store()

    def _restore_from_store(self) -> None:
        """Load cached fixtures from state_store on startup."""
        if not self.store:
            return
        for comp in self.competitions:
            try:
                cached = self.store.load_state(f"fixtures_{comp}")
                if cached and isinstance(cached, list):
                    self._fixtures[comp] = cached
                    logger.info("Restored %d %s fixtures from state_store", len(cached), comp.upper())
            except Exception:
                pass
        alerts = self.store.load_state("fixture_alerts_sent")
        if alerts and isinstance(alerts, dict):
            self._alerts_sent = {self._restore_alert_key(k): set(v) for k, v in alerts.items()}

    def _persist_fixtures(self, comp: str) -> None:
        if self.store:
            try:
                self.store.save_state(f"fixtures_{comp}", self._fixtures.get(comp, []))
            except Exception:
                pass

    def _persist_alerts(self) -> None:
        if self.store:
            try:
                serializable = {str(k): list(v) for k, v in self._alerts_sent.items()}
                self.store.save_state("fixture_alerts_sent", serializable)
            except Exception:
                pass

    # ── Main tick (called every scan) ─────────────────────────────────

    def tick(self, scan_count: int) -> None:
        """Called every scan cycle.  Manages all fixture timing internally."""
        now = time.time()

        # First tick: always refresh
        if not self._initialized:
            self._initialized = True
            self._do_refresh()
            self._check_daily_schedule()

        # Periodic refresh
        refresh_interval = self._refresh_hours * 3600
        if now - self._last_refresh > refresh_interval:
            self._do_refresh()

        # Daily schedule check (once per day per competition)
        self._check_daily_schedule()

        # Check for approaching matches (every scan — lightweight)
        self._check_approaching_matches()

        # Check for toss (every scan when in toss-watch window)
        self._check_toss()

    # ── Scan window ───────────────────────────────────────────────────

    def is_match_window(self) -> bool:
        """Return True if any match is within the scan window or currently live.

        The spotter should skip live-feed polling when this returns False.
        """
        now_utc = datetime.now(timezone.utc)
        window = timedelta(minutes=self._scan_window_minutes)

        for comp, fixtures in self._fixtures.items():
            for f in fixtures:
                start = self._parse_start_time(f)
                if start is None:
                    continue
                status = (f.get("status") or "").lower()
                # Match is live
                if status in ("live", "1st innings", "2nd innings", "innings break"):
                    return True
                # Match is within the window
                if start - window <= now_utc <= start + timedelta(hours=5):
                    return True

        return False

    # ── Fixture refresh ───────────────────────────────────────────────

    def _do_refresh(self) -> None:
        """Fetch fixtures from Sportmonks, with CricData as backup."""
        self._last_refresh = time.time()
        for comp in self.competitions:
            try:
                parsed: List[dict] = []
                source = "sportmonks"
                season_id = self.cricket.get_season_id(comp)
                if season_id:
                    fixtures_raw = self.cricket.get_fixtures(season_id, comp)
                    if fixtures_raw:
                        parsed = self._parse_fixtures(fixtures_raw, comp)
                if not parsed:
                    parsed = self._refresh_from_cricdata(comp)
                    source = "cricdata"
                if not parsed:
                    logger.warning("No fixtures available for %s from Sportmonks or CricData", comp)
                    continue

                self._fixtures[comp] = parsed
                self._persist_fixtures(comp)
                logger.info("Refreshed %d fixtures for %s via %s", len(parsed), comp.upper(), source)
            except Exception:
                logger.exception("Failed to refresh %s fixtures", comp)

    def _parse_fixtures(self, raw_fixtures: List[dict], competition: str) -> List[dict]:
        """Parse Sportmonks fixture data into our format."""
        results = []
        now_utc = datetime.now(timezone.utc)

        for f in raw_fixtures:
            start_str = f.get("starting_at") or f.get("datetime") or ""
            start = self._parse_iso(start_str)

            # Only keep future fixtures and those from today
            if start and start < now_utc - timedelta(hours=6):
                continue

            # Extract team names
            home = ""
            away = ""
            venue = ""
            lt = f.get("localteam", {})
            vt = f.get("visitorteam", {})
            if isinstance(lt, dict):
                home = lt.get("data", lt).get("name", "") if isinstance(lt.get("data"), dict) else lt.get("name", "")
            if isinstance(vt, dict):
                away = vt.get("data", vt).get("name", "") if isinstance(vt.get("data"), dict) else vt.get("name", "")
            venue_data = f.get("venue", {})
            if isinstance(venue_data, dict):
                venue_inner = venue_data.get("data", venue_data)
                if isinstance(venue_inner, dict):
                    venue = venue_inner.get("name", "")

            # Compute local display time
            tz_offset = _TZ_OFFSETS.get(competition, timedelta(hours=5, minutes=30))
            tz_name = _TZ_NAMES.get(competition, "IST")
            local_time = ""
            if start:
                local_dt = start + tz_offset
                local_time = local_dt.strftime(f"%I:%M %p {tz_name} (%d %b)")

            results.append({
                "fixture_id": f.get("id"),
                "competition": competition,
                "home": home,
                "away": away,
                "venue": venue,
                "start_time": start_str,
                "start_time_local": local_time,
                "status": f.get("status", ""),
                "round": f.get("round", ""),
                "toss_won_team_id": f.get("toss_won_team_id"),
                "elected": f.get("elected"),
            })

        results.sort(key=lambda x: x.get("start_time", ""))
        return results

    def _refresh_from_cricdata(self, competition: str) -> List[dict]:
        if not self._cricdata or not getattr(self._cricdata, "enabled", False):
            return []
        raw_matches = self._cricdata.get_series_matches(competition)
        if not raw_matches:
            return []
        return self._parse_cricdata_fixtures(raw_matches, competition)

    def _parse_cricdata_fixtures(self, raw_fixtures: List[dict], competition: str) -> List[dict]:
        results = []
        now_utc = datetime.now(timezone.utc)

        for f in raw_fixtures:
            start_str = f.get("dateTimeGMT") or f.get("date") or ""
            start = self._parse_iso(start_str)
            if start and start < now_utc - timedelta(hours=6):
                continue

            teams = f.get("teams") or []
            if len(teams) < 2:
                team_info = f.get("teamInfo") or []
                if isinstance(team_info, list):
                    teams = [
                        str(team.get("name") or team.get("shortname") or "")
                        for team in team_info[:2]
                        if isinstance(team, dict)
                    ]
            if len(teams) < 2:
                continue

            venue = str(f.get("venue") or "")
            tz_offset = _TZ_OFFSETS.get(competition, timedelta(hours=5, minutes=30))
            tz_name = _TZ_NAMES.get(competition, "IST")
            local_time = ""
            if start:
                local_dt = start + tz_offset
                local_time = local_dt.strftime(f"%I:%M %p {tz_name} (%d %b)")

            results.append({
                "fixture_id": str(f.get("id") or f.get("matchId") or f"{competition}_{len(results) + 1}"),
                "competition": competition,
                "home": teams[0],
                "away": teams[1],
                "venue": venue,
                "start_time": start_str,
                "start_time_local": local_time,
                "status": f.get("status", ""),
                "round": f.get("matchType", ""),
                "cricdata_match_id": str(f.get("id") or ""),
            })

        results.sort(key=lambda x: x.get("start_time", ""))
        return results

    def _resolve_cricdata_match_id(self, fixture: dict, competition: str) -> Optional[str]:
        match_id = fixture.get("cricdata_match_id")
        if match_id:
            return str(match_id)
        if not self._cricdata or not getattr(self._cricdata, "enabled", False):
            return None

        match = self._cricdata.find_match(
            fixture.get("home", ""),
            fixture.get("away", ""),
            competition=competition,
            start_time=fixture.get("start_time"),
            venue=fixture.get("venue", ""),
        )
        if not match or not match.get("id"):
            return None

        fixture["cricdata_match_id"] = str(match.get("id"))
        if match.get("venue") and not fixture.get("venue"):
            fixture["venue"] = match.get("venue")
        self._persist_fixtures(competition)
        return fixture["cricdata_match_id"]

    def _fetch_sportmonks_fixture(self, fixture_id: Any) -> Optional[dict]:
        try:
            fixture_id = int(fixture_id)
        except (TypeError, ValueError):
            return None

        resp = self.cricket._request(
            f"/fixtures/{fixture_id}",
            params={"include": "localteam,visitorteam,batting.batsman,bowling.bowler,lineup"},
        )
        data = self.cricket._extract_data(resp)
        return data if isinstance(data, dict) else None

    def _extract_lineup_players(self, toss_info: dict, team: str) -> List[dict]:
        lineup = toss_info.get("lineup") or []
        teams = toss_info.get("teams") or {}
        results = []

        for player in lineup:
            if not isinstance(player, dict):
                continue

            team_name = ""
            team_id = player.get("lineupteam_id") or player.get("team_id")
            if team_id in teams:
                team_name = teams.get(team_id, "")
            if not team_name:
                for key in ("team", "lineupteam", "country"):
                    data = player.get(key, {})
                    if isinstance(data, dict):
                        inner = data.get("data", data)
                        if isinstance(inner, dict) and inner.get("name"):
                            team_name = inner.get("name", "")
                            break
            if team_name and not self._team_name_match(team, team_name):
                continue

            name = player.get("fullname") or player.get("name")
            if not name:
                pdata = player.get("player", {})
                if isinstance(pdata, dict):
                    name = pdata.get("fullname") or pdata.get("name") or pdata.get("lastname")
            if not name:
                continue

            results.append({
                "name": name,
                "captain": bool(player.get("captain") or player.get("is_captain")),
                "keeper": bool(player.get("wicketkeeper") or player.get("is_wicketkeeper")),
                "role": player.get("position", ""),
            })

        return results

    def _find_cricdata_squad(self, squads: List[dict], team_name: str) -> Optional[dict]:
        for squad in squads or []:
            squad_name = squad.get("teamName") or squad.get("shortname") or ""
            if self._team_name_match(team_name, squad_name):
                return squad
        return None

    def _lookup_cricdata_player(self, players: List[dict], name: str) -> Optional[dict]:
        target = self._normalise_name(name)
        if not target:
            return None

        for player in players or []:
            candidate = self._normalise_name(player.get("name", ""))
            if candidate == target:
                return player

        target_tokens = set(target.split())
        for player in players or []:
            candidate = self._normalise_name(player.get("name", ""))
            cand_tokens = set(candidate.split())
            if target_tokens and cand_tokens and target_tokens & cand_tokens:
                return player
        return None

    def _build_player_stat_parts(self, name: str, competition: str) -> List[str]:
        stat_parts = []
        if self.stats_db:
            try:
                bs = self.stats_db.get_player_batting_stats(name)
                if bs and bs.get("innings", 0) >= 3:
                    stat_parts.append(f"SR {bs.get('avg_strike_rate', 0):.0f}")
            except Exception:
                pass
            try:
                bw = self.stats_db.get_bowler_stats(name)
                if bw and bw.get("innings", 0) >= 3:
                    stat_parts.append(f"econ {bw.get('avg_economy', 0):.1f}")
            except Exception:
                pass

        sdb = self.series_dbs.get(competition)
        if sdb:
            try:
                ss = sdb.get_player_series_stats(name)
                if "batting" in ss and ss["batting"]["innings"] >= 2:
                    batting = ss["batting"]
                    stat_parts.append(f"series: {batting['runs']}r @{batting['sr']:.0f}")
                if "bowling" in ss and ss["bowling"]["innings"] >= 2:
                    bowling = ss["bowling"]
                    stat_parts.append(f"{bowling['wickets']}w @{bowling['economy']:.1f}")
            except Exception:
                pass
        return stat_parts

    def _fetch_weather_pitch(self, home: str, away: str, competition: str, venue: str) -> str:
        parts = []
        if self._weather:
            weather_line = self._weather.format_weather_line(venue)
            if weather_line:
                parts.append(weather_line)

        if self._news_intel:
            try:
                intel = self._news_intel.get_pre_match_intel(home, away, competition)
                pitch_items = intel.get("pitch_report", []) if isinstance(intel, dict) else []
                weather_items = intel.get("weather", []) if isinstance(intel, dict) else []
                if pitch_items:
                    snippet = pitch_items[0].split(" — ")[0] if " — " in pitch_items[0] else pitch_items[0]
                    parts.append(f"🏟️ Pitch: {snippet[:120]}")
                elif weather_items and not parts:
                    snippet = weather_items[0].split(" — ")[0] if " — " in weather_items[0] else weather_items[0]
                    parts.append(f"⛅ Weather: {snippet[:120]}")
            except Exception:
                logger.debug("Failed to fetch pre-match intel for ground report", exc_info=True)

        return "\n".join(parts).strip()

    @staticmethod
    def _normalise_name(value: str) -> str:
        return " ".join(str(value or "").replace(".", " ").lower().split()).strip()

    @classmethod
    def _team_name_match(cls, left: str, right: str) -> bool:
        left_tokens = [token for token in cls._normalise_name(left).split() if len(token) > 2]
        right_tokens = [token for token in cls._normalise_name(right).split() if len(token) > 2]
        if not left_tokens or not right_tokens:
            return False
        if set(left_tokens) & set(right_tokens):
            return True
        return left_tokens[-1] == right_tokens[-1]

    @staticmethod
    def _restore_alert_key(value: Any) -> Any:
        text = str(value)
        return int(text) if text.isdigit() else text

    # ── Daily schedule ────────────────────────────────────────────────

    def _check_daily_schedule(self) -> None:
        now_utc = datetime.now(timezone.utc)
        today = now_utc.strftime("%Y-%m-%d")

        for comp in self.competitions:
            # Only send once per day per competition
            if self._last_daily_sent.get(comp) == today:
                continue
            # Send around the configured hour (with 30 min grace)
            if now_utc.hour != self._daily_hour_utc:
                continue

            fixtures = self._fixtures.get(comp, [])
            if not fixtures:
                continue

            msg = self._format_daily_schedule(fixtures, comp)
            if msg:
                self.telegram.send_alert_sync(msg, channel=comp, is_signal=False)
                self._last_daily_sent[comp] = today
                logger.info("Sent daily %s schedule", comp.upper())

    def _format_daily_schedule(self, fixtures: List[dict], competition: str) -> str:
        """Format today + tomorrow fixtures into a premium schedule message."""
        now_utc = datetime.now(timezone.utc)
        tz_offset = _TZ_OFFSETS.get(competition, timedelta(hours=5, minutes=30))
        local_now = now_utc + tz_offset
        today_str = local_now.strftime("%Y-%m-%d")
        tomorrow_str = (local_now + timedelta(days=1)).strftime("%Y-%m-%d")

        today_matches = []
        tomorrow_matches = []

        for f in fixtures:
            start = self._parse_start_time(f)
            if start is None:
                continue
            local_dt = start + tz_offset
            date_str = local_dt.strftime("%Y-%m-%d")
            if date_str == today_str:
                today_matches.append(f)
            elif date_str == tomorrow_str:
                tomorrow_matches.append(f)

        if not today_matches and not tomorrow_matches:
            return ""

        lines = [f"MATCH SCHEDULE — {competition.upper()} 2026\n"]

        if today_matches:
            lines.append(f"Today, {local_now.strftime('%d %b')}:")
            for f in today_matches:
                rnd = f.get("round", "")
                prefix = f"  Match {rnd}: " if rnd else "  "
                lines.append(f"{prefix}{f['home']} vs {f['away']}")
                if f.get("venue"):
                    lines.append(f"  Venue: {f['venue']}")
                lines.append(f"  Start: {f.get('start_time_local', 'TBD')}")
                lines.append("")

        if tomorrow_matches:
            tomorrow_dt = local_now + timedelta(days=1)
            lines.append(f"Tomorrow, {tomorrow_dt.strftime('%d %b')}:")
            for f in tomorrow_matches:
                rnd = f.get("round", "")
                prefix = f"  Match {rnd}: " if rnd else "  "
                lines.append(f"{prefix}{f['home']} vs {f['away']}")
                if f.get("venue"):
                    lines.append(f"  Venue: {f['venue']}")
                lines.append(f"  Start: {f.get('start_time_local', 'TBD')}")
                lines.append("")

        return "\n".join(lines).strip()

    # ── Pre-match approaching alert ───────────────────────────────────

    def _check_approaching_matches(self) -> None:
        """Send ground report for matches approaching within the alert window."""
        now_utc = datetime.now(timezone.utc)
        alert_window = timedelta(minutes=self._alert_minutes_before)

        for comp, fixtures in self._fixtures.items():
            for f in fixtures:
                fid = f.get("fixture_id")
                if not fid:
                    continue
                alerts = self._alerts_sent.setdefault(fid, set())
                if "ground_report" in alerts:
                    continue

                start = self._parse_start_time(f)
                if start is None:
                    continue

                time_to_start = start - now_utc
                if timedelta(0) < time_to_start <= alert_window:
                    self._send_ground_report(f, comp)
                    alerts.add("ground_report")
                    self._persist_alerts()

    def _send_ground_report(self, fixture: dict, competition: str) -> None:
        """Send premium pre-toss ground report to the competition channel."""
        from modules.copilot_telegram import team_tag, team_emoji, STAD, TIME

        home = fixture.get("home", "")
        away = fixture.get("away", "")
        venue = fixture.get("venue", "")
        home_tag = team_tag(home)
        away_tag = team_tag(away)

        lines = [
            f"{STAD} GROUND REPORT — {venue}",
            "",
        ]

        # Venue stats from historical DB
        has_venue_stats = False
        if self.stats_db:
            try:
                vs = self.stats_db.get_venue_stats(venue)
                if vs and vs.get("matches", 0) > 0:
                    has_venue_stats = True
                    lines.append(f"📊 Venue Stats ({vs['matches']} matches):")
                    avg1 = vs.get("avg_first_innings")
                    avg2 = vs.get("avg_second_innings")
                    if avg1:
                        lines.append(f"  Avg 1st innings: {avg1:.0f}")
                    if avg2:
                        lines.append(f"  Avg 2nd innings: {avg2:.0f}")
                    pp = vs.get("avg_powerplay_1st")
                    if pp:
                        lines.append(f"  Avg powerplay: {pp:.0f}")
                    lines.append("")
            except Exception:
                logger.debug("Failed to fetch venue stats for ground report", exc_info=True)

        if not has_venue_stats:
            lines.append("📊 New venue — no historical data yet")
            lines.append("")

        # Series-specific stats if available
        sdb = self.series_dbs.get(competition)
        if sdb:
            try:
                svs = sdb.get_venue_series_stats(venue)
                if svs.get("matches", 0) > 0:
                    lines.append(f"📈 This Season at {venue} ({svs['matches']} matches):")
                    lines.append(f"  Avg 1st: {svs['avg_inn1']:.0f}  |  Avg 2nd: {svs['avg_inn2']:.0f}")
                    lines.append("")

                h2h = sdb.get_head_to_head(home, away)
                if h2h.get("matches", 0) > 0:
                    lines.append(f"🏏 Season H2H ({h2h['matches']} matches):")
                    lines.append(f"  {home_tag}: {h2h.get(f'{home}_wins', 0)} wins")
                    lines.append(f"  {away_tag}: {h2h.get(f'{away}_wins', 0)} wins")
                    lines.append("")

                # Team form
                form_lines = []
                for team in (home, away):
                    form = sdb.get_team_form(team, last_n=5)
                    if form.get("matches", 0) > 0:
                        results_str = "".join(form.get("last_results", []))
                        form_lines.append(f"  {team_tag(team)}: {form['wins']}W {form['losses']}L [{results_str}]")
                if form_lines:
                    lines.append("🔥 Season Form:")
                    lines.extend(form_lines)
                    lines.append("")
            except Exception:
                logger.debug("Failed to fetch series stats for ground report", exc_info=True)

        # Historical H2H from stats DB
        if self.stats_db:
            try:
                h2h = self.stats_db.get_head_to_head(home, away, limit=10)
                if h2h.get("matches", 0) > 0:
                    last_str = "".join(h2h.get("last_results", [])[:5])
                    lines.append(f"🏏 Head to Head (last {h2h['matches']} matches):")
                    lines.append(f"  {home_tag}: {h2h['team1_wins']} wins (avg {h2h['team1_avg_score']:.0f})")
                    lines.append(f"  {away_tag}: {h2h['team2_wins']} wins (avg {h2h['team2_avg_score']:.0f})")
                    lines.append(f"  Recent: [{last_str}] ({home.split()[0]} perspective)")
                    lines.append("")
            except Exception:
                logger.debug("Failed to fetch H2H for ground report", exc_info=True)

        weather_pitch = self._fetch_weather_pitch(home, away, competition, venue)
        if weather_pitch:
            lines.append(weather_pitch)
            lines.append("")

        lines.append(f"{home_tag}")
        lines.append(f"  🆚")
        lines.append(f"{away_tag}")
        lines.append(f"")
        lines.append(f"{TIME} {fixture.get('start_time_local', 'TBD')}")
        lines.append("")
        lines.append("━" * 30)
        lines.append("🪙 Toss & Playing XI report at match time")
        lines.append("📊 Live signals during the match")
        lines.append("━" * 30)

        msg = "\n".join(lines).strip()
        self.telegram.send_alert_sync(msg, channel=competition, is_signal=False)
        logger.info("Sent ground report: %s vs %s at %s", home, away, venue)

    # ── Toss detection ────────────────────────────────────────────────

    def _check_toss(self) -> None:
        """Poll for toss info on matches in the toss-watch window."""
        now_utc = datetime.now(timezone.utc)
        toss_window = timedelta(minutes=self._toss_watch_minutes)

        for comp, fixtures in self._fixtures.items():
            for f in fixtures:
                fid = f.get("fixture_id")
                if not fid:
                    continue
                alerts = self._alerts_sent.setdefault(fid, set())
                if "toss_report" in alerts:
                    continue

                start = self._parse_start_time(f)
                if start is None:
                    continue

                time_to_start = start - now_utc
                # In toss window: from 15 min before to 30 min after scheduled start
                if -timedelta(minutes=30) <= time_to_start <= toss_window:
                    toss_info = self._poll_toss(f, comp)
                    if toss_info:
                        self._send_playing_xi_report(f, toss_info, comp)
                        alerts.add("toss_report")
                        self._persist_alerts()

    def _poll_toss(self, fixture: dict, competition: str) -> Optional[dict]:
        """Fetch toss info, preferring CricData for speed and Sportmonks for lineup."""
        fixture_id = fixture.get("fixture_id")
        cricdata_match_id = self._resolve_cricdata_match_id(fixture, competition)

        if cricdata_match_id and self._cricdata:
            try:
                info = self._cricdata.get_match_info(cricdata_match_id)
                if info:
                    toss_winner = info.get("tossWinner") or info.get("tosswinner") or ""
                    toss_decision = info.get("tossChoice") or info.get("tosschoice") or ""
                    if toss_winner and toss_decision:
                        sportmonks_data = self._fetch_sportmonks_fixture(fixture_id)
                        teams = {}
                        lineup = []
                        if sportmonks_data:
                            for key in ("localteam", "visitorteam"):
                                team_data = sportmonks_data.get(key, {})
                                if isinstance(team_data, dict):
                                    inner = team_data.get("data", team_data)
                                    if isinstance(inner, dict) and inner.get("id"):
                                        teams[int(inner.get("id"))] = inner.get("name", "")
                            lineup = sportmonks_data.get("lineup", {})
                            if isinstance(lineup, dict):
                                lineup = lineup.get("data", [])
                        return {
                            "toss_winner": toss_winner,
                            "toss_decision": toss_decision,
                            "teams": teams,
                            "lineup": lineup if isinstance(lineup, list) else [],
                            "cricdata_match_id": cricdata_match_id,
                            "cricdata_squad": self._cricdata.get_match_squad(cricdata_match_id),
                            "raw": info,
                        }
            except Exception:
                logger.debug("CricData toss poll failed for %s", cricdata_match_id, exc_info=True)

        try:
            data = self._fetch_sportmonks_fixture(fixture_id)
            if not isinstance(data, dict):
                return None

            toss_winner_id = data.get("toss_won_team_id")
            elected = data.get("elected")
            if not toss_winner_id or not elected:
                return None

            teams = {}
            for key in ("localteam", "visitorteam"):
                team_data = data.get(key, {})
                if isinstance(team_data, dict):
                    inner = team_data.get("data", team_data)
                    if isinstance(inner, dict):
                        tid = inner.get("id")
                        tname = inner.get("name", "")
                        if tid:
                            teams[int(tid)] = tname

            toss_winner = teams.get(int(toss_winner_id), "Unknown")
            lineup = data.get("lineup", {})
            if isinstance(lineup, dict):
                lineup = lineup.get("data", [])

            return {
                "toss_winner": toss_winner,
                "toss_decision": elected,
                "teams": teams,
                "lineup": lineup if isinstance(lineup, list) else [],
                "cricdata_match_id": cricdata_match_id,
                "cricdata_squad": self._cricdata.get_match_squad(cricdata_match_id) if cricdata_match_id and self._cricdata else [],
                "raw": data,
            }
        except Exception:
            logger.debug("Toss poll for fixture %s — no toss yet", fixture_id)
            return None

    def _send_playing_xi_report(self, fixture: dict, toss_info: dict, competition: str) -> None:
        """Send post-toss playing XI report with player stats."""
        home = fixture.get("home", "")
        away = fixture.get("away", "")
        toss_winner = toss_info.get("toss_winner", "")
        decision = toss_info.get("toss_decision", "")
        decision_upper = str(decision or "").upper()

        lines = [f"TOSS — {toss_winner} won, elected to {decision_upper}\n"]

        if str(decision).lower() in ("batting", "bat"):
            batting_team = toss_winner
            bowling_team = away if self._team_name_match(toss_winner, home) else home
        else:
            bowling_team = toss_winner
            batting_team = away if self._team_name_match(toss_winner, home) else home

        squads = {}
        if self.espn:
            try:
                squads = self.espn.get_squads_for_match(home, away, competition) or {}
            except Exception:
                logger.debug("ESPN squads not available")

        cricdata_squads = toss_info.get("cricdata_squad") or []

        for label, team in [
            (f"{batting_team} (batting first)", batting_team),
            (f"{bowling_team} (bowling first)", bowling_team),
        ]:
            lines.append(f"\n{label}:")

            squad_data = None
            for side in ("home", "away"):
                side_data = squads.get(side, {})
                if side_data and self._team_name_match(team, side_data.get("team", "")):
                    squad_data = side_data
                    break

            players = list(squad_data.get("players", [])) if squad_data else []
            if not players:
                players = self._extract_lineup_players(toss_info, team)
                cricdata_team = self._find_cricdata_squad(cricdata_squads, team)
                cricdata_players = cricdata_team.get("players", []) if cricdata_team else []
                for player in players:
                    cricdata_player = self._lookup_cricdata_player(cricdata_players, player.get("name", ""))
                    if cricdata_player:
                        player.setdefault("role", cricdata_player.get("role", ""))
                        player.setdefault("batting_style", cricdata_player.get("battingStyle", ""))
                        player.setdefault("bowling_style", cricdata_player.get("bowlingStyle", ""))

            if players:
                for player in players:
                    name = player.get("name", "Unknown")
                    tags = ""
                    if player.get("captain"):
                        tags += " (C)"
                    if player.get("keeper"):
                        tags += " (WK)"
                    role = player.get("role_abbr", "") or player.get("role", "")
                    style_bits = []
                    if player.get("batting_style"):
                        style_bits.append(player.get("batting_style"))
                    if player.get("bowling_style"):
                        style_bits.append(player.get("bowling_style"))
                    stat_parts = self._build_player_stat_parts(name, competition)
                    extras = []
                    if role:
                        extras.append(f"[{role}]")
                    if style_bits:
                        extras.append(" / ".join(style_bits))
                    if stat_parts:
                        extras.append(", ".join(stat_parts))
                    extra_str = f" | {' | '.join(extras)}" if extras else ""
                    lines.append(f"  {name}{tags}{extra_str}")
            else:
                lines.append("  (Playing XI not yet available)")

        msg = "\n".join(lines).strip()
        self.telegram.send_alert_sync(msg, channel=competition, is_signal=False)
        logger.info("Sent playing XI report: %s vs %s (toss: %s %s)", home, away, toss_winner, decision)

    # ── Helpers ────────────────────────────────────────────────────────

    def _parse_start_time(self, fixture: dict) -> Optional[datetime]:
        """Parse the start_time field into a UTC datetime."""
        raw = fixture.get("start_time", "")
        return self._parse_iso(raw)

    @staticmethod
    def _parse_iso(raw: str) -> Optional[datetime]:
        """Parse an ISO 8601 string to a UTC datetime."""
        if not raw:
            return None
        try:
            # Sportmonks uses "2026-03-29T14:00:00.000000Z" format
            raw = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            return None

    def get_fixtures(self, competition: str) -> List[dict]:
        """Return cached fixtures for a competition."""
        return self._fixtures.get(competition, [])

    def get_todays_matches(self, competition: str) -> List[dict]:
        """Return today's fixtures for a competition."""
        now_utc = datetime.now(timezone.utc)
        tz_offset = _TZ_OFFSETS.get(competition, timedelta(hours=5, minutes=30))
        today = (now_utc + tz_offset).strftime("%Y-%m-%d")

        results = []
        for f in self._fixtures.get(competition, []):
            start = self._parse_start_time(f)
            if start:
                local_date = (start + tz_offset).strftime("%Y-%m-%d")
                if local_date == today:
                    results.append(f)
        return results
