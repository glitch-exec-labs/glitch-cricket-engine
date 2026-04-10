"""
IPL Edge Spotter — LIVE AUTOMATED BETTING BOT.

Architecture:
  - Sportmonks: live ball-by-ball every 15s
  - Our Model: predicts fair values from live match state
  - The Odds API: consensus match winner from 27 bookmakers
  - Cloudbet: live odds for fancy markets + AUTOMATIC BET EXECUTION
  - Speed Edge: detects trigger events and bets before odds adjust

Fully automated — detects edges, calculates Kelly stake, places bets on Cloudbet.
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from config import load_config
from modules.cricket_client import CricketClient
from modules.edge_detector import EdgeDetector
from modules.match_state import MatchState
from modules.predictor import IPLPredictor
from modules.session_markets import (
    SESSION_PREDICTION_KEYS,
    is_completed_session_market,
    market_display_name,
)
from modules.stats_db import StatsDB
from modules.state_store import StateStore
from modules.telegram_bot import (
    TelegramNotifier,
    format_edge_alert,
    format_pre_match_report,
)

try:
    from modules.odds_client import OddsClient
except ImportError:
    OddsClient = None

try:
    from modules.odds_tracker import OddsTracker
except ImportError:
    OddsTracker = None

try:
    from modules.theodds_client import TheOddsClient
except ImportError:
    TheOddsClient = None

try:
    from modules.speed_edge import SpeedEdge
except ImportError:
    SpeedEdge = None

try:
    from modules.bet_executor import BetExecutor
    from modules.multi_executor import MultiBetExecutor
except ImportError:
    BetExecutor = None
    MultiBetExecutor = None

try:
    from modules.risk_manager import RiskManager
except ImportError:
    RiskManager = None

try:
    from modules.news_intel import NewsIntel
except ImportError:
    NewsIntel = None

try:
    from modules.match_context import MatchContext
except ImportError:
    MatchContext = None

try:
    from modules.match_dossier import MatchDossier
except ImportError:
    MatchDossier = None

try:
    from modules.psl_context import PSLContext
except ImportError:
    PSLContext = None

try:
    from modules.llm_intel import LLMIntel
except ImportError:
    LLMIntel = None

try:
    from modules.odds_logger import OddsLogger
except ImportError:
    OddsLogger = None

try:
    from modules.match_recorder import MatchRecorder
except ImportError:
    MatchRecorder = None

try:
    from modules.shadow_tracker import ShadowTracker
except ImportError:
    ShadowTracker = None

try:
    from modules.paper_simulator import PaperSimulator
except ImportError:
    PaperSimulator = None

try:
    from modules.chase_state import ChaseStateMachine
except ImportError:
    ChaseStateMachine = None

try:
    from modules.espn_client import ESPNClient
except ImportError:
    ESPNClient = None

try:
    from modules.ml_collector import MLCollector
except ImportError:
    MLCollector = None

try:
    from modules.ml_predictor import MLPredictor
except ImportError:
    MLPredictor = None


try:
    from modules.match_copilot import MatchCopilot
except ImportError:
    MatchCopilot = None

try:
    from modules.liveline_listener import LiveLineListener
except ImportError:
    LiveLineListener = None

try:
    from series import get_profile, SeriesProfile
except ImportError:
    get_profile = None
    SeriesProfile = None

try:
    from modules.smart_staking import SmartStakingEngine
except ImportError:
    SmartStakingEngine = None

try:
    from modules.live_bet_tracker import LiveBetTracker
except ImportError:
    LiveBetTracker = None

try:
    from modules.fixture_manager import FixtureManager
except ImportError:
    FixtureManager = None

try:
    from modules.cricdata_client import CricDataClient
except ImportError:
    CricDataClient = None

try:
    from modules.series_db import SeriesDB
except ImportError:
    SeriesDB = None

try:
    from modules.copilot_telegram import (
        format_session_call as fmt_session_call,
        format_session_bundle as fmt_session_bundle,
        format_mw_call as fmt_mw_call,
        format_over_update as fmt_over_update,
        format_book_alert as fmt_book_alert,
        format_mw_swing as fmt_mw_swing,
        format_session_summary as fmt_session_summary,
        format_ball_commentary as fmt_ball_commentary,
        format_over_summary as fmt_over_summary,
    )
except ImportError:
    fmt_session_call = None
    fmt_session_bundle = None
    fmt_mw_call = None
    fmt_over_update = None
    fmt_book_alert = None
    fmt_mw_swing = None
    fmt_session_summary = None
    fmt_ball_commentary = None
    fmt_over_summary = None

logger = logging.getLogger("ipl_spotter.spotter")

VERSION = "1.0.0"


class IPLEdgeSpotter:
    """Main engine that scans live IPL matches and shows model predictions."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

        # Core modules
        self.cricket_client = CricketClient(config)
        self.stats_db = StatsDB(config.get("db_path", "data/ipl_stats.db"))
        self.predictor = IPLPredictor(config, stats_db=self.stats_db)
        self.edge_detector = EdgeDetector(config)
        self.telegram = TelegramNotifier(config)

        # Cloudbet — only for on-demand odds fetch and bet execution
        self.odds_client = None
        if OddsClient is not None:
            try:
                self.odds_client = OddsClient(config)
                logger.info("OddsClient ready (on-demand only, not polling)")
            except Exception:
                logger.warning("OddsClient failed to init", exc_info=True)

        # The Odds API — consensus fair probability from 27 bookmakers (free, 500 req/mo)
        # Disabled by default during live play to reduce latency; Cloudbet MW odds + model suffice
        self.theodds = None
        if TheOddsClient is not None and config.get("theodds_enabled", False):
            try:
                self.theodds = TheOddsClient(config)
                if self.theodds.enabled:
                    logger.info("The Odds API enabled — 27 bookmaker consensus for match winner")
            except Exception:
                logger.warning("TheOddsClient failed to init", exc_info=True)
        elif not config.get("theodds_enabled", False):
            logger.info("The Odds API disabled for live latency — using Cloudbet MW odds + model")

        # News intelligence (Brave Search)
        self.news_intel = None
        if NewsIntel is not None:
            try:
                self.news_intel = NewsIntel(config)
            except Exception:
                logger.warning("NewsIntel failed to init", exc_info=True)

        # LLM intelligence (Claude API)
        self.llm_intel = None
        if LLMIntel is not None:
            try:
                self.llm_intel = LLMIntel(config)
            except Exception:
                logger.warning("LLMIntel failed to init", exc_info=True)

        # Odds tracker for when we do fetch odds
        self.odds_tracker = OddsTracker() if OddsTracker is not None else None

        # Odds logger — persists every Cloudbet snapshot for model fine-tuning
        self.odds_logger = None
        if OddsLogger is not None:
            try:
                self.odds_logger = OddsLogger()
            except Exception:
                logger.warning("OddsLogger failed to init", exc_info=True)

        # Match recorder — comprehensive per-scan capture for simulation replay
        self.match_recorder = None
        if MatchRecorder is not None:
            try:
                self.match_recorder = MatchRecorder()
            except Exception:
                logger.warning("MatchRecorder failed to init", exc_info=True)

        # Shadow tracker — paper-trades every signal for v2 accuracy measurement
        self.shadow_tracker = None
        if ShadowTracker is not None:
            try:
                self.shadow_tracker = ShadowTracker()
                logger.info("ShadowTracker enabled — logging all signals to shadow_ledger.db")
            except Exception:
                logger.warning("ShadowTracker failed to init", exc_info=True)

        # Chase state machine — 5-state pressure classifier for 2nd innings
        self.chase_state = None
        if ChaseStateMachine is not None:
            try:
                self.chase_state = ChaseStateMachine()
                logger.info("ChaseStateMachine enabled")
            except Exception:
                logger.debug("ChaseStateMachine failed to init", exc_info=True)

        # Paper simulator — $100K virtual bankroll, all markets including MW
        self.paper_sim = None
        if PaperSimulator is not None:
            try:
                _sim_bankroll = float(config.get("paper_sim_bankroll", 100_000))
                self.paper_sim = PaperSimulator(bankroll=_sim_bankroll)
                logger.info("PaperSimulator enabled — bankroll=$%.0f", self.paper_sim.bankroll)
            except Exception:
                logger.warning("PaperSimulator failed to init", exc_info=True)

        # CricData client — toss/squad/scorecard supplement
        self.cricdata = None
        if CricDataClient is not None:
            try:
                self.cricdata = CricDataClient(config)
            except Exception:
                logger.warning("CricDataClient failed to init", exc_info=True)

        # ESPN client — free speed supplement + player stats + post-match data
        self.espn = None
        if ESPNClient is not None:
            try:
                self.espn = ESPNClient()
            except Exception:
                logger.warning("ESPNClient failed to init", exc_info=True)

        # ML data collector — saves live snapshots for model training
        self.ml_collector = None
        if MLCollector is not None:
            try:
                self.ml_collector = MLCollector()
            except Exception:
                logger.warning("MLCollector failed to init", exc_info=True)

        # ML predictor — XGBoost model (falls back to stats model if not trained yet)
        self.ml_predictor = None
        if MLPredictor is not None:
            try:
                self.ml_predictor = MLPredictor()
                if self.ml_predictor.available:
                    logger.info("MLPredictor ACTIVE — XGBoost model loaded")
                else:
                    logger.info("MLPredictor: no trained model yet — collecting data")
            except Exception:
                logger.warning("MLPredictor failed to init", exc_info=True)

        # Match context filter — vetoes bets when match situation doesn't support them
        self.match_context = MatchContext() if MatchContext is not None else None

        # PSL pre-match context (compensates for no live line channel)
        self.psl_context = PSLContext(self.stats_db) if PSLContext is not None else None

        # Match dossier — player profiles built after toss
        self.match_dossier = None
        if MatchDossier is not None:
            try:
                self.match_dossier = MatchDossier(config, self.stats_db)
            except Exception:
                logger.warning("MatchDossier failed to init", exc_info=True)

        # Speed edge detector
        self.speed_edge = SpeedEdge() if SpeedEdge is not None else None
        if self.speed_edge:
            logger.info("Speed Edge detector enabled — will alert on trigger events")

        # Ferrari Live Line listener — IPL Indian book session lines via Telegram userbot
        self.liveline: Optional[LiveLineListener] = None
        if LiveLineListener is not None:
            try:
                ll = LiveLineListener(config)
                if ll.enabled:
                    ll.start()
                    self.liveline = ll
                    logger.info("Ferrari LiveLine listener started — channel: %s",
                                config.get("liveline_channel", ""))
                else:
                    logger.info("Ferrari LiveLine disabled — set telegram_api_id/hash/liveline_channel")
            except Exception:
                logger.warning("LiveLineListener failed to start", exc_info=True)

        # State persistence — SQLite-backed store for open/closed bets
        self.state_store = StateStore(config.get("state_db_path", "data/bot_state.db"))

        # Live bet execution
        self.bet_executor = None
        if BetExecutor is not None:
            try:
                if MultiBetExecutor is not None and config.get("cloudbet_accounts"):
                    self.bet_executor = MultiBetExecutor(config)
                    logger.info("MultiBetExecutor enabled — %d accounts LIVE",
                                len(self.bet_executor.executors))
                else:
                    self.bet_executor = BetExecutor(config, state_store=self.state_store)
                    logger.info("BetExecutor enabled — LIVE BETTING ACTIVE")
            except Exception:
                logger.warning("BetExecutor failed to init", exc_info=True)

        # Risk manager
        self.risk_manager = None
        if RiskManager is not None:
            self.risk_manager = RiskManager(config)

        # Match Co-Pilot
        self.copilot: Optional[MatchCopilot] = None
        if MatchCopilot and config.get("copilot_enabled", False):
            self.copilot = MatchCopilot(config)
            logger.info("Match Co-Pilot ENABLED")
            if self.risk_manager:
                if self.odds_client:
                    logger.info(
                        "RiskManager ready — live bankroll sync pending, max_bets=%d",
                        self.risk_manager.max_open_bets,
                    )
                else:
                    logger.info(
                        "RiskManager: bankroll=$%.2f, max_stake=$%.2f, max_bets=%d",
                        self.risk_manager.bankroll_usd,
                        self.risk_manager.max_position_size_usd,
                        self.risk_manager.max_open_bets,
                    )

        # Smart staking engine — percentage-based capital management with streak logic
        self.smart_staking = SmartStakingEngine(config) if SmartStakingEngine is not None else None
        if self.smart_staking:
            logger.info("SmartStakingEngine enabled")

        # Live bet tracker — monitors open bets against live scores
        self.live_tracker = LiveBetTracker(config) if LiveBetTracker is not None else None
        if self.live_tracker:
            logger.info("LiveBetTracker enabled")

        # Series databases — per-competition per-season accumulating stats
        self._series_dbs: Dict[str, Any] = {}
        if SeriesDB is not None:
            for comp in config.get("competitions", ["ipl"]):
                try:
                    self._series_dbs[comp] = SeriesDB(competition=comp)
                except Exception:
                    logger.warning("SeriesDB failed to init for %s", comp, exc_info=True)

        # Fixture manager — schedule-aware pre-match lifecycle
        self.fixture_manager = None
        if FixtureManager is not None and config.get("fixture_enabled", True):
            try:
                self.fixture_manager = FixtureManager(
                    config=config,
                    cricket_client=self.cricket_client,
                    odds_client=self.odds_client,
                    telegram=self.telegram,
                    state_store=self.state_store,
                    stats_db=self.stats_db,
                    series_dbs=self._series_dbs,
                    espn_client=self.espn if hasattr(self, 'espn') else None,
                    match_dossier=self.match_dossier if hasattr(self, 'match_dossier') else None,
                )
                logger.info("FixtureManager enabled for %s", ", ".join(config.get("competitions", ["ipl"])))
            except Exception:
                logger.warning("FixtureManager failed to init", exc_info=True)

        # Runtime state
        self.running: bool = False
        self.active_matches: Dict[int, MatchState] = {}
        self.match_info: Dict[int, dict] = {}  # match_id → {home, away, venue}
        self.alerts_sent: Set[tuple] = set()
        # Persistent tracking of which matches have had pre-match reports sent.
        # Survives bot restarts via state_store.
        self._pre_match_sent: Set[int] = set()
        if hasattr(self, 'state_store') and self.state_store:
            _saved_pm = self.state_store.load_state("pre_match_sent")
            if _saved_pm and isinstance(_saved_pm, list):
                self._pre_match_sent = set(_saved_pm)
        self.scan_count: int = 0
        # Series profiles per active match (resolved once, cached for match lifetime)
        self._match_profiles: Dict[int, Any] = {}
        # Startup reconciliation flag — don't bet until first reconcile is done
        self._startup_reconciled: bool = False
        # Model prediction history for stability gate: {(match_id, market): [predictions]}
        self._prediction_history: Dict[tuple, list] = {}

        # Previous predictions for detecting sharp model shifts
        self._prev_predictions: Dict[int, dict] = {}
        self._innings_states: Dict[int, Any] = {}  # match_id → latest InningsState
        # Cooldown for model shift Telegram messages — don't spam every scan
        self._last_model_shift_sent: Dict[int, float] = {}  # match_id → timestamp
        self._model_shift_cooldown: float = config.get("model_shift_cooldown_seconds", 180.0)  # 3 min

        # Speed edge direction memory: {match_id: {"direction": "OVER"/"UNDER", "fired_at": float, "edge": float}}
        # Prevents the bot sending OVER then UNDER (or vice-versa) within the cooldown window.
        self._last_speed_edge_direction: Dict[int, dict] = {}
        # Seconds within which a contradictory direction is suppressed (unless edge is very large)
        self._speed_edge_direction_cooldown: int = config.get("speed_edge_direction_cooldown", 600)  # 10 min
        # Minimum extra edge (runs) required to override the direction cooldown
        self._speed_edge_flip_override_edge: float = config.get("speed_edge_flip_override_edge", 20.0)

        # Edge signal direction memory: {(match_id, innings, market): {"direction", "fired_at", "edge_runs", "wickets"}}
        # Prevents sending OVER then immediately UNDER on the same market.
        self._last_signal_direction: Dict[tuple, dict] = {}
        # Cooldown before opposite direction is allowed on the same market
        self._signal_direction_cooldown: int = config.get("signal_direction_cooldown", 900)  # 15 min
        # Edge (runs) required to override the direction cooldown on a flip
        self._signal_flip_min_edge: float = config.get("signal_flip_min_edge", 15.0)

        self.scan_interval: int = config.get("scan_interval_seconds", 15)
        self.confidence_threshold: str = config.get("confidence_threshold", "MEDIUM")

        self._thread: Optional[threading.Thread] = None
        logger.info("Cricket Edge Spotter v%s initialised (scan every %ds)", VERSION, self.scan_interval)

    def _refresh_live_bankroll(self, force: bool = False) -> float | None:
        """Refresh bankroll and max stake from the live Cloudbet balance."""
        if not self.odds_client or not self.risk_manager:
            return None
        if not force and self.scan_count % 20 != 0:
            return None

        currency = self.config.get("default_currency", "USD")
        live_balance = self.odds_client.get_balance(currency)
        if live_balance is None:
            return None

        previous_bankroll = self.risk_manager.bankroll_usd
        previous_max_stake = self.risk_manager.max_position_size_usd
        self.risk_manager.update_bankroll(live_balance)

        # Let multi-executor know primary bankroll so it can size client stakes correctly
        if hasattr(self.bet_executor, "update_primary_bankroll"):
            self.bet_executor.update_primary_bankroll(live_balance)

        if force or abs(previous_bankroll - self.risk_manager.bankroll_usd) >= 0.01:
            logger.info(
                "Bankroll synced from Cloudbet balance: $%.2f (max stake $%.2f)",
                self.risk_manager.bankroll_usd,
                self.risk_manager.max_position_size_usd,
            )
        elif abs(previous_max_stake - self.risk_manager.max_position_size_usd) >= 0.01:
            logger.info(
                "Max stake refreshed from bankroll pct: $%.2f",
                self.risk_manager.max_position_size_usd,
            )

        return self.risk_manager.bankroll_usd

    # ── Lifecycle ──────────────────────────────────────────────────────

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._scan_loop, daemon=True, name="ipl-scan")
        self._thread.start()
        logger.info("Scan loop started")

    def stop(self) -> None:
        self.running = False
        logger.info("Stop requested")

    # ── Scan loop ──────────────────────────────────────────────────────

    def _scan_loop(self) -> None:
        while self.running:
            scan_start = time.monotonic()
            try:
                self._run_scan()
            except Exception:
                logger.exception("Error in scan cycle")

            scan_duration = time.monotonic() - scan_start
            remaining = max(0, self.scan_interval - scan_duration)
            if scan_duration > self.scan_interval:
                logger.warning(
                    "Scan took %.1fs (interval=%ds) — consider increasing interval",
                    scan_duration, self.scan_interval,
                )
            elapsed = 0
            while elapsed < remaining and self.running:
                time.sleep(1)
                elapsed += 1

    def _run_scan(self) -> None:
        self.scan_count += 1

        # On first scan after startup, reconcile open bets with Cloudbet
        if not self._startup_reconciled:
            if self.bet_executor and hasattr(self.bet_executor, 'reconcile_with_exchange'):
                try:
                    self.bet_executor.reconcile_with_exchange()
                except Exception:
                    logger.exception("Startup reconciliation failed")
            self._startup_reconciled = True
            logger.info("Startup reconciliation complete — betting enabled")

        # Fixture manager tick — handles schedule, pre-match reports, toss detection
        if self.fixture_manager:
            try:
                self.fixture_manager.tick(self.scan_count)
            except Exception:
                logger.debug("Fixture tick error", exc_info=True)

        # Skip live-feed polling when no match is in the window
        if self.fixture_manager and not self.fixture_manager.is_match_window():
            if self.scan_count % 100 == 0:
                logger.info("No matches in window — skipping live scan")
            return

        self._refresh_live_bankroll()

        # Check bet settlements every scan
        self._check_settlements()

        competitions = self.config.get("competitions", ["ipl"])
        all_live = []

        def _fetch_comp(comp: str) -> List[dict]:
            matches = self.cricket_client.get_live_matches(comp)
            for m in matches:
                m["_competition"] = comp
            return matches

        if len(competitions) > 1:
            with ThreadPoolExecutor(max_workers=len(competitions)) as pool:
                futures = {pool.submit(_fetch_comp, c): c for c in competitions}
                for fut in as_completed(futures):
                    try:
                        all_live.extend(fut.result())
                    except Exception:
                        logger.exception("Error fetching %s", futures[fut])
        else:
            for comp in competitions:
                all_live.extend(_fetch_comp(comp))

        if not all_live:
            if self.scan_count % 20 == 1:  # Don't spam
                comps = "/".join(c.upper() for c in competitions)
                logger.info("No live %s matches — waiting...", comps)
            return

        live_match_ids = set()
        for match in all_live:
            match_id = match.get("id")
            if match_id is None:
                continue
            live_match_ids.add(match_id)
            try:
                self._process_match(match, match_id)
            except Exception:
                logger.exception("Error processing match %s", match_id)

        # ── Detect completed matches (no longer in live feed) ─────────
        for ended_id in list(self.active_matches.keys()):
            if ended_id not in live_match_ids:
                self._on_match_completed(ended_id)

    def _on_match_completed(self, match_id: int) -> None:
        """Called when a match disappears from the live feed (match ended)."""
        state = self.active_matches.pop(match_id, None)
        info = self.match_info.get(match_id, {})
        if not state:
            return

        home = info.get("home", "")
        away = info.get("away", "")
        comp = info.get("competition", "ipl")
        logger.info("MATCH COMPLETED: %s vs %s (match_id=%d)", home, away, match_id)

        sdb = self._series_dbs.get(comp)
        if sdb and not sdb.has_match(match_id):
            try:
                phase_runs = {}
                if hasattr(state, "phase_runs") and state.phase_runs:
                    phase_runs = {
                        "pp_inn1": state.phase_runs.get("powerplay", 0),
                        "mid_inn1": state.phase_runs.get("middle", 0),
                        "death_inn1": state.phase_runs.get("death", 0),
                    }

                summary = self._get_cricdata_match_summary(home, away, comp)
                batting_cards = summary.get("batting_cards") or self._build_state_batting_cards(state, home)
                bowling_cards = summary.get("bowling_cards") or self._build_state_bowling_cards(state, away)
                innings_totals = summary.get("innings_totals") or self._build_state_innings_totals(state)

                winner = info.get("winner", "") or summary.get("winner", "")
                toss = info.get("toss_winner", "") or summary.get("toss_winner", "")
                toss_dec = info.get("toss_decision", "") or summary.get("toss_decision", "")
                venue = summary.get("venue") or state.venue or ""
                match_date = summary.get("date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")

                sdb.record_match(
                    fixture_id=match_id,
                    match_number=0,
                    home=home,
                    away=away,
                    venue=venue,
                    date=match_date,
                    toss_winner=toss,
                    toss_decision=toss_dec,
                    inn1_total=innings_totals.get("inn1_total", 0),
                    inn1_wickets=innings_totals.get("inn1_wickets", 0),
                    inn2_total=innings_totals.get("inn2_total", 0),
                    inn2_wickets=innings_totals.get("inn2_wickets", 0),
                    winner=winner,
                    phase_runs=phase_runs,
                    batting_cards=batting_cards,
                    bowling_cards=bowling_cards,
                )
                logger.info("SeriesDB: recorded completed match %d (%s vs %s)", match_id, home, away)

                if winner:
                    sdb.update_standings(home, won=(winner == home))
                    sdb.update_standings(away, won=(winner == away))
            except Exception:
                logger.debug("Failed to record match %d to series DB", match_id, exc_info=True)

        # Settle shadow-tracked signals for this match
        if self.shadow_tracker:
            try:
                actual_scores: dict[str, Any] = {}
                if state:
                    actual_scores["innings_total"] = state.total_runs
                    actual_scores["20_over"] = state.total_runs
                    if hasattr(state, "phase_runs") and state.phase_runs:
                        actual_scores["6_over"] = state.phase_runs.get("powerplay", 0)
                        actual_scores["15_over"] = (
                            state.phase_runs.get("powerplay", 0)
                            + state.phase_runs.get("middle", 0)
                        )
                    # 10-over total: PP + partial middle (not directly in phase_runs)
                    # Use total score at 10 overs if available, else skip
                    if hasattr(state, "ten_over_total") and state.ten_over_total:
                        actual_scores["10_over"] = state.ten_over_total
                winner = info.get("winner", "")
                if winner:
                    actual_scores["match_winner"] = winner
                if actual_scores:
                    settled = self.shadow_tracker.settle_match(match_id, actual_scores)
                    logger.info("ShadowTracker: settled %d signals for match %d", settled, match_id)
            except Exception:
                logger.debug("ShadowTracker settle failed for match %d", match_id, exc_info=True)

        # Paper simulator settlement — uses same actuals as shadow tracker
        if self.paper_sim:
            try:
                # Build comprehensive actuals for paper sim (both innings)
                paper_actuals: dict[str, Any] = {}
                if state:
                    paper_actuals["innings_total"] = state.total_runs
                    paper_actuals["20_over"] = state.total_runs
                    if hasattr(state, "phase_runs") and state.phase_runs:
                        paper_actuals["6_over"] = state.phase_runs.get("powerplay", 0)
                        paper_actuals["15_over"] = (
                            state.phase_runs.get("powerplay", 0)
                            + state.phase_runs.get("middle", 0)
                        )
                    if hasattr(state, "ten_over_total") and state.ten_over_total:
                        paper_actuals["10_over"] = state.ten_over_total
                    # Store per-innings totals for settlement
                    if hasattr(state, "innings_totals"):
                        for inn_num, inn_total in (state.innings_totals or {}).items():
                            paper_actuals[f"inn{inn_num}_innings_total"] = inn_total
                            paper_actuals[f"inn{inn_num}_20_over"] = inn_total
                winner = info.get("winner", "")
                if winner:
                    paper_actuals["match_winner"] = winner
                if paper_actuals:
                    ps_settled = self.paper_sim.settle_match(match_id, paper_actuals)
                    if ps_settled:
                        logger.info(
                            "PaperSim: settled %d bets for match %d — bankroll=$%.0f",
                            ps_settled, match_id, self.paper_sim.bankroll,
                        )
            except Exception:
                logger.debug("PaperSimulator settle failed for match %d", match_id, exc_info=True)

        self._match_profiles.pop(match_id, None)
        self._prev_predictions.pop(match_id, None)
        self._innings_states.pop(match_id, None)
        self._last_model_shift_sent.pop(match_id, None)
        self._last_speed_edge_direction.pop(match_id, None)
        keys_to_remove = [k for k in self._last_signal_direction if k[0] == match_id]
        for k in keys_to_remove:
            self._last_signal_direction.pop(k, None)
        keys_to_remove = [k for k in self._prediction_history if k[0] == match_id]
        for k in keys_to_remove:
            self._prediction_history.pop(k, None)

    def _get_cricdata_match_summary(self, home: str, away: str, competition: str) -> dict[str, Any]:
        if not self.cricdata or not getattr(self.cricdata, "enabled", False):
            return {}

        try:
            match = self.cricdata.find_match(home, away, competition=competition)
            if not match or not match.get("id"):
                return {}

            scorecard = self.cricdata.get_match_scorecard(str(match.get("id")))
            if not isinstance(scorecard, dict):
                return {}

            summary = {
                "batting_cards": [],
                "bowling_cards": [],
                "innings_totals": self._extract_cricdata_totals(scorecard),
                "winner": scorecard.get("matchWinner") or "",
                "toss_winner": scorecard.get("tossWinner") or scorecard.get("tosswinner") or "",
                "toss_decision": scorecard.get("tossChoice") or scorecard.get("tosschoice") or "",
                "venue": scorecard.get("venue") or "",
                "date": self._extract_cricdata_date(scorecard),
            }

            innings_list = scorecard.get("scorecard", [])
            if not isinstance(innings_list, list):
                innings_list = []

            for index, innings in enumerate(innings_list, start=1):
                batting_team = self._extract_cricdata_innings_team(innings, index, home, away)
                bowling_team = away if batting_team == home else home
                batting_entries = innings.get("batting", []) if isinstance(innings, dict) else []
                bowling_entries = innings.get("bowling", []) if isinstance(innings, dict) else []

                for position, entry in enumerate(batting_entries, start=1):
                    batsman = entry.get("batsman", {}) if isinstance(entry, dict) else {}
                    name = batsman.get("name") or entry.get("name") or ""
                    if not name:
                        continue
                    summary["batting_cards"].append({
                        "innings": index,
                        "player": name,
                        "team": batting_team,
                        "runs": self._safe_int(entry.get("r") or entry.get("runs")),
                        "balls": self._safe_int(entry.get("b") or entry.get("balls")),
                        "fours": self._safe_int(entry.get("4s") or entry.get("fours")),
                        "sixes": self._safe_int(entry.get("6s") or entry.get("sixes")),
                        "strike_rate": self._safe_float(entry.get("sr") or entry.get("strikeRate")),
                        "position": position,
                    })

                for entry in bowling_entries:
                    bowler = entry.get("bowler", {}) if isinstance(entry, dict) else {}
                    name = bowler.get("name") or entry.get("name") or ""
                    if not name:
                        continue
                    summary["bowling_cards"].append({
                        "innings": index,
                        "player": name,
                        "team": bowling_team,
                        "overs": self._safe_float(entry.get("o") or entry.get("overs")),
                        "runs_conceded": self._safe_int(entry.get("r") or entry.get("runs") or entry.get("conceded")),
                        "wickets": self._safe_int(entry.get("w") or entry.get("wickets")),
                        "economy": self._safe_float(entry.get("econ") or entry.get("er") or entry.get("economy")),
                    })

            return summary
        except Exception:
            logger.debug("Failed to fetch CricData scorecard for %s vs %s", home, away, exc_info=True)
            return {}

    def _build_state_batting_cards(self, state: MatchState, default_team: str) -> list[dict[str, Any]]:
        batting_cards = []
        if hasattr(state, "batting_card") and state.batting_card:
            for i, card in enumerate(state.batting_card):
                name = (
                    card.get("name")
                    or card.get("player")
                    or card.get("batsman", {}).get("fullname")
                    or card.get("batsman", {}).get("name")
                    or ""
                )
                if not name:
                    continue
                batting_cards.append({
                    "innings": state.current_innings,
                    "player": name,
                    "team": state.batting_team or default_team,
                    "runs": self._safe_int(card.get("score") or card.get("runs")),
                    "balls": self._safe_int(card.get("balls") or card.get("ball")),
                    "fours": self._safe_int(card.get("fours") or card.get("four_x")),
                    "sixes": self._safe_int(card.get("sixes") or card.get("six_x")),
                    "strike_rate": self._safe_float(card.get("strike_rate") or card.get("rate")),
                    "position": i + 1,
                })
        return batting_cards

    def _build_state_bowling_cards(self, state: MatchState, default_team: str) -> list[dict[str, Any]]:
        bowling_cards = []
        if hasattr(state, "bowling_card") and state.bowling_card:
            for card in state.bowling_card:
                name = (
                    card.get("name")
                    or card.get("player")
                    or card.get("bowler", {}).get("fullname")
                    or card.get("bowler", {}).get("name")
                    or ""
                )
                if not name:
                    continue
                bowling_cards.append({
                    "innings": state.current_innings,
                    "player": name,
                    "team": state.bowling_team or default_team,
                    "overs": self._safe_float(card.get("overs")),
                    "runs_conceded": self._safe_int(card.get("runs") or card.get("runs_conceded")),
                    "wickets": self._safe_int(card.get("wickets")),
                    "economy": self._safe_float(card.get("rate") or card.get("economy")),
                })
        return bowling_cards

    def _build_state_innings_totals(self, state: MatchState) -> dict[str, int]:
        if state.current_innings == 1:
            return {
                "inn1_total": self._safe_int(state.total_runs),
                "inn1_wickets": self._safe_int(state.wickets),
                "inn2_total": 0,
                "inn2_wickets": 0,
            }
        return {
            "inn1_total": self._safe_int((state.target_runs - 1) if state.target_runs else 0),
            "inn1_wickets": 10,
            "inn2_total": self._safe_int(state.total_runs),
            "inn2_wickets": self._safe_int(state.wickets),
        }

    def _extract_cricdata_totals(self, scorecard: dict[str, Any]) -> dict[str, int]:
        scores = scorecard.get("score", [])
        if not isinstance(scores, list) or not scores:
            return {}

        inn1 = scores[0] if len(scores) > 0 and isinstance(scores[0], dict) else {}
        inn2 = scores[1] if len(scores) > 1 and isinstance(scores[1], dict) else {}
        return {
            "inn1_total": self._safe_int(inn1.get("r") or inn1.get("runs")),
            "inn1_wickets": self._safe_int(inn1.get("w") or inn1.get("wickets")),
            "inn2_total": self._safe_int(inn2.get("r") or inn2.get("runs")),
            "inn2_wickets": self._safe_int(inn2.get("w") or inn2.get("wickets")),
        }

    def _extract_cricdata_innings_team(self, innings: dict[str, Any], innings_index: int, home: str, away: str) -> str:
        label = str(innings.get("inning") or innings.get("name") or "")
        if " inning" in label.lower():
            team_name = label.rsplit(" Inning", 1)[0].strip()
            if team_name:
                return team_name
        return home if innings_index == 1 else away

    def _extract_cricdata_date(self, scorecard: dict[str, Any]) -> str:
        raw = str(scorecard.get("dateTimeGMT") or scorecard.get("date") or "")
        if not raw:
            return ""
        return raw.split("T", 1)[0]

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    # ── Match processing ───────────────────────────────────────────────

    def _process_match(self, match: dict, match_id: int) -> None:
        home, away, venue = self._extract_match_info(match)
        state = MatchState.from_sportmonks(match)
        previous_state = self.active_matches.get(match_id)

        competition = match.get("_competition", "ipl")

        if previous_state is None:
            self.match_info[match_id] = {"home": home, "away": away, "venue": venue, "competition": competition}
            # Resolve series profile for this match (cached for match lifetime)
            if get_profile is not None:
                self._match_profiles[match_id] = get_profile(competition=competition)
                logger.info("Match %d: using %s profile", match_id, self._match_profiles[match_id].display_name)

            # Reset Ferrari parser state when a new match is first seen — prevents
            # stale session lines from a previous match bleeding into this one.
            if self.liveline and competition == "ipl":
                try:
                    self.liveline.get_parser().reset_state()
                    logger.info("Ferrari parser state reset for new match: %s vs %s", home, away)
                except Exception:
                    pass

            # Check if match is already in progress (restart scenario)
            match_in_progress = state.balls_faced > 0 or state.total_runs > 0
            if match_in_progress:
                logger.info(
                    "REJOINED MATCH: %s vs %s at %s (%d/%d, %.1f ov) — skipping pre-match & old balls",
                    home, away, venue, state.total_runs, state.wickets, state.overs_completed,
                )
                # Store state without sending anything — next scan will pick up only new balls
                self.active_matches[match_id] = state
                self._prev_predictions[match_id] = {}  # prevent model shift alert
                # Build dossier from live batting/bowling cards
                self._build_match_dossier(match_id, home, away, venue, state, competition)

                # PSL: still send context even if match already started — venue/team intel is always useful
                if competition == "psl" and self.psl_context:
                    try:
                        _ch = competition
                        ctx = self.psl_context.build(match_id, home, away, venue)
                        psl_msg = self.psl_context.format_telegram(ctx)

                        # Replace header to show this is a mid-match join
                        psl_msg = psl_msg.replace(
                            "⚠️ No live line available — use this for manual trading",
                            f"⚠️ No live line available | Bot rejoined: {state.total_runs}/{state.wickets} ({state.overs_completed:.1f} ov)",
                        )

                        # Enrich with ESPN playing XI if available
                        if self.espn:
                            squads = self.espn.get_squads_for_match(home, away, "psl")
                            if squads:
                                lines = ["\n🔢 <b>Playing XI (ESPN)</b>"]
                                for side in ("home", "away"):
                                    squad = squads.get(side, {})
                                    team  = squad.get("team", "")
                                    players = squad.get("players", [])
                                    if players:
                                        xi = []
                                        for p in players:
                                            tag = ""
                                            if p.get("captain"): tag += " (C)"
                                            if p.get("keeper"):  tag += " (WK)"
                                            role = p.get("role_abbr", "")
                                            xi.append(f"{p['name']}{tag}" + (f" [{role}]" if role else ""))
                                        lines.append(f"<b>{team}</b>: {', '.join(xi)}")
                                psl_msg += "\n" + "\n".join(lines)

                        self.telegram.send_alert_sync(psl_msg, channel=None, is_signal=False, parse_mode="HTML")
                        logger.info("PSL context sent (mid-match rejoin): %s vs %s", home, away)
                    except Exception:
                        logger.warning("PSL context (rejoin) failed", exc_info=True)

                return
            else:
                if match_id not in self._pre_match_sent:
                    logger.info("NEW MATCH: %s vs %s at %s (competition=%s)", home, away, venue, competition)
                    self._send_pre_match_report(match_id, home, away, venue, match)
                    self._pre_match_sent.add(match_id)
                    if hasattr(self, 'state_store') and self.state_store:
                        self.state_store.save_state("pre_match_sent", list(self._pre_match_sent))
                else:
                    logger.info("SKIPPED pre-match (already sent): %s vs %s (match_id=%d)", home, away, match_id)
                self._build_match_dossier(match_id, home, away, venue, state, competition)
        elif previous_state.current_innings != state.current_innings:
            logger.info(
                "NEW INNINGS: %s vs %s -> innings %d, batting %s",
                home,
                away,
                state.current_innings,
                state.batting_team,
            )
            self.edge_detector.clear_locks(match_id, innings=previous_state.current_innings)

            # ── Innings break: reset Ferrari + shadow positions ──────────────
            # When Sportmonks switches to innings 2, we are in the innings break.
            # Reset Ferrari parser so stale 1st innings lines don't bleed into
            # 2nd innings session estimates. Also settle any open copilot shadow
            # positions from innings 1 against the actual final score.
            if competition == "ipl" and self.liveline:
                try:
                    self.liveline.get_parser().reset_state()
                    logger.info("Ferrari parser reset for innings %d", state.current_innings)
                except Exception:
                    pass

            if self.copilot:
                try:
                    inn1_total = previous_state.total_runs
                    open_inn1 = self.copilot.position_book.get_open_sessions(
                        match_id, innings=previous_state.current_innings
                    )
                    for _pos in open_inn1:
                        self.copilot.position_book.settle_session(_pos, float(inn1_total))
                        logger.info(
                            "Inn1 shadow settled: %s %s entry=%.0f actual=%d → %s",
                            _pos.direction, _pos.market, _pos.entry_line, inn1_total, _pos.status,
                        )
                except Exception:
                    logger.debug("Inn1 shadow settle error", exc_info=True)

            # Send innings break notification to Telegram only if chase has not started yet.
            # Sportmonks can jump innings at the same time as first-ball data (e.g., 0.1 ov).
            # In that case, sending "Signals paused" is misleading.
            _inn1_score  = previous_state.total_runs
            _inn1_wkts   = previous_state.wickets
            _inn1_overs  = previous_state.overs_completed
            _target      = _inn1_score + 1
            _batting_2nd = state.batting_team or away
            _chase_not_started = (
                state.current_innings >= 2
                and float(state.overs_completed or 0.0) == 0.0
                and int(state.total_runs or 0) == 0
            )
            if _chase_not_started:
                _break_msg = (
                    f"⏸️ *INNINGS BREAK* — {home} vs {away}\n\n"
                    f"1st innings: *{_inn1_score}/{_inn1_wkts}* ({_inn1_overs:.1f} ov)\n"
                    f"Target: *{_target}* — {_batting_2nd} need {_target} to win\n\n"
                    f"Signals paused — waiting for 2nd innings to start 🏏"
                )
                try:
                    self.telegram.send_alert_sync(_break_msg, channel=None, is_signal=False, parse_mode=None)
                    logger.info("Innings break notification sent: %s vs %s target=%d", home, away, _target)
                except Exception:
                    logger.debug("Innings break Telegram failed", exc_info=True)
            else:
                logger.info(
                    "Skip innings-break message: chase already started at %.1f ov (%d/%d)",
                    state.overs_completed,
                    state.total_runs,
                    state.wickets,
                )
            # ──────────────────────────────────────────────────────────────────

            # ── ML: finalise the completed innings ───────────────────
            if self.ml_collector and previous_state:
                try:
                    prev_inn = previous_state.current_innings
                    inn_total = previous_state.total_runs
                    pp  = sum(r for ov, r in previous_state.over_runs.items() if ov < 6) if hasattr(previous_state, "over_runs") else 0
                    mid = sum(r for ov, r in previous_state.over_runs.items() if 6 <= ov < 15) if hasattr(previous_state, "over_runs") else 0
                    dth = sum(r for ov, r in previous_state.over_runs.items() if ov >= 15) if hasattr(previous_state, "over_runs") else 0
                    toss_info = self.match_info.get(match_id, {})
                    self.ml_collector.finalise_match(
                        match_id=match_id, innings=prev_inn,
                        innings_total=inn_total, pp_runs=pp,
                        middle_runs=mid, death_runs=dth,
                        competition=competition, venue=venue,
                        home=home, away=away,
                        toss_winner=toss_info.get("toss_winner", ""),
                        toss_decision=toss_info.get("toss_decision", ""),
                    )
                    logger.info("ML: innings %d finalised — %d runs collected", prev_inn, inn_total)

                    # ESPN post-match: fetch ball-by-ball commentary in background
                    # for ML training enrichment (3h cache, only useful after innings)
                    if self.espn and prev_inn >= 1:
                        try:
                            espn_score = self.espn.get_live_score_for_match(home, away, competition)
                            if espn_score and espn_score.get("espn_event_id"):
                                import threading
                                def _fetch_espn_plays(eid, comp):
                                    plays = self.espn.get_post_match_plays(eid, comp)
                                    if plays:
                                        logger.info("ESPN post-match: %d balls saved for ML (event=%s)", len(plays), eid)
                                t = threading.Thread(
                                    target=_fetch_espn_plays,
                                    args=(espn_score["espn_event_id"], competition),
                                    daemon=True,
                                )
                                t.start()
                        except Exception:
                            logger.debug("ESPN post-match fetch error", exc_info=True)
                except Exception:
                    logger.debug("ML finalise error", exc_info=True)

        # ── ESPN speed supplement: patch score if ESPN is ahead ─────────
        # ESPN cache TTL is 1-5s vs Sportmonks 8-15s.
        # If ESPN already shows a higher score, Sportmonks is lagging —
        # update state so edge detection runs on the latest known score.
        _espn_speed_trigger = False
        if self.espn:
            try:
                espn_score = self.espn.get_live_score_for_match(home, away, competition)
                if espn_score:
                    for inn in espn_score.get("innings", []):
                        if inn.get("batting") and inn.get("score", 0) > state.total_runs:
                            lag = inn["score"] - state.total_runs
                            logger.info(
                                "ESPN ahead of Sportmonks by %d runs (%d vs %d) — patching state",
                                lag, inn["score"], state.total_runs,
                            )
                            # Detect wicket before Sportmonks
                            espn_wickets = inn.get("wickets", 0)
                            sm_wickets = previous_state.wickets if previous_state else 0
                            if espn_wickets > sm_wickets:
                                logger.info(
                                    "ESPN SPEED: wicket detected before Sportmonks "
                                    "(ESPN %d wkts vs Sportmonks %d wkts)",
                                    espn_wickets, sm_wickets,
                                )
                                _espn_speed_trigger = True

                            # Detect big over (10+ runs) before Sportmonks
                            sm_runs = previous_state.total_runs if previous_state else 0
                            sm_overs = previous_state.overs_completed if previous_state else 0.0
                            overs_diff = inn.get("overs", 0) - sm_overs
                            runs_diff = inn["score"] - sm_runs
                            if runs_diff >= 10 and 0 < overs_diff <= 1.5:
                                logger.info(
                                    "ESPN SPEED: big over detected before Sportmonks "
                                    "(%d runs in ~%.1f overs, ESPN %d vs Sportmonks %d)",
                                    runs_diff, overs_diff, inn["score"], sm_runs,
                                )
                                _espn_speed_trigger = True

                            state.total_runs = inn["score"]
                            state.wickets    = inn.get("wickets", state.wickets)
                            # overs: only update if ESPN shows more
                            if inn.get("overs", 0) > state.overs_completed:
                                state.overs_completed = inn["overs"]
            except Exception:
                logger.debug("ESPN speed patch error", exc_info=True)

        new_balls = self._count_new_balls(previous_state, state)
        state_changed = self._state_changed(previous_state, state) or _espn_speed_trigger
        self.active_matches[match_id] = state
        self.match_info[match_id] = {"home": home, "away": away, "venue": venue, "competition": competition}

        # Update live bet tracker with current score
        if self.live_tracker and state:
            tracker_events = self.live_tracker.update(
                match_id=match_id,
                innings=state.current_innings,
                runs=state.total_runs,
                wickets=state.wickets,
                overs=state.overs_completed,
            )
            for tracked_bet, event_type, follow_up in tracker_events:
                _ch = self.match_info.get(match_id, {}).get("competition", "ipl")
                if event_type == "EARLY_WIN":
                    msg = self.live_tracker.format_early_win(tracked_bet)
                    logger.info("LIVE TRACKER: %s", msg.replace("\n", " | "))
                    self.telegram.send_alert_sync(msg, channel=None, is_signal=False)
                    if follow_up:
                        fu_msg = self.live_tracker.format_follow_up(follow_up)
                        self.telegram.send_alert_sync(fu_msg, channel=_ch, is_signal=True)
                elif event_type == "LIKELY_LOSS":
                    msg = self.live_tracker.format_likely_loss(tracked_bet)
                    logger.info("LIVE TRACKER: %s", msg.replace("\n", " | "))
                    self.telegram.send_alert_sync(msg, channel=None, is_signal=False)
                    if follow_up:
                        fu_msg = self.live_tracker.format_follow_up(follow_up)
                        self.telegram.send_alert_sync(fu_msg, channel=None, is_signal=False)
                elif event_type == "SETTLED":
                    msg = self.live_tracker.format_settlement(tracked_bet)
                    logger.info("LIVE TRACKER: %s", msg.replace("\n", " | "))

        if new_balls > 0 or state_changed:
            logger.info(
                "Match %s: %s %d/%d (%.1f ov) | new_balls=%d state_changed=%s",
                match_id, state.batting_team, state.total_runs, state.wickets,
                state.overs_completed, new_balls, state_changed,
            )

        # ── ML data collection: save snapshot at end of each over ────
        if new_balls > 0 and self.ml_collector:
            try:
                over_num = int(state.overs_completed)
                # Only record when a full over just completed
                prev_over = int(previous_state.overs_completed) if previous_state else -1
                if over_num > prev_over and over_num > 0:
                    pp_runs = sum(
                        r for ov, r in state.over_runs.items() if ov < 6
                    ) if hasattr(state, "over_runs") else 0
                    last_over_runs = state.over_runs.get(over_num - 1, 0) if hasattr(state, "over_runs") else 0
                    if over_num <= 6:
                        phase = "powerplay"
                    elif over_num <= 15:
                        phase = "middle"
                    else:
                        phase = "death"

                    # Batsman/bowler context
                    striker_sr, striker_runs = 0.0, 0
                    bowler_econ, bowler_wickets = 0.0, 0
                    if hasattr(state, "active_batsmen") and state.active_batsmen:
                        bat = state.active_batsmen[0]
                        balls = bat.get("balls", 0) or 0
                        runs  = bat.get("runs", 0) or 0
                        striker_sr   = round((runs / balls * 100) if balls > 0 else 0.0, 1)
                        striker_runs = runs
                    if hasattr(state, "active_bowlers") and state.active_bowlers:
                        bowl = state.active_bowlers[0]
                        bovs = bowl.get("overs", 0) or 0
                        brun = bowl.get("runs", 0) or 0
                        bwkts = bowl.get("wickets", 0) or 0
                        bowler_econ    = round((brun / bovs) if bovs > 0 else 0.0, 1)
                        bowler_wickets = bwkts

                    toss_info = self.match_info.get(match_id, {})
                    self.ml_collector.record_over(
                        match_id=match_id,
                        competition=competition,
                        venue=venue,
                        home=home,
                        away=away,
                        innings=state.current_innings,
                        over_num=over_num,
                        score=state.total_runs,
                        wickets=state.wickets,
                        last_over_runs=last_over_runs,
                        pp_runs=pp_runs,
                        phase=phase,
                        toss_winner=toss_info.get("toss_winner", ""),
                        toss_decision=toss_info.get("toss_decision", ""),
                        striker_sr=striker_sr,
                        striker_runs=striker_runs,
                        bowler_econ=bowler_econ,
                        bowler_wickets=bowler_wickets,
                    )
            except Exception:
                logger.debug("ML collect error", exc_info=True)

        # Send ball-by-ball commentary even if state hasn't changed for predictions
        # (new balls might come in without triggering state_changed due to extras etc.)
        if new_balls > 0 and self.copilot and self.copilot.enabled:
            _ch = self.match_info.get(match_id, {}).get("competition", "ipl")
            self._send_ball_commentary(
                match_id, home, away, state, new_balls, previous_state, _ch,
            )

        if state_changed:
            # Speed edge: detect trigger events (wicket, big over, collapse etc)
            triggers = []
            if self.speed_edge:
                triggers = self.speed_edge.detect_triggers(state)

            # Fetch Cloudbet odds: always on trigger, otherwise every 2nd scan
            should_fetch = (
                (self.speed_edge and bool(triggers))
                or self.scan_count % 2 == 0
                or state.current_innings >= 2
            )

            # Start Cloudbet fetch in parallel with model predictions
            cloudbet_future = None
            if should_fetch and self.odds_client:
                competition = match.get("_competition", "ipl")
                _pool = ThreadPoolExecutor(max_workers=1)
                cloudbet_future = _pool.submit(
                    self._fetch_cloudbet_odds_for_match,
                    home, away, competition,
                    batting_team_side=self._get_batting_team_side(state, home, away),
                )
                _pool.shutdown(wait=False)

            # Run model predictions (concurrent with Cloudbet fetch)
            competition = match.get("_competition", "ipl")
            predictions = self._run_predictions(match_id, state, competition=competition)

            # Collect Cloudbet result
            cloudbet_odds = None
            if cloudbet_future is not None:
                try:
                    cloudbet_odds = cloudbet_future.result(timeout=12)
                except Exception:
                    logger.warning("Cloudbet parallel fetch timed out or failed")

                if new_balls > 0 and triggers and self.speed_edge:
                    _speed_trigger_telegram = bool(self.config.get("speed_trigger_telegram", False))
                    for trigger in triggers:
                        suppress, short_alert, reason = self._classify_speed_trigger(
                            state,
                            cloudbet_odds,
                            trigger,
                        )
                        if suppress:
                            logger.info("Speed edge suppressed: %s", reason)
                            continue
                        # Only send HIGH severity triggers to Telegram (wickets, collapses)
                        # MEDIUM triggers (partnerships, big overs) are logged but not sent
                        severity = trigger.get("severity", "MEDIUM")
                        if severity != "HIGH":
                            logger.info("Speed trigger skipped (MEDIUM): %s", trigger["type"])
                            continue
                        if not _speed_trigger_telegram:
                            logger.info("Speed trigger alert muted by config: %s", trigger.get("type", ""))
                            continue
                        alert = self.speed_edge.format_speed_alert(
                            home,
                            away,
                            trigger,
                            state,
                            include_action=not short_alert,
                            include_window=not short_alert,
                        )
                        print(f"\n{alert}")
                        logger.info("TRIGGER: %s [%s]", trigger["type"], trigger["severity"])
                        # Speed triggers are internal-only — don't send to client channels
                        self.telegram.send_alert_sync(alert, channel=None, is_signal=False, parse_mode=None)

                # Speed edge: check if Cloudbet adjusted or if we have a window
                if cloudbet_odds and triggers and self.speed_edge:
                    prev_pred = self._prev_predictions.get(match_id, {})
                    for trigger in triggers:
                        suppress, _, reason = self._classify_speed_trigger(
                            state,
                            cloudbet_odds,
                            trigger,
                        )
                        if suppress:
                            logger.info("Speed edge opportunity suppressed: %s", reason)
                            continue
                        opp = self.speed_edge.evaluate_speed_opportunity(
                            trigger, prev_pred, predictions, cloudbet_odds,
                        )
                        if opp:
                            # ── Direction-flip cooldown ──────────────────────────────
                            # If we sent the opposite direction recently, suppress unless
                            # edge is large enough to justify the flip.
                            new_dir = "OVER" if "OVER" in opp.get("recommendation", "") else "UNDER"
                            last_se = self._last_speed_edge_direction.get(match_id)
                            if last_se:
                                elapsed = time.time() - last_se["fired_at"]
                                if (
                                    last_se["direction"] != new_dir
                                    and elapsed < self._speed_edge_direction_cooldown
                                    and opp["edge_size"] < self._speed_edge_flip_override_edge
                                ):
                                    logger.warning(
                                        "Speed edge SUPPRESSED (direction flip %s→%s within %.0fs, edge %.1f < %.0f): %s",
                                        last_se["direction"], new_dir, elapsed,
                                        opp["edge_size"], self._speed_edge_flip_override_edge,
                                        opp["trigger"],
                                    )
                                    continue
                            # Record this direction
                            self._last_speed_edge_direction[match_id] = {
                                "direction": new_dir,
                                "fired_at": time.time(),
                                "edge": opp["edge_size"],
                            }
                            # ────────────────────────────────────────────────────────

                            msg = (
                                f"🎯 *SPEED EDGE CONFIRMED* — {home} vs {away}\n\n"
                                f"Trigger: {opp['trigger']}\n"
                                f"Model shifted: {opp['model_shift']:+.1f} runs\n"
                                f"Cloudbet line: {opp['cloudbet_line']}\n"
                                f"Cloudbet moved: {'YES' if opp['cloudbet_moved'] else 'NO ⚡'}\n"
                                f"Edge size: {opp['edge_size']:.1f} runs\n\n"
                                f"💰 {opp['recommendation']}"
                            )
                            print(f"\n{msg}")
                            logger.info("SPEED EDGE: %s | edge=%.1f", opp["trigger"], opp["edge_size"])
                            # Speed edge messages are internal-only — don't spam the channel.
                            # Auto-bet below will still fire if conditions are met.

                            # AUTO-BET on speed edge
                            if not opp.get("cloudbet_moved", True):
                                innings_market = (cloudbet_odds or {}).get("innings_total", {})
                                if not innings_market or "line" not in innings_market:
                                    continue
                                model_expected = predictions.get("innings_total", {}).get("expected")
                                if model_expected is None:
                                    continue
                                is_over = "OVER" in opp.get("recommendation", "")
                                speed_edge_as_edge = {
                                    "market": "innings_total",
                                    "direction": "OVER" if is_over else "UNDER",
                                    "bookmaker_line": innings_market["line"],
                                    "model_expected": model_expected,
                                    "edge_runs": round(abs(model_expected - innings_market["line"]), 1),
                                    "odds": innings_market.get("over_odds", 1.85) if is_over else innings_market.get("under_odds", 1.85),
                                    "ev_pct": opp["edge_size"] * 2,  # rough EV from edge size
                                    "confidence": "HIGH",
                                }
                                self._send_edge_alert(
                                    match_id,
                                    speed_edge_as_edge,
                                    home,
                                    away,
                                    cloudbet_odds,
                                    trigger="SPEED_EDGE",
                                    innings=state.current_innings,
                                )

            # Show live match state + odds in terminal
            self._print_match_state(match_id, home, away, state, predictions, cloudbet_odds)

            # Detect edges: model vs Cloudbet lines
            if cloudbet_odds:
                self._check_edges(match_id, home, away, state, predictions, cloudbet_odds)

                # Log odds snapshot for model fine-tuning
                if self.odds_logger:
                    try:
                        comp = self.match_info.get(match_id, {}).get("competition", "ipl")
                        self.odds_logger.log_odds(
                            match_id, home, away, state.venue, comp,
                            state.current_innings, state.overs_completed,
                            state.total_runs, state.wickets,
                            cloudbet_odds, predictions,
                        )
                    except Exception:
                        logger.warning("Odds logging failed", exc_info=True)

            # ── Match Recorder — capture full scan state for replay ──
            if self.match_recorder:
                try:
                    comp = self.match_info.get(match_id, {}).get("competition", "ipl")
                    # Ball-by-ball log (deduped internally)
                    self.match_recorder.record_ball(match_id, state, home, away, comp)
                    # Full scan snapshot (predictions + odds + Ferrari state)
                    ferrari_state = None
                    if hasattr(self, "liveline") and self.liveline:
                        parser = self.liveline.get_parser() if hasattr(self.liveline, "get_parser") else None
                        if parser:
                            ferrari_state = parser.get_state() if hasattr(parser, "get_state") else None
                    self.match_recorder.record_scan(
                        match_id, state, predictions, cloudbet_odds,
                        ferrari_state=ferrari_state, competition=comp,
                    )
                except Exception:
                    logger.debug("MatchRecorder failed", exc_info=True)

            # ── Match Co-Pilot ──────────────────────────────────────
            if self.copilot and self.copilot.enabled:
                self._run_copilot(
                    match_id, home, away, state, predictions, cloudbet_odds,
                    new_balls=new_balls, previous_state=previous_state,
                )

            # Check for sharp model shifts
            self._check_model_shifts(match_id, home, away, state, predictions)

            # Store for next comparison
            self._prev_predictions[match_id] = predictions

    def _run_copilot(self, match_id, home, away, state, predictions, cloudbet_odds,
                     new_balls: int = 0, previous_state: MatchState | None = None):
        """Run co-pilot: live commentary feed with embedded trading signals."""
        if not self.copilot:
            return
        config = self.config
        overs = state.overs_completed
        innings = state.current_innings
        # Route messages to competition-specific Telegram channel
        _ch = self.match_info.get(match_id, {}).get("competition", "ipl")

        # ── Innings break guard ────────────────────────────────────────────────
        # When Sportmonks switches to innings 2 but no balls have been bowled yet,
        # we are in the innings break. All session predictions are meaningless —
        # the model doesn't know 2nd innings context (target, required run rate) yet.
        # Suppress everything until the first ball of the chase is recorded.
        if innings >= 2 and overs == 0.0 and state.total_runs == 0:
            logger.info(
                "INNINGS BREAK suppression: %s vs %s innings=%d overs=%.1f — no signals until 2nd innings starts",
                home, away, innings, overs,
            )
            return
        # ──────────────────────────────────────────────────────────────────────

        # 1. Estimate session lines from Cloudbet
        cb_lines = {}
        if cloudbet_odds:
            for mk in ["6_over", "10_over", "15_over", "20_over"]:
                cb_m = cloudbet_odds.get(mk, {})
                if cb_m and "line" in cb_m:
                    cb_lines[mk] = cb_m
            # PSL / no-session-market fallback: map innings_total line → 20_over
            if "20_over" not in cb_lines:
                cb_it = cloudbet_odds.get("innings_total", {})
                if cb_it and cb_it.get("line", 0) > 0:
                    cb_lines["20_over"] = cb_it

        # ── Stale Cloudbet line guard ─────────────────────────────────────────
        # Cloudbet session lines can be set before the match and go stale fast.
        # If a session line is ≤ current score + a small buffer, it's obviously
        # going to be exceeded and the signal would be meaningless noise.
        # Remove stale lines so the model fallback (or Ferrari) is used instead.
        _current_score = state.total_runs
        # How many runs above current score the line must be to be considered live.
        # Larger buffer = more aggressive staleness detection.
        # Set conservatively so valid Cloudbet live lines are NOT dropped.
        _stale_buffer = {
            "6_over": 5, "10_over": 5, "15_over": 5, "20_over": 5, "innings_total": 5,
        }
        for _mk in list(cb_lines.keys()):
            _cb_line_val = cb_lines[_mk].get("line", 0)
            _buf = _stale_buffer.get(_mk, 20)
            if _cb_line_val > 0 and _cb_line_val <= _current_score + _buf:
                logger.info(
                    "Stale Cloudbet line dropped: %s line=%.1f ≤ score(%d)+buffer(%d) — using model",
                    _mk, _cb_line_val, _current_score, _buf,
                )
                del cb_lines[_mk]
        # ─────────────────────────────────────────────────────────────────────

        # ── Ferrari Live Line override (IPL only) ─────────────────────────────
        # For IPL matches, replace / supplement Cloudbet session lines with the
        # actual Indian book lines from the Ferrari Fast Line Telegram channel.
        # Ferrari format: "114-116 10 OVER" → YES=114, NO=116
        # We inject the NO line (116) as the boundary — that's what you bet
        # against when taking YES: you need score > NO to win.
        if _ch == "ipl" and self.liveline:
            ll_state = self.liveline.get_parser().get_state()
            _over_map = {
                "6_over":  ("session_6",  "session_no_6"),
                "10_over": ("session_10", "session_no_10"),
                "15_over": ("session_15", "session_no_15"),
                "20_over": ("session_20", "session_no_20"),
            }
            # Minimum plausible YES line for each market.
            # Filters out PSL per-over run totals (e.g. "3-5 10 OVER" = runs in over #10)
            _ferrari_min_line = {
                "6_over": 40, "10_over": 70, "15_over": 100, "20_over": 130,
            }
            for mk, (yes_key, no_key) in _over_map.items():
                yes_val = ll_state.get(yes_key, 0)
                no_raw = ll_state.get(no_key, 0)
                if yes_val <= 0:
                    continue
                # Sanity: reject if too low to be a real session line or below current score
                _min_line = _ferrari_min_line.get(mk, 40)
                if yes_val < _min_line or yes_val < _current_score:
                    logger.warning(
                        "Ferrari line IGNORED %s: yes=%d (min=%d, score=%d) — stale/per-over data",
                        mk, yes_val, _min_line, _current_score,
                    )
                    continue
                # Ferrari sends two formats:
                #   A) "114-116 10 OVER"  → YES=114, NO=116 (both absolute, NO > YES)
                #   B) "120-1 10 OVER"    → YES=120, SPREAD=1, so actual NO = 120+1 = 121
                # Distinguish: if the second number < YES, it's a spread; otherwise absolute.
                if no_raw > 0 and no_raw < yes_val:
                    actual_no = yes_val + no_raw   # spread format
                elif no_raw >= yes_val:
                    actual_no = no_raw             # absolute format
                else:
                    actual_no = yes_val + 1        # no data — assume 1-run spread
                cb_lines[mk] = {
                    "line": actual_no, "yes": yes_val, "no": actual_no, "source": "liveline",
                }
                logger.info(
                    "Ferrari line %s: YES=%d NO=%d (raw_no=%d) injecting line=%d",
                    mk, yes_val, actual_no, no_raw, actual_no,
                )
        # ─────────────────────────────────────────────────────────────────────

        est_lines = self.copilot.estimate_session_lines(overs, state.total_runs, cb_lines, predictions)

        # 2. Collect all signals for this scan (will embed into over update)
        signals: List[dict] = []

        # Session calls
        calls = self.copilot.evaluate_session_calls(match_id, predictions, est_lines, overs, innings=innings)
        session_prediction_key = {
            "6_over": "powerplay_total",
            "10_over": "ten_over_total",
            "15_over": "fifteen_over_total",
            "20_over": "innings_total",
        }
        # ── Innings total is the ANCHOR for direction consistency ──────────────
        # Use the innings total prediction vs its line to determine the overall
        # direction the model thinks this innings is heading.
        #   anchor_dir = "YES" if model thinks batting team scores above line
        #   anchor_dir = "NO"  if model thinks batting team scores below line
        #   anchor_dir = None  if no innings total line available
        inn_line_data = est_lines.get("20_over", {})
        pred_inn = predictions.get("innings_total", {}).get("expected", 0)
        inn_line_no  = inn_line_data.get("no",  0)
        inn_line_yes = inn_line_data.get("yes", 0)
        if pred_inn > 0 and inn_line_no > 0:
            anchor_dir = "YES" if pred_inn > inn_line_no else "NO"
        else:
            anchor_dir = None

        # Also check existing open positions for cross-market consistency
        open_positions = {
            p.market: p.direction
            for p in self.copilot.position_book.get_open_sessions(match_id, innings=innings)
        }

        for call in calls:
            model_prediction = predictions.get(
                session_prediction_key.get(call["market"], ""),
                {},
            ).get("expected", call["line"])

            market    = call["market"]
            direction = call["direction"]
            edge      = call.get("edge", 0)

            # Suppress if this call contradicts the innings-total anchor direction.
            # Exception: very high edge (>=10 runs) overrides the anchor — the shorter
            # session might be a genuine line mistake worth betting regardless.
            if anchor_dir and direction != anchor_dir and edge < 10:
                logger.warning(
                    "Session suppressed (contradicts innings anchor %s): %s %s edge=%.1f",
                    anchor_dir, direction, market, edge,
                )
                continue

            # Suppress if this call contradicts an edge-detector signal already sent
            # to Telegram in this match. E.g. edge detector sent "UNDER 15_over"
            # (= NO), copilot should not then send "YES innings" to the channel.
            _edge_dir_map = {"UNDER": "NO", "OVER": "YES"}
            _copilot_dir_map = {"NO": "UNDER", "YES": "OVER"}
            _edge_contradicts = False
            for _sent in self.alerts_sent:
                # alerts_sent = set of (match_id, innings, market, direction, line)
                if not isinstance(_sent, tuple) or len(_sent) < 4:
                    continue
                _s_mid, _s_inn, _s_mkt, _s_dir = _sent[0], _sent[1], _sent[2], _sent[3]
                if _s_mid != match_id or _s_inn != innings:
                    continue
                # Map edge direction (OVER/UNDER) to copilot direction (YES/NO)
                _s_copilot_dir = _edge_dir_map.get(_s_dir, _s_dir)
                if _s_copilot_dir != direction:
                    # Edge detector sent opposite direction on any session market
                    _edge_contradicts = True
                    logger.info(
                        "Copilot session suppressed: %s %s contradicts edge signal %s %s",
                        direction, market, _s_dir, _s_mkt,
                    )
                    break
            if _edge_contradicts:
                continue

            # Suppress if this call contradicts an existing open position on a LONGER session.
            # e.g. we already have YES on innings — don't now send NO on 10-over.
            skip_call = False
            mkt_order = {"6_over": 1, "10_over": 2, "15_over": 3, "20_over": 4}
            this_rank = mkt_order.get(market, 0)
            for open_mkt, open_dir in open_positions.items():
                open_rank = mkt_order.get(open_mkt, 0)
                if open_rank > this_rank and open_dir != direction and edge < 10:
                    skip_call = True
                    logger.warning(
                        "Session suppressed: %s %s contradicts open %s %s",
                        direction, market, open_dir, open_mkt,
                    )
                    break

            if skip_call:
                continue

            signals.append({
                "type": "session",
                "direction": direction,
                "market": call["display_name"],
                "line": call["line"],
                "edge": edge,
                "model": model_prediction,
                "stake": call["stake"],
            })
            logger.info(
                "COPILOT SESSION: innings=%d %s %s %.0f",
                innings, direction, call["market"], call["line"],
            )

        # MW call
        model_home_prob = predictions.get("match_winner", {}).get("home_prob", 0.5)
        home_odds = 0
        away_odds = 0
        if config.get("match_winner_tracking", True) and cloudbet_odds and overs >= 2.0:
            mw = cloudbet_odds.get("match_winner", {})
            home_odds = mw.get("home_odds", 0)
            away_odds = mw.get("away_odds", 0)
            mw_call = self.copilot.evaluate_mw_call(match_id, home, away, model_home_prob, home_odds, away_odds)
            if mw_call:
                # Cross-check: don't back the BOWLING team's MW while also having a YES on
                # the batting team's innings total — those bets directly contradict each other.
                batting_team = state.batting_team or ""
                mw_team = mw_call["team"]
                mw_contradicts_session = False
                if batting_team and mw_team:
                    batting_is_mw_team = self._teams_fuzzy_match(batting_team, mw_team)
                    inn_pred = predictions.get("innings_total", {}).get("expected", 0)

                    # ── 1st innings check ──────────────────────────────────────────
                    # Don't back the bowling team when the batting team's model innings
                    # is over the bookmaker line (batting team is expected to score big).
                    if not batting_is_mw_team:
                        inn_line = cloudbet_odds.get("innings_total", {}).get("line", 0)
                        if inn_line > 0 and inn_pred > inn_line + 5:
                            mw_contradicts_session = True
                            logger.warning(
                                "MW suppressed (1st inn): backing %s (bowling) but model innings %.0f > line %.0f for %s",
                                mw_team, inn_pred, inn_line, batting_team,
                            )

                    # ── 2nd innings check ─────────────────────────────────────────
                    # Don't back the BATTING/chasing team when our own innings model
                    # predicts they won't reach the target, OR when the chase situation
                    # is objectively hopeless (RRR/wickets hard gate).
                    if batting_is_mw_team and innings == 2:
                        target = state.target_runs or 0
                        # No target set yet — can't evaluate chase, suppress
                        if target <= 0:
                            mw_contradicts_session = True
                            logger.warning("MW suppressed (2nd inn): no target set for %s", mw_team)
                        # Require model to predict comfortably above target (+10 buffer)
                        # before backing the chasing team — prevents boundary-cluster spikes
                        # from flipping the signal when chase is still very hard.
                        elif inn_pred > 0 and inn_pred < target + 10:
                            mw_contradicts_session = True
                            logger.warning(
                                "MW suppressed (2nd inn): backing %s (batting) but model innings %.0f < target+10 (%d)",
                                mw_team, inn_pred, target + 10,
                            )
                        elif self._chase_is_hopeless(state):
                            mw_contradicts_session = True

                # Don't flip MW sides during a match — if we already backed
                # team A (via edge detector or copilot), don't signal team B
                # unless it's explicitly a hedge.
                if not mw_contradicts_session:
                    _mw_lock_key = f"mw_backed:{match_id}"
                    _prev_backed = getattr(self, "_mw_backed_team", {}).get(_mw_lock_key)
                    if _prev_backed and _prev_backed != mw_call["team"]:
                        mw_contradicts_session = True
                        logger.info(
                            "MW suppressed (copilot): already backed %s, won't flip to %s "
                            "(hedge via CB HEDGE OPPORTUNITY instead)",
                            _prev_backed, mw_call["team"],
                        )

                if not mw_contradicts_session:
                    # Record which team we backed
                    if not hasattr(self, "_mw_backed_team"):
                        self._mw_backed_team = {}
                    self._mw_backed_team[f"mw_backed:{match_id}"] = mw_call["team"]

                    fair_prob = model_home_prob if mw_call["team"] == home else (1.0 - model_home_prob)
                    signals.append({
                        "type": "mw",
                        "direction": mw_call["direction"],
                        "team": mw_call["team"],
                        "odds": mw_call["odds"],
                        "ev_pct": mw_call["ev_pct"],
                        "fair_prob": fair_prob,
                        "stake": mw_call["stake"],
                    })
                    logger.info("COPILOT MW: %s %s @ %.2f", mw_call["direction"], mw_call["team"], mw_call["odds"])

            # MW swing
            if home_odds > 0 and away_odds > 0:
                swing = self.copilot.check_mw_swing(match_id, home, away, home_odds, away_odds)
                if swing:
                    signals.append({
                        "type": "mw_swing",
                        "direction": "",
                        "detail": f"{swing['direction']} ({swing['swing_pct']:.0f}% swing)",
                    })

        # Book opportunities
        current_mw_odds = {}
        if cloudbet_odds and "match_winner" in cloudbet_odds:
            mw_d = cloudbet_odds["match_winner"]
            if mw_d.get("home_odds"): current_mw_odds[home] = mw_d["home_odds"]
            if mw_d.get("away_odds"): current_mw_odds[away] = mw_d["away_odds"]

        books = self.copilot.check_book_opportunities(
            match_id, est_lines, current_mw_odds, current_innings=innings,
        )
        for book in books:
            signals.append({
                "type": "book",
                "direction": "",
                "action": book.action,
                "profit": book.guaranteed_profit,
            })
            logger.info("COPILOT BOOK: %s profit=%.0f", book.market, book.guaranteed_profit)
            # Auto-book shadow position
            if book.market == "match_winner":
                for pos in self.copilot.position_book.get_open_mw(match_id):
                    if book.exit_odds:
                        self.copilot.position_book.book_mw(pos, book.exit_odds)
                        break
            else:
                for pos in self.copilot.position_book.get_open_sessions(match_id, innings=innings):
                    if pos.market == book.market and book.exit_line:
                        self.copilot.position_book.book_session(pos, book.exit_line)
                        break

        # ── Cloudbet MW hedge check ───────────────────────────────────────────────
        # Checks ACTUAL Cloudbet open bets (not just shadow positions).
        # If we backed team X and their odds have shortened significantly, we
        # back the opponent to lock in guaranteed profit.
        # Formula: hedge_stake = entry_stake × entry_odds / opponent_odds
        if self.bet_executor and cloudbet_odds and "match_winner" in cloudbet_odds:
            _mw_cb = cloudbet_odds["match_winner"]
            _mw_sels = _mw_cb.get("selections", {})
            _cb_home_odds = _mw_sels.get("home", {}).get("price", 0.0)
            _cb_away_odds = _mw_sels.get("away", {}).get("price", 0.0)
            _hedge_min_move = float(self.config.get("cloudbet_hedge_min_odds_move", 0.35))

            for _ref, _cb_bet in list(self.bet_executor.open_bets.items()):
                # Only active MW bets for this match
                if _cb_bet.market != "match_winner":
                    continue
                if _cb_bet.status not in ("PENDING", "ACCEPTED"):
                    continue
                if (_cb_bet.home_team != home and _cb_bet.away_team != home
                        and _cb_bet.home_team != away and _cb_bet.away_team != away):
                    continue

                _entry_odds = _cb_bet.price
                _entry_stake = _cb_bet.stake_usd
                _backed_team = _cb_bet.direction  # team name we backed

                # Find current odds for the team we backed vs opponent
                _is_home = self._teams_fuzzy_match(_backed_team, home)
                _cur_backed_odds = _cb_home_odds if _is_home else _cb_away_odds
                _opp_team = away if _is_home else home
                _cur_opp_odds = _cb_away_odds if _is_home else _cb_home_odds

                if _cur_backed_odds <= 0 or _cur_opp_odds <= 0:
                    continue

                # Hedge condition: backed team odds have shortened (they're now favoured)
                # Entry odds - current odds >= threshold (we entered at longer price)
                _odds_move = _entry_odds - _cur_backed_odds
                if _odds_move < _hedge_min_move:
                    continue

                # Calculate hedge stake and guarantee
                _hedge_stake = round(_entry_stake * _entry_odds / _cur_opp_odds, 2)
                # Scenario A: backed team wins → collect entry_stake×(entry_odds-1), lose hedge_stake
                _if_backed_wins = round(_entry_stake * (_entry_odds - 1) - _hedge_stake, 2)
                # Scenario B: opponent wins → lose entry_stake, collect hedge_stake×(opp_odds-1)
                _if_opp_wins = round(-_entry_stake + _hedge_stake * (_cur_opp_odds - 1), 2)
                _guaranteed = round(min(_if_backed_wins, _if_opp_wins), 2)

                # Dedup: only alert once per odds band (0.10-wide buckets)
                _odds_band = int(_cur_backed_odds * 10)
                _hedge_dedup_key = f"cb_hedge:{_ref[:8]}:{_odds_band}"
                if _hedge_dedup_key in self.copilot._sent_calls:
                    continue
                self.copilot._sent_calls.add(_hedge_dedup_key)

                logger.info(
                    "CB HEDGE OPPORTUNITY: backed=%s entry=%.2f cur=%.2f move=%.2f "
                    "hedge=%s @ %.2f stake=$%.2f guarantee=$%.2f",
                    _backed_team, _entry_odds, _cur_backed_odds, _odds_move,
                    _opp_team, _cur_opp_odds, _hedge_stake, _guaranteed,
                )

                # Build hedge alert message
                _hedge_msg = (
                    f"💰 HEDGE NOW — {home} vs {away}\n"
                    f"  Entry: {_backed_team} @ {_entry_odds:.2f} × ${_entry_stake:.2f}\n"
                    f"  Now: {_backed_team} @ {_cur_backed_odds:.2f} (moved {-_odds_move:+.2f})\n"
                    f"  → Back {_opp_team} @ {_cur_opp_odds:.2f} × ${_hedge_stake:.2f}\n"
                    f"  Locks in: ${_guaranteed:.2f} guaranteed\n"
                    f"  (if {_backed_team} wins: ${_if_backed_wins:.2f} | "
                    f"if {_opp_team} wins: ${_if_opp_wins:.2f})"
                )

                self.telegram.send_alert_sync(_hedge_msg, channel=None, is_signal=False)
                print(f"\n{_hedge_msg}")

                # Auto-place hedge if configured and guarantee is meaningful
                _auto_hedge = self.config.get("cloudbet_auto_hedge", False)
                _min_hedge_profit = float(self.config.get("cloudbet_hedge_min_profit", 0.50))
                if _auto_hedge and _guaranteed >= _min_hedge_profit and not _cb_bet.paper:
                    _hedge_market_url = self._build_market_url(
                        "match_winner", _opp_team, 0.0, cloudbet_odds
                    )
                    _hedge_event_id = self._find_cloudbet_event_id(home, away, match_id=match_id)
                    if _hedge_market_url and _hedge_event_id:
                        _hedge_bet = self.bet_executor.place_bet(
                            event_id=str(_hedge_event_id),
                            market_url=_hedge_market_url,
                            price=_cur_opp_odds,
                            stake=_hedge_stake,
                            market="match_winner",
                            direction=_opp_team,
                            line=0.0,
                            home=home,
                            away=away,
                            ev_pct=0.0,
                            trigger="HEDGE",
                            innings=innings,
                            current_overs=overs,
                        )
                        if _hedge_bet:
                            _placed = self.bet_executor.format_bet_placed(_hedge_bet)
                            self.telegram.send_alert_sync(
                                f"✅ HEDGE PLACED\n{_placed}", channel=None, is_signal=False
                            )
                            logger.info(
                                "HEDGE PLACED: %s @ %.2f stake=$%.2f ref=%s",
                                _opp_team, _cur_opp_odds, _hedge_stake, _hedge_bet.reference_id[:8],
                            )
                        else:
                            logger.warning("HEDGE FAILED: %s @ %.2f", _opp_team, _cur_opp_odds)
        # ── End Cloudbet MW hedge check ───────────────────────────────────────────

        # 3. Ball-by-ball is handled by _send_ball_commentary (called before _run_copilot)
        # Over summary at end of each over (with MW odds + positions)
        over_updates = config.get("over_by_over_updates", True)
        if over_updates:
            over_num = int(overs)
            if over_num > 0 and self.copilot.should_send_over_update(match_id, over_num, innings=innings):
                pos_lines = []
                for p in self.copilot.position_book.get_open_sessions(match_id, innings=innings):
                    est_cur = est_lines.get(p.market, {}).get("no", 0)
                    diff = est_cur - p.entry_line if p.direction == "YES" else p.entry_line - est_cur
                    # Replace underscores in market names to avoid breaking Telegram Markdown
                    mkt_display = p.market.replace("_", "-")
                    pos_lines.append(f"{p.direction} {p.entry_line:.0f} ({mkt_display}): {diff:+.0f}")
                mw_data = cloudbet_odds.get("match_winner", {}) if cloudbet_odds else {}

                over_runs_val = state.over_runs.get(over_num - 1, 0) if over_num > 0 else 0

                if fmt_over_summary:
                    msg = fmt_over_summary(
                        over_num=over_num, innings=innings, batting_team=state.batting_team,
                        score=state.total_runs, wickets=state.wickets,
                        run_rate=state.current_run_rate,
                        projected_total=predictions.get("innings_total", {}).get("expected", 0),
                        over_runs=over_runs_val,
                        home=home, away=away,
                        mw_home_odds=mw_data.get("home_odds", 0),
                        mw_away_odds=mw_data.get("away_odds", 0),
                        positions_summary=" | ".join(pos_lines) if pos_lines else "",
                        player_adjustment=predictions.get("innings_total", {}).get("player_adj"),
                    )
                    # Over summary uses plain text — no parse_mode to avoid underscore issues
                    # (market names like 6_over, 10_over have underscores that break Markdown)
                    self.telegram.send_alert_sync(msg, channel=None, is_signal=False, parse_mode=None)
                    self.copilot.mark_message_sent()
                    # Attach any remaining signals to the over summary via old format
                    if signals and fmt_over_update:
                        msg = fmt_over_update(
                            over_num=over_num, innings=innings, batting_team=state.batting_team,
                            score=state.total_runs, wickets=state.wickets,
                            run_rate=state.current_run_rate,
                            projected_total=predictions.get("innings_total", {}).get("expected", 0),
                            player_adjustment=predictions.get("innings_total", {}).get("player_adj"),
                            active_batsmen=state.active_batsmen,
                            active_bowler=state.active_bowler,
                            mw_home_odds=mw_data.get("home_odds", 0),
                            mw_away_odds=mw_data.get("away_odds", 0),
                            home=home, away=away,
                            positions_summary=" | ".join(pos_lines) if pos_lines else "",
                            signals=signals,
                        )
                        self.telegram.send_alert_sync(msg, channel=None, is_signal=False, parse_mode=None)
                        signals = []

                    logger.info(
                        "COPILOT OVER SUMMARY: innings=%d over=%d score=%d/%d",
                        innings, over_num, state.total_runs, state.wickets,
                    )

        # 4. Bundle all session signals into one message (cleaner than one per session)
        # Session signals are actionable trading calls — never throttled by can_send_message().
        session_sigs = [s for s in signals if s.get("type") == "session"]
        if session_sigs and fmt_session_bundle:
            # Normalise market display names (6_over → 6-over etc.)
            for s in session_sigs:
                s["market"] = s["market"].replace("_", "-")
            bowling_team = state.bowling_team or (away if state.batting_team == home else home)
            bundle_msg = fmt_session_bundle(
                signals=session_sigs,
                batting_team=state.batting_team or home,
                bowling_team=bowling_team,
                score=state.total_runs,
                wickets=state.wickets,
                overs=state.overs_completed,
                innings=innings,
                target=state.target_runs or 0,
                first_innings_total=(state.target_runs - 1) if state.target_runs else 0,
            )
            if bundle_msg:  # may be empty if consistency check dropped all signals
                self.telegram.send_alert_sync(bundle_msg, channel=_ch, is_signal=True, parse_mode=None)
                self.copilot.mark_message_sent()
            # Remove session signals — already sent in bundle
            signals = [s for s in signals if s.get("type") != "session"]

        # Send remaining non-session signals (MW, book, mw_swing)
        #    Trading signals (MW, book) are never throttled — they're actionable.
        for sig in signals:
            sig_type = sig.get("type", "")
            if sig_type == "mw" and fmt_mw_call:
                msg = fmt_mw_call(
                    team=sig["team"], direction=sig["direction"],
                    odds=sig["odds"], stake=sig["stake"],
                    fair_prob=sig["fair_prob"], home=home, away=away)
                # MW signals private only — model not reliable enough for MW yet
                self.telegram.send_alert_sync(msg, channel=None, is_signal=False, parse_mode=None)
            elif sig_type == "book" and fmt_book_alert:
                msg = fmt_book_alert(
                    market_type="", market_name="",
                    action=sig["action"], guaranteed_profit=sig["profit"],
                    math_breakdown="")
                self.telegram.send_alert_sync(msg, channel=None, is_signal=False, parse_mode=None)
            elif sig_type == "mw_swing" and fmt_mw_swing and self.copilot.can_send_message():
                msg = fmt_mw_swing(
                    team_moved=sig["detail"], old_odds=0, new_odds=0,
                    home=home, away=away,
                    home_odds=home_odds, away_odds=away_odds,
                    model_prob=model_home_prob)
                self.telegram.send_alert_sync(msg, channel=None, is_signal=False, parse_mode=None)
                self.copilot.mark_message_sent()

    def _send_ball_commentary(
        self, match_id: int, home: str, away: str,
        state: MatchState, new_balls: int,
        previous_state: MatchState | None, channel: str,
    ) -> None:
        """Send ball-by-ball commentary to Telegram — runs on every new ball."""
        if not fmt_ball_commentary:
            return
        if not self.config.get("ball_by_ball_updates", True):
            return

        prev_ball_count = len(previous_state.balls) if previous_state else 0
        new_ball_events = state.balls[prev_ball_count:]

        if not new_ball_events:
            return

        # Get predictions for projection (use cached if available)
        predictions = self._prev_predictions.get(match_id, {})
        projected = predictions.get("innings_total", {}).get("expected", 0)
        if not projected:
            projected = state.projected_innings_total()

        # Build running totals for each ball (not just final state)
        prev_runs = previous_state.total_runs if previous_state else 0
        prev_wickets = previous_state.wickets if previous_state else 0
        running_runs = prev_runs
        running_wickets = prev_wickets

        for i, ball_event in enumerate(new_ball_events):
            running_runs += ball_event.get("runs", 0)
            if ball_event.get("is_wicket"):
                running_wickets += 1

            is_last = (i == len(new_ball_events) - 1)

            # Use summary-corrected score for the last ball (Sportmonks sometimes corrects)
            score = state.total_runs if is_last else running_runs
            wickets = state.wickets if is_last else running_wickets

            over_num = ball_event.get("over", 0)
            ball_num = ball_event.get("ball", 0)
            overs_display = f"{over_num}.{ball_num}"

            # Calculate running RR for this ball
            balls_so_far = prev_ball_count + i + 1
            running_rr = running_runs / (balls_so_far / 6.0) if balls_so_far > 0 else 0.0

            msg = fmt_ball_commentary(
                ball=ball_event,
                batting_team=state.batting_team,
                score=score,
                wickets=wickets,
                overs_display=overs_display,
                run_rate=state.current_run_rate if is_last else running_rr,
                active_batsmen=state.active_batsmen,
                active_bowler=state.active_bowler,
                projected_total=projected,
                target=state.target_runs,
                innings=state.current_innings,
                home=home, away=away,
            )
            # Ball commentary uses plain text — no parse_mode to avoid issues with
            # special chars in player names or emoji variation selectors
            self.telegram.send_alert_sync(msg, channel=None, is_signal=False, parse_mode=None)

        logger.info(
            "BALL-BY-BALL: %s %d/%d (%d new balls)",
            state.batting_team, state.total_runs, state.wickets, len(new_ball_events),
        )

    @staticmethod
    def _chase_is_hopeless(state: MatchState) -> bool:
        """Return True when the 2nd-innings chase situation makes a win essentially impossible.

        This is a hard cricket-logic gate — fires regardless of what the model says,
        because the model can be slow to update on rapidly deteriorating chases.

        Rules (any one triggers suppression):
          1. Required Run Rate > 16  (physically impossible to sustain)
          2. RRR > 12 AND 5+ wickets down  (tail batting at high run rate)
          3. 7+ wickets down AND still need > 30 runs  (effectively all out soon)
          4. Need > 85% of target still with ≤ 6 overs left (no hope even with all wickets)
        """
        if state.current_innings != 2:
            return False
        target = state.target_runs or 0
        if target <= 0:
            return False
        overs_done = state.overs_completed
        overs_left = max(20.0 - overs_done, 0.1)
        runs_needed = target - state.total_runs
        wickets = state.wickets

        if runs_needed <= 0:
            return False  # already won

        rrr = runs_needed / overs_left

        if rrr > 16:
            logger.warning(
                "MW chase hopeless: RRR %.1f > 16 (%d/%d @ %.1f ov, need %d from %.1f ov)",
                rrr, state.total_runs, wickets, overs_done, runs_needed, overs_left,
            )
            return True
        if rrr > 10.5 and wickets >= 5:
            # 10.5+ per over with half the team gone is a very difficult chase
            logger.warning(
                "MW chase hopeless: RRR %.1f > 10.5 with %d wkts down",
                rrr, wickets,
            )
            return True
        if wickets >= 7 and runs_needed > 30:
            logger.warning(
                "MW chase hopeless: %d wickets down, need %d runs",
                wickets, runs_needed,
            )
            return True
        if overs_left <= 6 and runs_needed > 0.85 * target:
            logger.warning(
                "MW chase hopeless: need %d (85%%+ of target) from %.1f overs",
                runs_needed, overs_left,
            )
            return True
        return False

    @staticmethod
    def _teams_fuzzy_match(name1: str, name2: str) -> bool:
        """Fuzzy match team names — handles Sportmonks vs Cloudbet naming differences.

        Requires first word match (team city) to avoid false positives like
        'Chennai Super Kings' matching 'Lucknow Super Giants'.
        """
        n1 = name1.lower().strip()
        n2 = name2.lower().strip()
        # Exact or substring
        if n1 in n2 or n2 in n1:
            return True
        # First word (city name) MUST match
        w1 = n1.split()
        w2 = n2.split()
        if w1 and w2 and w1[0] == w2[0]:
            return True
        # First 6+ chars match (handles "Rawalpindiz" vs "Rawalpindi Pindiz")
        min_len = min(len(n1), len(n2), 6)
        if min_len >= 5 and n1[:min_len] == n2[:min_len]:
            return True
        return False

    def _extract_match_info(self, match: dict) -> tuple:
        def _get_name(team_data):
            if isinstance(team_data, dict):
                if "data" in team_data:
                    return team_data["data"].get("name", "?")
                return team_data.get("name", "?")
            return "?"

        home = _get_name(match.get("localteam", {}))
        away = _get_name(match.get("visitorteam", {}))
        venue_data = match.get("venue", {})
        if isinstance(venue_data, dict):
            venue = venue_data.get("data", venue_data).get("name", "Unknown") if "data" in venue_data or "name" in venue_data else "Unknown"
        else:
            venue = "Unknown"
        return home, away, venue

    @staticmethod
    def _count_new_balls(previous_state: MatchState | None, current_state: MatchState) -> int:
        if previous_state is None:
            return len(current_state.balls)

        if previous_state.current_innings != current_state.current_innings:
            return len(current_state.balls)

        prev_ids = {ball.get("source_id") for ball in previous_state.balls if ball.get("source_id") is not None}
        current_ids = [ball.get("source_id") for ball in current_state.balls if ball.get("source_id") is not None]
        if prev_ids and current_ids:
            return sum(1 for ball_id in current_ids if ball_id not in prev_ids)

        return max(0, len(current_state.balls) - len(previous_state.balls))

    @staticmethod
    def _state_changed(previous_state: MatchState | None, current_state: MatchState) -> bool:
        if previous_state is None:
            return True

        return any((
            previous_state.current_innings != current_state.current_innings,
            previous_state.total_runs != current_state.total_runs,
            previous_state.wickets != current_state.wickets,
            abs(previous_state.overs_completed - current_state.overs_completed) > 1e-6,
            previous_state.batting_team != current_state.batting_team,
        ))

    @staticmethod
    def _get_batting_team_side(state: MatchState, home: str, away: str) -> str | None:
        batting = (state.batting_team or "").strip().lower()
        if batting == (home or "").strip().lower():
            return "home"
        if batting == (away or "").strip().lower():
            return "away"
        return None

    @staticmethod
    def _normalise_toss_decision(decision: Any) -> Optional[str]:
        if decision is None:
            return None
        value = str(decision).strip().lower()
        if not value:
            return None
        return {
            "batting": "bat",
            "bowling": "bowl",
        }.get(value, value)

    def _parse_toss_info(self, match: dict) -> dict[str, Any]:
        teams: dict[int, str] = {}
        for key in ("localteam", "visitorteam"):
            team = match.get(key) or {}
            if not isinstance(team, dict):
                continue
            team_data = team.get("data") if isinstance(team.get("data"), dict) else team
            team_id = team_data.get("id")
            team_name = team_data.get("name")
            if team_id is not None and team_name:
                teams[int(team_id)] = team_name

        toss_winner_team_id = match.get("toss_won_team_id")
        toss_decision = self._normalise_toss_decision(match.get("elected"))
        toss_winner = teams.get(int(toss_winner_team_id)) if toss_winner_team_id is not None else None

        if toss_winner and toss_decision:
            return {
                "toss_winner": toss_winner,
                "toss_decision": toss_decision,
                "toss_pending": False,
                "toss_available": True,
            }

        status = str(match.get("status", "")).lower()
        toss_pending = not match.get("live") and "innings" not in status and "live" not in status
        return {
            "toss_winner": toss_winner,
            "toss_decision": toss_decision,
            "toss_pending": toss_pending,
            "toss_available": False if not toss_pending else None,
        }

    def _has_open_market_bet(self, market: str, innings: int, home: str, away: str) -> bool:
        if not self.bet_executor:
            return False
        return self.bet_executor.has_open_bet(
            market_key=market,
            innings=innings,
            home=home,
            away=away,
        )

    def _is_direction_locked(
        self,
        match_id: int,
        market: str,
        innings: int,
        direction: str,
    ) -> bool:
        if not direction:
            return False
        locked_direction = self.edge_detector.get_locked_direction(match_id, market, innings)
        return bool(locked_direction and locked_direction != direction)

    def _should_skip_edge(
        self,
        match_id: int,
        market: str,
        innings: int,
        home: str,
        away: str,
        direction: str = "",
    ) -> bool:
        if self._has_open_market_bet(market, innings, home, away):
            logger.info(
                "EDGE SKIP: %s innings=%d already has an open bet for %s vs %s",
                market,
                innings,
                home,
                away,
            )
            return True

        if self._is_direction_locked(match_id, market, innings, direction):
            locked = self.edge_detector.get_locked_direction(match_id, market, innings)
            logger.info(
                "EDGE SKIP: %s %s innings=%d locked to %s",
                market,
                direction,
                innings,
                locked,
            )
            return True

        # Check copilot shadow positions — if we already signalled the OPPOSITE
        # direction to the user (for Indian book), optionally block the auto-bet
        # to avoid contradicting the user's position.
        if self.config.get("autobet_respect_shadow_positions", True) and self.copilot and direction:
            _market_key = {
                "6-over": "6_over", "10-over": "10_over", "15-over": "15_over",
                "20-over": "20_over", "innings_total": "innings_total",
                "innings total": "innings_total",
            }.get(market.lower(), market)

            _dir_map = {"OVER": "YES", "YES": "OVER", "UNDER": "NO", "NO": "UNDER"}
            _opposite = {"OVER": "UNDER", "UNDER": "OVER", "YES": "NO", "NO": "YES"}
            _opp_dir = _opposite.get(direction.upper(), "")

            for pos in self.copilot.position_book.get_open_sessions(match_id, innings=innings):
                if pos.market != _market_key:
                    continue
                _pos_dir = pos.direction.upper()
                # Shadow position direction in YES/NO — convert to OVER/UNDER for comparison
                _pos_dir_mapped = _dir_map.get(_pos_dir, _pos_dir)
                if _pos_dir in (_opp_dir, _dir_map.get(_opp_dir, "")):
                    logger.warning(
                        "AUTOBET BLOCKED: %s %s contradicts copilot shadow position %s %s — "
                        "user may have bet Indian book based on that signal",
                        market, direction, _market_key, pos.direction,
                    )
                    # Alert user that model flipped direction
                    try:
                        _ch = self.match_info.get(match_id, {}).get("competition", "ipl")
                        _state = self.active_matches.get(match_id)
                        _score_str = f"{_state.total_runs}/{_state.wickets} ({_state.overs_completed:.1f} ov)" if _state else ""
                        _flip_msg = (
                            f"⚠️ MODEL FLIP — {home} vs {away}\n"
                            f"  {market}: signal was {pos.direction} @ {pos.entry_line:.0f}, "
                            f"model now says {direction}\n"
                            f"  Score: {_score_str}\n"
                            f"  Cloudbet auto-bet BLOCKED — check Indian book position manually"
                        )
                        self.telegram.send_alert_sync(_flip_msg, channel=None, is_signal=False)
                    except Exception:
                        pass
                    return True

        return False

    @staticmethod
    def _market_has_live_prices(market_data: dict | None, price_keys: tuple[str, ...]) -> bool:
        if not isinstance(market_data, dict):
            return False
        for key in price_keys:
            value = market_data.get(key)
            try:
                if float(value) > 0:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    def _has_live_speed_market(self, cloudbet_odds: dict | None) -> bool:
        if not isinstance(cloudbet_odds, dict):
            return False

        innings_market = cloudbet_odds.get("innings_total")
        match_winner_market = cloudbet_odds.get("match_winner")
        return (
            self._market_has_live_prices(innings_market, ("over_odds", "under_odds"))
            and self._market_has_live_prices(match_winner_market, ("home_odds", "away_odds"))
        )

    def _classify_speed_trigger(
        self,
        state: MatchState,
        cloudbet_odds: dict | None,
        trigger: dict[str, Any] | None = None,
    ) -> tuple[bool, bool, str]:
        if not self.speed_edge:
            return False, False, ""

        overs_completed = state.overs_completed
        if self.speed_edge.should_suppress_alert(overs_completed):
            return True, False, f"match in final overs ({overs_completed:.1f})"

        if not self._has_live_speed_market(cloudbet_odds):
            return True, False, "markets suspended"

        if trigger and not self.speed_edge.should_suggest_innings_total(overs_completed):
            action = str(trigger.get("recommended_action", "")).lower()
            if "innings total" in action:
                return True, False, f"innings total nearly settled ({overs_completed:.1f})"

        return False, self.speed_edge.should_shorten_alert(overs_completed), ""

    def _enforce_session_direction_consistency(
        self,
        match_id: int,
        innings: int,
        edges: List[dict],
    ) -> List[dict]:
        """Keep session-market directions consistent within a single scan.

        If both OVER and UNDER appear across 6/10/12/15/20-over session markets,
        keep only the stronger side (by summed absolute edge) and drop the rest.
        """
        session_markets = {
            "6_over", "powerplay_runs", "10_over", "12_over", "15_over", "20_over",
        }
        session_edges = [
            e for e in edges
            if e.get("market") in session_markets and e.get("direction") in ("OVER", "UNDER")
        ]
        directions = {e.get("direction") for e in session_edges}
        if len(directions) <= 1:
            return edges

        strength = {"OVER": 0.0, "UNDER": 0.0}
        max_edge = {"OVER": 0.0, "UNDER": 0.0}
        for edge in session_edges:
            direction = edge.get("direction")
            try:
                edge_abs = abs(float(edge.get("edge_runs", 0.0) or 0.0))
            except (TypeError, ValueError):
                edge_abs = 0.0
            strength[direction] += edge_abs
            if edge_abs > max_edge[direction]:
                max_edge[direction] = edge_abs

        if (strength["OVER"], max_edge["OVER"]) >= (strength["UNDER"], max_edge["UNDER"]):
            keep_direction = "OVER"
        else:
            keep_direction = "UNDER"

        filtered: List[dict] = []
        dropped: List[str] = []
        for edge in edges:
            market = edge.get("market")
            direction = edge.get("direction")
            if market in session_markets and direction in ("OVER", "UNDER") and direction != keep_direction:
                dropped.append(f"{market}:{direction}")
                continue
            filtered.append(edge)

        if dropped:
            logger.warning(
                "Session consistency filter: match=%s inn=%d mixed directions -> keep=%s drop=%s",
                match_id,
                innings,
                keep_direction,
                ",".join(dropped),
            )
        return filtered

    def _iter_session_edges(
        self,
        match_id: int,
        home: str,
        away: str,
        state: MatchState,
        predictions: dict,
        cloudbet_odds: dict,
    ) -> List[dict]:
        """Return model edges for active session markets only.

        Wait to observe the match before betting:
          6_over  → bet only after 2 overs
          10_over → bet only after 4 overs
          15_over → bet only after 6 overs
          20_over → bet only after 8 overs
        """
        edges: List[dict] = []
        innings = state.current_innings
        overs = state.overs_completed
        session_keys: List[str] = []

        # Minimum overs before generating an edge (and sending a Telegram signal).
        # Kept very low so early signals reach Telegram for manual Indian book trading.
        # The auto-bet gate (2-over minimum) is enforced separately in _send_edge_alert
        # JOB 2 — so Telegram fires early but Cloudbet only bets after 2 overs.
        # innings_total / 20_over still need 10 overs — too noisy for any signal before that.
        MIN_OVERS_BEFORE_BET = {
            "6_over": 0.1,
            "powerplay_runs": 0.1,
            "10_over": float(self.config.get("signal_min_overs_10_over", 5.0)),
            "15_over": 7.0,
            "20_over": float(self.config.get("signal_min_overs_20_over", 10.0)),
            "innings_total": float(self.config.get("signal_min_overs_20_over", 10.0)),
        }

        if "6_over" in cloudbet_odds:
            session_keys.append("6_over")
        elif "powerplay_runs" in cloudbet_odds:
            session_keys.append("powerplay_runs")

        for market_key in ("10_over", "15_over", "20_over"):
            if market_key in cloudbet_odds:
                session_keys.append(market_key)

        for market_key in session_keys:
            if is_completed_session_market(market_key, overs):
                continue
            if self._has_open_market_bet(market_key, innings, home, away):
                continue
            # Wait to observe match before betting
            min_overs = MIN_OVERS_BEFORE_BET.get(market_key, 2.0)
            if overs < min_overs:
                continue

            odds_data = cloudbet_odds.get(market_key)
            prediction = self.predictor.get_prediction_for_market(predictions, market_key)
            if not odds_data or not prediction or "line" not in odds_data:
                continue

            edge = self.edge_detector.evaluate_line(
                market=market_key,
                model_expected=prediction["expected"],
                model_std_dev=prediction["std_dev"],
                bookmaker_line=odds_data["line"],
                over_odds=odds_data.get("over_odds", 1.9),
                under_odds=odds_data.get("under_odds", 1.9),
            )
            if edge and not self._should_skip_edge(
                match_id, market_key, innings, home, away, edge.get("direction", "")
            ):
                edges.append(
                    self._enrich_edge_with_prediction_context(edge, prediction, predictions)
                )

        return edges

    def _feed_new_balls(self, state: MatchState, balls_data: list) -> int:
        new_count = 0
        for i, raw_ball in enumerate(balls_data):
            if i < len(state.balls):
                continue  # Already processed

            parsed = self.cricket_client.parse_ball_event(raw_ball)
            over_ball = parsed.get("over_ball", 0.0)
            try:
                over_num = int(float(over_ball))
                ball_num = round((float(over_ball) - over_num) * 10)
            except (TypeError, ValueError):
                over_num, ball_num = 0, 1

            state.add_ball({
                "over": over_num,
                "ball": ball_num if ball_num > 0 else 1,
                "runs": parsed.get("runs", 0),
                "is_wicket": parsed.get("is_wicket", False),
                "extras": 0,
                "is_legal": parsed.get("is_legal", True),
            })
            new_count += 1
        return new_count

    def _sync_state_from_live_summary(self, state: MatchState, match: dict) -> None:
        """Use Sportmonks runs/venue summary as source of truth for live score display.

        Ball-by-ball can occasionally trail by a delivery or arrive out of order on
        live games. When Sportmonks provides an innings summary, prefer its score,
        wickets, overs, and venue so the displayed state stays aligned.
        """
        runs_data = match.get("runs") or []
        if isinstance(runs_data, dict):
            runs_data = runs_data.get("data", [])
        if not isinstance(runs_data, list) or not runs_data:
            return

        current_innings = 1
        if isinstance(state.current_innings, int):
            current_innings = state.current_innings

        summary = None
        for innings_summary in runs_data:
            if innings_summary.get("inning") == current_innings:
                summary = innings_summary
                break
        if summary is None:
            summary = runs_data[-1]

        overs = summary.get("overs")
        score = summary.get("score")
        wickets = summary.get("wickets")

        try:
            if overs is not None:
                overs_float = float(overs)
                state.overs_completed = overs_float
                completed_overs = int(overs_float)
                current_ball = int(round((overs_float - completed_overs) * 10))
                state.current_over = completed_overs
                state.current_ball = current_ball
                state.balls_faced = completed_overs * 6 + current_ball
        except (TypeError, ValueError):
            pass

        try:
            if score is not None:
                state.total_runs = int(score)
        except (TypeError, ValueError):
            pass

        try:
            if wickets is not None:
                state.wickets = int(wickets)
        except (TypeError, ValueError):
            pass

        venue_data = match.get("venue", {})
        if isinstance(venue_data, dict):
            venue_name = venue_data.get("name")
            if "data" in venue_data and isinstance(venue_data["data"], dict):
                venue_name = venue_data["data"].get("name", venue_name)
            if venue_name:
                state.venue = venue_name

    # ── Model predictions ──────────────────────────────────────────────

    def _run_predictions(self, match_id: int, state: MatchState, competition: str = "ipl") -> dict:
        # Set competition-specific rates (IPL vs PSL)
        self.predictor.set_competition(competition)

        venue_stats = self.stats_db.get_venue_stats(state.venue)
        rates = self.predictor._rates()
        venue_avg = venue_stats.get("avg_first_innings") or rates["default_venue_avg"]
        venue_avg_pp = venue_stats.get("avg_powerplay_1st") or rates["pp_base_1st"]

        # ── Blend with series DB (current season) if available ────────
        # Historical stats (8000+ matches) give a stable baseline.
        # Series stats (this season at this venue) capture recent pitch
        # and team trends.  Blend: 70% historical + 30% series.
        # As the series progresses and gets more data, it becomes more
        # reliable — but we keep historical as the anchor.
        sdb = self._series_dbs.get(competition)
        if sdb:
            try:
                svs = sdb.get_venue_series_stats(state.venue)
                if svs.get("matches", 0) >= 2:
                    # Enough series data to blend
                    if svs.get("avg_inn1"):
                        venue_avg = 0.7 * venue_avg + 0.3 * svs["avg_inn1"]
                    if svs.get("avg_pp_inn1"):
                        venue_avg_pp = 0.7 * venue_avg_pp + 0.3 * svs["avg_pp_inn1"]
                    logger.debug(
                        "Series blend for %s: venue_avg=%.1f (hist+series), pp=%.1f",
                        state.venue, venue_avg, venue_avg_pp,
                    )
            except Exception:
                pass  # series DB query failed, use historical only

        info = self.match_info.get(match_id, {})

        # Build InningsState for resource-aware predictions
        _innings_state = None
        try:
            from modules.innings_state import build_innings_state
            _squad = None

            # Try ESPN first (most reliable for playing XI)
            if self.espn:
                try:
                    _squads = self.espn.get_squads_for_match(
                        info.get("home", ""), info.get("away", ""), competition
                    )
                    if _squads:
                        for side in ("home", "away"):
                            s = _squads.get(side, {})
                            if s.get("team", "").lower() in (state.batting_team or "").lower() or \
                               (state.batting_team or "").lower() in s.get("team", "").lower():
                                _squad = s.get("players", [])
                                break
                except Exception:
                    pass

            # Fallback: build squad from the batting card itself
            # If we have no ESPN data, the batting card at least tells us who has batted
            # and the bowling card tells us the opponent's bowlers (who can also bat).
            # This gives InningsState SOME resource info even without squad data.
            if not _squad and state.batting_card:
                # We can't know who's yet to bat, but we CAN tell InningsState
                # about the players who HAVE batted (for out/batting status).
                # build_innings_state handles squad=None gracefully — it just
                # won't populate batters_yet_to_bat.
                pass

            _name_matcher = getattr(self.predictor, "name_matcher", None) if hasattr(self, "predictor") else None
            _innings_state = build_innings_state(
                match_state=state,
                squad=_squad,
                player_db=self.stats_db,
                name_matcher=_name_matcher,
            )
        except Exception:
            logger.debug("InningsState build failed", exc_info=True)

        # Cache innings_state for use in _send_edge_alert chase gating
        if _innings_state is not None:
            self._innings_states[match_id] = _innings_state

        predictions = self.predictor.predict(
            state,
            home=info.get("home"),
            away=info.get("away"),
            venue_avg=venue_avg,
            venue_avg_pp=venue_avg_pp,
            innings_state=_innings_state,
        )

        # ── Override with XGBoost model when available ───────────────
        # XGBoost is trained on 1454 real matches; replaces hardcoded base rates.
        # Player adjustments from predictor.py still apply on top.
        if self.ml_predictor and self.ml_predictor.available:
            try:
                ml = self.ml_predictor.predict_from_state(state, competition=competition, innings_state=_innings_state)
                if ml:
                    # innings_total → overrides predictions["innings_total"]["expected"]
                    if "innings_total" in predictions and "innings_total" in ml:
                        old = predictions["innings_total"]["expected"]
                        new = max(float(state.total_runs), ml["innings_total"])
                        predictions["innings_total"]["expected"] = round(new, 1)
                        predictions["innings_total"]["ml_raw"] = ml["innings_total"]
                        predictions["innings_total"]["stats_expected"] = old

                    # powerplay_total
                    if "powerplay_total" in predictions and "pp_total" in ml:
                        predictions["powerplay_total"]["expected"] = round(ml["pp_total"], 1)
                        predictions["powerplay_runs"]["expected"] = round(ml["pp_total"], 1)

                    # fifteen_over_total (xgb middle + pp)
                    if "fifteen_over_total" in predictions and "middle_total" in ml and "pp_total" in ml:
                        ml_15 = ml["pp_total"] + ml["middle_total"]
                        predictions["fifteen_over_total"]["expected"] = round(ml_15, 1)

                    logger.info(
                        "XGBoost predictions: innings=%.1f pp=%.1f mid=%.1f death=%.1f",
                        ml.get("innings_total", 0), ml.get("pp_total", 0),
                        ml.get("middle_total", 0), ml.get("death_total", 0),
                    )
            except Exception:
                logger.warning("ML prediction override failed", exc_info=True)

        return predictions

    @staticmethod
    def _enrich_edge_with_prediction_context(
        edge: dict[str, Any],
        prediction: dict[str, Any] | None,
        predictions: dict[str, Any],
    ) -> dict[str, Any]:
        enriched = dict(edge)
        if prediction:
            if "base_expected" in prediction:
                enriched["base_expected"] = prediction["base_expected"]
            if "player_adj" in prediction:
                enriched["player_adj"] = prediction["player_adj"]
        player_context = predictions.get("player_context")
        if player_context:
            enriched["player_context"] = player_context
        return enriched

    # ── Cloudbet odds (single match, fine for rate limit) ────────────

    def _fetch_cloudbet_odds_for_match(
        self,
        home: str,
        away: str,
        competition: str = "ipl",
        batting_team_side: str | None = None,
    ) -> Optional[dict]:
        """Fetch Cloudbet odds for the current live match only. 1 API call per scan."""
        if self.odds_client is None:
            return None

        try:
            events = self.odds_client.get_events(competition)
            for event in events:
                if event.get("type") != "EVENT_TYPE_EVENT":
                    continue
                # Match by team names — fuzzy matching using first word overlap
                h = (event.get("home") or {}).get("name", "")
                a = (event.get("away") or {}).get("name", "")
                if not h or not a:
                    continue
                # Try both orderings — Cloudbet home/away can differ from Sportmonks
                matched = (
                    (self._teams_fuzzy_match(home, h) and self._teams_fuzzy_match(away, a)) or
                    (self._teams_fuzzy_match(home, a) and self._teams_fuzzy_match(away, h))
                )
                if matched:
                    # Detect if home/away are swapped vs Sportmonks
                    cb_swapped = self._teams_fuzzy_match(home, a)  # SM home == CB away
                    # Pass batting_team_side relative to Cloudbet's home/away ordering
                    cb_batting_side = batting_team_side
                    if cb_swapped:
                        cb_batting_side = "away" if batting_team_side == "home" else "home"
                    logger.info("Cloudbet matched: %s vs %s → %s vs %s%s",
                                home, away, h, a, " [swapped]" if cb_swapped else "")
                    all_odds = self.odds_client.get_all_market_odds(
                        event,
                        batting_team_side=cb_batting_side,
                    )
                    # If home/away are swapped, normalise MW odds back to Sportmonks ordering
                    # so that model_home_prob (Sportmonks home) compares against the right odds.
                    if cb_swapped and all_odds and "match_winner" in all_odds:
                        mw = all_odds["match_winner"]
                        all_odds["match_winner"] = {
                            **mw,
                            "home_odds":       mw.get("away_odds", 0),
                            "away_odds":       mw.get("home_odds", 0),
                            "home_market_url": mw.get("away_market_url", ""),
                            "away_market_url": mw.get("home_market_url", ""),
                            "selections": {
                                "home": mw.get("selections", {}).get("away", {}),
                                "away": mw.get("selections", {}).get("home", {}),
                            },
                        }
                        logger.debug("MW odds normalised for Cloudbet swap: home=%.2f away=%.2f",
                                     all_odds["match_winner"]["home_odds"],
                                     all_odds["match_winner"]["away_odds"])
                    # Track in odds tracker
                    if self.odds_tracker and all_odds:
                        for mkt_key, mkt_data in all_odds.items():
                            if mkt_data:
                                self.odds_tracker.record_snapshot(event.get("id"), mkt_key, mkt_data)
                    mkt_count = len(all_odds) if all_odds else 0
                    logger.info("Cloudbet odds: %d markets found", mkt_count)
                    return all_odds
            logger.info("Cloudbet: no match found for %s vs %s in %d events", home, away, len(events))
        except Exception as exc:
            logger.warning("Cloudbet odds fetch failed: %s", exc)

        return None

    def _check_edges(self, match_id: int, home: str, away: str, state: MatchState,
                     predictions: dict, cloudbet_odds: dict) -> None:
        """Compare model predictions vs Cloudbet lines and alert on edges."""
        edges = self._iter_session_edges(match_id, home, away, state, predictions, cloudbet_odds)
        innings = state.current_innings

        if bool(self.config.get("strict_session_direction_consistency", True)):
            edges = self._enforce_session_direction_consistency(match_id, innings, edges)

        # Innings total — wait at least 10 overs before betting (model too noisy early)
        inn_line = None
        if state.overs_completed >= 10.0 and "20_over" not in cloudbet_odds and not self._has_open_market_bet("innings_total", innings, home, away):
            inn_line = cloudbet_odds.get("innings_total")
        inn_pred = predictions.get("innings_total")
        if inn_line and inn_pred and "line" in inn_line:
            edge = self.edge_detector.evaluate_line(
                market="innings_total",
                model_expected=inn_pred["expected"],
                model_std_dev=inn_pred["std_dev"],
                bookmaker_line=inn_line["line"],
                over_odds=inn_line.get("over_odds", 1.9),
                under_odds=inn_line.get("under_odds", 1.9),
            )
            if edge and not self._should_skip_edge(
                match_id, "innings_total", innings, home, away, edge.get("direction", "")
            ):
                edges.append(
                    self._enrich_edge_with_prediction_context(edge, inn_pred, predictions)
                )

        # Over runs (per-over lines)
        for key, odds_data in cloudbet_odds.items():
            if self._has_open_market_bet(key, innings, home, away):
                continue
            if key.startswith("over_runs") and isinstance(odds_data, dict) and "line" in odds_data:
                next_pred = predictions.get("next_over")
                if next_pred:
                    edge = self.edge_detector.evaluate_line(
                        market=key,
                        model_expected=next_pred["expected"],
                        model_std_dev=next_pred["std_dev"],
                        bookmaker_line=odds_data["line"],
                        over_odds=odds_data.get("over_odds", 1.9),
                        under_odds=odds_data.get("under_odds", 1.9),
                    )
                    if edge and not self._should_skip_edge(
                        match_id, key, innings, home, away, edge.get("direction", "")
                    ):
                        edges.append(
                            self._enrich_edge_with_prediction_context(edge, next_pred, predictions)
                        )

        # Match winner edge detection
        _cfg = self.config if isinstance(getattr(self, "config", None), dict) else {}
        try:
            _mw_min_overs = float(_cfg.get("match_winner_min_overs", 2.0))
        except (TypeError, ValueError):
            _mw_min_overs = 2.0

        winner_odds = None if self._has_open_market_bet("match_winner", innings, home, away) else cloudbet_odds.get("match_winner")
        if winner_odds and state.overs_completed < _mw_min_overs:
            logger.info(
                "MW suppressed: %.1f overs < min %.1f",
                state.overs_completed,
                _mw_min_overs,
            )
            winner_odds = None
        if winner_odds:
            sels = winner_odds.get("selections", {})
            h_price = sels.get("home", {}).get("price", 0)
            a_price = sels.get("away", {}).get("price", 0)

            # Use The Odds API consensus if available, otherwise use our model probability
            home_fair_prob = None
            away_fair_prob = None
            if self.theodds:
                fair = self.theodds.get_fair_probability(home, away)
                if fair and fair.get("bookmakers_count", 0) > 0:
                    home_fair_prob = fair["home_fair_prob"]
                    away_fair_prob = fair["away_fair_prob"]
            if home_fair_prob is None:
                # Fallback: use model win probability from predictor
                mw_pred = predictions.get("match_winner", {})
                home_fair_prob = mw_pred.get("home_prob")
                away_fair_prob = 1.0 - home_fair_prob if home_fair_prob is not None else None

            # 2nd innings MW contradiction helper — reuse same logic as _run_copilot.
            # If the chasing team is predicted to fall short of the target, don't signal
            # or bet on them as match winner regardless of the odds edge.
            _inn_pred = predictions.get("innings_total", {}).get("expected", 0)
            _target = state.target_runs or 0

            def _mw_suppressed_2nd_inn(team: str) -> bool:
                if innings != 2:
                    return False
                batting = state.batting_team or ""
                # If we have no target set yet, we can't evaluate the chase —
                # suppress to avoid backing the chasing team with no data.
                if _target <= 0:
                    logger.warning("MW suppressed (2nd inn, no target set): %s", team)
                    return True
                # Hard gate: chase situation is objectively hopeless (RRR/wickets)
                if self._chase_is_hopeless(state):
                    if batting and self._teams_fuzzy_match(batting, team):
                        return True
                # Soft gate: model predicts chasing team falls short
                if _inn_pred <= 0:
                    return False
                if not batting:
                    return False
                chasing_team_backed = self._teams_fuzzy_match(batting, team)
                if chasing_team_backed and _inn_pred < _target + 10:
                    logger.warning(
                        "MW suppressed (2nd inn, _check_edges): backing %s (batting) "
                        "but model innings %.0f < target+10 (%d)",
                        team, _inn_pred, _target + 10,
                    )
                    return True
                return False

            # 1st innings MW contradiction gate: don't back the batting team
            # when our session signals are UNDER (projected below baseline).
            # In Indian book terms: if we're saying NO on runs, we should be
            # backing the BOWLING team (or staying out), not the batting team.
            def _mw_contradicts_session(team: str) -> bool:
                if innings != 1:
                    return False
                batting = state.batting_team or ""
                if not batting:
                    return False
                backing_batting = self._teams_fuzzy_match(batting, team)
                if not backing_batting:
                    return False
                # Check if session UNDER edges exist in this scan
                has_under = any(
                    e.get("direction") == "UNDER"
                    for e in edges
                    if e.get("market") != "match_winner"
                )
                # Also check if projected total is below venue baseline
                proj = _inn_pred or predictions.get("innings_total", {}).get("expected", 0)
                venue_mod = self.predictor.get_venue_modifier(state.venue) if hasattr(self, "predictor") else 0
                baseline = (self.predictor._rates().get("default_venue_avg", 166) + venue_mod) if hasattr(self, "predictor") else 166
                proj_below_baseline = proj > 0 and proj < baseline - 5

                if has_under and proj_below_baseline:
                    logger.info(
                        "MW suppressed (1st inn contradiction): backing batting "
                        "team %s but UNDER signals active and proj %.0f < baseline %.0f",
                        team, proj, baseline,
                    )
                    return True
                return False

            # Don't flip MW sides — if we already backed a team, suppress the other
            def _mw_already_locked(team: str) -> bool:
                if not hasattr(self, "_mw_backed_team"):
                    self._mw_backed_team = {}
                lock_key = f"mw_backed:{match_id}"
                prev = self._mw_backed_team.get(lock_key)
                if prev and prev != team:
                    logger.info(
                        "MW suppressed (edge): already backed %s, won't flip to %s",
                        prev, team,
                    )
                    return True
                return False

            def _mw_record_lock(team: str) -> None:
                if not hasattr(self, "_mw_backed_team"):
                    self._mw_backed_team = {}
                self._mw_backed_team[f"mw_backed:{match_id}"] = team

            if home_fair_prob and h_price > 1:
                edge = self.edge_detector.evaluate_match_winner(
                    model_win_prob=home_fair_prob,
                    bookmaker_odds=h_price,
                    team=home,
                )
                if edge and not self._should_skip_edge(
                    match_id, "match_winner", innings, home, away, edge.get("team", "")
                ) and not _mw_suppressed_2nd_inn(home) and not _mw_contradicts_session(home) and not _mw_already_locked(home):
                    _mw_record_lock(home)
                    edges.append(edge)
            if away_fair_prob and a_price > 1:
                edge = self.edge_detector.evaluate_match_winner(
                    model_win_prob=away_fair_prob,
                    bookmaker_odds=a_price,
                    team=away,
                )
                if edge and not self._should_skip_edge(
                    match_id, "match_winner", innings, home, away, edge.get("team", "")
                ) and not _mw_suppressed_2nd_inn(away) and not _mw_contradicts_session(away) and not _mw_already_locked(away):
                    _mw_record_lock(away)
                    edges.append(edge)

        for edge in edges:
            self._send_edge_alert(
                match_id,
                edge,
                home,
                away,
                cloudbet_odds,
                trigger="MODEL_EDGE",
                innings=innings,
                current_overs=state.overs_completed,
            )

    # ── Terminal display ───────────────────────────────────────────────

    def _print_match_state(self, match_id: int, home: str, away: str, state: MatchState,
                           predictions: dict = None, cloudbet_odds: dict = None) -> None:
        projected = state.projected_innings_total()
        phase = state.phase.upper()

        print(f"\n{'='*65}")
        print(f"  🏏 {home} vs {away}  |  {state.total_runs}/{state.wickets} ({state.overs_completed:.1f} ov)")
        print(f"  Phase: {phase}  |  RR: {state.current_run_rate:.2f}  |  Projected: {projected:.0f}")
        print(f"{'─'*65}")

        # Phase breakdown
        pp = state.get_phase_runs("powerplay")
        mid = state.get_phase_runs("middle")
        death = state.get_phase_runs("death")
        if pp > 0:
            print(f"  PP(1-6): {pp}", end="")
        if mid > 0:
            print(f"  |  Mid(7-15): {mid}", end="")
        if death > 0:
            print(f"  |  Death(16-20): {death}", end="")
        if pp > 0 or mid > 0:
            print()

        # Model predictions
        print(f"{'─'*65}")
        print(f"  📊 MODEL PREDICTIONS:")

        venue_stats = self.stats_db.get_venue_stats(state.venue)
        venue_avg = venue_stats.get("avg_first_innings") or 172.0

        innings_pred = self.predictor.predict_innings_total(state, venue_avg=venue_avg)
        print(f"     Innings Total: {innings_pred['expected']:.0f}  (±{innings_pred['std_dev']:.0f})  [{innings_pred['confidence']}]")

        if state.phase == "powerplay":
            pp_pred = self.predictor.predict_powerplay_total(
                state.batting_team, state.bowling_team, state.venue,
                venue_avg_pp=venue_stats.get("avg_powerplay_1st"),
            )
            print(f"     Powerplay:     {pp_pred['expected']:.0f}  (±{pp_pred['std_dev']:.0f})  [{pp_pred['confidence']}]")

        next_over = self.predictor.predict_next_over_runs(state)
        print(f"     Next Over:     {next_over['expected']:.1f}  (±{next_over['std_dev']:.1f})")

        # Venue context
        matches_at_venue = venue_stats.get("matches", 0)
        if matches_at_venue > 0:
            print(f"  📍 Venue avg: {venue_avg:.0f} ({matches_at_venue} matches)")

        # Cloudbet live odds (fancy markets)
        if cloudbet_odds:
            print(f"{'─'*65}")
            print(f"  💰 CLOUDBET LIVE ODDS:")
            for mkt_key, mkt_data in cloudbet_odds.items():
                if mkt_data is None:
                    continue
                if mkt_key == "match_winner":
                    sels = mkt_data.get("selections", {})
                    hp = sels.get("home", {}).get("price", "?")
                    ap = sels.get("away", {}).get("price", "?")
                    print(f"     🏏 Winner: {home} @ {hp}  |  {away} @ {ap}")
                elif "line" in mkt_data:
                    line = mkt_data["line"]
                    over = mkt_data.get("over_odds", "?")
                    under = mkt_data.get("under_odds", "?")
                    team = mkt_data.get("team", "")
                    market_label = market_display_name(mkt_key)
                    label = f"{market_label} ({team})" if team else market_label
                    # Show model comparison if available
                    model_val = ""
                    if predictions:
                        pred = self.predictor.get_prediction_for_market(predictions, mkt_key) or {}
                        if pred and "expected" in pred:
                            diff = pred["expected"] - line
                            arrow = "↑" if diff > 0 else "↓"
                            model_val = f"  ← Model: {pred['expected']:.1f} ({arrow}{abs(diff):.1f})"
                    print(f"     📊 {label}: {line}  O:{over} U:{under}{model_val}")

        # Consensus match winner odds (from The Odds API, 27 bookmakers)
        if self.theodds and self.theodds.enabled:
            fair = self.theodds.get_fair_probability(home, away)
            if fair and fair.get("bookmakers_count", 0) > 0:
                print(f"{'─'*65}")
                hp = fair["home_fair_prob"] * 100
                ap = fair["away_fair_prob"] * 100
                print(f"  💰 MARKET CONSENSUS ({fair['bookmakers_count']} books):")
                print(f"     {home}: {hp:.1f}% (best {fair['home_best_odds']} @ {fair['home_best_book']})")
                print(f"     {away}: {ap:.1f}% (best {fair['away_best_odds']} @ {fair['away_best_book']})")
                if fair.get("pinnacle_home"):
                    print(f"     📌 Pinnacle: {home} @ {fair['pinnacle_home']}  |  {away} @ {fair['pinnacle_away']}")
                if self.theodds.requests_remaining is not None:
                    print(f"     (API quota: {self.theodds.requests_remaining} remaining)")

        print(f"{'='*65}")

    # ── Sharp model shift detection ────────────────────────────────────

    def _check_model_shifts(self, match_id: int, home: str, away: str, state: MatchState, predictions: dict) -> None:
        """Detect when model predictions shift sharply — these are edge opportunities."""
        prev = self._prev_predictions.get(match_id)
        if prev is None:
            return

        # No model shift alerts after 17 overs — too late to act
        if state.overs_completed >= 17.0:
            return

        # Cooldown — don't send more than once per 3 minutes per match
        now = time.time()
        last_sent = self._last_model_shift_sent.get(match_id, 0.0)
        if now - last_sent < self._model_shift_cooldown:
            return

        shifts = []

        # Only alert on significant moves — 20+ runs (was 10, too noisy)
        curr_total = predictions.get("innings_total", {}).get("expected", 0)
        prev_total = prev.get("innings_total", {}).get("expected", 0)
        if prev_total > 0 and abs(curr_total - prev_total) >= 20:
            direction = "↑" if curr_total > prev_total else "↓"
            shifts.append(f"Innings Total: {prev_total:.0f} → {curr_total:.0f} {direction}")

        if shifts:
            _ch = self.match_info.get(match_id, {}).get("competition", "ipl")
            msg = f"Model shift: {home} vs {away} ({state.total_runs}/{state.wickets}, {state.overs_completed:.1f} ov)\n"
            for s in shifts:
                msg += f"  {s}\n"

            print(f"\n{msg}")
            logger.info("Model shift detected: %s", shifts)
            self._last_model_shift_sent[match_id] = now

    # ── On-demand odds fetch ───────────────────────────────────────────

    def fetch_odds_now(self, match_id: int = None) -> None:
        """
        Manually fetch Cloudbet odds and compare against model.
        Call this when YOU want to check — doesn't run automatically.
        """
        if self.odds_client is None:
            print("❌ OddsClient not configured — add cloudbet_api_key to config")
            return

        try:
            events = self.odds_client.get_ipl_events()
        except Exception as e:
            print(f"❌ Failed to fetch Cloudbet odds: {e}")
            return

        if not events:
            print("No IPL events on Cloudbet right now")
            return

        for event in events:
            eid = event.get("id")
            if event.get("type") != "EVENT_TYPE_EVENT":
                continue

            home_data = event.get("home") or {}
            away_data = event.get("away") or {}
            home = home_data.get("name", "?") if isinstance(home_data, dict) else "?"
            away = away_data.get("name", "?") if isinstance(away_data, dict) else "?"

            if match_id and eid != match_id:
                continue

            print(f"\n{'='*65}")
            print(f"  💰 CLOUDBET ODDS — {home} vs {away}")
            print(f"{'─'*65}")

            all_odds = self.odds_client.get_all_market_odds(event)
            for mkt_type, odds_data in all_odds.items():
                if odds_data is None:
                    continue

                if mkt_type == "match_winner":
                    sels = odds_data.get("selections", {})
                    h_price = sels.get("home", {}).get("price", "?")
                    a_price = sels.get("away", {}).get("price", "?")
                    print(f"  🏏 Winner: {home} @ {h_price}  |  {away} @ {a_price}")
                elif "line" in odds_data:
                    line = odds_data.get("line", "?")
                    over = odds_data.get("over_odds", "?")
                    under = odds_data.get("under_odds", "?")
                    team = odds_data.get("team", "")
                    label = f"{mkt_type} ({team})" if team else mkt_type
                    print(f"  📊 {label}: Line {line}  |  O: {over}  U: {under}")

                    # Compare with model if we have active match state
                    self._compare_with_model(mkt_type, odds_data, home, away)

                # Track in odds tracker
                if self.odds_tracker:
                    self.odds_tracker.record_snapshot(eid, mkt_type, odds_data)

            print(f"{'='*65}")

    def _compare_with_model(self, market: str, odds_data: dict, home: str, away: str) -> None:
        """Print model vs bookmaker comparison for a market."""
        # Find matching active match
        for mid, info in self.match_info.items():
            if info["home"] == home and info["away"] == away:
                state = self.active_matches.get(mid)
                if state is None:
                    continue

                predictions = self._run_predictions(mid, state)

                prediction = self.predictor.get_prediction_for_market(predictions, market)
                if prediction and "expected" in prediction:
                    model = prediction["expected"]
                    line = odds_data.get("line", 0)
                    diff = model - line
                    arrow = "OVER ↑" if diff > 0 else "UNDER ↓"
                    print(
                        f"        → {market_display_name(market)}: Model {model:.0f} vs Line: {line}  "
                        f"({arrow} by {abs(diff):.1f})"
                    )
                break

    # ── Match Dossier ────────────────────────────────────────────────

    def _build_match_dossier(self, match_id: int, home: str, away: str, venue: str,
                              state: MatchState, competition: str) -> None:
        """Build player + venue dossier from live match data."""
        if not self.match_dossier:
            return
        try:
            dossier = self.match_dossier.build_from_sportmonks(
                match_id, home, away, venue,
                batting_card=state.batting_card,
                bowling_card=state.bowling_card,
                competition=competition.upper(),
                batting_first=state.batting_team or "",
            )
            if dossier:
                msg = self.match_dossier.format_dossier(match_id)
                _ch = self.match_info.get(match_id, {}).get("competition", "ipl")
                self.telegram.send_alert_sync(msg, channel=None, is_signal=False)
                logger.info("Match dossier sent: %s vs %s (%d players)", home, away, len(dossier.get("players", {})))
        except Exception:
            logger.warning("Failed to build match dossier", exc_info=True)

    # ── Alerts ─────────────────────────────────────────────────────────

    def _send_pre_match_report(self, match_id: int, home: str, away: str, venue: str, match: dict) -> None:
        _ch = self.match_info.get(match_id, {}).get("competition", "ipl")
        venue_stats = self.stats_db.get_venue_stats(venue) or {}
        toss_info = self._parse_toss_info(match)
        report = {
            "home": home,
            "away": away,
            "venue": venue,
            "venue_avg_score": venue_stats.get("avg_first_innings", "N/A"),
            "venue_avg_first_innings": venue_stats.get("avg_first_innings", "N/A"),
            "venue_avg_second_innings": venue_stats.get("avg_second_innings", "N/A"),
            "model_predicted_total": venue_stats.get("avg_first_innings", 172),
            "model_home_score": venue_stats.get("avg_first_innings", 172),
            "model_away_score": venue_stats.get("avg_second_innings", 165),
            **toss_info,
        }
        message = format_pre_match_report(report)
        print(f"\n{message}")
        logger.info("Pre-match report sent: %s vs %s (channel=%s)", home, away, _ch)
        self.telegram.send_alert_sync(message, channel=_ch, is_signal=False)

        # PSL only: send rich pre-match context (no live line available for PSL)
        if _ch == "psl" and self.psl_context:
            try:
                ctx = self.psl_context.build(match_id, home, away, venue)
                psl_msg = self.psl_context.format_telegram(ctx)

                # Enrich with ESPN playing XI if available
                if self.espn:
                    squads = self.espn.get_squads_for_match(home, away, "psl")
                    if squads:
                        lines = ["\n🔢 <b>Playing XI (ESPN)</b>"]
                        for side in ("home", "away"):
                            squad = squads.get(side, {})
                            team  = squad.get("team", "")
                            players = squad.get("players", [])
                            if players:
                                xi = []
                                for p in players:
                                    tag = ""
                                    if p.get("captain"): tag += " (C)"
                                    if p.get("keeper"):  tag += " (WK)"
                                    role = p.get("role_abbr", "")
                                    xi.append(f"{p['name']}{tag}" + (f" [{role}]" if role else ""))
                                lines.append(f"<b>{team}</b>: {', '.join(xi)}")
                        psl_msg += "\n" + "\n".join(lines)
                        logger.info("ESPN playing XI added to PSL context")

                self.telegram.send_alert_sync(psl_msg, channel=_ch, is_signal=False, parse_mode="HTML")
                logger.info("PSL pre-match context sent: %s vs %s", home, away)
            except Exception:
                logger.warning("PSL context failed", exc_info=True)

        # Fetch and send pre-match news intelligence
        if self.news_intel and self.news_intel.enabled:
            try:
                from datetime import date
                today = date.today().strftime("%Y-%m-%d")
                competition = _ch.upper()
                intel = self.news_intel.get_pre_match_intel(
                    home, away, venue=venue, competition=competition, date=today,
                )
                if intel:
                    intel_msg = self.news_intel.format_intel_report(intel, home, away, venue)
                    print(f"\n{intel_msg}")
                    logger.info("Pre-match intel sent: %s vs %s", home, away)
                    self.telegram.send_alert_sync(intel_msg, channel=None, is_signal=False)

                    # Parse through LLM for structured intel + model adjustments
                    if self.llm_intel and self.llm_intel.enabled:
                        try:
                            llm_parsed = self.llm_intel.parse_news_intel(
                                intel, home, away, venue=venue, competition=competition,
                            )
                            if llm_parsed:
                                llm_msg = self.llm_intel.format_llm_report(llm_parsed, home, away)
                                print(f"\n{llm_msg}")
                                logger.info("LLM intel sent: %s vs %s", home, away)
                                self.telegram.send_alert_sync(llm_msg, channel=None, is_signal=False)
                        except Exception:
                            logger.warning("LLM intel parsing failed", exc_info=True)
            except Exception:
                logger.warning("Failed to fetch pre-match intel", exc_info=True)

    def _send_edge_alert(self, match_id: int, edge: dict, home: str, away: str,
                         cloudbet_odds: dict = None, trigger: str = "MODEL_EDGE",
                         innings: int = 1, current_overs: float | None = None) -> None:
        """Two separate jobs:
        1. SIGNAL → Telegram (always, for Indian book) — low threshold
        2. AUTO-BET → Cloudbet (strict, context-checked) — high threshold
        """
        market = edge.get("market", "")
        direction = edge.get("direction", "")
        if market == "match_winner" and not direction:
            direction = edge.get("team", "")
            edge["direction"] = direction  # write back so should_bet() sees it
        line = edge.get("bookmaker_line", edge.get("odds", ""))
        ev_pct = edge.get("ev_pct", 0)
        odds = edge.get("odds", edge.get("bookmaker_odds", 1.9))
        confidence = edge.get("confidence", "LOW")
        edge_runs = abs(edge.get("edge_runs", 0))

        if current_overs is not None and is_completed_session_market(market, current_overs):
            return

        # Collect open bets for this match (used by both signal and autobet context checks)
        _open_bets_for_match = []
        if self.bet_executor:
            _primary = self.bet_executor.primary if hasattr(self.bet_executor, "primary") else self.bet_executor
            for _ob in _primary.open_bets.values():
                if _ob.home_team == home and _ob.away_team == away:
                    _open_bets_for_match.append({
                        "market": _ob.market,
                        "direction": _ob.direction,
                        "line": _ob.line,
                        "innings": _ob.innings,
                    })

        # Resolve series profile for this match
        _profile = self._match_profiles.get(match_id)

        # ── Per-market minimum overs gate ───────────────────────────────────────
        # Keep longer session markets from firing too early regardless of which
        # path produced the edge (normal model edge, speed edge, etc.).
        _active_state = self.active_matches.get(match_id)
        _overs_now = _active_state.overs_completed if _active_state else (current_overs or 0)
        _signal_min_overs_by_market = {
            "10_over": self.config.get("signal_min_overs_10_over", 5.0),
            "20_over": _profile.innings_total_min_overs if _profile else self.config.get("signal_min_overs_20_over", self.config.get("innings_total_min_overs", 10.0)),
            "innings_total": _profile.innings_total_min_overs if _profile else self.config.get("signal_min_overs_20_over", self.config.get("innings_total_min_overs", 10.0)),
        }
        _market_min_overs = _signal_min_overs_by_market.get(market)
        if _market_min_overs is not None and _overs_now < _market_min_overs:
            logger.info(
                "%s suppressed — only %.1f overs completed (min %.1f)",
                market,
                _overs_now,
                _market_min_overs,
            )
            if self.match_recorder and _active_state:
                try:
                    self.match_recorder.record_signal(
                        match_id, _active_state, signal_type="SESSION",
                        market=market, direction=direction,
                        line=float(line) if line else 0, odds=float(odds),
                        ev_pct=ev_pct, edge_runs=edge_runs,
                        action="SUPPRESSED",
                        suppression_reason=f"min_overs ({_overs_now:.1f} < {_market_min_overs:.1f})",
                        competition=_ch if '_ch' in dir() else "ipl",
                    )
                except Exception:
                    pass
            return
        # ─────────────────────────────────────────────────────────────────────────

        _ch = self.match_info.get(match_id, {}).get("competition", "ipl")

        # ── Model stability gate ─────────────────────────────────────────────
        # If the model prediction for this market is oscillating wildly,
        # suppress the signal to avoid whipsaw signals.
        if _profile and market not in ("match_winner", "over_runs"):
            _pred_key = (match_id, market)
            _pred_val = edge.get("model_expected", 0.0)
            if _pred_val:
                hist = self._prediction_history.setdefault(_pred_key, [])
                hist.append(_pred_val)
                _window = _profile.model_stability_window
                if len(hist) > _window + 1:
                    hist[:] = hist[-(_window + 1):]
                if len(hist) >= _window + 1:
                    _max_shift = max(
                        abs(hist[-i] - hist[-i - 1])
                        for i in range(1, _window + 1)
                    )
                    if _max_shift > _profile.model_max_shift_per_scan:
                        logger.info(
                            "SIGNAL SUPPRESSED: model unstable on %s (max_shift=%.1f > %.1f)",
                            market, _max_shift, _profile.model_max_shift_per_scan,
                        )
                        if self.match_recorder:
                            _rec_state = self.active_matches.get(match_id)
                            if _rec_state:
                                try:
                                    self.match_recorder.record_signal(
                                        match_id, _rec_state, signal_type="SESSION",
                                        market=market, direction=direction,
                                        line=float(line) if line else 0, odds=float(odds),
                                        ev_pct=ev_pct, edge_runs=edge_runs,
                                        action="SUPPRESSED",
                                        suppression_reason=f"model_unstable (shift={_max_shift:.1f})",
                                        competition=_ch,
                                    )
                                except Exception:
                                    pass
                        return

        # ══════════════════════════════════════════════════════════
        # JOB 1: SIGNAL → Telegram (for Indian book)
        # Low threshold — send on any decent edge, all phases
        # ══════════════════════════════════════════════════════════

        # over_runs is a single-over market (lasts ~3 minutes). It fires at the
        # same time as 10_over / 15_over signals and looks like spam / contradicts
        # them (different bowler shown, different timeframe). Suppress from Telegram
        # but still allow auto-bet in JOB 2 below.
        _skip_signal = market.startswith("over_runs")

        signal_min_ev = _profile.signal_min_ev_pct if _profile else self.config.get("signal_min_ev_pct", 5.0)
        signal_min_edge = _profile.signal_min_edge_runs if _profile else self.config.get("signal_min_edge_runs", 2.0)

        # v2.1: Chase state machine overrides signal_min_ev for 2nd innings
        _chase_info = None
        _score_state_for_chase = self.active_matches.get(match_id)
        _cached_innings_state = self._innings_states.get(match_id)
        if self.chase_state and _score_state_for_chase:
            try:
                _chase_info = self.chase_state.classify(_score_state_for_chase, _cached_innings_state)
                if _chase_info:
                    if _chase_info.get("suppress_signals"):
                        logger.info("SIGNAL SUPPRESSED: %s — %s", _chase_info.get("label", ""), market)
                        return
                    chase_min_ev = _chase_info.get("min_ev_pct", signal_min_ev)
                    if chase_min_ev > signal_min_ev:
                        signal_min_ev = chase_min_ev
            except Exception:
                pass

        # Apply live context gate to Telegram OVER signals too (session/innings totals)
        # so exposed-tail situations (e.g., 8 down) do not generate misleading YES alerts.
        _SIGNAL_CONTEXT_GATED_MARKETS = {
            "powerplay_runs", "6_over", "10_over", "15_over", "20_over", "innings_total",
        }
        _signal_context_suppressed = False
        _signal_state = self.active_matches.get(match_id)
        _early_chase_signal = bool(
            _signal_state is not None
            and _signal_state.current_innings == 2
            and float(_signal_state.overs_completed or 0.0) < 2.0
        )
        _chase_under_context_max_overs = float(self.config.get("signal_chase_under_context_max_overs", 10.5))
        _gate_under_in_chase = bool(
            _signal_state is not None
            and _signal_state.current_innings == 2
            and direction == "UNDER"
            and market in _SIGNAL_CONTEXT_GATED_MARKETS
            and float(_signal_state.overs_completed or 0.0) <= _chase_under_context_max_overs
        )
        _gate_signal_with_context = direction == "OVER" or _gate_under_in_chase
        if (
            not _skip_signal
            and self.match_context is not None
            and _gate_signal_with_context
            and market in _SIGNAL_CONTEXT_GATED_MARKETS
            and (not _early_chase_signal or direction == "UNDER")
        ):
            if _signal_state is not None:
                _signal_ok, _signal_reason = self.match_context.should_bet(
                    edge,
                    _signal_state,
                    match_id=match_id,
                    open_bets=_open_bets_for_match,
                )
                if not _signal_ok:
                    # Keep Telegram signals flowing on soft/early context vetoes
                    # while still suppressing high-risk or premature chase reads.
                    _hard_signal_veto = (
                        "Wicket pressure" in _signal_reason
                        or "5+ wickets down" in _signal_reason
                        or "Contradicts open" in _signal_reason
                        or "Death overs" in _signal_reason
                        or "Required RR" in _signal_reason
                        or "UNDER too early" in _signal_reason
                        or "Chase on track" in _signal_reason
                    )
                    if _hard_signal_veto:
                        _signal_context_suppressed = True
                        logger.info(
                            "SIGNAL SUPPRESSED: %s (%s %s)",
                            _signal_reason,
                            market,
                            direction,
                        )
                    else:
                        logger.info(
                            "SIGNAL CONTEXT SOFT-VETO IGNORED: %s (%s %s)",
                            _signal_reason,
                            market,
                            direction,
                        )

        # Dedup by (match, innings, market, direction) — NOT by line.
        # Once we tell clients "10-over NO", we don't send another "10-over NO"
        # at a different line. One directional call per market per innings is enough.
        signal_dedup = (match_id, innings, market, direction)
        if (
            not _skip_signal
            and not _signal_context_suppressed
            and signal_dedup not in self.alerts_sent
            and ev_pct >= signal_min_ev
        ):
            if market != "match_winner" or edge_runs == 0:
                # Session signal — check edge runs
                if edge_runs >= signal_min_edge or market == "match_winner":
                    # ── Direction-flip guard ──────────────────────────────────────
                    # Suppress if we recently sent the OPPOSITE direction on this market.
                    # Allow override only when: enough wickets fell OR edge is very large.
                    _sig_key = (match_id, innings, market)
                    _last_sig = self._last_signal_direction.get(_sig_key)
                    _suppress_flip = False
                    if _last_sig and market != "match_winner":
                        _elapsed = time.time() - _last_sig["fired_at"]
                        _state_now = self.active_matches.get(match_id)
                        _wickets_now = _state_now.wickets if _state_now else _last_sig.get("wickets", 0)
                        _wickets_delta = _wickets_now - _last_sig.get("wickets", 0)
                        _line_now = float(line) if line else 0.0
                        _line_delta = abs(_line_now - float(_last_sig.get("line", _line_now)))
                        _edge_delta = abs(edge_runs - float(_last_sig.get("edge_runs", edge_runs)))
                        _repeat_cooldown = float(self.config.get("signal_repeat_cooldown", self._signal_direction_cooldown))
                        _repeat_min_line_move = float(self.config.get("signal_repeat_min_line_move", 15.0))
                        _repeat_min_edge_change = float(self.config.get("signal_repeat_min_edge_change", 10.0))
                        if (
                            _last_sig["direction"] == direction
                            and _elapsed < _repeat_cooldown
                            and _line_delta < _repeat_min_line_move
                            and _edge_delta < _repeat_min_edge_change
                            and _wickets_delta < 1
                        ):
                            _suppress_flip = True
                            logger.info(
                                "SIGNAL SUPPRESSED (repeat %s on %s within %.0fs, line delta %.1f < %.1f, edge delta %.1f < %.1f)",
                                direction,
                                market,
                                _elapsed,
                                _line_delta,
                                _repeat_min_line_move,
                                _edge_delta,
                                _repeat_min_edge_change,
                            )
                        elif (
                            _last_sig["direction"] != direction
                            and _elapsed < self._signal_direction_cooldown
                            and edge_runs < self._signal_flip_min_edge
                            and _wickets_delta < 2  # allow flip after 2+ wickets fall
                        ):
                            _suppress_flip = True
                            logger.warning(
                                "SIGNAL SUPPRESSED (direction flip %s→%s on %s within %.0fs, "
                                "edge %.1f runs < %.0f, wickets delta %d): suppressing UNDER after OVER",
                                _last_sig["direction"], direction, market, _elapsed,
                                edge_runs, self._signal_flip_min_edge, _wickets_delta,
                            )
                    if not _suppress_flip:
                        _wickets_state = self.active_matches.get(match_id)
                        self._last_signal_direction[_sig_key] = {
                            "direction": direction,
                            "fired_at": time.time(),
                            "edge_runs": edge_runs,
                            "wickets": _wickets_state.wickets if _wickets_state else 0,
                            "line": float(line) if line else 0.0,
                        }
                    # ─────────────────────────────────────────────────────────────
                    if not _suppress_flip:
                        self.alerts_sent.add(signal_dedup)
                        timestamp = datetime.now(timezone.utc).strftime("%H:%M UTC")
                        message = format_edge_alert(home, away, edge, timestamp=timestamp)
                        # Append live scoreboard so the user can see match context
                        _score_state = self.active_matches.get(match_id)
                        if _score_state:
                            _sc = _score_state.total_runs
                            _wk = _score_state.wickets
                            _ov = _score_state.overs_completed
                            _rr = _score_state.current_run_rate
                            _tgt = _score_state.target_runs
                            _inn = _score_state.current_innings
                            _score_line = f"  Score: {_sc}/{_wk} ({_ov:.1f} ov) RR:{_rr:.1f}"
                            if _inn == 2 and _tgt:
                                _need = _tgt - _sc
                                _overs_left = round(20.0 - _ov, 1)
                                _score_line += f" | Need {_need} off {_overs_left} ov (target {_tgt})"
                            message = message + "\n" + _score_line
                        print(f"\n{message}")
                        logger.info("SIGNAL: %s %s %s (EV %.1f%%)", market, direction, line, ev_pct)
                        # MW signals private only — session signals go to client channel
                        _sig_channel = None if market == "match_winner" else _ch
                        self.telegram.send_alert_sync(message, channel=_sig_channel, is_signal=(market != "match_winner"))
                        # Record signal to match_recorder
                        if self.match_recorder:
                            try:
                                self.match_recorder.record_signal(
                                    match_id, _score_state or state if 'state' in dir() else self.active_matches.get(match_id),
                                    signal_type="SESSION" if market != "match_winner" else "MW",
                                    market=market, direction=direction,
                                    line=float(line) if line else 0, odds=float(odds),
                                    model_expected=edge.get("model_expected", 0),
                                    model_std_dev=edge.get("model_std_dev", 0),
                                    ev_pct=ev_pct, edge_runs=edge_runs,
                                    confidence=confidence, action="SENT",
                                    competition=_ch,
                                )
                            except Exception:
                                pass
                        # Shadow-trade the signal for v2 accuracy tracking
                        logger.info("V2 HOOKS: shadow=%s paper=%s market=%s dir=%s",
                                    bool(self.shadow_tracker), bool(self.paper_sim), market, direction)
                        if self.shadow_tracker:
                            try:
                                self.shadow_tracker.log_signal(
                                    match_id=match_id,
                                    home=home, away=away,
                                    venue=getattr(_score_state, "venue", "") if _score_state else "",
                                    signal_type="MW" if market == "match_winner" else "SESSION",
                                    direction=direction,
                                    market=market,
                                    entry_line=float(line) if line else 0.0,
                                    model_expected=edge.get("model_expected", 0),
                                    edge_runs=edge_runs,
                                    odds=float(odds),
                                    ev_pct=ev_pct,
                                    confidence=confidence,
                                    stake=self.config.get("shadow_default_stake_inr", 500),
                                )
                            except Exception:
                                logger.warning("ShadowTracker log failed", exc_info=True)
                        # Paper simulator — place virtual bet on every signal (sessions + MW)
                        if self.paper_sim:
                            try:
                                _ps_state = _score_state or (self.active_matches.get(match_id) if hasattr(self, 'active_matches') else None)
                                self.paper_sim.place_bet(
                                    match_id=match_id,
                                    home=home, away=away,
                                    venue=getattr(_ps_state, "venue", "") if _ps_state else "",
                                    competition=_ch or "ipl",
                                    innings=innings,
                                    market=market,
                                    direction=direction,
                                    line=float(line) if line else 0.0,
                                    odds=float(odds),
                                    ev_pct=ev_pct,
                                    edge_runs=edge_runs,
                                    model_expected=edge.get("model_expected", 0),
                                    confidence=confidence,
                                    overs=getattr(_ps_state, "overs_completed", 0) if _ps_state else 0,
                                    score=getattr(_ps_state, "total_runs", 0) if _ps_state else 0,
                                    wickets=getattr(_ps_state, "wickets", 0) if _ps_state else 0,
                                )
                            except Exception:
                                logger.warning("PaperSimulator bet failed", exc_info=True)
                        # Lock direction on signal send (not just bet placement)
                        # This prevents later auto-bets from contradicting the sent signal
                        if market != "match_winner":
                            self.edge_detector.lock_market(match_id, market, direction, innings)

        # ══════════════════════════════════════════════════════════
        # JOB 2: AUTO-BET → Cloudbet (strict checks)
        # High threshold — only bet when model + context + player data all agree
        # ══════════════════════════════════════════════════════════
        if not self.bet_executor or not self.risk_manager:
            return

        # Match winner autobets disabled — sessions only on Cloudbet
        if market == "match_winner":
            logger.info("AUTOBET DISABLED: match_winner auto-bet disabled (session-only mode)")
            return

        # Honor profile's alert-only mode (unknown/conservative series)
        if _profile and _profile.alert_on_edge_only:
            logger.info("AUTOBET DISABLED: series profile alert_on_edge_only=True (%s)", _profile.series_key)
            return

        # Stricter thresholds for auto-betting (from series profile)
        autobet_min_ev = _profile.autobet_min_ev_pct if _profile else self.config.get("autobet_min_ev_pct", 10.0)
        autobet_min_edge = _profile.autobet_min_edge_runs if _profile else self.config.get("autobet_min_edge_runs", 4.0)

        if ev_pct < autobet_min_ev:
            logger.debug("AUTOBET SKIP: EV %.1f%% below autobet threshold %.1f%%", ev_pct, autobet_min_ev)
            return

        if market != "match_winner" and edge_runs < autobet_min_edge:
            logger.debug("AUTOBET SKIP: edge %.1f runs below threshold %.1f", edge_runs, autobet_min_edge)
            return

        # ── Early-overs session auto-bet gate ────────────────────────────────
        # For session markets (6-over, 10-over, 15-over) don't auto-bet before
        # 2 overs — model has very little data before that and signals are noisy.
        # Telegram signal still fires from 0.1 overs (JOB 1) so the user can trade
        # manually on the Indian book if they judge it worthy.
        # Exception: a very strong EV (>= session_autobet_early_ev, default 20%)
        # overrides the minimum — a rare line mistake big enough to act on immediately.
        # Per-market minimum overs for Cloudbet auto-bet.
        # Shorter sessions need less data; longer sessions need more overs
        # before the line is reliable and the position has enough value.
        # Config can override per-market: "session_autobet_min_overs_15": 8.0
        _SESSION_AUTOBET_MIN_OVERS = {
            "6_over":        _profile.session_autobet_min_overs_6 if _profile else self.config.get("session_autobet_min_overs_6", 2.0),
            "powerplay_runs": _profile.session_autobet_min_overs_6 if _profile else self.config.get("session_autobet_min_overs_pp", 2.0),
            "10_over":       _profile.session_autobet_min_overs_10 if _profile else self.config.get("session_autobet_min_overs_10", 4.0),
            "15_over":       _profile.session_autobet_min_overs_15 if _profile else self.config.get("session_autobet_min_overs_15", 7.0),
        }
        _SESSION_EARLY_GATE = set(_SESSION_AUTOBET_MIN_OVERS.keys())
        if market in _SESSION_EARLY_GATE:
            _early_state = self.active_matches.get(match_id)
            _overs_now = _early_state.overs_completed if _early_state else (current_overs or 0)
            _session_min_overs = _SESSION_AUTOBET_MIN_OVERS[market]
            _early_override_ev = _profile.session_autobet_early_ev if _profile else self.config.get("session_autobet_early_ev", 20.0)
            if _overs_now < _session_min_overs and ev_pct < _early_override_ev:
                logger.info(
                    "AUTOBET SKIP: %s at %.1f overs (min %.1f ov, EV %.1f%% < override %.0f%%) "
                    "— signal sent to Telegram, no Cloudbet bet yet",
                    market, _overs_now, _session_min_overs, ev_pct, _early_override_ev,
                )
                return
        # ─────────────────────────────────────────────────────────────────────

        # ── Late-innings auto-bet cutoff ─────────────────────────────────────
        # innings_total / 20_over cutoff is innings-dependent:
        #   1st innings: bet up to just before 19 overs (total still being set)
        #   2nd innings: cut off at 17 overs (chase can end abruptly, risky)
        # Signal still goes to Telegram (JOB 1) so user can act manually on Indian book.
        _LATE_CUTOFF_MARKETS = {"innings_total", "20_over"}
        if market in _LATE_CUTOFF_MARKETS:
            _late_state = self.active_matches.get(match_id)
            _overs_now = _late_state.overs_completed if _late_state else (current_overs or 0)
            _inn_now = (_late_state.current_innings if _late_state else innings) or innings
            if _inn_now == 2:
                _autobet_max_overs = _profile.innings_total_max_overs_inn2 if _profile else self.config.get("innings_total_max_overs_inn2", 17.0)
            else:
                _autobet_max_overs = _profile.innings_total_max_overs_inn1 if _profile else self.config.get("innings_total_max_overs_inn1", 19.0)
            if _overs_now >= _autobet_max_overs:
                logger.info(
                    "AUTOBET SKIP: %s inn%d at %.1f overs (max %.0f) — too late to auto-bet",
                    market, _inn_now, _overs_now, _autobet_max_overs,
                )
                return
        # ─────────────────────────────────────────────────────────────────────

        # Skip if already bet on this market
        if self._should_skip_edge(match_id, market, innings, home, away, direction):
            return

        # Match context check — is the match situation right for this bet?
        state = self.active_matches.get(match_id)
        if state and self.match_context:
            context_ok, context_reason = self.match_context.should_bet(
                edge, state, match_id=match_id, open_bets=_open_bets_for_match,
            )
            if not context_ok:
                logger.info("AUTOBET VETOED: %s (%s %s)", context_reason, market, direction)
                return
            else:
                logger.info("AUTOBET CONFIRMED: %s (%s %s)", context_reason, market, direction)

        # Risk check (before locking — lock only on confirmed placement)
        # Use PRIMARY account's open bets only — client accounts manage their own
        # risk independently. Aggregating both inflates the count and incorrectly
        # blocks the client account when main is at its limit.
        _primary_executor = (
            self.bet_executor.primary
            if hasattr(self.bet_executor, "primary")
            else self.bet_executor
        )
        open_count = len(_primary_executor.open_bets)
        can_bet, reason = self.risk_manager.can_place_bet(ev_pct, odds, market, open_count)
        if not can_bet:
            logger.info("BET BLOCKED: %s", reason)
            print(f"  ⛔ Bet blocked: {reason}")
            return

        # Calculate Kelly stake
        stake = self.risk_manager.calculate_stake(ev_pct, odds, market=market)
        if stake <= 0:
            # Primary bankroll may be depleted — but client accounts might still bet.
            # Only skip entirely if there are no non-primary accounts.
            _has_client_accounts = (
                hasattr(self.bet_executor, "executors")
                and len(self.bet_executor.executors) > 1
            )
            if not _has_client_accounts:
                logger.info("BET SKIP: stake=0 (EV too low for Kelly)")
                return
            logger.info(
                "BET SKIP on primary (stake=0, bankroll=%.2f) — "
                "proceeding for client accounts with independent Kelly sizing",
                self.risk_manager.bankroll_usd,
            )
            # stake stays 0 — multi_executor will recalculate per client account

        # Build market_url for Cloudbet
        market_url = self._build_market_url(market, direction, line, cloudbet_odds)
        if not market_url:
            logger.warning("Could not build market_url for %s %s %s", market, direction, line)
            return

        # Find the Cloudbet event_id
        event_id = self._find_cloudbet_event_id(home, away, match_id=match_id)
        if not event_id:
            logger.warning("Could not find Cloudbet event_id for %s vs %s", home, away)
            return

        # Lock market NOW — only after all pre-checks have passed
        self.edge_detector.lock_market(match_id, market, direction, innings)

        # PLACE THE BET
        bet = self.bet_executor.place_bet(
            event_id=str(event_id),
            market_url=market_url,
            price=odds,
            stake=stake,
            market=market,
            direction=direction,
            line=line,
            home=home,
            away=away,
            ev_pct=ev_pct,
            trigger=trigger,
            innings=innings,
            current_overs=current_overs,
        )

        if bet:
            self.risk_manager.record_bet_placed(market, stake)
            # Track the bet for live score monitoring
            if self.live_tracker and bet.reference_id:
                self.live_tracker.add_bet(
                    reference_id=bet.reference_id,
                    match_id=match_id,
                    market=market,
                    direction=direction,
                    line=float(line) if line else 0.0,
                    stake=stake,
                    odds=odds,
                    innings=innings,
                    home=home,
                    away=away,
                )
            # Save to bet tracking database
            if hasattr(self, 'state_store') and self.state_store:
                import json as _json
                _score_state = self.active_matches.get(match_id)
                _score_str = ""
                if _score_state:
                    _score_str = f"{_score_state.total_runs}/{_score_state.wickets} ({_score_state.overs_completed:.1f} ov)"
                _staking_status = self.smart_staking.get_status() if self.smart_staking else {}
                try:
                    self.state_store.save_bet_tracking({
                        "reference_id": bet.reference_id,
                        "match_id": match_id,
                        "innings": innings,
                        "home_team": home,
                        "away_team": away,
                        "market": market,
                        "direction": direction,
                        "target_line": float(line) if line else 0.0,
                        "target_over": {"6_over": 6.0, "10_over": 10.0, "15_over": 15.0, "innings_total": 20.0}.get(market, 20.0),
                        "stake_usd": stake,
                        "stake_pct": stake / self.risk_manager.bankroll_usd if self.risk_manager and self.risk_manager.bankroll_usd > 0 else 0,
                        "odds": odds,
                        "ev_pct": ev_pct,
                        "trigger": trigger,
                        "status": bet.status,
                        "result": None,
                        "pnl": None,
                        "bankroll_at_bet": self.risk_manager.bankroll_usd if self.risk_manager else 0,
                        "score_at_bet": _score_str,
                        "score_at_settle": None,
                        "score_snapshots": None,
                        "placed_at": bet.placed_at.isoformat() if hasattr(bet.placed_at, 'isoformat') else str(bet.placed_at),
                        "settled_at": None,
                        "streak_at_bet": _staking_status.get("streak", 0),
                        "market_streak_at_bet": _staking_status.get("market_streaks", {}).get(market, 0),
                    })
                except Exception:
                    logger.debug("Failed to save bet tracking record", exc_info=True)
            if bet.status == "ACCEPTED":
                # Already confirmed by Cloudbet (rare — usually starts as PENDING)
                placed_msg = self.bet_executor.format_bet_placed(bet)
                print(f"\n{placed_msg}")
                logger.info("BET PLACED: %s %s %s stake=$%.2f @ %.2f", market, direction, line, stake, odds)
                self.telegram.send_alert_sync(placed_msg, channel=None, is_signal=False)
            else:
                # PENDING_ACCEPTANCE: Cloudbet received the bet but hasn't confirmed yet.
                # We log it quietly and wait for check_settlements to confirm ACCEPTED.
                # If Cloudbet rejects it, the bet disappears and gets marked ABANDONED
                # — the rejection warning will be sent then.
                logger.info(
                    "BET PENDING CONFIRMATION: %s %s %.1f ref=%s — waiting for Cloudbet to accept",
                    market, direction, line, bet.reference_id[:8],
                )
        else:
            # Unlock market so it can retry next scan (e.g. MARKET_SUSPENDED)
            self.edge_detector.clear_locks(match_id, innings=innings)
            logger.warning("BET FAILED: %s %s %s — unlocked for retry", market, direction, line)

    def _build_market_url(self, market: str, direction: str, line: float,
                          cloudbet_odds: dict = None) -> Optional[str]:
        """Build Cloudbet marketUrl from edge info."""
        # Map our market names to Cloudbet market keys
        market_map = {
            "innings_total": "cricket.team_totals",
            "powerplay_runs": "cricket.team_total_from_0_over_to_x_over",
            "6_over": "cricket.team_total_from_0_over_to_x_over",
            "10_over": "cricket.team_total_from_0_over_to_x_over",
            "12_over": "cricket.team_total_from_0_over_to_x_over",
            "15_over": "cricket.team_total_from_0_over_to_x_over",
            "20_over": "cricket.team_total_from_0_over_to_x_over",
            "over_runs": "cricket.over_team_total",
            "match_winner": "cricket.winner",
            "team_sixes": "cricket.team_total_sixes",
            "team_fours": "cricket.team_total_fours",
        }

        cb_market = market_map.get(market)
        if not cb_market:
            return None

        outcome = (direction or "").lower()  # "over", "under", "home", "away"

        if market == "match_winner":
            # Use pre-built market URLs from Cloudbet if available
            if cloudbet_odds and "match_winner" in cloudbet_odds:
                mw = cloudbet_odds["match_winner"]
                team = direction or ""  # for MW, direction holds team name or home/away
                sels = mw.get("selections", {})
                # Try to match team to home/away
                if sels.get("home", {}).get("market_url"):
                    if team.lower() in ("home", "") or self._teams_fuzzy_match(team, mw.get("home_team", "")):
                        return sels["home"]["market_url"]
                if sels.get("away", {}).get("market_url"):
                    if team.lower() in ("away", "") or self._teams_fuzzy_match(team, mw.get("away_team", "")):
                        return sels["away"]["market_url"]
            # Fallback — ensure we always append home or away
            if not outcome or outcome in ("", "home"):
                return f"{cb_market}/home"
            return f"{cb_market}/away"

        # For o/u markets, try to find the exact marketUrl from cloudbet_odds
        if cloudbet_odds and market in cloudbet_odds:
            mkt_data = cloudbet_odds[market]
            if isinstance(mkt_data, dict):
                url = mkt_data.get("market_url_over" if direction == "OVER" else "market_url_under")
                if url:
                    return url

        # Fallback: construct from pattern
        params = f"total={line}"
        return f"{cb_market}/{outcome}?{params}"

    def _find_cloudbet_event_id(self, home: str, away: str, match_id: int | None = None) -> Optional[str]:
        """Find the Cloudbet event ID for a match."""
        if not self.odds_client:
            return None
        try:
            # Use match competition if known, otherwise try all
            competition = self.match_info.get(match_id, {}).get("competition") if match_id else None
            competitions = [competition] if competition else self.config.get("competitions", ["ipl"])
            for comp in competitions:
                events = self.odds_client.get_events(comp)
                for event in events:
                    if event.get("type") != "EVENT_TYPE_EVENT":
                        continue
                    h = (event.get("home") or {}).get("name", "")
                    a = (event.get("away") or {}).get("name", "")
                    if (self._teams_fuzzy_match(home, h) and self._teams_fuzzy_match(away, a)) or \
                       (self._teams_fuzzy_match(home, a) and self._teams_fuzzy_match(away, h)):
                        return event.get("id")
        except Exception as exc:
            logger.warning("Event ID lookup failed for %s vs %s: %s", home, away, exc)
        return None

    # ── Settlement checking ────────────────────────────────────────────

    def _check_settlements(self) -> None:
        """Check if any open bets have settled."""
        if not self.bet_executor or not self.bet_executor.open_bets:
            return

        settled = self.bet_executor.check_settlements()

        # ── Newly confirmed bets (PENDING → ACCEPTED) ──────────────────────
        # Send the "BET PLACED" Telegram notification only after Cloudbet truly
        # confirms the bet — not just on the initial PENDING_ACCEPTANCE response.
        newly_confirmed = self.bet_executor.pop_newly_confirmed()
        for bet in newly_confirmed:
            _ch = "ipl"
            for mid, info in self.match_info.items():
                if info.get("home") == bet.home_team and info.get("away") == bet.away_team:
                    _ch = info.get("competition", "ipl")
                    break
            placed_msg = self.bet_executor.format_bet_placed(bet)
            print(f"\n{placed_msg}")
            logger.info("BET CONFIRMED: %s %s %.1f stake=$%.2f @ %.2f",
                        bet.market, bet.direction, bet.line, bet.stake_usd, bet.price)
            self.telegram.send_alert_sync(placed_msg, channel=None, is_signal=False)
        # ─────────────────────────────────────────────────────────────────────

        if not settled:
            return

        for bet in settled:
            _ch = "ipl"  # default
            for mid, info in self.match_info.items():
                if info.get("home") == bet.home_team and info.get("away") == bet.away_team:
                    _ch = info.get("competition", "ipl")
                    break

            if bet.status == "ABANDONED":
                # Cloudbet never confirmed this bet — notify user so they know
                abandoned_msg = (
                    f"❌ *BET REJECTED* by Cloudbet — {bet.home_team} vs {bet.away_team}\n\n"
                    f"📊 {bet.market.replace('_', ' ').title()} {bet.direction} {bet.line}\n"
                    f"💰 Stake: ${bet.stake_usd:.2f} @ {bet.price:.2f}\n"
                    f"🔖 Ref: {bet.reference_id[:8]}...\n\n"
                    f"⚠️ Bet was submitted but Cloudbet did not confirm it — market may have been suspended or balance was insufficient."
                )
                print(f"\n{abandoned_msg}")
                logger.warning("BET ABANDONED: %s %s %.1f — Cloudbet never confirmed",
                               bet.market, bet.direction, bet.line)
                self.telegram.send_alert_sync(abandoned_msg, channel=None, is_signal=False)
                # Release the risk/lock so the market can be retried
                if self.risk_manager:
                    self.risk_manager.record_bet_settled(0.0, stake=bet.stake_usd)
                continue

            if self.risk_manager:
                self.risk_manager.record_bet_settled(bet.pnl, stake=bet.stake_usd)
            msg = self.bet_executor.format_bet_settled(bet)
            print(f"\n{msg}")
            logger.info("BET SETTLED: %s %s → %s pnl=$%.2f",
                        bet.market, bet.direction, bet.status, bet.pnl)
            self.telegram.send_alert_sync(msg, channel=None, is_signal=False)
            # Update smart staking with the result
            if self.smart_staking:
                self.smart_staking.record_result(
                    market=bet.market,
                    result=bet.status,
                    pnl=bet.pnl,
                    stake=bet.stake_usd,
                    bankroll_before=self.risk_manager.bankroll_usd if self.risk_manager else 0,
                    odds=bet.price,
                    direction=bet.direction,
                    line=bet.line,
                )
            # Remove from live tracker
            if self.live_tracker:
                self.live_tracker.remove_bet(bet.reference_id)
            # Update bet tracking database
            if hasattr(self, 'state_store') and self.state_store:
                try:
                    _score_state_s = None
                    for _mid, _minfo in self.match_info.items():
                        if _minfo.get("home") == bet.home_team:
                            _ss = self.active_matches.get(_mid)
                            if _ss:
                                _score_state_s = f"{_ss.total_runs}/{_ss.wickets} ({_ss.overs_completed:.1f} ov)"
                            break
                    self.state_store.update_bet_tracking(bet.reference_id, {
                        "status": bet.status,
                        "result": bet.status,
                        "pnl": bet.pnl,
                        "settled_at": bet.settled_at.isoformat() if bet.settled_at else None,
                        "score_at_settle": _score_state_s,
                    })
                except Exception:
                    logger.debug("Failed to update bet tracking on settlement", exc_info=True)

        # Print portfolio status
        status = self.bet_executor.get_status()
        risk_status = self.risk_manager.get_status() if self.risk_manager else {}
        print(f"\n  📊 Open: {status.get('open_bets', 0)} | "
              f"Today: {status.get('trades_today', 0)} trades | "
              f"PnL: ${status.get('daily_pnl', 0):+.2f} | "
              f"Total: ${status.get('total_pnl', 0):+.2f}")


# ── CLI ────────────────────────────────────────────────────────────────


PIDFILE = "/tmp/ipl_spotter.pid"


def _acquire_lock() -> bool:
    """Ensure only one bot instance runs. Returns True if lock acquired."""
    import os, signal as _signal
    if os.path.exists(PIDFILE):
        try:
            old_pid = int(open(PIDFILE).read().strip())
            # Verify it's actually a spotter process before killing
            cmdline = open(f"/proc/{old_pid}/cmdline").read()
            if "spotter" in cmdline:
                logger.info("Sending SIGTERM to old bot process %d", old_pid)
                os.kill(old_pid, _signal.SIGTERM)
                # Wait up to 5s for clean exit before escalating
                for _ in range(10):
                    time.sleep(0.5)
                    try:
                        os.kill(old_pid, 0)  # check if still alive
                    except ProcessLookupError:
                        break
                else:
                    logger.warning("Process %d didn't exit — sending SIGKILL", old_pid)
                    os.kill(old_pid, _signal.SIGKILL)
                    time.sleep(1)
        except (ProcessLookupError, ValueError, PermissionError, FileNotFoundError):
            pass  # old process already dead or not ours
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))
    return True


def main() -> None:
    _acquire_lock()

    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(
                    encoding="utf-8",
                    errors="replace",
                    line_buffering=True,
                    write_through=True,
                )
            except Exception:
                pass

    parser = argparse.ArgumentParser(
        prog="ipl-spotter",
        description="Cricket Edge Spotter — LIVE AUTOMATED BETTING BOT",
    )
    parser.add_argument("--config", default=None, help="Path to config JSON")
    args = parser.parse_args()

    config = load_config(args.config)
    if not config:
        print("[IPL-SPOTTER] Failed to load config — exiting")
        return

    # Logging to terminal
    log_level = config.get("log_level", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    spotter = IPLEdgeSpotter(config)

    betting_status = "LIVE BETTING" if spotter.bet_executor else "ALERTS ONLY"
    copilot_status = "ON" if (spotter.copilot and spotter.copilot.enabled) else "OFF"
    currency = config.get("default_currency", "USD")

    # Fetch live balance from Cloudbet and use as bankroll
    live_balance = spotter._refresh_live_bankroll(force=True)
    balance_str = f"${live_balance:.2f}" if live_balance is not None else "N/A"
    max_stake = spotter.risk_manager.max_position_size_usd if spotter.risk_manager else 0.0

    shadow_min = config.get("shadow_min_stake_inr", 200)
    shadow_max = config.get("shadow_max_stake_inr", 1000)

    competitions = config.get("competitions", ["ipl"])
    comps_str = ", ".join(c.upper() for c in competitions)

    print(f"""
╔══════════════════════════════════════════════════════════╗
║        🏏 Cricket Edge Spotter v{VERSION}                     ║
║        ⚡ MODE: {betting_status:<20s}                  ║
╠══════════════════════════════════════════════════════════╣
║  Competitions: {comps_str:<42s}║
║  Cloudbet Balance: {balance_str:<10s} ({currency})                     ║
║  Max stake: ${max_stake:.2f} | Kelly: {config.get('fractional_kelly', 0.25):.0%}                      ║
║  Co-Pilot: {copilot_status} | Shadow: INR {shadow_min}-{shadow_max}/bet               ║
║  Scan interval: {spotter.scan_interval}s                                    ║
║                                                          ║
║  Data: Sportmonks (ball-by-ball) + The Odds API (27 books)║
║  Odds: Cloudbet (16 fancy markets)                       ║
║  Edge: Speed + Model + Consensus                         ║
║                                                          ║
║  Press Ctrl+C to stop                                    ║
╚══════════════════════════════════════════════════════════╝
""")

    spotter.start()

    try:
        while spotter.running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[IPL-SPOTTER] Shutting down...")
        spotter.stop()

        # Final summary
        if spotter.bet_executor:
            status = spotter.bet_executor.get_status()
            print(f"\n  📊 SESSION SUMMARY:")
            print(f"     Trades: {status.get('trades_today', 0)}")
            print(f"     Open:   {status.get('open_bets', 0)}")
            print(f"     P&L:    ${status.get('total_pnl', 0):+.2f}")
            print(f"     Win%:   {status.get('win_rate', 0):.0f}%")

        spotter.stats_db.close()
        if hasattr(spotter, 'state_store') and spotter.state_store:
            spotter.state_store.close()
        import os
        try:
            os.remove(PIDFILE)
        except OSError:
            pass
        print("[IPL-SPOTTER] Done")


if __name__ == "__main__":
    main()
