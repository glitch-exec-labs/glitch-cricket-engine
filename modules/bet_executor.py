"""
IPL Edge Spotter -- bet_executor.py
Places bets on Cloudbet's API and tracks open/settled bets.

Supports both paper (simulated) and live betting modes.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import requests

from modules.session_markets import (
    is_completed_session_market,
    market_display_name,
    session_market_from_url,
)

logger = logging.getLogger("ipl_spotter.bet_executor")

CLOUDBET_BET_PLACE_URL = "https://sports-api.cloudbet.com/pub/v3/bets/place"
CLOUDBET_BET_STATUS_URL = "https://sports-api.cloudbet.com/pub/v3/bets/{reference_id}/status"
CLOUDBET_BET_HISTORY_URL = "https://sports-api.cloudbet.com/pub/v3/bets/history"
CLOUDBET_ODDS_EVENT_URL = "https://sports-api.cloudbet.com/pub/v2/odds/events"
DEFAULT_TIMEOUT = 20

# Cloudbet protobuf Status enum values (from cloudbet/response.proto).
# The API uses these exact strings in JSON responses.
# Terminal settlement statuses — bet is resolved:
_TERMINAL_WIN = {"WIN", "HALF_WIN", "WON"}           # WON for backward compat
_TERMINAL_LOSS = {"LOSS", "HALF_LOSS", "LOST"}        # LOST for backward compat
_TERMINAL_VOID = {"PUSH", "PARTIAL", "CANCELLED", "VOID"}
_TERMINAL_STATUSES = _TERMINAL_WIN | _TERMINAL_LOSS | _TERMINAL_VOID
# Non-terminal — still alive on the exchange:
_ACTIVE_STATUSES = {"ACCEPTED", "PENDING", "PENDING_ACCEPTANCE"}


@dataclass
class LiveBet:
    reference_id: str
    event_id: str
    home_team: str
    away_team: str
    innings: int
    market: str  # e.g. "innings_total", "powerplay_runs"
    market_url: str  # Cloudbet marketUrl
    direction: str  # "OVER" or "UNDER" or "HOME" or "AWAY"
    line: float  # e.g. 185.5
    price: float  # odds e.g. 1.85
    stake_usd: float
    ev_pct: float
    trigger: str  # what caused the bet: "SPEED_EDGE", "MODEL_EDGE", "MANUAL"
    paper: bool
    status: str  # PENDING, ACCEPTED, WON, LOST, CANCELLED
    placed_at: datetime
    settled_at: Optional[datetime] = None
    pnl: float = 0.0
    cashout_eligible: bool = False
    cashout_available: bool = False
    cashout_price: Optional[float] = None
    min_bet: float = 0.0


class BetExecutor:
    """
    Places bets via Cloudbet and tracks open/settled positions.

    Initialised with a config dict containing:
      - cloudbet_api_key
      - default_currency (default: "USDC")
      - accept_price_change (default: "ALL")
      - allowed_currencies (default: ["USDC", "USDT"])
      - max_position_size_usd (default: 25.0)
    """

    def __init__(self, config: dict, paper_mode: bool = False, state_store=None):
        self.config = config
        self.paper_mode = paper_mode
        self.state_store = state_store

        self.api_key: str = config.get("cloudbet_api_key", "")
        self.default_currency: str = config.get("default_currency", "USDC")
        self.accept_price_change: str = config.get("accept_price_change", "ALL")
        self.allowed_currencies: List[str] = config.get(
            "allowed_currencies", ["USDC", "USDT"]
        )
        self.max_position_size: float = float(
            config.get("max_position_size_usd", 25.0)
        )
        self.timeout: int = DEFAULT_TIMEOUT

        # If Cloudflare blocks trading endpoints, pause live placement attempts
        # for a cooldown window to avoid repeated 403 spam.
        self._cloudflare_block_until: float = 0.0
        self._cloudflare_block_cooldown_seconds: int = int(
            config.get("cloudflare_block_cooldown_seconds", 600)
        )

        # Cloudbet status/history endpoints can be brittle: /bets/history is
        # currently returning 404 on this API, and /bets/{ref}/status is heavily
        # rate-limited. Disable dead history fallback after one confirmed 404 and
        # back off status polling when Cloudbet returns 429.
        self._history_endpoint_supported: Optional[bool] = None
        self._status_backoff_until: float = 0.0

        if not self.api_key and not paper_mode:
            logger.warning(
                "cloudbet_api_key not set -- live betting will fail"
            )

        self._session = requests.Session()
        self._session.headers.update(
            {
                "X-API-Key": self.api_key,
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        # Tracking state
        self.open_bets: Dict[str, LiveBet] = {}
        self.closed_bets: List[LiveBet] = []
        self.daily_pnl: float = 0.0
        self.total_pnl: float = 0.0

        # Track bets that just transitioned PENDING → ACCEPTED so the caller
        # (spotter.py) can send the "BET PLACED" Telegram notification only after
        # Cloudbet actually confirms the bet, not just on PENDING_ACCEPTANCE.
        self._newly_confirmed: List[LiveBet] = []

        # Count consecutive settlement lookup failures per bet.
        # After _MAX_SETTLEMENT_FAILURES scans with no status, the bet is
        # marked ABANDONED and removed from open_bets (stops 404 spam).
        self._settlement_failures: Dict[str, int] = {}
        self._MAX_SETTLEMENT_FAILURES: int = 20  # ~160s at 8s scan interval
        self._pending_verification: Dict[str, int] = {}

        # Restore persisted state if a store is available
        if self.state_store is not None:
            self._restore_state()

    # -- State persistence helpers -------------------------------------------------

    def _restore_state(self) -> None:
        """Load persisted bets from state_store on startup."""
        try:
            self.open_bets = self.state_store.load_open_bets()
            self.closed_bets = self.state_store.load_closed_bets(limit=500)
            self.daily_pnl = self.state_store.load_daily_pnl()
            self.total_pnl = sum(b.pnl for b in self.closed_bets)
            logger.info(
                "Restored state: %d open bets, %d closed bets, daily_pnl=%.4f",
                len(self.open_bets), len(self.closed_bets), self.daily_pnl,
            )
        except Exception:
            logger.exception("Failed to restore state from store")

    def _persist_open_bet(self, bet: LiveBet) -> None:
        """Persist an open bet if store is available."""
        if self.state_store is not None:
            try:
                self.state_store.save_open_bet(bet)
            except Exception:
                logger.exception("Failed to persist open bet %s", bet.reference_id[:8])

    def _persist_settlement(self, bet: LiveBet) -> None:
        """Move a bet from open to closed in the store."""
        if self.state_store is not None:
            try:
                self.state_store.remove_open_bet(bet.reference_id)
                self.state_store.save_closed_bet(bet)
                self.state_store.save_daily_pnl(self.daily_pnl)
            except Exception:
                logger.exception("Failed to persist settlement for %s", bet.reference_id[:8])

    # -- Public API ----------------------------------------------------------------

    def place_bet(
        self,
        event_id: str,
        market_url: str,
        price: float,
        stake: float,
        currency: str | None = None,
        market: str = "",
        direction: str = "",
        line: float = 0.0,
        home: str = "",
        away: str = "",
        ev_pct: float = 0.0,
        trigger: str = "",
        innings: int = 1,
        current_overs: float | None = None,
    ) -> Optional[LiveBet]:
        """Place a bet (paper or live) and return the LiveBet, or None on failure."""
        # HARD BLOCK: no duplicate bets on same market/innings
        if self.has_open_bet(market_key=market, innings=innings, home=home, away=away):
            logger.warning("BLOCKED: already have open bet on %s innings=%d for %s vs %s", market, innings, home, away)
            return None

        # Also block similar markets (innings_total ≈ 20_over, both are full innings bets)
        SIMILAR_MARKETS = {
            "innings_total": ["20_over"],
            "20_over": ["innings_total"],
            "powerplay_runs": ["6_over"],
            "6_over": ["powerplay_runs"],
        }
        for similar in SIMILAR_MARKETS.get(market, []):
            if self.has_open_bet(market_key=similar, innings=innings, home=home, away=away):
                logger.warning("BLOCKED: similar market %s already open (have %s)", market, similar)
                return None

        currency = currency or self.default_currency
        if currency not in self.allowed_currencies:
            logger.error("Currency %s not in allowed list %s", currency, self.allowed_currencies)
            return None

        session_market, _target_over = session_market_from_url(market_url, market)
        if current_overs is not None and session_market and is_completed_session_market(session_market, current_overs):
            logger.warning(
                "Rejecting stale session bet: %s at %.1f overs",
                session_market,
                current_overs,
            )
            return None

        HARD_CAP_USD = 25.0
        cap = min(self.max_position_size, HARD_CAP_USD)
        if stake > cap:
            logger.warning("Stake $%.2f clamped to cap $%.2f", stake, cap)
        stake = min(stake, cap)

        if stake <= 0:
            logger.warning("BET SKIPPED: %s %s — zero or negative stake (%.4f)", market, direction, stake)
            return None

        reference_id = str(uuid.uuid4())

        now = time.time()
        if not self.paper_mode and now < self._cloudflare_block_until:
            wait_s = int(max(0, self._cloudflare_block_until - now))
            logger.warning(
                "BET SKIP: Cloudflare trading block active (%ds remaining) for %s %s",
                wait_s,
                market,
                direction,
            )
            return None

        if self.paper_mode:
            return self._place_paper_bet(
                reference_id, event_id, market_url, price, stake,
                market, direction, line, home, away, ev_pct, trigger, innings,
            )
        else:
            return self._place_live_bet(
                reference_id, event_id, market_url, price, stake, currency,
                market, direction, line, home, away, ev_pct, trigger, innings,
            )

    def has_open_bet(
        self,
        market_key: str,
        innings: int,
        home: str = "",
        away: str = "",
        direction: str | None = None,
    ) -> bool:
        """Return True if an open bet already exists for this market/innings."""
        return self.get_open_bet(
            market_key=market_key,
            innings=innings,
            home=home,
            away=away,
            direction=direction,
        ) is not None

    def get_open_bet(
        self,
        market_key: str,
        innings: int,
        home: str = "",
        away: str = "",
        direction: str | None = None,
    ) -> Optional[LiveBet]:
        """Return the first matching open bet for this market/innings, if any."""
        for bet in self.open_bets.values():
            if bet.market != market_key or bet.innings != innings:
                continue
            if direction and bet.direction != direction:
                continue
            if home and bet.home_team != home:
                continue
            if away and bet.away_team != away:
                continue
            return bet
        return None

    def check_settlements(self) -> List[LiveBet]:
        """Check all open bets for settlement. Returns list of newly settled bets."""
        settled: List[LiveBet] = []
        ref_ids = list(self.open_bets.keys())

        for ref_id in ref_ids:
            bet = self.open_bets[ref_id]

            if self.paper_mode:
                # Paper bets stay open until manually settled
                continue

            # Skip freshly placed bets — give Cloudbet time to propagate
            age_seconds = (datetime.now(timezone.utc) - bet.placed_at).total_seconds()
            if age_seconds < 5:
                continue

            if time.time() < self._status_backoff_until:
                continue

            status, raw_data = self._get_bet_status_with_data(ref_id)
            if raw_data and isinstance(raw_data, dict) and raw_data.get("_skip_failure_increment"):
                continue
            if status is None:
                # Increment failure counter — abandon after too many misses
                self._settlement_failures[ref_id] = self._settlement_failures.get(ref_id, 0) + 1
                fail_count = self._settlement_failures[ref_id]
                if fail_count >= self._MAX_SETTLEMENT_FAILURES:
                    # Before abandoning, do one final history check
                    history_status = self._get_bet_status_from_history(ref_id)
                    if history_status in _TERMINAL_STATUSES or history_status == "ACCEPTED":
                        logger.info(
                            "Bet %s: found in history as %s after %d poll failures",
                            ref_id[:8], history_status, fail_count,
                        )
                        status = history_status
                        self._settlement_failures.pop(ref_id, None)
                        # Fall through to normal status handling below
                    else:
                        logger.warning(
                            "Bet %s: %d consecutive lookup failures (age=%.0fs) — marking ABANDONED",
                            ref_id[:8], fail_count, age_seconds,
                        )
                        bet.status = "ABANDONED"
                        bet.settled_at = datetime.now(timezone.utc)
                        bet.pnl = 0.0
                        del self.open_bets[ref_id]
                        self.closed_bets.append(bet)
                        self._settlement_failures.pop(ref_id, None)
                        self._persist_settlement(bet)
                        continue
                else:
                    if fail_count % 5 == 0:
                        logger.debug(
                            "Bet %s: no status returned (fail=%d age=%.0fs current=%s)",
                            ref_id[:8], fail_count, age_seconds, bet.status,
                        )
                    continue

            # Status found — reset failure counter
            self._settlement_failures.pop(ref_id, None)

            # Extract cashout data from status response if available
            if raw_data and isinstance(raw_data, dict):
                self._update_cashout_from_response(bet, raw_data)

            if status in _TERMINAL_STATUSES:
                bet.status = status
                bet.settled_at = datetime.now(timezone.utc)

                if status in _TERMINAL_WIN:
                    # HALF_WIN: half stake wins at full odds, half returned
                    if status == "HALF_WIN":
                        bet.pnl = (bet.stake_usd / 2) * (bet.price - 1)
                    else:
                        bet.pnl = bet.stake_usd * (bet.price - 1)
                elif status in _TERMINAL_LOSS:
                    if status == "HALF_LOSS":
                        bet.pnl = -(bet.stake_usd / 2)
                    else:
                        bet.pnl = -bet.stake_usd
                else:  # PUSH, PARTIAL, CANCELLED, VOID — stake returned
                    bet.pnl = 0.0

                self.daily_pnl += bet.pnl
                self.total_pnl += bet.pnl

                del self.open_bets[ref_id]
                self.closed_bets.append(bet)
                settled.append(bet)
                self._persist_settlement(bet)

                logger.info(
                    "Bet %s settled: %s  pnl=%.4f (age=%.0fs)",
                    ref_id[:8], status, bet.pnl, age_seconds,
                )
            elif status in _ACTIVE_STATUSES:
                # Always update to latest status — keep polling until terminal
                if bet.status != status:
                    logger.info(
                        "Bet %s status: %s -> %s (age=%.0fs)",
                        ref_id[:8], bet.status, status, age_seconds,
                    )
                    prev_status = bet.status
                    bet.status = status
                    # PENDING → ACCEPTED: bet is now truly live on Cloudbet.
                    if prev_status == "PENDING" and status == "ACCEPTED":
                        # Check createTime — if epoch, verify via history before confirming
                        create_time = ""
                        if raw_data and isinstance(raw_data, dict):
                            create_time = raw_data.get("createTime", "")
                        if create_time and not create_time.startswith("1970"):
                            # Clean acceptance — createTime is real
                            self._newly_confirmed.append(bet)
                        else:
                            # Epoch or missing createTime — queue for history verification
                            logger.warning(
                                "Bet %s: ACCEPTED but createTime=%s — verifying via history",
                                ref_id[:8], create_time or "(empty)",
                            )
                            self._pending_verification[ref_id] = 3
                    self._persist_open_bet(bet)

        # ── Process pending history verifications ──────────────────────────
        for ref_id in list(self._pending_verification.keys()):
            bet = self.open_bets.get(ref_id)
            if bet is None:
                self._pending_verification.pop(ref_id, None)
                continue

            history_status = self._get_bet_status_from_history(ref_id)
            if history_status == "ACCEPTED":
                logger.info("Bet %s: confirmed in history — truly accepted", ref_id[:8])
                self._newly_confirmed.append(bet)
                self._pending_verification.pop(ref_id)
            elif history_status in _TERMINAL_STATUSES:
                logger.info("Bet %s: found settled in history as %s during verification", ref_id[:8], history_status)
                self._pending_verification.pop(ref_id)
                # Will be picked up on next check_settlements cycle
            else:
                self._pending_verification[ref_id] -= 1
                if self._pending_verification[ref_id] <= 0:
                    logger.error(
                        "Bet %s: ACCEPTED on status endpoint but not confirmed in history "
                        "after verification polls — treating as UNCONFIRMED",
                        ref_id[:8],
                    )
                    bet.status = "UNCONFIRMED"
                    bet.pnl = -bet.stake_usd
                    bet.settled_at = datetime.now(timezone.utc)
                    del self.open_bets[ref_id]
                    self.closed_bets.append(bet)
                    self._persist_settlement(bet)
                    self._pending_verification.pop(ref_id)

        return settled

    def _update_cashout_from_response(self, bet: LiveBet, data: dict) -> None:
        """Extract cashout availability from a Cloudbet status response.

        NOTE: As of the current Cloudbet API (protobuf v3), cashout is NOT
        exposed in the Trading API — it is a UI-only feature.  The
        PublicBetResponse protobuf contains no cashout field.  This method
        is kept as a defensive future-proof: if Cloudbet adds cashout to
        the API later, it will be picked up automatically.
        """
        cashout_data = data.get("cashout")
        if cashout_data is None:
            return

        if isinstance(cashout_data, bool):
            bet.cashout_available = cashout_data
        elif isinstance(cashout_data, dict):
            bet.cashout_available = cashout_data.get("available", False)
            try:
                price = cashout_data.get("price")
                if price is not None:
                    bet.cashout_price = float(price)
            except (TypeError, ValueError):
                pass
        elif cashout_data:
            bet.cashout_available = bool(cashout_data)

        if bet.cashout_available:
            logger.info(
                "Bet %s: cashout field detected in API response (price=%s) — "
                "this was previously undocumented, logging for analysis",
                bet.reference_id[:8],
                bet.cashout_price,
            )

    def pop_newly_confirmed(self) -> List["LiveBet"]:
        """Return and clear the list of bets that just confirmed PENDING → ACCEPTED.

        Called by spotter after each settlement check to send Telegram notifications
        only for bets that Cloudbet has truly confirmed (not just PENDING_ACCEPTANCE).
        """
        confirmed = list(self._newly_confirmed)
        self._newly_confirmed.clear()
        return confirmed

    def reconcile_with_exchange(self) -> None:
        """On startup, check Cloudbet /bets/history for bets that settled
        or confirmed while we were down.  Called once after _restore_state().
        """
        if not self.open_bets:
            logger.info("Reconciliation: no persisted open bets — nothing to do")
            return

        logger.info("Reconciling %d persisted open bets with Cloudbet history...", len(self.open_bets))
        try:
            resp = self._session.get(
                CLOUDBET_BET_HISTORY_URL,
                params={"limit": 50},
                timeout=self.timeout,
            )
            if resp.status_code == 404:
                self._history_endpoint_supported = False
                logger.warning("Reconciliation: Cloudbet history endpoint returned HTTP 404 — disabling history fallback")
                return
            if resp.status_code >= 400:
                logger.warning("Reconciliation: history fetch returned HTTP %d — skipping", resp.status_code)
                return
            self._history_endpoint_supported = True
            history = resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning("Reconciliation: history fetch failed: %s — skipping", exc)
            return

        # Build lookup: referenceId → status from exchange
        exchange_status: Dict[str, str] = {}
        items: list = []
        if isinstance(history, list):
            items = history
        elif isinstance(history, dict):
            for key in ("bets", "items", "results", "history", "data"):
                if isinstance(history.get(key), list):
                    items = history[key]
                    break

        for item in items:
            if not isinstance(item, dict):
                continue
            ref = item.get("referenceId") or item.get("reference_id")
            status = item.get("status")
            if ref and status:
                exchange_status[ref] = status

        for ref_id, bet in list(self.open_bets.items()):
            ex_status = exchange_status.get(ref_id)
            age = (datetime.now(timezone.utc) - bet.placed_at).total_seconds()

            if ex_status in _TERMINAL_STATUSES:
                logger.info("Reconcile: bet %s settled as %s while offline (age=%.0fs)", ref_id[:8], ex_status, age)
                bet.status = ex_status
                bet.settled_at = datetime.now(timezone.utc)
                if ex_status in _TERMINAL_WIN:
                    half = ex_status == "HALF_WIN"
                    bet.pnl = (bet.stake_usd / 2 if half else bet.stake_usd) * (bet.price - 1)
                elif ex_status in _TERMINAL_LOSS:
                    half = ex_status == "HALF_LOSS"
                    bet.pnl = -(bet.stake_usd / 2 if half else bet.stake_usd)
                else:
                    bet.pnl = 0.0
                self.daily_pnl += bet.pnl
                self.total_pnl += bet.pnl
                del self.open_bets[ref_id]
                self.closed_bets.append(bet)
                self._persist_settlement(bet)

            elif ex_status == "ACCEPTED" and bet.status == "PENDING":
                logger.info("Reconcile: bet %s confirmed ACCEPTED while offline", ref_id[:8])
                bet.status = "ACCEPTED"
                self._persist_open_bet(bet)

            elif ex_status is None and age > 120:
                logger.warning(
                    "Reconcile: bet %s (age=%.0fs) not in exchange history — marking UNCONFIRMED",
                    ref_id[:8], age,
                )
                bet.status = "UNCONFIRMED"
                bet.pnl = -bet.stake_usd
                bet.settled_at = datetime.now(timezone.utc)
                del self.open_bets[ref_id]
                self.closed_bets.append(bet)
                self._persist_settlement(bet)

        logger.info("Reconciliation done: %d open bets remain", len(self.open_bets))

    def get_status(self) -> dict:
        """Return a summary of current betting state."""
        today_bets = [
            b
            for b in self.closed_bets
            if b.placed_at.date() == datetime.now(timezone.utc).date()
        ]
        wins = [b for b in today_bets if b.status == "WON"]
        total_settled = len(today_bets)

        return {
            "open_bets": len(self.open_bets),
            "daily_pnl": round(self.daily_pnl, 4),
            "total_pnl": round(self.total_pnl, 4),
            "trades_today": total_settled + len(self.open_bets),
            "win_rate": (
                round(len(wins) / total_settled * 100, 1)
                if total_settled > 0
                else 0.0
            ),
        }

    def format_bet_placed(self, bet: LiveBet) -> str:
        """Format a Telegram message for a placed bet."""
        mode = " [PAPER]" if bet.paper else ""
        market_label = market_display_name(bet.market)
        return (
            f"\u2705 BET PLACED{mode} \u2014 {bet.home_team} vs {bet.away_team}\n"
            f"\n"
            f"\U0001f4ca {market_label} {bet.direction} {bet.line}\n"
            f"\U0001f4b0 Stake: ${bet.stake_usd:.2f} @ {bet.price:.2f}\n"
            f"\U0001f4c8 EV: {'+' if bet.ev_pct >= 0 else ''}{bet.ev_pct:.1f}%\n"
            f"\U0001f3af Trigger: {bet.trigger}\n"
            f"\U0001f516 Ref: {bet.reference_id[:8]}..."
        )

    def format_bet_settled(self, bet: LiveBet) -> str:
        """Format a Telegram message for a settled bet."""
        if bet.status == "WON":
            icon = "\U0001f3c6 BET WON"
        elif bet.status == "LOST":
            icon = "\u274c BET LOST"
        else:
            icon = "\u2796 BET VOID"
        market_label = market_display_name(bet.market)

        def _fmt_usd(val: float) -> str:
            if val >= 0:
                return f"+${val:.2f}"
            return f"-${abs(val):.2f}"

        return (
            f"{icon} \u2014 {bet.home_team} vs {bet.away_team}\n"
            f"\n"
            f"\U0001f4ca {market_label} {bet.direction} {bet.line}\n"
            f"\U0001f4b0 PnL: {_fmt_usd(bet.pnl)}\n"
            f"\U0001f4c8 Daily: {_fmt_usd(self.daily_pnl)} | "
            f"Total: {_fmt_usd(self.total_pnl)}"
        )

    # -- Internal helpers ----------------------------------------------------------

    def _place_paper_bet(
        self,
        reference_id: str,
        event_id: str,
        market_url: str,
        price: float,
        stake: float,
        market: str,
        direction: str,
        line: float,
        home: str,
        away: str,
        ev_pct: float,
        trigger: str,
        innings: int,
    ) -> LiveBet:
        """Simulate bet acceptance in paper mode."""
        bet = LiveBet(
            reference_id=reference_id,
            event_id=event_id,
            home_team=home,
            away_team=away,
            innings=innings,
            market=market,
            market_url=market_url,
            direction=direction,
            line=line,
            price=price,
            stake_usd=stake,
            ev_pct=ev_pct,
            trigger=trigger,
            paper=True,
            status="ACCEPTED",
            placed_at=datetime.now(timezone.utc),
        )
        self.open_bets[reference_id] = bet
        self._persist_open_bet(bet)
        logger.info(
            "[PAPER] Bet placed: %s %s %s @ %.3f  stake=$%.2f  ref=%s",
            market, direction, line, price, stake, reference_id[:8],
        )
        return bet

    def _place_live_bet(
        self,
        reference_id: str,
        event_id: str,
        market_url: str,
        price: float,
        stake: float,
        currency: str,
        market: str,
        direction: str,
        line: float,
        home: str,
        away: str,
        ev_pct: float,
        trigger: str,
        innings: int,
    ) -> Optional[LiveBet]:
        """Place a real bet on Cloudbet."""
        # Re-fetch live price and selection metadata (cashout, minBet)
        live_price, selection = self._get_live_price_and_selection(event_id, market_url)
        if live_price is not None:
            logger.info(
                "Live price for %s: %.3f (was %.3f)",
                market_url, live_price, price,
            )
            price = live_price

        # Extract selection metadata (minStake, maxStake, status).
        # Per Cloudbet protobuf (feed.proto), Selection fields are:
        #   outcome, params, price, min_stake, max_stake, probability, status, side
        # JSON keys use camelCase: minStake, maxStake, etc.
        # NOTE: cashout is NOT exposed in the Cloudbet API — it's UI-only.
        cashout_eligible = False
        min_bet_val = 0.0
        if selection:
            try:
                min_bet_val = float(selection.get("minStake", 0) or selection.get("min_stake", 0) or 0)
            except (TypeError, ValueError):
                min_bet_val = 0.0
            _max_stake = 0.0
            try:
                _max_stake = float(selection.get("maxStake", 0) or selection.get("max_stake", 0) or 0)
            except (TypeError, ValueError):
                pass
            _sel_status = selection.get("status", "unknown")
            logger.info(
                "Selection metadata for %s: minStake=%.2f maxStake=%.2f status=%s prob=%.4f",
                market_url, min_bet_val, _max_stake, _sel_status,
                float(selection.get("probability", 0) or 0),
            )

        payload = {
            "reference_id": reference_id,
            "stake": f"{stake:.2f}",
            "price": f"{price:.3f}",
            "event_id": str(event_id),
            "market_url": market_url,
            "currency": currency,
            "accept_price_change": self.accept_price_change,
            "side": "BACK",
        }

        try:
            resp = self._session.post(
                CLOUDBET_BET_PLACE_URL,
                json=payload,
                timeout=self.timeout,
            )
            if resp.status_code >= 400:
                body_prefix = (resp.text or "")[:500]
                if resp.status_code == 403 and (
                    "Attention Required" in body_prefix or "Cloudflare" in body_prefix
                ):
                    self._cloudflare_block_until = time.time() + self._cloudflare_block_cooldown_seconds
                    logger.error(
                        "Cloudbet trading blocked by Cloudflare (HTTP 403). "
                        "Auto-bet paused for %ds.",
                        self._cloudflare_block_cooldown_seconds,
                    )
                logger.error(
                    "Cloudbet bet placement failed: HTTP %d: %s",
                    resp.status_code, body_prefix[:300],
                )
                return None

            data = resp.json()
        except requests.RequestException as exc:
            logger.error("Cloudbet bet placement request failed: %s", exc)
            return None
        except ValueError:
            logger.error("Cloudbet bet placement returned invalid JSON")
            return None

        logger.info(
            "[LIVE] Cloudbet response for %s: %s",
            reference_id[:8], str(data)[:500],
        )

        api_status = data.get("status", "UNKNOWN")

        # Handle rejection statuses
        if api_status in ("REJECTED", "MALFORMED_REQUEST"):
            error = data.get("error", "")
            logger.warning("Bet %s by Cloudbet: %s (error: %s)", api_status, reference_id[:8], error)
            return None

        if api_status == "MARKET_SUSPENDED":
            logger.warning("Market suspended for %s — will retry next scan", reference_id[:8])
            return None

        # Validate referenceId in response if present
        returned_ref = data.get("referenceId") or data.get("reference_id")
        if returned_ref and returned_ref != reference_id:
            logger.warning(
                "Cloudbet returned different referenceId: sent=%s got=%s",
                reference_id[:8], str(returned_ref)[:8],
            )
            reference_id = returned_ref

        # Map API status to our internal status.
        # PENDING_ACCEPTANCE = Cloudbet received the request but hasn't validated it yet.
        # We keep it as PENDING until the status endpoint confirms ACCEPTED — this prevents
        # the Telegram "BET PLACED" notification from firing before the bet is truly live.
        if api_status == "ACCEPTED":
            internal_status = "ACCEPTED"
        elif api_status in ("PENDING_ACCEPTANCE", "PENDING"):
            internal_status = "PENDING"
        else:
            logger.warning("Unknown Cloudbet status: %s for %s", api_status, reference_id[:8])
            internal_status = "PENDING"

        # Extract createTime from the placement response for acceptance verification
        create_time_raw = data.get("createTime", "")

        bet = LiveBet(
            reference_id=reference_id,
            event_id=event_id,
            home_team=home,
            away_team=away,
            innings=innings,
            market=market,
            market_url=market_url,
            direction=direction,
            line=line,
            price=price,
            stake_usd=stake,
            ev_pct=ev_pct,
            trigger=trigger,
            paper=False,
            status=internal_status,
            placed_at=datetime.now(timezone.utc),
            cashout_eligible=cashout_eligible,
            min_bet=min_bet_val,
        )
        self.open_bets[reference_id] = bet
        self._persist_open_bet(bet)

        # If already ACCEPTED but createTime is epoch, queue for history verification
        if internal_status == "ACCEPTED" and (
            not create_time_raw or create_time_raw.startswith("1970")
        ):
            logger.warning(
                "Bet %s: ACCEPTED but createTime=%s — queuing for history verification",
                reference_id[:8], create_time_raw or "(empty)",
            )
            self._pending_verification[reference_id] = 3  # verify over next 3 polls

        logger.info(
            "[LIVE] Bet placed: %s %s %s @ %.3f  stake=$%.2f  status=%s  ref=%s  cashout=%s",
            market, direction, line, price, stake, internal_status, reference_id[:8],
            cashout_eligible,
        )
        return bet

    def _get_live_price(self, event_id: str, market_url: str) -> Optional[float]:
        """Fetch current live price for a market from Cloudbet odds API."""
        data = self._fetch_event_odds(event_id)
        if data is None:
            logger.warning("Could not fetch live odds for event %s", event_id)
            return None
        return self._extract_price_from_odds(data, market_url)

    def _get_live_price_and_selection(
        self, event_id: str, market_url: str
    ) -> tuple[Optional[float], Optional[dict]]:
        """Fetch live price and the full selection dict (for cashout detection)."""
        data = self._fetch_event_odds(event_id)
        if data is None:
            return None, None
        sel = self._extract_selection_from_odds(data, market_url)
        if sel is None:
            return None, None
        try:
            price = float(sel.get("price", 0))
        except (TypeError, ValueError):
            price = None
        return price, sel

    def _extract_selection_from_odds(
        self, event_data: dict, market_url: str
    ) -> Optional[dict]:
        """Walk the Cloudbet odds response to find the full selection dict
        matching *market_url*.

        Returns the raw selection dict (contains price, cashout, minBet,
        status, params, etc.) or None if not found.
        """
        if "?" in market_url:
            path_part, query_part = market_url.split("?", 1)
        else:
            path_part = market_url
            query_part = ""

        parts = path_part.split("/")
        market_key = parts[0] if parts else ""
        outcome = parts[1] if len(parts) > 1 else ""
        params = parse_qs(query_part)

        markets = event_data.get("markets", {})
        market_data = markets.get(market_key)
        if not market_data:
            return None

        submarkets = market_data.get("submarkets", {})
        for _sub_key, sub_data in submarkets.items():
            selections = sub_data.get("selections", [])
            for sel in selections:
                if sel.get("outcome", "").lower() != outcome.lower():
                    continue

                # Check params match
                sel_params = sel.get("params", "")
                if isinstance(sel_params, str):
                    sel_parsed = parse_qs(sel_params)
                else:
                    sel_parsed = sel_params

                match = True
                for key, values in params.items():
                    sel_vals = sel_parsed.get(key, [])
                    if isinstance(sel_vals, str):
                        sel_vals = [sel_vals]
                    if values != sel_vals:
                        match = False
                        break

                if match:
                    return sel

        return None

    def _extract_price_from_odds(
        self, event_data: dict, market_url: str
    ) -> Optional[float]:
        """Walk the Cloudbet odds response to find the price matching market_url."""
        sel = self._extract_selection_from_odds(event_data, market_url)
        if sel is None:
            return None
        try:
            return float(sel.get("price", 0))
        except (TypeError, ValueError):
            return None

    def _fetch_event_odds(self, event_id: str) -> Optional[dict]:
        """Fetch the full Cloudbet odds response for an event."""
        url = f"{CLOUDBET_ODDS_EVENT_URL}/{event_id}"
        try:
            resp = self._session.get(url, timeout=self.timeout)
            if resp.status_code >= 400:
                return None
            return resp.json()
        except (requests.RequestException, ValueError):
            return None

    def _get_bet_status_with_data(self, reference_id: str) -> tuple[Optional[str], Optional[dict]]:
        """Fetch the current status and raw response data from Cloudbet.

        Returns (status_string, raw_data_dict).  raw_data is the direct
        status endpoint response when available, used to extract createTime
        and cashout fields.  Falls back to history if the direct endpoint fails.
        """
        try:
            resp = self._session.get(
                CLOUDBET_BET_STATUS_URL.format(reference_id=reference_id),
                timeout=self.timeout,
            )
            if resp.status_code == 429:
                reset = resp.headers.get("x-ratelimit-reset")
                backoff_until = time.time() + 60.0
                try:
                    if reset:
                        backoff_until = max(backoff_until, float(reset))
                except (TypeError, ValueError):
                    pass
                self._status_backoff_until = backoff_until
                logger.warning(
                    "Cloudbet status endpoint rate-limited (HTTP 429) — backing off for %.0fs",
                    max(1.0, self._status_backoff_until - time.time()),
                )
                return None, {"_skip_failure_increment": True}
            if resp.status_code >= 400:
                logger.debug(
                    "Bet status endpoint for %s: HTTP %d — trying history",
                    reference_id[:8], resp.status_code,
                )
                status = self._get_bet_status_from_history(reference_id)
                return status, None

            data = resp.json()
            logger.debug("Bet status response for %s: %s", reference_id[:8], str(data)[:300])
        except (requests.RequestException, ValueError) as exc:
            logger.warning(
                "Failed to fetch bet status for %s: %s — trying history",
                reference_id[:8], exc,
            )
            status = self._get_bet_status_from_history(reference_id)
            return status, None

        status = self._extract_status_from_payload(data, reference_id)
        if status is not None:
            return status, data if isinstance(data, dict) else None

        # Direct endpoint didn't give us a status — try history
        logger.debug("No status from direct endpoint for %s — trying history", reference_id[:8])
        status = self._get_bet_status_from_history(reference_id)
        return status, None

    def _get_bet_status(self, reference_id: str) -> Optional[str]:
        """Fetch the current status of a bet from Cloudbet's status endpoint."""
        try:
            resp = self._session.get(
                CLOUDBET_BET_STATUS_URL.format(reference_id=reference_id),
                timeout=self.timeout,
            )
            if resp.status_code == 429:
                reset = resp.headers.get("x-ratelimit-reset")
                backoff_until = time.time() + 60.0
                try:
                    if reset:
                        backoff_until = max(backoff_until, float(reset))
                except (TypeError, ValueError):
                    pass
                self._status_backoff_until = backoff_until
                logger.warning(
                    "Cloudbet status endpoint rate-limited (HTTP 429) — backing off for %.0fs",
                    max(1.0, self._status_backoff_until - time.time()),
                )
                return None
            if resp.status_code >= 400:
                logger.debug(
                    "Bet status endpoint for %s: HTTP %d — trying history",
                    reference_id[:8], resp.status_code,
                )
                return self._get_bet_status_from_history(reference_id)

            data = resp.json()
            logger.debug("Bet status response for %s: %s", reference_id[:8], str(data)[:300])
        except (requests.RequestException, ValueError) as exc:
            logger.warning(
                "Failed to fetch bet status for %s: %s — trying history",
                reference_id[:8], exc,
            )
            return self._get_bet_status_from_history(reference_id)

        status = self._extract_status_from_payload(data, reference_id)
        if status is not None:
            return status

        # Direct endpoint didn't give us a status — try history
        logger.debug("No status from direct endpoint for %s — trying history", reference_id[:8])
        return self._get_bet_status_from_history(reference_id)

    def _get_bet_status_from_history(self, reference_id: str) -> Optional[str]:
        """Fallback status lookup using recent Cloudbet bet history."""
        if self._history_endpoint_supported is False:
            return None
        try:
            resp = self._session.get(
                CLOUDBET_BET_HISTORY_URL,
                params={"limit": 10},
                timeout=self.timeout,
            )
            if resp.status_code == 404:
                if self._history_endpoint_supported is not False:
                    logger.warning(
                        "Cloudbet bet history endpoint returned HTTP 404 — disabling history fallback"
                    )
                self._history_endpoint_supported = False
                return None
            if resp.status_code >= 400:
                logger.warning(
                    "Could not fetch bet history for %s: HTTP %d",
                    reference_id[:8], resp.status_code,
                )
                return None
            self._history_endpoint_supported = True
            data = resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning(
                "Failed to fetch bet history for %s: %s",
                reference_id[:8], exc,
            )
            return None

        return self._extract_status_from_payload(data, reference_id)

    @staticmethod
    def _extract_status_from_payload(payload: Any, reference_id: str) -> Optional[str]:
        """Extract a bet status from a Cloudbet status/history payload.

        For top-level dicts from the direct status endpoint (which returns
        data for the requested bet without echoing the referenceId), we
        accept a match when the payload has a status and either the
        referenceId matches *or* is absent.  In nested/list contexts we
        require an explicit referenceId match to avoid cross-bet confusion.
        """
        if isinstance(payload, dict):
            payload_ref = payload.get("referenceId") or payload.get("reference_id")
            if payload.get("status") and (payload_ref is None or payload_ref == reference_id):
                return payload.get("status")

            for key in ("bets", "items", "results", "history", "data"):
                nested = payload.get(key)
                if nested is None:
                    continue
                status = BetExecutor._extract_status_from_nested(nested, reference_id)
                if status is not None:
                    return status

        if isinstance(payload, list):
            return BetExecutor._extract_status_from_nested(payload, reference_id)

        return None

    @staticmethod
    def _extract_status_from_nested(payload: Any, reference_id: str) -> Optional[str]:
        """Extract a bet status from nested Cloudbet history data (strict ref matching)."""
        if isinstance(payload, dict):
            payload_ref = payload.get("referenceId") or payload.get("reference_id")
            if payload.get("status") and payload_ref == reference_id:
                return payload.get("status")

            for key in ("bets", "items", "results", "history", "data"):
                nested = payload.get(key)
                if nested is None:
                    continue
                status = BetExecutor._extract_status_from_nested(nested, reference_id)
                if status is not None:
                    return status

        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                payload_ref = item.get("referenceId") or item.get("reference_id")
                if payload_ref == reference_id and item.get("status"):
                    return item.get("status")

        return None
