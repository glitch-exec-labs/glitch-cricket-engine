"""Multi-account bet executor — fans out bets to multiple Cloudbet accounts.

Wraps multiple BetExecutor instances. When place_bet() is called,
it places the same bet on ALL accounts. Settlement tracking is per-account.

Usage in config:
  "cloudbet_accounts": [
    {"name": "main", "api_key": "key1"},
    {"name": "client", "api_key": "key2"}
  ]
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import requests

from modules.bet_executor import BetExecutor, LiveBet

CLOUDBET_BALANCE_URL = "https://sports-api.cloudbet.com/pub/v1/account/currencies/{currency}/balance"

logger = logging.getLogger("ipl_spotter.multi_executor")


class MultiBetExecutor:
    """Wraps multiple BetExecutor instances for multi-account betting."""

    def __init__(self, config: dict, paper_mode: bool = False):
        self.executors: Dict[str, BetExecutor] = {}
        self.primary_name: str = ""

        accounts = config.get("cloudbet_accounts", [])
        if not accounts:
            # Single account fallback — use main config
            self.executors["main"] = BetExecutor(config, paper_mode=paper_mode)
            self.primary_name = "main"
            logger.info("Single account mode: main")
        else:
            for acc in accounts:
                name = acc.get("name", f"account_{len(self.executors)}")
                acc_config = dict(config)
                acc_config["cloudbet_api_key"] = acc["api_key"]
                # Per-account overrides
                if "max_position_size_usd" in acc:
                    acc_config["max_position_size_usd"] = acc["max_position_size_usd"]
                if "default_stake_usd" in acc:
                    acc_config["default_stake_usd"] = acc["default_stake_usd"]
                if "default_currency" in acc:
                    acc_config["default_currency"] = acc["default_currency"]
                    # Ensure this currency is in the allowed list
                    allowed = list(acc_config.get("allowed_currencies", []))
                    if acc["default_currency"] not in allowed:
                        allowed.append(acc["default_currency"])
                        acc_config["allowed_currencies"] = allowed

                # Each account gets its own state store for bet persistence
                acc_store = None
                try:
                    from modules.state_store import StateStore
                    acc_store = StateStore(db_path=f"data/bot_state_{name}.db")
                except Exception:
                    pass
                self.executors[name] = BetExecutor(acc_config, paper_mode=paper_mode, state_store=acc_store)
                if not self.primary_name:
                    self.primary_name = name
                logger.info("Account '%s' initialized", name)

            logger.info("Multi-account mode: %d accounts (%s)",
                        len(self.executors), ", ".join(self.executors.keys()))

        # Per-account balance cache for independent stake sizing
        self._primary_bankroll: float = 0.0
        self._account_balances: Dict[str, float] = {}
        self._balance_last_fetched: Dict[str, float] = {}
        self._balance_cache_ttl: float = 30.0  # seconds

    def update_primary_bankroll(self, bankroll: float) -> None:
        """Called by spotter after main-account balance sync, so we can size client stakes correctly."""
        if bankroll > 0:
            self._primary_bankroll = bankroll

    @property
    def primary(self) -> BetExecutor:
        """Return the primary executor (used for status, open_bets, etc.)."""
        return self.executors[self.primary_name]

    # ── Delegate properties to primary ────────────────────────────────

    @property
    def open_bets(self) -> Dict[str, LiveBet]:
        """Aggregate open bets from ALL accounts for contradiction checking."""
        all_bets: Dict[str, LiveBet] = {}
        for executor in self.executors.values():
            all_bets.update(executor.open_bets)
        return all_bets

    @property
    def closed_bets(self) -> List[LiveBet]:
        return self.primary.closed_bets

    @property
    def daily_pnl(self) -> float:
        return sum(e.daily_pnl for e in self.executors.values())

    @property
    def total_pnl(self) -> float:
        return sum(e.total_pnl for e in self.executors.values())

    @property
    def paper_mode(self) -> bool:
        return self.primary.paper_mode

    # ── Place bet on ALL accounts ─────────────────────────────────────

    def place_bet(self, **kwargs) -> Optional[LiveBet]:
        """Place the same bet on all accounts. Returns the primary's result.

        For non-primary accounts, stake is rescaled using that account's own
        live balance so client accounts bet with their own funds even when the
        main account bankroll is depleted.

        Special case: when the primary stake is $0 (bankroll depleted) and the
        primary is intentionally skipped, but a client account successfully places
        a bet, the client's LiveBet is returned so the caller can properly record
        the bet, send Telegram notifications, and keep the market locked.
        """
        primary_result = None
        _primary_stake_zero = float(kwargs.get("stake", 0)) <= 0
        _first_client_result: Optional[LiveBet] = None

        for name, executor in self.executors.items():
            try:
                account_kwargs = dict(kwargs)
                if name != self.primary_name:
                    rescaled = self._get_account_stake(name, executor, kwargs)
                    if rescaled <= 0:
                        logger.info(
                            "[%s] Skipping bet — rescaled stake is $0 (balance too low or unavailable)",
                            name,
                        )
                        continue
                    account_kwargs["stake"] = rescaled
                else:
                    # Primary account: skip silently if stake is 0 (bankroll depleted)
                    if float(account_kwargs.get("stake", 0)) <= 0:
                        logger.info("[%s] Skipping — primary stake is $0 (bankroll depleted)", name)
                        continue

                result = executor.place_bet(**account_kwargs)
                if name == self.primary_name:
                    primary_result = result
                elif result and _first_client_result is None:
                    _first_client_result = result
                if result:
                    logger.info("[%s] Bet placed: %s %s @ %.2f stake=$%.2f",
                                name, account_kwargs.get("market", ""), account_kwargs.get("direction", ""),
                                account_kwargs.get("price", 0), account_kwargs.get("stake", 0))
                else:
                    logger.warning("[%s] Bet failed: %s %s",
                                   name, account_kwargs.get("market", ""), account_kwargs.get("direction", ""))
            except Exception:
                logger.exception("[%s] Bet placement error", name)

        # If primary failed or was skipped (depleted bankroll) but a client placed
        # successfully, return the client's bet so the caller:
        #   - sends the Telegram "bet placed" notification
        #   - keeps the market locked (no clear_locks / retry loop)
        #   - records the cooldown via record_bet_placed
        # This covers two scenarios:
        #   A) primary stake=$0 → intentionally skipped
        #   B) primary stake>0 but Cloudbet rejects it (balance $0.31 < stake)
        if primary_result is None and _first_client_result is not None:
            logger.info(
                "Primary bet not placed (stake=$0 or rejected) — returning client bet to keep market locked"
            )
            return _first_client_result

        return primary_result

    def _get_account_stake(self, name: str, executor: BetExecutor, kwargs: dict) -> float:
        """Calculate the appropriate stake for a non-primary account using its own balance.

        Two strategies depending on whether the primary account placed a bet:

        A) Primary stake > 0: scale proportionally.
           kelly_fraction ≈ primary_stake / primary_bankroll
           client_stake   = client_balance × kelly_fraction

        B) Primary stake = 0 (bankroll depleted): recalculate from scratch.
           Uses ev_pct + price already in kwargs to run Kelly independently
           against the client account's own live balance.
        """
        primary_stake = float(kwargs.get("stake", 0))

        # Refresh client balance cache
        now = time.time()
        last_fetched = self._balance_last_fetched.get(name, 0.0)
        if now - last_fetched > self._balance_cache_ttl:
            balance = self._fetch_account_balance(name, executor)
            if balance is not None:
                self._account_balances[name] = balance
            self._balance_last_fetched[name] = now

        client_balance = self._account_balances.get(name, 0.0)
        if client_balance <= 0:
            logger.warning("[%s] Could not fetch balance — skipping", name)
            return 0.0

        cap = min(float(executor.max_position_size), 25.0)
        min_stake = float(executor.config.get("min_stake_usd", 0.10))

        if primary_stake > 0 and self._primary_bankroll > 0:
            # Strategy A: proportional scaling
            kelly_fraction = primary_stake / self._primary_bankroll
            client_stake = client_balance * kelly_fraction
            method = f"proportional (kelly={kelly_fraction:.4f})"
        else:
            # Strategy B: independent Kelly from ev_pct + price
            ev_pct = float(kwargs.get("ev_pct", 0))
            odds = float(kwargs.get("price", 0))
            market = kwargs.get("market", "")

            if ev_pct <= 0 or odds <= 1.0:
                logger.info("[%s] Skipping — no ev_pct/price to size stake independently", name)
                return 0.0

            min_ev = float(executor.config.get("min_ev_pct", 5.0))
            if ev_pct < min_ev:
                logger.info("[%s] Skipping — EV %.1f%% below min %.1f%%", name, ev_pct, min_ev)
                return 0.0

            fractional_kelly = float(executor.config.get("fractional_kelly", 0.25))
            raw_kelly = (ev_pct / 100.0) / (odds - 1.0) * fractional_kelly

            # Market multiplier (mirrors RiskManager.MARKET_STAKE_MULT)
            _mkt_mult = {
                "10_over": 1.5, "15_over": 1.5, "20_over": 1.3,
                "innings_total": 1.3, "6_over": 1.0, "powerplay_runs": 1.0,
                "over_runs": 0.5, "match_winner": 0.8,
            }.get(market, 1.0)

            client_stake = client_balance * raw_kelly * _mkt_mult

            # Apply default_stake_usd floor if configured
            default_stake = float(executor.config.get("default_stake_usd", 0.0))
            if default_stake > 0:
                client_stake = max(client_stake, default_stake * _mkt_mult)

            method = f"independent Kelly (ev={ev_pct:.1f}% odds={odds:.2f} kelly={raw_kelly:.4f})"

        client_stake = min(client_stake, cap)

        if client_stake < min_stake:
            logger.info("[%s] Computed stake $%.2f below minimum $%.2f — skipping", name, client_stake, min_stake)
            return 0.0

        logger.info(
            "[%s] Stake: balance=$%.2f → $%.2f via %s",
            name, client_balance, client_stake, method,
        )
        return round(client_stake, 2)

    def _fetch_account_balance(self, name: str, executor: BetExecutor) -> Optional[float]:
        """Fetch live USDC/USD balance for this account using its own API session."""
        currencies = list(executor.allowed_currencies) if executor.allowed_currencies else ["USDC", "USDT", "USD"]
        for currency in currencies:
            url = CLOUDBET_BALANCE_URL.format(currency=currency)
            try:
                resp = executor._session.get(url, timeout=10)
                if resp.ok:
                    data = resp.json()
                    bal = float(data.get("amount", 0))
                    if bal > 0:
                        logger.info("[%s] Balance fetched: $%.2f %s", name, bal, currency)
                        return bal
            except (requests.RequestException, ValueError, TypeError):
                logger.exception("[%s] Failed to fetch balance for currency %s", name, currency)
        logger.warning("[%s] No positive balance found across currencies: %s", name, currencies)
        return None

    def has_open_bet(self, **kwargs) -> bool:
        """Check ALL accounts for open bets — prevents duplicates across accounts."""
        return any(e.has_open_bet(**kwargs) for e in self.executors.values())

    def get_open_bet(self, **kwargs) -> Optional[LiveBet]:
        return self.primary.get_open_bet(**kwargs)

    # ── Check settlements on ALL accounts ─────────────────────────────

    def check_settlements(self) -> List[LiveBet]:
        """Check settlements on all accounts. Returns primary's settled list."""
        primary_settled = []
        for name, executor in self.executors.items():
            try:
                settled = executor.check_settlements()
                if name == self.primary_name:
                    primary_settled = settled
                for bet in settled:
                    logger.info("[%s] Bet settled: %s %s → %s pnl=$%.2f",
                                name, bet.market, bet.direction, bet.status, bet.pnl)
            except Exception:
                logger.exception("[%s] Settlement check error", name)
        return primary_settled

    # ── Delegate formatting to primary ────────────────────────────────

    def format_bet_placed(self, bet: LiveBet) -> str:
        msg = self.primary.format_bet_placed(bet)
        if len(self.executors) > 1:
            # If bet belongs to a non-primary executor (primary bankroll depleted),
            # make it clear which account placed it.
            primary_bets = self.primary.open_bets
            if bet.reference_id not in primary_bets:
                # Find which account placed this bet
                for name, executor in self.executors.items():
                    if name != self.primary_name and bet.reference_id in executor.open_bets:
                        msg += f"\n   (client account: {name} — primary bankroll depleted)"
                        break
                else:
                    msg += f"\n   (client account — primary bankroll depleted)"
            else:
                msg += f"\n   (placed on {len(self.executors)} accounts)"
        return msg

    def format_bet_settled(self, bet: LiveBet) -> str:
        return self.primary.format_bet_settled(bet)

    def pop_newly_confirmed(self) -> List[LiveBet]:
        """Collect newly PENDING→ACCEPTED bets from ALL accounts."""
        confirmed: List[LiveBet] = []
        for executor in self.executors.values():
            confirmed.extend(executor.pop_newly_confirmed())
        return confirmed

    def get_status(self) -> dict:
        """Aggregate status across all accounts."""
        primary_status = self.primary.get_status()
        if len(self.executors) <= 1:
            return primary_status

        total_pnl = sum(e.total_pnl for e in self.executors.values())
        daily_pnl = sum(e.daily_pnl for e in self.executors.values())
        # Use the aggregated open_bets property — avoids double-counting the same
        # logical market position that was placed on multiple accounts simultaneously.
        open_bets = len(self.open_bets)

        primary_status["total_pnl"] = round(total_pnl, 4)
        primary_status["daily_pnl"] = round(daily_pnl, 4)
        primary_status["open_bets"] = open_bets
        primary_status["accounts"] = len(self.executors)
        return primary_status
