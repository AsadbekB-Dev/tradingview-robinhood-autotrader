from __future__ import annotations
from datetime import datetime, timezone
import math

from app.models import TradeSignal, ContractScore, PositionCandidate


class RiskGate:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.trades_today = 0
        self.stop_for_day = False

    def on_stop_day(self):
        self.stop_for_day = True


    def validate_entry_signal(self, signal: TradeSignal) -> tuple[bool, str]:
        if signal.event != "ENTRY":
            return True, "not an entry"
        if signal.symbol != "QQQ":
            return False, f"unsupported symbol {signal.symbol}"
        if signal.direction not in {"LONG", "SHORT"}:
            return False, "ENTRY signal is missing a valid direction"
        values = {"entry": signal.entry, "target": signal.target, "stop": signal.stop}
        if any(v is None for v in values.values()):
            return False, "ENTRY signal must contain entry, target and stop"
        for name, value in values.items():
            value = float(value)
            if not math.isfinite(value) or value <= 0:
                return False, f"ENTRY {name} is invalid"
        entry, target, stop = float(signal.entry), float(signal.target), float(signal.stop)
        if signal.direction == "LONG" and not (target > entry > stop):
            return False, "invalid LONG geometry: require target > entry > stop"
        if signal.direction == "SHORT" and not (target < entry < stop):
            return False, "invalid SHORT geometry: require target < entry < stop"
        return True, "valid entry geometry"

    def signal_is_fresh(self, signal: TradeSignal) -> tuple[bool, str]:
        age = (
            datetime.now(timezone.utc).replace(tzinfo=None) - signal.received_at
        ).total_seconds()
        max_age = float(self.cfg.get("require_fresh_signal_seconds", 25))
        if age > max_age:
            return False, f"stale received signal ({age:.1f}s)"
        if bool(self.cfg.get("require_origin_timestamp", False)) and signal.origin_at is None:
            return False, "ENTRY signal is missing TradingView TV_TS origin timestamp"
        if signal.origin_at is not None:
            origin_age = (
                datetime.now(timezone.utc).replace(tzinfo=None) - signal.origin_at
            ).total_seconds()
            max_origin_age = float(self.cfg.get("require_fresh_origin_signal_seconds", max_age))
            if origin_age > max_origin_age:
                return False, f"stale TradingView-origin signal ({origin_age:.1f}s)"
            max_future_skew = float(self.cfg.get("max_origin_clock_skew_seconds", 10.0))
            if origin_age < -max_future_skew:
                return False, f"TradingView timestamp is {abs(origin_age):.1f}s in the future"
        return True, "fresh"

    def effective_trade_budget(
        self,
        buying_power: float | None = None,
        portfolio_value: float | None = None,
    ) -> float:
        mode = str(self.cfg.get("budget_mode", "fixed_dollars")).lower()
        fixed_budget = float(self.cfg.get("max_trade_budget_dollars", 200.0))

        if mode == "percent_of_portfolio" and portfolio_value is not None:
            pct = max(float(self.cfg.get("trade_budget_pct_of_portfolio", 0.20)), 0.0)
            budget = float(portfolio_value) * pct
            hard_cap = self.cfg.get("max_trade_budget_dollars")
            if hard_cap is not None:
                budget = min(budget, float(hard_cap))
        else:
            budget = fixed_budget

        if buying_power is not None and self.cfg.get("respect_live_buying_power", True):
            reserve = max(float(self.cfg.get("buying_power_reserve_dollars", 0.0)), 0.0)
            usable = max(float(buying_power) - reserve, 0.0)
            budget = min(budget, usable)
        return max(budget, 0.0)

    def approve_budgeted_entry(
        self,
        signal: TradeSignal,
        candidate: PositionCandidate,
        open_positions: int = 0,
    ) -> tuple[bool, str]:
        if self.stop_for_day:
            return False, "strategy issued STOP TRADING FOR TODAY"

        valid, why = self.validate_entry_signal(signal)
        if not valid:
            return False, why

        max_open = self.cfg.get("max_open_positions")
        if max_open is not None and open_positions >= int(max_open):
            return False, "max open positions reached"

        fresh, why = self.signal_is_fresh(signal)
        if not fresh:
            return False, why

        max_budget = float(self.cfg.get("max_trade_budget_dollars", 200.0))
        if candidate.capital_required > max_budget + 1e-9:
            return False, "candidate exceeds configured trade budget"

        if candidate.qty < 1:
            return False, "candidate quantity is zero"

        return True, "approved"

    # Legacy v0.1 sizing method kept for backward compatibility.
    def approve_entry(
        self,
        signal: TradeSignal,
        ranked: ContractScore,
        open_positions: int,
    ) -> tuple[bool, str, int]:
        if self.stop_for_day:
            return False, "strategy issued STOP TRADING FOR TODAY", 0

        max_open = self.cfg.get("max_open_positions", 1)
        if max_open is not None and open_positions >= int(max_open):
            return False, "max open positions reached", 0

        fresh, why = self.signal_is_fresh(signal)
        if not fresh:
            return False, why, 0

        per_contract = ranked.contract.ask * 100.0
        if per_contract <= 0:
            return False, "invalid option premium", 0

        budget = float(
            self.cfg.get(
                "max_trade_budget_dollars",
                self.cfg.get("max_premium_dollars", 350.0),
            )
        )
        qty = int(budget // per_contract)
        if qty < 1:
            return False, f"premium ${per_contract:.2f} exceeds configured budget", 0

        return True, "approved", qty

    def record_entry(self):
        self.trades_today += 1
