from __future__ import annotations
from datetime import date
import math

from app.models import (
    TradeSignal,
    OptionContract,
    ContractScore,
    PositionCandidate,
)


class ContractSelector:
    def __init__(self, cfg: dict):
        self.cfg = cfg

    @staticmethod
    def _dte(expiration: date, today: date) -> int:
        return max(0, (expiration - today).days)

    @staticmethod
    def _project_price(
        contract: OptionContract,
        underlying_move: float,
        minutes: float = 10.0,
    ) -> float:
        """
        Fast local approximation for ranking, not a guaranteed option price.
        Uses delta/gamma/theta when Robinhood exposes them.
        """
        side_sign = 1.0 if contract.side == "CALL" else -1.0
        d = abs(contract.delta) if contract.delta is not None else 0.45
        g = max(contract.gamma or 0.0, 0.0)
        theta_per_day = contract.theta or 0.0

        signed_move_for_option = side_sign * underlying_move
        change = d * signed_move_for_option + 0.5 * g * (underlying_move ** 2)
        change += theta_per_day * (minutes / (24.0 * 60.0))
        return max(0.01, contract.mid + change)

    def rank(
        self,
        signal: TradeSignal,
        chain: list[OptionContract],
        today: date | None = None,
    ) -> list[ContractScore]:
        """Legacy single-contract ranking kept for backward-compatible selector tests."""
        if signal.event != "ENTRY" or not signal.direction:
            return []
        if signal.entry is None or signal.target is None or signal.stop is None:
            raise ValueError("ENTRY signal must contain entry, target and stop")

        today = today or date.today()
        side = "CALL" if signal.direction == "LONG" else "PUT"
        c = self.cfg
        out: list[ContractScore] = []

        target_under_move = signal.target - signal.entry
        stop_under_move = signal.stop - signal.entry

        for opt in chain:
            if opt.side != side:
                continue

            dte = self._dte(opt.expiration, today)
            if not (c.get("min_dte", 0) <= dte <= c.get("max_dte", 3)):
                continue

            mid = opt.mid
            if not (c.get("min_premium", 0.01) <= mid <= c.get("max_premium", 999.0)):
                continue
            if opt.open_interest < c.get("min_open_interest", 0):
                continue
            if opt.volume < c.get("min_volume", 0):
                continue
            if opt.spread_pct > c.get("max_spread_pct", 0.50):
                continue
            if opt.buy_blocked:
                continue

            ad = abs(opt.delta) if opt.delta is not None else c.get("target_delta", 0.48)
            min_delta = c.get("min_delta")
            max_delta = c.get("max_delta")
            if opt.delta is not None and min_delta is not None and max_delta is not None:
                if not (min_delta <= ad <= max_delta):
                    continue

            target_price = self._project_price(opt, target_under_move)
            stop_price = self._project_price(opt, stop_under_move)
            entry_cost = max(opt.ask, 0.01)
            target_ret = (target_price - entry_cost) / entry_cost
            stop_ret = (stop_price - entry_cost) / entry_cost

            downside = max(abs(min(stop_ret, 0.0)), 0.01)
            reward_risk = max(target_ret, 0.0) / downside

            target_delta = c.get("target_delta", 0.48)
            delta_fit = 1.0 - min(abs(ad - target_delta) / 0.40, 1.0)
            target_premium = max(c.get("target_premium", 1.50), 0.01)
            price_fit = 1.0 - min(abs(mid - target_premium) / target_premium, 1.0)
            max_spread = max(c.get("max_spread_pct", 0.50), 1e-6)
            spread_fit = max(0.0, 1.0 - opt.spread_pct / max_spread)
            liquidity = min(
                1.0,
                0.5 * math.log1p(opt.volume) / math.log1p(3000)
                + 0.5 * math.log1p(opt.open_interest) / math.log1p(15000),
            )

            projected_roi_component = max(-1.0, min(target_ret, 1.5))
            rr_component = min(reward_risk / 4.0, 1.0)

            score = (
                c.get("weight_projected_roi", 3.0) * projected_roi_component
                + c.get("weight_reward_risk", 2.2) * rr_component
                + c.get("weight_delta_fit", 1.0) * delta_fit
                + c.get("weight_liquidity", 1.0) * liquidity
                + c.get("weight_spread", 1.6) * spread_fit
                + c.get("weight_price_fit", 0.6) * price_fit
            )

            out.append(
                ContractScore(
                    contract=opt,
                    score=score,
                    projected_target_return=target_ret,
                    projected_stop_return=stop_ret,
                    reward_risk=reward_risk,
                    spread_pct=opt.spread_pct,
                    notes=[
                        f"DTE={dte}",
                        f"delta={ad:.2f}",
                        f"spread={opt.spread_pct:.1%}",
                        f"targetROI≈{target_ret:.1%}",
                        f"stopROI≈{stop_ret:.1%}",
                        f"RR≈{reward_risk:.2f}",
                    ],
                )
            )

        out.sort(key=lambda x: x.score, reverse=True)
        return out

    def rank_positions(
        self,
        signal: TradeSignal,
        chain: list[OptionContract],
        trade_budget_dollars: float,
        today: date | None = None,
        relax_execution_quality: bool = False,
        current_underlying_price: float | None = None,
    ) -> list[PositionCandidate]:
        """
        Rank CONTRACT + QUANTITY together.

        This is intentionally less rigid than the old selector. It rejects only
        obvious execution problems, then uses delta, spread, liquidity and the
        TradingView target/stop to score how the whole position behaves under
        the configured dollar budget.
        """
        if signal.event != "ENTRY" or not signal.direction:
            return []
        if signal.entry is None or signal.target is None or signal.stop is None:
            raise ValueError("ENTRY signal must contain entry, target and stop")
        if trade_budget_dollars <= 0:
            return []

        today = today or date.today()
        side = "CALL" if signal.direction == "LONG" else "PUT"
        c = self.cfg
        out: list[PositionCandidate] = []

        # Rank from the *current* QQQ spot, not from the TradingView signal's
        # original entry reference. On a fast 0DTE signal, part of the move can
        # already be gone by the time Robinhood quotes the option. Using the
        # original distance would overstate remaining upside and understate risk.
        reference_underlying = float(current_underlying_price) if current_underlying_price is not None else float(signal.entry)
        target_move = float(signal.target) - reference_underlying
        stop_move = float(signal.stop) - reference_underlying
        hold_minutes = float(c.get("expected_hold_minutes", 10.0))

        for opt in chain:
            if opt.side != side or opt.buy_blocked:
                continue

            dte = self._dte(opt.expiration, today)
            if not (int(c.get("min_dte", 0)) <= dte <= int(c.get("max_dte", 3))):
                continue

            ask = max(float(opt.ask or 0.0), 0.0)
            if ask <= 0:
                continue

            per_contract = ask * 100.0
            qty = int(trade_budget_dollars // per_contract)
            max_contracts = int(c.get("max_contracts_per_trade", 10) or 10)
            if max_contracts > 0:
                qty = min(qty, max_contracts)
            if qty < 1:
                continue

            # Execution quality is intentionally light. Volume and open interest
            # are SOFT ranking signals, not reasons to miss a valid near-price 0DTE
            # trade. The only production hard quality gate here is an unusably
            # wide live spread. Local closed-market tests may relax even that.
            if not relax_execution_quality:
                if opt.spread_pct > float(c.get("hard_max_spread_pct", 0.45)):
                    continue
                if int(opt.open_interest or 0) < int(c.get("hard_min_open_interest", 0) or 0):
                    continue
                if int(opt.volume or 0) < int(c.get("hard_min_volume", 0) or 0):
                    continue

            ad = abs(opt.delta) if opt.delta is not None else 0.40
            if opt.delta is not None and ad < float(c.get("hard_min_abs_delta", 0.03)):
                continue

            target_mid = self._project_price(opt, target_move, hold_minutes)
            stop_mid = self._project_price(opt, stop_move, hold_minutes)

            # Entry is realistically paid near the ask. A future exit is sold
            # near the bid, not magically at the projected midpoint. Preserve the
            # current dollar spread as a conservative round-trip friction model:
            # projected_bid = projected_mid - half current spread. This charges
            # roughly the full bid/ask spread across buy-at-ask -> sell-at-bid.
            spread_dollars = max(float(opt.ask) - float(opt.bid), 0.0)
            half_spread = spread_dollars / 2.0
            target_price = max(0.01, target_mid - half_spread)
            stop_price = max(0.01, stop_mid - half_spread)

            target_ret = (target_price - ask) / ask
            stop_ret = (stop_price - ask) / ask

            capital = qty * per_contract
            target_profit = qty * 100.0 * (target_price - ask)
            stop_profit = qty * 100.0 * (stop_price - ask)

            # Projected option economics are a ranking heuristic, not a hard
            # profitability oracle. 0DTE IV/gamma/spreads can move faster than the
            # local approximation, and a hard zero-dollar gate would silently
            # suppress SCALPER's intentionally tiny reaction setups. Only apply a
            # hard projected-profit floor when the operator explicitly configures
            # one; otherwise let the score penalize weak economics.
            hard_min_target_profit = c.get("hard_min_projected_target_profit_dollars")
            if hard_min_target_profit is not None and target_profit < float(hard_min_target_profit):
                continue

            downside = max(abs(min(stop_profit, 0.0)), 1.0)
            reward_risk = max(target_profit, 0.0) / downside
            budget_utilization = min(capital / trade_budget_dollars, 1.0)

            # Soft quality features. None of these independently blocks a trade.
            target_delta = float(c.get("target_delta", 0.45))
            delta_width = max(float(c.get("delta_soft_width", 0.45)), 0.01)
            delta_fit = max(0.0, 1.0 - abs(ad - target_delta) / delta_width)

            spread_soft_ceiling = max(float(c.get("spread_soft_ceiling", 0.25)), 1e-6)
            spread_fit = max(0.0, 1.0 - opt.spread_pct / spread_soft_ceiling)

            liquidity = min(
                1.0,
                0.20 * math.log1p(max(opt.volume, 0)) / math.log1p(10000)
                + 0.80 * math.log1p(max(opt.open_interest, 0)) / math.log1p(20000),
            )

            # Score the actual budgeted position, not just one contract.
            target_roi_on_budget = target_profit / trade_budget_dollars
            stop_loss_on_budget = max(-stop_profit / trade_budget_dollars, 0.0)
            target_component = max(-1.0, min(target_roi_on_budget, 1.5))
            rr_component = min(reward_risk / 4.0, 1.0)

            # A small penalty for extreme lottery-like delta, but not a hard wall.
            extreme_delta_penalty = 0.0
            if ad < 0.10:
                extreme_delta_penalty = (0.10 - ad) / 0.10

            score = (
                float(c.get("weight_position_target", 4.0)) * target_component
                + float(c.get("weight_reward_risk", 2.0)) * rr_component
                + float(c.get("weight_liquidity", 1.1)) * liquidity
                + float(c.get("weight_spread", 1.4)) * spread_fit
                + float(c.get("weight_delta_fit", 0.8)) * delta_fit
                + float(c.get("weight_budget_utilization", 0.25)) * budget_utilization
                - float(c.get("weight_extreme_delta_penalty", 0.9)) * extreme_delta_penalty
                - float(c.get("weight_stop_loss", 0.35)) * stop_loss_on_budget
            )

            out.append(
                PositionCandidate(
                    contract=opt,
                    qty=qty,
                    capital_required=round(capital, 2),
                    score=score,
                    projected_target_price=target_price,
                    projected_stop_price=stop_price,
                    projected_target_profit_dollars=target_profit,
                    projected_stop_profit_dollars=stop_profit,
                    projected_target_return=target_ret,
                    projected_stop_return=stop_ret,
                    reward_risk=reward_risk,
                    spread_pct=opt.spread_pct,
                    budget_utilization=budget_utilization,
                    notes=[
                        f"DTE={dte}",
                        f"qty={qty}",
                        f"capital=${capital:.2f}/${trade_budget_dollars:.2f}",
                        f"delta={ad:.3f}",
                        f"gamma={float(opt.gamma or 0.0):.4f}",
                        f"theta={float(opt.theta or 0.0):.4f}",
                        f"spread={opt.spread_pct:.1%}",
                        f"remainingTargetMove={target_move:+.3f}",
                        f"remainingStopMove={stop_move:+.3f}",
                        f"targetExec≈${target_price:.2f}",
                        f"stopExec≈${stop_price:.2f}",
                        f"targetPnL≈${target_profit:.2f}",
                        f"stopPnL≈${stop_profit:.2f}",
                        f"RR≈{reward_risk:.2f}",
                    ],
                )
            )

        out.sort(key=lambda x: x.score, reverse=True)
        return out
