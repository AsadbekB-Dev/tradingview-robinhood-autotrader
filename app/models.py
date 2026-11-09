from __future__ import annotations
from datetime import datetime, date, timezone
from typing import Literal, Optional
from pydantic import BaseModel, Field

Direction = Literal["LONG", "SHORT"]
OptionSide = Literal["CALL", "PUT"]


class TradeSignal(BaseModel):
    source: str = "MAIN"
    event: Literal["ENTRY", "EXIT", "STOP_DAY"]
    symbol: str = "QQQ"
    direction: Optional[Direction] = None
    setup: Optional[str] = None
    entry: Optional[float] = None
    target: Optional[float] = None
    stop: Optional[float] = None
    lead: Optional[float] = None
    opportunity: Optional[float] = None
    tier: Optional[str] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    raw: str
    origin_at: Optional[datetime] = None
    signal_id: Optional[str] = None
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))


class OptionContract(BaseModel):
    symbol: str
    instrument_id: Optional[str] = None
    expiration: date
    strike: float
    side: OptionSide
    bid: float
    ask: float
    volume: int = 0
    open_interest: int = 0
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    iv: Optional[float] = None
    mark: Optional[float] = None
    last_trade: Optional[float] = None
    bid_size: Optional[int] = None
    ask_size: Optional[int] = None
    vega: Optional[float] = None
    rho: Optional[float] = None
    chance_of_profit_long: Optional[float] = None
    chance_of_profit_short: Optional[float] = None
    low_fill_rate_buy_price: Optional[float] = None
    high_fill_rate_buy_price: Optional[float] = None
    low_fill_rate_sell_price: Optional[float] = None
    high_fill_rate_sell_price: Optional[float] = None
    break_even_price: Optional[float] = None
    updated_at: Optional[str] = None
    sellout_datetime: Optional[str] = None
    buy_blocked: bool = False
    expires_soon_warning: bool = False

    @property
    def mid(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2.0
        if self.mark is not None and self.mark > 0:
            return self.mark
        return max(self.bid, self.ask)

    @property
    def spread_pct(self) -> float:
        m = self.mid
        return (self.ask - self.bid) / m if m > 0 else 999.0


class ContractScore(BaseModel):
    contract: OptionContract
    score: float
    projected_target_return: float
    projected_stop_return: float
    reward_risk: float
    spread_pct: float
    notes: list[str] = Field(default_factory=list)


class PositionCandidate(BaseModel):
    contract: OptionContract
    qty: int
    capital_required: float
    score: float
    projected_target_price: float
    projected_stop_price: float
    projected_target_profit_dollars: float
    projected_stop_profit_dollars: float
    projected_target_return: float
    projected_stop_return: float
    reward_risk: float
    spread_pct: float
    budget_utilization: float
    notes: list[str] = Field(default_factory=list)


class Position(BaseModel):
    id: str
    underlying: str
    direction: Direction
    contract: OptionContract
    qty: int
    entry_underlying: float
    entry_option_price: float
    opened_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
