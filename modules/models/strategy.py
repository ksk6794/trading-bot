from decimal import Decimal
from typing import List, Optional, Any

from pydantic import BaseModel, condecimal, validator
from pydantic.types import conint

from modules.models.line import BookUpdateModel
from modules.models.types import (
    OrderId, PositionId, OrderSide, PositionSide, PositionStatus, Timestamp, Symbol,
    StrategyId, Condition, Timeframe, Indicator
)


class StopLossConfig(BaseModel):
    rate: condecimal(gt=Decimal('0'), le=Decimal('1'))


class TakeProfitConfig(BaseModel):
    class Step(BaseModel):
        level: condecimal(gt=Decimal('0'), le=Decimal('10'))    # 0.1 - 10%, 1 - 100%, 10 - 1000%
        stake: condecimal(gt=Decimal('0'), le=Decimal('1'))

    steps: List[Step]

    @property
    def steps_count(self):
        return len(self.steps)

    @validator('steps')
    def validate_steps(cls, value):
        total = sum([i.stake for i in value])

        if total != 1:
            raise ValueError('The sum of stakes should be equal to 1')

        return value


class PositionModel(BaseModel):
    id: PositionId
    symbol: Symbol
    side: PositionSide
    strategy_id: StrategyId
    status: PositionStatus
    quantity: Decimal
    total_quantity: Decimal
    entry_price: Decimal
    exit_price: Decimal
    orders: List[OrderId]
    create_timestamp: Timestamp
    update_timestamp: Optional[Timestamp]

    @property
    def is_open(self):
        return self.status == PositionStatus.OPEN

    @property
    def is_closed(self):
        return self.status == PositionStatus.CLOSED

    def calc_pnl(self, price: BookUpdateModel) -> Decimal:
        pnl = Decimal('0')

        if self.side is PositionSide.LONG:
            pnl = (price.bid - self.entry_price) * self.quantity

        elif self.side == PositionSide.SHORT:
            pnl = (self.entry_price - price.ask) * self.quantity

        return pnl

    def get_entry_order_side(self) -> OrderSide:
        return OrderSide.BUY if self.side is PositionSide.LONG else OrderSide.SELL

    def get_exit_order_side(self) -> OrderSide:
        return OrderSide.SELL if self.side is PositionSide.LONG else OrderSide.BUY


class StrategyRules(BaseModel):
    class StrategyCondition(BaseModel):
        class IndicatorParameter(BaseModel):
            field: str
            value: Any

        class IndicatorCondition(BaseModel):
            field: str
            condition: Condition
            value: Decimal

        position_side: PositionSide
        order_side: OrderSide
        timeframe: Timeframe
        indicator: Indicator
        parameters: List[IndicatorParameter]
        conditions: List[IndicatorCondition]
        save_signal_candles: conint(ge=1, le=10) = 1

    id: StrategyId
    name: str

    binance_testnet: bool = False
    binance_public_key: Optional[str]
    binance_private_key: Optional[str]

    trailing: bool = False
    balance_stake: condecimal(gt=Decimal('0'), le=Decimal('1'))
    leverage: conint(ge=1, le=25) = 1
    trailing_callback_rate: Optional[Decimal]

    symbols: List[Symbol]
    conditions: List[StrategyCondition]
    conditions_trigger_count: int

    stop_loss: Optional[StopLossConfig] = None
    take_profit: Optional[TakeProfitConfig] = None
