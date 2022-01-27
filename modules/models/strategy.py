from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, condecimal, validator

from modules.models.line import BookUpdateModel
from modules.models.types import OrderId, PositionSide, Timestamp, Symbol, PositionStatus, PositionId, OrderSide


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
    strategy: str
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
