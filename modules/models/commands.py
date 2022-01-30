from __future__ import annotations

import logging
from decimal import Decimal
from typing import Dict, Optional

from pydantic import BaseModel, condecimal

from modules.models.line import BookUpdateModel
from modules.models.exchange import ContractModel
from modules.models.types import PositionId, OrderId, OrderSide, PositionSide


class Command(BaseModel):
    contract: ContractModel
    next_time: bool = False

    def __hash__(self):
        def _serialize(data: Dict):
            res = []
            for a, b in data.items():
                if isinstance(b, dict):
                    b = _serialize(b)
                res.append(f'{a}={b}')
            return '(' + ';'.join(res) + ')'
        return hash(_serialize(self.dict()).encode())


class TrailingStop(Command):
    price: BookUpdateModel
    order_side: OrderSide
    callback_rate: condecimal(gt=Decimal('0'), le=Decimal('0.02'))
    next_command: Command

    @property
    def stop_size(self) -> Decimal:
        if self.order_side == OrderSide.BUY:
            return self.price.bid * self.callback_rate
        else:
            return self.price.ask * self.callback_rate

    @property
    def stop_loss(self) -> Decimal:
        if self.order_side == OrderSide.BUY:
            return self.price.bid + self.stop_size
        else:
            return self.price.ask - self.stop_size

    def update(self, price: BookUpdateModel):
        triggered = False
        precision = self.contract.price_decimals

        if self.order_side == OrderSide.BUY:
            if (price.bid + self.stop_size) < self.stop_loss:
                self.price = price
                logging.info(f'New low observed: '
                             f'Updating stop loss to {self.stop_loss:.{precision}f}')

            elif price.bid >= self.stop_loss:
                triggered = True
                logging.info(f'Buy triggered | '
                             f'Price: {price.bid:.{precision}f} | '
                             f'Stop loss: {self.stop_loss:.{precision}f}')

        elif self.order_side == OrderSide.SELL:
            if (price.ask - self.stop_size) > self.stop_loss:
                self.price = price
                logging.info(f'New high observed: '
                             f'Updating stop loss to {self.stop_loss:.{precision}f}')

            elif price.ask <= self.stop_loss:
                triggered = True
                logging.info(f'Sell triggered | '
                             f'Price: {price.ask:.{precision}f} | '
                             f'Stop loss: {self.stop_loss:.{precision}f}')

        return triggered


class PlaceOrder(Command):
    position_side: PositionSide
    order_side: OrderSide
    quantity: Decimal
    context: Optional[Dict]


class Notify(Command):
    position_id: PositionId
    order_id: OrderId
    message: str
