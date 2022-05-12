from typing import Dict, Optional, List

from modules.models import OrderModel, PositionModel
from modules.models.types import OrderId, PositionId, OrderSide, PositionSide, Symbol


class LocalStorage:
    def __init__(self):
        self._positions: Dict[Symbol, Dict[PositionSide, PositionModel]] = {}
        self._orders: Dict[Symbol, Dict[OrderId: OrderModel]] = {}

    def set_snapshot(self, symbol: Symbol, positions: List[PositionModel], orders: List[OrderModel]):
        for position in positions:
            self.set_position(position.symbol, position)
        self.set_orders(symbol, orders)

    def set_position(self, symbol: Symbol, position: PositionModel):
        self._positions.setdefault(symbol, {})[position.side] = position

    def drop_position(self, symbol: Symbol, position_side: PositionSide):
        self._positions.get(symbol, {}).pop(position_side, None)

    def get_position(self, symbol: Symbol, position_side: PositionSide) -> Optional[PositionModel]:
        return self._positions.get(symbol, {}).get(position_side)

    def set_orders(self, symbol: Symbol, orders: List[OrderModel]):
        self._orders[symbol] = {order.id: order for order in orders}

    def get_order(self, symbol: Symbol, order_id: OrderId) -> Optional[OrderModel]:
        return self._orders.get(symbol, order_id)

    def get_orders(
            self,
            symbol: Symbol,
            position_id: PositionId,
            order_side: Optional[OrderSide] = None
    ) -> List[OrderModel]:
        orders = []

        for order in self._orders.get(symbol, {}).values():
            if order.position_id != position_id:
                continue

            if order_side and order.side != order_side:
                continue

            orders.append(order)

        return orders

    def add_order(self, symbol: Symbol, order: OrderModel):
        self._orders.setdefault(symbol, {})[order.id] = order

    def drop_orders(self, symbol: Symbol, position_id: PositionId):
        self._orders[symbol] = {
            order_id: order
            for order_id, order in self._orders[symbol].items()
            if order.position_id != position_id
        }
