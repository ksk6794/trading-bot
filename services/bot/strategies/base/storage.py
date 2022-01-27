from typing import Dict, Optional, List

from modules.models import OrderModel, PositionModel
from modules.models.types import OrderId, PositionId, OrderSide, PositionSide


class LocalStorage:
    def __init__(self):
        self._positions: Dict[PositionSide, PositionModel] = {}
        self._orders: Dict[OrderId: OrderModel] = {}

    def set_snapshot(self, positions: List[PositionModel], orders: List[OrderModel]):
        for position in positions:
            self.set_position(position)
        self.set_orders(orders)

    def set_position(self, position: PositionModel):
        self._positions[position.side] = position

    def drop_position(self, position_side: PositionSide):
        self._positions.pop(position_side, None)

    def get_position(self, position_side: PositionSide) -> Optional[PositionModel]:
        return self._positions.get(position_side)

    def set_orders(self, orders: List[OrderModel]):
        self._orders = {order.id: order for order in orders}

    def get_order(self, order_id: OrderId) -> Optional[OrderModel]:
        return self._orders.get(order_id)

    def get_orders(self, position_id: PositionId, order_side: Optional[OrderSide] = None) -> List[OrderModel]:
        orders = []

        for order in self._orders.values():
            if order.position_id != position_id:
                continue

            if order_side and order.side != order_side:
                continue

            orders.append(order)

        return orders

    def add_order(self, order: OrderModel):
        self._orders[order.id] = order

    def drop_orders(self, position_id: PositionId):
        self._orders = {
            order_id: order
            for order_id, order in self._orders.items()
            if order.position_id != position_id
        }
