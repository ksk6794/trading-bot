from decimal import Decimal

from modules.models import TakeProfitConfig, StopLossConfig
from modules.models.types import OrderSide, PositionSide, TickType

from .base import BaseStrategy


class TestStrategy(BaseStrategy):
    # Base strategy configuration
    name = 'test'
    # stop_loss = StopLossConfig(
    #     rate=Decimal('0.001'),
    # )
    # take_profit = TakeProfitConfig(
    #     steps=[
    #         {'level': Decimal('0.001'), 'stake': Decimal('1')},
    #     ]
    # )

    def check_signal(self, tick_type: TickType):
        pass
        # create = False
        # position = self.storage.get_position(position_side=PositionSide.LONG)
        #
        # if not position:
        #     quantity = Decimal('0.002')
        #
        #     if not quantity:
        #         return
        #
        #     self.open_long(quantity)
        #     create = True
        #
        # if position and not create:
        #     self.close_long(position.quantity)
