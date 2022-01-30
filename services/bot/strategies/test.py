from decimal import Decimal

from modules.models import TakeProfitConfig, StopLossConfig
from modules.models.types import OrderSide, PositionSide, TickType

from .base import BaseStrategy


class TestStrategy(BaseStrategy):
    # Base strategy configuration
    name = 'test'
    stop_loss = StopLossConfig(
        rate=Decimal('0.005'),
    )
    take_profit = TakeProfitConfig(
        steps=[
            {'level': Decimal('0.005'), 'stake': Decimal('1')},
        ]
    )

    def check_signal(self, tick_type: TickType):
        rsi = self.candles.get_rsi()
        print(f'RSI: {rsi}')
        position = self.storage.get_position(position_side=PositionSide.LONG)

        if not position:
            balance_stake = Decimal('0.1')
            quantity = self.calc_trade_quantity(balance_stake, OrderSide.BUY)

            if not quantity:
                return

            self.open_long(
                quantity=quantity,
                trailing=True,
            )
