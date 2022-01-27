from decimal import Decimal

from modules.models import TakeProfitConfig, PositionModel, StopLossConfig
from modules.models.types import OrderSide, PositionSide, TickType

from .base import BaseStrategy


class PumpStrategy(BaseStrategy):
    # Base strategy configuration
    name = 'pump'
    stop_loss = StopLossConfig(
        rate=Decimal('0.05'),
    )
    take_profit = TakeProfitConfig(
        steps=[
            {'level': Decimal('0.005'), 'stake': Decimal('0.35')},
            {'level': Decimal('0.01'), 'stake': Decimal('0.25')},
            {'level': Decimal('0.015'), 'stake': Decimal('0.25')},
            {'level': Decimal('0.02'), 'stake': Decimal('0.15')},
        ]
    )

    # Strategy specific configuration
    # level_balance = {
    #     1: Decimal('0.1'),
    #     2: Decimal('0.2'),
    #     3: Decimal('0.5'),
    #     4: Decimal('0.75'),
    #     5: Decimal('1'),
    # }

    def check_signal(self, tick_type: TickType):
        # rsi = self.candles.get_rsi()
        # level = self.candles.get_dump_level()
        # print(f'RSI: {rsi};')

        price = self.price.bid
        position = self.storage.get_position(position_side=PositionSide.SHORT)
        balance_stake = Decimal('0.1')
        quantity = self.calc_trade_quantity(balance_stake, OrderSide.SELL)

        if not quantity:
            return

        if position:
            exit_orders = self.storage.get_orders(position.id, OrderSide.BUY)

            if len(exit_orders) >= 2:
                return

            enter_orders = self.storage.get_orders(position.id, OrderSide.SELL)
            enter_orders = list(sorted(enter_orders, key=lambda x: x.timestamp))

            if len(enter_orders) >= 2:
                return

            # Сравнить разницу средней цены входа с текущей ценой
            pct_change = self._pct_change(position.entry_price, price)

            if pct_change > -5:
                return

        print('open', bool(position))
        self.open_short(
            quantity=quantity,
            trailing=True
        )

    @staticmethod
    def _pct_change(prev_price: Decimal, cur_price: Decimal):
        return (prev_price - cur_price) / cur_price * 100
