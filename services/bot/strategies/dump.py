import logging
from decimal import Decimal

from modules.models import TakeProfitConfig, PositionModel, StopLossConfig
from modules.models.types import OrderSide, PositionSide, TickType

from .base import BaseStrategy


class DumpStrategy(BaseStrategy):
    # Base strategy configuration
    name = 'dump'
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
        rsi = self.candles.get_rsi()
        # level = self.candles.get_dump_level()
        print(f'RSI: {rsi};')

        # Более слабые dump не интересны
        if rsi <= 20:
            price = self.price.ask
            position = self.storage.get_position(position_side=PositionSide.LONG)
            balance_stake = Decimal('0.1')
            quantity = self.calc_trade_quantity(balance_stake, OrderSide.BUY)

            if not quantity:
                return

            if position:
                sell_orders = self.storage.get_orders(position.id, OrderSide.SELL)

                # Не докупать в позицию, где уже было 3 продажи (take profit)
                if len(sell_orders) >= 3:
                    return

                buy_orders = self.storage.get_orders(position.id, OrderSide.BUY)
                buy_orders = list(sorted(buy_orders, key=lambda x: x.timestamp))

                # Разрешить не более 3 покупок в позицию
                if len(buy_orders) >= 3:
                    return

                # Сравнить разницу средней цены входа с текущей ценой
                pct_change = self._pct_change(position.entry_price, price)

                if pct_change < 7:
                    return

            self.open_long(
                quantity=quantity,
                trailing=True
            )

    # async def check_stop_loss(self, position: PositionModel):
    #     entry_price = position.entry_price
    #     stop_loss_rate = self.stop_loss.rate
    #     exit_orders = self.storage.get_orders(position.id, OrderSide.SELL)
    #
    #     if len(exit_orders) > 2:
    #         # Stop loss от цены входа предыдущего take profit
    #         exit_orders = list(sorted(exit_orders, key=lambda x: x.timestamp))
    #         exit_order = exit_orders[-2]
    #         entry_price = exit_order.entry_price
    #
    #     price = self.price.bid
    #     trigger = entry_price * (1 - stop_loss_rate)
    #     triggered = price <= trigger
    #
    #     if triggered:
    #         logging.warning(
    #             f'Stop loss triggered! '
    #             f'position_id={position.id}; '
    #             f'trigger={trigger}; '
    #             f'price={price};'
    #         )
    #         self.close_position(
    #             position=position,
    #             context={'reason': 'Stop loss triggered'}
    #         )

    @staticmethod
    def _pct_change(prev_price: Decimal, cur_price: Decimal):
        return (prev_price - cur_price) / cur_price * 100
