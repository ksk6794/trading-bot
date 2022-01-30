from decimal import Decimal

from modules.models import StopLossConfig, TakeProfitConfig
from modules.models.types import OrderSide, PositionSide, TickType

from .base import BaseStrategy


class ScalpStrategy(BaseStrategy):
    # Base strategy configuration
    name = 'scalp'
    trailing_callback_rate = Decimal('0.002')
    stop_loss = StopLossConfig(
        rate=Decimal('0.015'),
    )
    take_profit = TakeProfitConfig(
        steps=[
            {'level': Decimal('0.005'), 'stake': Decimal('0.5')},
            {'level': Decimal('0.008'), 'stake': Decimal('0.5')},
        ]
    )
    balance_stake = Decimal('0.1')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._rsi = None
        self._stoch = None

    def check_signal(self, tick_type: TickType):
        self._rsi = self.candles.get_rsi()
        self._stoch = self.candles.get_stochastic()
        print(f'PRICE: {self.price};', f'RSI: {self._rsi};', f'STOCH: {self._stoch};')

        # LONG
        if (
                self._rsi and self._rsi <= 40 and
                self._stoch['%K'] and self._stoch['%D'] and self._stoch['%K'] <= 35 and self._stoch['%D'] <= 35
        ):
            position = self.storage.get_position(PositionSide.LONG)

            if position:
                entry_orders = self.storage.get_orders(position.id, OrderSide.BUY)

                if len(entry_orders) >= 2:
                    return

                # Сравнить разницу средней цены входа с текущей ценой
                pct_change = self._pct_change(position.entry_price, self.price.ask)

                if pct_change < 0.5:
                    return

            quantity = self.calc_trade_quantity(self.balance_stake, OrderSide.BUY)

            if not quantity:
                return

            self.open_long(
                quantity=quantity,
                trailing=True,
            )

        # SHORT
        if (
                self._rsi and self._rsi >= 60 and
                self._stoch['%K'] and self._stoch['%D'] and self._stoch['%K'] >= 65 and self._stoch['%D'] >= 65
        ):
            position = self.storage.get_position(PositionSide.SHORT)

            if position:
                entry_orders = self.storage.get_orders(position.id, OrderSide.SELL)

                if len(entry_orders) >= 2:
                    return

                # Сравнить разницу средней цены входа с текущей ценой
                pct_change = self._pct_change(position.entry_price, self.price.bid)

                if pct_change > -0.5:
                    return

            quantity = self.calc_trade_quantity(self.balance_stake, OrderSide.SELL)

            if not quantity:
                return

            self.open_short(
                quantity=quantity,
                trailing=True,
            )

    @staticmethod
    def _pct_change(prev_price: Decimal, cur_price: Decimal):
        return (prev_price - cur_price) / cur_price * 100
