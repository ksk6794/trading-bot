from decimal import Decimal

from modules.models import StopLossConfig, TakeProfitConfig
from modules.models.types import OrderSide, PositionSide, TickType, Symbol, Timeframe

from .base import BaseStrategy


class ScalpStrategy(BaseStrategy):
    # Base strategy configuration
    name = 'scalp'
    trailing_callback_rate = Decimal('0.001')
    stop_loss = StopLossConfig(
        rate=Decimal('0.025'),
    )
    take_profit = TakeProfitConfig(
        steps=[
            {'level': Decimal('0.005'), 'stake': Decimal('0.2')},
            {'level': Decimal('0.010'), 'stake': Decimal('0.4')},
            {'level': Decimal('0.015'), 'stake': Decimal('0.4')},
        ]
    )
    balance_stake = Decimal('0.25')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._rsi = None
        self._stoch = None

    def check_signal(self):
        candles = self.candles[(Symbol('BTCUSDT'), Timeframe('5m'))]

        self._rsi = candles.get_rsi()
        self._stoch = candles.get_stochastic()

        # LONG
        if (
                self._rsi and self._rsi <= 35 and
                self._stoch['%K'] and self._stoch['%D'] and self._stoch['%K'] <= 40 and self._stoch['%D'] <= 40
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
                self._rsi and self._rsi >= 65 and
                self._stoch['%K'] and self._stoch['%D'] and self._stoch['%K'] >= 60 and self._stoch['%D'] >= 60
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
