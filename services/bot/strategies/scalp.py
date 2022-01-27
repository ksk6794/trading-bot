from decimal import Decimal

from modules.models import StopLossConfig, PositionModel
from modules.models.types import OrderSide, PositionSide, TickType

from .base import BaseStrategy


class ScalpStrategy(BaseStrategy):
    # Base strategy configuration
    name = 'scalp'
    candles_limit = 100
    depth_limit = 0
    signal_check_interval = None
    trailing_callback_rate = Decimal('0.005')
    stop_loss = StopLossConfig(
        rate=Decimal('0.015'),
    )
    balance_stake = Decimal('0.4')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._rsi = None
        self._stoch = None
        self._bb = None
        self._watch = False

    def check_signal(self, tick_type: TickType):
        if tick_type is TickType.NEW_CANDLE:
            index = -2
            prev_candle = self.candles[-2]

            self._rsi = self.candles.get_rsi(index=index)
            self._stoch = self.candles.get_stochastic(index=index)
            self._bb = self.candles.get_bollinger_bands(index=index, length=50, width=2)

            if (
                    not self._watch and
                    self._rsi and self._rsi <= 30 and
                    self._stoch['%K'] and self._stoch['%D'] and self._stoch['%K'] <= 20 and self._stoch['%D'] <= 20 and
                    self._bb and self._bb['bb_lower'] and prev_candle.close < self._bb['bb_lower']
            ):
                self._watch = True
                return

            if self._watch and prev_candle.close > self._bb['bb_lower']:
                self._watch = False
                position = self.storage.get_position(PositionSide.LONG)

                if position:
                    open_orders = self.storage.get_orders(position.id, OrderSide.BUY)

                    if len(open_orders) > 2:
                        return

                quantity = self.calc_trade_quantity(self.balance_stake, OrderSide.BUY)

                if not quantity:
                    return

                self.open_long(
                    quantity=quantity,
                    trailing=True,
                    context={
                        'timestamp': self.candles[-1].timestamp,
                        'low': prev_candle.low,
                        'high': prev_candle.high,
                        'rsi': self._rsi,
                    }
                )

    def check_take_profit(self, position: PositionModel):
        price = self.price.ask

        if (
                self._rsi and self._rsi >= 70 and
                self._stoch['%K'] and self._stoch['%D'] and self._stoch['%K'] >= 80 and self._stoch['%D'] >= 80 and
                self._bb['bb_ma'] and price >= self._bb['bb_ma']
        ):
            self.close_position(position)

    @staticmethod
    def _pct_change(prev_price: Decimal, cur_price: Decimal):
        return (prev_price - cur_price) / cur_price * 100
