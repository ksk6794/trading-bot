from datetime import timedelta, datetime
from decimal import Decimal
from typing import List, Optional, Dict, Union

from modules.models import CandleModel, TradeUpdateModel
from modules.models.types import Timeframe, TickType

from .constants import TIMEFRAME_S
from .technical import TechnicalAnalysis


class Candles:
    def __init__(self, timeframe: Timeframe, candles_limit: int = 100):
        self.timeframe_ms = TIMEFRAME_S[timeframe] * 1000
        self._technical = TechnicalAnalysis(timeframe)
        self._candles_limit = candles_limit
        self._raw: List[Dict] = []

    def __len__(self):
        return len(self._raw)

    def __getitem__(self, item) -> Optional[Union[CandleModel, List[CandleModel]]]:
        try:
            res = self._raw[item]
        except IndexError:
            res = None

        rows: List[Dict] = res if isinstance(res, list) else [res] if res else []
        candles = [CandleModel(**i) for i in rows]
        return candles[0] if candles else candles or None

    def __getattr__(self, item):
        if self._technical.df is None:
            self._technical.build_dataframe(self._raw)
        return getattr(self._technical, item)

    def set_snapshot(self, candles: List[CandleModel]):
        self._raw.clear()
        candles = candles[-self._candles_limit:]

        for prev, cur in zip(candles, candles[1:]):
            self._append(prev)
            missing_cnt = int((cur.timestamp - prev.timestamp) / self.timeframe_ms) - 1

            # Filling in gaps in a snapshot
            for n in range(1, missing_cnt + 1):
                new_ts = prev.timestamp + self.timeframe_ms * n
                price = prev.close
                new_candle = CandleModel(
                    timestamp=new_ts,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=0
                )
                self._append(new_candle)

    def update(self, model: TradeUpdateModel) -> Optional[TickType]:
        tick_type = None
        last_candle = self._raw and CandleModel(**self._raw[-1])

        if not last_candle:
            t = datetime.fromtimestamp(model.timestamp / 1000)
            dt = t.replace(second=0, microsecond=0, minute=0, hour=t.hour) + timedelta(hours=t.minute // 30)
            ts = int(dt.timestamp() * 1000)
            last_candle = CandleModel(
                timestamp=ts,
                open=model.price,
                high=model.price,
                low=model.price,
                close=model.price,
                volume=0,
            )
            self._append(last_candle)
            return None

        # Same candle
        if model.timestamp < last_candle.timestamp + self.timeframe_ms:
            tick_type = TickType.SAME_CANDLE
            volume = last_candle.volume + model.quantity
            self._update(index=-1, close=model.price, volume=volume)

            if model.price > last_candle.high:
                last_candle.high = model.price

            elif model.price < last_candle.low:
                last_candle.low = model.price

        # Missing candles
        elif model.timestamp >= last_candle.timestamp + self.timeframe_ms * 2:
            tick_type = TickType.MISSING_CANDLE
            missing_cnt = int((model.timestamp - last_candle.timestamp) / self.timeframe_ms) - 1

            for n in range(1, missing_cnt + 1):
                new_ts = last_candle.timestamp + self.timeframe_ms * n
                price = last_candle.close
                new_candle = CandleModel(
                    timestamp=new_ts,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=0,
                )
                self._append(new_candle)

        # New candle
        elif model.timestamp >= last_candle.timestamp + self.timeframe_ms:
            tick_type = TickType.NEW_CANDLE
            new_ts = last_candle.timestamp + self.timeframe_ms
            new_candle = CandleModel(
                timestamp=new_ts,
                open=model.price,
                high=model.price,
                low=model.price,
                close=model.price,
                volume=model.quantity,
            )
            self._append(new_candle)

        if tick_type:
            self._technical.reset()

        if tick_type is TickType.NEW_CANDLE:
            if self._technical.df is None:
                self._technical.build_dataframe(self._raw)

        return tick_type

    def _append(self, candle: CandleModel):
        self._raw.append(candle.dict())

        if len(self._raw) > self._candles_limit:
            self._raw.pop(0)

    def _update(self, index: int, close: Decimal, volume: Decimal):
        self._raw[index]['close'] = close
        self._raw[index]['volume'] = volume

        if close < self._raw[index]['low']:
            self._raw[index]['low'] = close

        if close > self._raw[index]['high']:
            self._raw[index]['high'] = close
