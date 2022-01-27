from typing import Optional, List, Tuple, Dict
from decimal import Decimal

import numpy as np
import pandas
import pandas as pd
from matplotlib import pyplot as plt

from helpers import to_decimal

from modules.models.types import Timeframe

from .constants import TIMEFRAME_FREQ


class TechnicalAnalysis:
    def __init__(self, timeframe: Timeframe):
        self.timeframe_freq = TIMEFRAME_FREQ[timeframe]
        self._df = None
        self._initial_columns = {'timestamp', 'low', 'high', 'open', 'close', 'volume'}

    @property
    def df(self):
        return self._df

    def reset(self):
        self._df = None

    def build_dataframe(self, raw: List[Dict]):
        df = pd.DataFrame(data=raw, columns=self._initial_columns)

        # Convert the DataFrame into a time series with the date as the index/key
        idx = pd.to_datetime(df['timestamp'], unit='ms')
        tsidx = pd.DatetimeIndex(idx, dtype='datetime64[ns]', freq=self.timeframe_freq)
        df.set_index(tsidx, inplace=True)
        df = df.drop(columns=['timestamp'])
        df.index.names = ['ts']
        df['timestamp'] = tsidx

        # Correct column types
        df['low'] = df['low'].astype(float)
        df['high'] = df['high'].astype(float)
        df['open'] = df['open'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)

        # Reset pandas dataframe index
        df.reset_index()

        self._df = df

    ##############################
    #    TECHNICAL INDICATORS    #
    ##############################

    def get_ma(self, index: int = -1, period: int = 12):
        self._set_ma(period)

        ma = self._get(f'ma_{period}', index)
        return ma and to_decimal(ma)

    def get_ema(self, index: int = -1, period: int = 12):
        self._set_ema(period)

        ema = self._get(f'ema_{period}', index)
        return ema and to_decimal(ema)

    def get_rsi(self, index: int = -1, period: int = 14) -> Optional[Decimal]:
        if 'rsi' not in self._df:
            self._set_rsi(period)

        rsi = self._get('rsi', index)
        return rsi and to_decimal(rsi)

    def get_roc(self, index: int = -1, period: int = 18) -> Optional[Decimal]:
        if 'roc' not in self._df:
            self._set_roc(period)

        roc = self._get('roc', index)
        return roc and to_decimal(roc)

    def get_stochastic(self, index: int = -1, k_period: int = 14, d_period: int = 3) -> Dict[str, Optional[Decimal]]:
        self._set_stochastic(k_period, d_period)

        k = self._get('%K', index)
        d = self._get('%D', index)

        return {
            '%K': k and to_decimal(k),
            '%D': d and to_decimal(d),
        }

    def get_obv(self, index: int = -1) -> Dict[str, Optional[Decimal]]:
        if 'obv' not in self._df:
            self._set_obv()

        obv = self._get('obv', index)
        obv_pc = self._get('obv_pc', index)

        return {
            'obv': obv and to_decimal(obv),
            'obv_pc': obv_pc and to_decimal(obv_pc),
        }

    def get_eri_signals(self, index: int = -1) -> Dict[str, bool]:
        if 'eri_buy' not in self._df:
            self._set_eri_signals()

        return {
            'eri_buy': bool(self._get('eri_buy', index)),
            'eri_sell': bool(self._get('eri_sell', index)),
        }

    def get_ema_signals(self, index: int = -1) -> Dict[str, bool]:
        if 'ema_golden_cross' not in self._df:
            self._set_ema_signals()

        return {
            'ema_golden_cross': bool(self._get('ema_golden_cross', index)),
            'ema_golden_cross_co': bool(self._get('ema_golden_cross_co', index)),
            'ema_death_cross': bool(self._get('ema_death_cross', index)),
            'ema_death_cross_co': bool(self._get('ema_death_cross_co', index)),
        }

    def get_sma_signals(self, index: int = -1) -> Dict[str, bool]:
        if 'sma_golden_cross' not in self._df:
            self._set_sma_signals()

        return {
            'sma_golden_cross': bool(self._get('sma_golden_cross', index)),
            'sma_death_cross': bool(self._get('sma_death_cross', index)),
            'sma_golden_cross_co': bool(self._get('sma_golden_cross_co', index)),
            'sma_death_cross_co': bool(self._get('sma_death_cross_co', index)),
        }

    def get_macd_signals(
            self,
            index: int = -1,
            fast_length: int = 12,
            slow_length: int = 26,
            signal_smoothing: int = 9
    ) -> Dict[str, bool]:
        if 'macd_gt_signal' not in self._df:
            self._set_macd_signals(fast_length, slow_length, signal_smoothing)

        return {
            'macd_gt_signal': bool(self._get('macd_gt_signal', index)),
            'macd_gt_signal_co': bool(self._get('macd_gt_signal_co', index)),
            'macd_lt_signal': bool(self._get('macd_lt_signal', index)),
            'macd_lt_signal_co': bool(self._get('macd_lt_signal_co', index)),
        }

    def get_ichimoku_signals(self, index: int = -1) -> Dict[str, bool]:
        if 'ichimoku_golden_cross' not in self._df:
            self._set_ichimoku_signals()

        return {
            'ichimoku_golden_cross': bool(self._get('ichimoku_golden_cross', index)),
            'ichimoku_death_cross': bool(self._get('ichimoku_death_cross', index)),
            'price_below_cloud': bool(self._get('price_below_cloud', index)),
            'price_above_cloud': bool(self._get('price_above_cloud', index)),
        }

    def get_bollinger_bands(self, index: int = -1, length: int = 20, width: int = 2):
        self._set_bollinger_bands(length, width)

        bb_upper = self._get('bb_u', index)
        bb_ma = self._get('bb_ma', index)
        bb_lower = self._get('bb_l', index)

        return {
            'bb_upper': bb_upper and to_decimal(bb_upper),
            'bb_ma': bb_ma and to_decimal(bb_ma),
            'bb_lower': bb_lower and to_decimal(bb_lower)
        }

    def get_bollinger_bands_signals(self, index: int = -1, length: int = 20, width: int = 2):
        self._set_bollinger_bands_signals(length, width)

        bb_buy = self._get('bb_buy', index)
        bb_sell = self._get('bb_sell', index)

        return {
            'bb_buy': bb_buy and bool(bb_buy),
            'bb_sell': bb_sell and bool(bb_sell),
        }

    def is_shooting_star(self, index: int = -1):
        self._set_candle_shooting_star()

        shooting_star = self._get('shooting_star', index)
        return shooting_star and bool(shooting_star)

    def is_hanging_man(self, index: int = -1):
        self._set_candle_hanging_man()

        hanging_map = self._get('hanging_man', index)
        return hanging_map and bool(hanging_map)

    def is_evening_star(self, index: int = -1):
        self._set_candle_evening_star()

        evening_star = self._get('evening_star', index)
        return evening_star and bool(evening_star)

    def is_hammer(self, index: int = -1):
        self._set_candle_hammer()

        hammer = self._get('hammer', index)
        return hammer and bool(hammer)

    def is_inverted_hammer(self, index: int = -1):
        self._set_candle_inverted_hammer()

        inverted_hammer = self._get('inverted_hammer', index)
        return inverted_hammer and bool(inverted_hammer)

    def is_morning_star(self, index: int = -1):
        self._set_candle_morning_star()

        morning_star = self._get('morning_star', index)
        return morning_star and bool(morning_star)

    def is_abandoned_baby(self, index: int = -1):
        self._set_candle_abandoned_baby()

        abandoned_baby = self._get('abandoned_baby', index)
        return abandoned_baby and bool(abandoned_baby)

    def get_pump_level(self, index: int = -1, period: int = 18, sensitivity_factor: int = 1) -> int:
        self._set_pump_signal(period, sensitivity_factor)

        levels = [self._get(f'level_{n}_pump_signal', index) for n in range(1, 6)]
        return levels.index(True) + 1 if True in levels else 0

    def get_dump_level(self, index: int = -1, period: int = 18, sensitivity_factor: int = 1) -> int:
        self._set_dump_signal(period, sensitivity_factor)

        levels = [self._get(f'level_{n}_dump_signal', index) for n in range(1, 6)]
        return levels.index(True) + 1 if True in levels else 0

    def _set_rsi(self, period: int):
        """
        Calculates the Relative Strength Index.
        :return: The RSI value of the previous candlestick
        """
        delta = self._df['close'].diff().dropna()
        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0
        down[down > 0] = 0
        avg_gain = up.ewm(com=(period - 1), min_periods=period).mean()
        avg_loss = down.abs().ewm(com=(period - 1), min_periods=period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - 100 / (1 + rs)
        rsi.round(2)
        self._df['rsi'] = rsi

    def _set_stochastic(self, k_period, d_period):
        self._df['n_high'] = self._df['high'].rolling(k_period).max()
        self._df['n_low'] = self._df['low'].rolling(k_period).min()
        self._df['%K'] = (self._df['close'] - self._df['n_low']) * 100 / (self._df['n_high'] - self._df['n_low'])
        self._df['%D'] = self._df['%K'].rolling(d_period).mean()

    def _set_roc(self, period: int):
        """
        Calculates the rate of change
        """
        self._df['roc'] = self._df['close'].diff(period) / self._df['close'].shift(period) * 100

    def _set_macd(self, fast_length: int = 12, slow_length: int = 26, signal_smoothing: int = 9):
        """
        Calculates the MACD and its Signal line.
        """
        ema_fast_key = f'ema_{fast_length}'
        ema_slow_key = f'ema_{slow_length}'

        if ema_fast_key not in self._df:
            self._set_ema(fast_length)

        if ema_slow_key not in self._df:
            self._set_ema(slow_length)

        # Exponential Moving Average method
        ema_fast = self._df[ema_fast_key]
        ema_slow = self._df[ema_slow_key]

        self._df['macd'] = ema_fast - ema_slow
        self._df['macd_signal'] = self._df['macd'].ewm(span=signal_smoothing).mean()

    def _set_ma(self, period: int = 12):
        assert 200 >= period >= 5

        self._df[f'ma_{period}'] = self._df['close'].rolling(window=period).mean()

    def _set_ema(self, period: int = 12):
        """
        Calculates the Exponential Moving Average (EMA)
        """
        assert 200 >= period >= 5

        self._df[f'ema_{period}'] = self._df['close'].ewm(span=period, adjust=False).mean()

    def _set_sma(self, period: int = 12):
        """
        Calculates the Simple Moving Average (SMA)
        """
        assert 200 >= period >= 5

        self._df[f'sma_{period}'] = self._df['close'].rolling(period, min_periods=1).mean()

    def _set_obv(self):
        """
        Calculates On-Balance Volume (OBV)
        """
        self._df['obv'] = np.where(
            self._df['close'] == self._df['close'].shift(1), 0,
            np.where(self._df['close'] > self._df['close'].shift(1), self._df['volume'],
                     np.where(self._df['close'] < self._df['close'].shift(1), -self._df['volume'],
                              self._df.iloc[0].volume))).cumsum()
        self._df['obv_pc'] = np.round(pd.Series(self._df['obv']).pct_change().fillna(0), 2)

    def _set_eri(self):
        """
        Calculates Elder Ray Index (ERI)
        """
        if 'ema_13' not in self._df:
            self._set_ema(13)

        self._df['bull_power'] = self._df['high'] - self._df['ema_13']
        self._df['bear_power'] = self._df['low'] - self._df['ema_13']

    def _set_ichimoku(self):
        """
        Calculates the ichimoku cloud
        """
        # Tenkan-sen (Conversion Line): (9-period high + 9-period low)/2))
        period9_high = self._df['high'].rolling(window=9).max()
        period9_low = self._df['low'].rolling(window=9).min()
        self._df['tenkan_sen'] = (period9_high + period9_low) / 2

        # Kijun-sen (Base Line): (26-period high + 26-period low)/2))
        period26_high = self._df['high'].rolling(window=26).max()
        period26_low = self._df['low'].rolling(window=26).min()
        self._df['kijun_sen'] = (period26_high + period26_low) / 2

        # Senkou Span A (Leading Span A): (Conversion Line + Base Line)/2))
        self._df['senkou_span_a'] = ((self._df.tenkan_sen + self._df.kijun_sen) / 2).shift(26)

        # Senkou Span B (Leading Span B): (52-period high + 52-period low)/2))
        self._df['period52_high'] = self._df['high'].rolling(window=52).max()
        self._df['period52_low'] = self._df['low'].rolling(window=52).min()
        self._df['senkou_span_b'] = ((self._df.period52_high + self._df.period52_low) / 2).shift(26)

        # The most current closing price plotted 22 time periods behind (optional)
        self._df['chikou_span'] = self._df['close'].shift(-22)  # 22 according to investopedia

    def _set_bollinger_bands(self, length: int = 20, width: int = 2):
        """
        Calculates Bollinger Bands
        """
        self._df['bb_tp'] = (self._df['high'] + self._df['low'] + self._df['close']) / 3
        self._df['bb_ma'] = self._df['bb_tp'].rolling(length, min_periods=length).mean()
        self._df['bb_sigma'] = self._df['bb_tp'].rolling(length, min_periods=length).std()
        self._df['bb_u'] = self._df['bb_ma'] + width * self._df['bb_sigma']
        self._df['bb_l'] = self._df['bb_ma'] - width * self._df['bb_sigma']

    def _set_candle_shooting_star(self):
        """
        Candlestick Detected: Shooting Star ("Weak - Reversal - Bearish Pattern - Down")
        """

        self._df['shooting_star'] = (
                ((self._df['open'].shift(1) < self._df['close'].shift(1)) & (
                        self._df['close'].shift(1) < self._df['open']))
                & (self._df['high'] - np.maximum(self._df['open'], self._df['close']) >= (
                abs(self._df['open'] - self._df['close']) * 3))
                & ((np.minimum(self._df['close'], self._df['open']) - self._df['low']) <= abs(
            self._df['open'] - self._df['close'])))

    def _set_candle_hanging_man(self):
        """
        Candlestick Detected: Hanging Man ("Weak - Continuation - Bearish Pattern - Down")
        """

        self._df['hanging_man'] = (
                ((self._df['high'] - self._df['low']) > (4 * (self._df['open'] - self._df['close'])))
                & (((self._df['close'] - self._df['low']) / (.001 + self._df['high'] - self._df['low'])) >= 0.75)
                & (((self._df['open'] - self._df['low']) / (.001 + self._df['high'] - self._df['low'])) >= 0.75)
                & (self._df['high'].shift(1) < self._df['open'])
                & (self._df['high'].shift(2) < self._df['open']))

    def _set_candle_evening_star(self):
        """
        Candlestick Detected: Evening Star ("Strong - Reversal - Bearish Pattern - Down")
        """

        self._df['evening_star'] = (
                ((np.minimum(self._df['open'].shift(1), self._df['close'].shift(1)) > self._df['close'].shift(2))
                 & (self._df['close'].shift(2) > self._df['open'].shift(2)))
                & ((self._df['close'] < self._df['open'])
                   & (self._df['open'] < np.minimum(self._df['open'].shift(1), self._df['close'].shift(1)))))

    def _set_candle_hammer(self):
        """* Candlestick Detected: Hammer ("Weak - Reversal - Bullish Signal - Up"""

        self._df['hammer'] = (
                ((self.df['high'] - self.df['low']) > 3 * (self.df['open'] - self.df['close']))
                & (((self.df['close'] - self.df['low']) / (.001 + self.df['high'] - self.df['low'])) > 0.6)
                & (((self.df['open'] - self.df['low']) / (.001 + self.df['high'] - self.df['low'])) > 0.6))

    def _set_candle_inverted_hammer(self):
        """
        Candlestick Detected: Inverted Hammer ("Weak - Continuation - Bullish Pattern - Up")
        """

        self._df['inverted_hammer'] = (
                ((self._df['high'] - self._df['low']) > 3 * (self._df['open'] - self._df['close']))
                & ((self._df['high'] - self._df['close']) / (.001 + self._df['high'] - self._df['low']) > 0.6)
                & ((self._df['high'] - self._df['open']) / (.001 + self._df['high'] - self._df['low']) > 0.6))

    def _set_candle_morning_star(self):
        """
        Candlestick Detected: Morning Star ("Strong - Reversal - Bullish Pattern - Up")
        """

        self._df['morning_star'] = (
                ((np.maximum(self.df['open'].shift(1), self.df['close'].shift(1)) < self.df['close'].shift(2))
                 & (self.df['close'].shift(2) < self.df['open'].shift(2)))
                & ((self.df['close'] > self.df['open'])
                   & (self.df['open'] > np.maximum(self.df['open'].shift(1), self.df['close'].shift(1)))))

    def _set_candle_abandoned_baby(self):
        """
        Candlestick Detected: Abandoned Baby ("Reliable - Reversal - Bullish Pattern - Up")
        """

        self._df['abandoned_baby'] = (
                (self.df['open'] < self.df['close'])
                & (self.df['high'].shift(1) < self.df['low'])
                & (self.df['open'].shift(2) > self.df['close'].shift(2))
                & (self.df['high'].shift(1) < self.df['low'].shift(2)))

    ##############################
    #           SIGNALS          #
    ##############################

    def _set_ema_signals(self):
        if 'ema_12' not in self._df:
            self._set_ema(12)

        if 'ema_26' not in self._df:
            self._set_ema(26)

        # true if EMA12 is above the EMA26
        self._df['ema_golden_cross'] = self._df['ema_12'] > self._df['ema_26']
        # true if the current frame is where EMA12 crosses over above
        self._df['ema_golden_cross_co'] = self._df['ema_golden_cross'].ne(self._df['ema_golden_cross'].shift())
        self._df.loc[self._df['ema_golden_cross'] == False, 'ema_golden_cross_co'] = False

        # true if the EMA12 is below the EMA26
        self._df['ema_death_cross'] = self._df['ema_12'] < self._df['ema_26']
        # true if the current frame is where EMA12 crosses over below
        self._df['ema_death_cross_co'] = self._df['ema_death_cross'].ne(self._df['ema_death_cross'].shift())
        self._df.loc[self._df['ema_death_cross'] == False, 'ema_death_cross_co'] = False

    def _set_sma_signals(self):
        if 'sma_50' not in self._df:
            self._set_sma(50)

        if 'sma_200' not in self._df:
            self._set_sma(200)

        self._df['sma_golden_cross'] = self._df['sma_50'] > self._df['sma_200']
        self._df['sma_death_cross'] = self._df['sma_50'] < self._df['sma_200']

        # true if SMA50 is above the SMA200
        self._df['sma_golden_cross'] = self._df['sma_50'] > self._df['sma_200']
        # true if the current frame is where SMA50 crosses over above
        self._df['sma_golden_cross_co'] = self._df['sma_golden_cross'].ne(self._df['sma_golden_cross'].shift())
        self._df.loc[self._df['sma_golden_cross'] == False, 'sma_golden_cross_co'] = False

        # true if the SMA50 is below the SMA200
        self._df['sma_death_cross'] = self._df['sma_50'] < self._df['sma_200']
        # true if the current frame is where SMA50 crosses over below
        self._df['sma_death_cross_co'] = self._df['sma_death_cross'].ne(self._df['sma_death_cross'].shift())
        self._df.loc[self._df['sma_death_cross'] == False, 'sma_death_cross_co'] = False

    def _set_macd_signals(self, fast_length: int = 12, slow_length: int = 26, signal_smoothing: int = 9):
        self._set_macd(fast_length, slow_length, signal_smoothing)

        # true if MACD is above the Signal
        self._df['macd_gt_signal'] = self._df['macd'] > self._df['macd_signal']
        # true if the current frame is where MACD crosses over above
        self._df['macd_gt_signal_co'] = self._df['macd_gt_signal'].ne(self._df.macd_gt_signal.shift())
        self._df.loc[self._df['macd_gt_signal'] == False, 'macd_gt_signal_co'] = False

        # true if the MACD is below the Signal
        self._df['macd_lt_signal'] = self._df['macd'] < self._df['macd_signal']
        # true if the current frame is where MACD crosses over below
        self._df['macd_lt_signal_co'] = self._df['macd_lt_signal'].ne(self._df.macd_lt_signal.shift())
        self._df.loc[self._df['macd_lt_signal'] == False, 'macd_lt_signal_co'] = False

    def _set_ichimoku_signals(self):
        if 'tenkan_sen' not in self._df:
            self._set_ichimoku()

        self._df['ichimoku_golden_cross'] = self._df['tenkan_sen'] > self._df['kijun_sen']
        self._df['ichimoku_death_cross'] = self._df['tenkan_sen'] < self._df['kijun_sen']

        # Close price is below the cloud - Bullish signal
        self._df['price_below_cloud'] = self._df['close'].lt(
            np.minimum(self._df['senkou_span_a'], self._df['senkou_span_b']))

        # Close price is above the cloud - Bearish signal
        self._df['price_above_cloud'] = self._df['close'].gt(
            np.maximum(self._df['senkou_span_a'], self._df['senkou_span_b']))

    def _set_eri_signals(self):
        """
        Calculates Elder Ray Index (ERI) Signals
        """
        if 'bull_power' not in self._df or 'bear_power' not in self._df:
            self._set_eri()

        # bear power’s value is negative but increasing (i.e. becoming less bearish)
        # bull power’s value is increasing (i.e. becoming more bullish)
        eri_buy = (((self._df['bear_power'] < 0) & (self._df['bear_power'] > self._df['bear_power'].shift(1))) |
                   (self._df['bull_power'] > self._df['bull_power'].shift(1)))

        # bull power’s value is positive but decreasing (i.e. becoming less bullish)
        # bear power’s value is decreasing (i.e., becoming more bearish)
        eri_sell = (((self._df['bull_power'] > 0) & (self._df['bull_power'] < self._df['bull_power'].shift(1))) |
                    (self._df['bear_power'] < self._df['bear_power'].shift(1)))

        self._df['eri_buy'] = eri_buy
        self._df['eri_sell'] = eri_sell

    def _set_bollinger_bands_signals(self, length: int = 20, width: int = 2):
        """
        Buy/Sell Bollinger Bands signals
        """
        if 'bb_u' not in self._df:
            self._set_bollinger_bands(length, width)

        self._df['bb_buy'] = self._df['close'] < self._df['bb_l']
        self._df['bb_sell'] = self._df['close'] > self._df['bb_u']

    def _set_pump_signal(self, period: int, sensitivity_factor: int):
        if 'roc' not in self._df:
            self._set_roc(period)

        level_1 = 6 * sensitivity_factor
        level_2 = 9 * sensitivity_factor
        level_3 = 12 * sensitivity_factor
        level_4 = 20 * sensitivity_factor
        level_5 = 30 * sensitivity_factor

        self._df['level_1_pump_signal'] = (self._df['roc'] >= level_1) & (self._df['roc'] < level_2)
        self._df['level_2_pump_signal'] = (self._df['roc'] >= level_2) & (self._df['roc'] < level_3)
        self._df['level_3_pump_signal'] = (self._df['roc'] >= level_3) & (self._df['roc'] < level_4)
        self._df['level_4_pump_signal'] = (self._df['roc'] >= level_4) & (self._df['roc'] < level_5)
        self._df['level_5_pump_signal'] = self._df['roc'] >= level_5

    def _set_dump_signal(self, period: int, sensitivity_factor: int):
        if 'roc' not in self._df:
            self._set_roc(period)

        level_1 = 6 * sensitivity_factor * -1
        level_2 = 9 * sensitivity_factor * -1
        level_3 = 12 * sensitivity_factor * -1
        level_4 = 20 * sensitivity_factor * -1
        level_5 = 30 * sensitivity_factor * -1

        self._df['level_1_dump_signal'] = (self._df['roc'] <= level_1) & (self._df['roc'] > level_2)
        self._df['level_2_dump_signal'] = (self._df['roc'] <= level_2) & (self._df['roc'] > level_3)
        self._df['level_3_dump_signal'] = (self._df['roc'] <= level_3) & (self._df['roc'] > level_4)
        self._df['level_4_dump_signal'] = (self._df['roc'] <= level_4) & (self._df['roc'] > level_5)
        self._df['level_5_dump_signal'] = self._df['roc'] <= level_5

    def _support_resistance_levels(self, show=False) -> List:
        """
        Support and Resistance levels.
        """
        levels = []

        for i in range(2, self._df.shape[0] - 2):
            if self._is_support(i):
                l = self._df['low'][i]
                if self._is_far_from_level(l, levels):
                    levels.append((i, l))
            elif self._is_resistance(i):
                l = self._df['high'][i]
                if self._is_far_from_level(l, levels):
                    levels.append((i, l))

        if show:
            self._show(levels)

        return levels

    def _is_support(self, i: float):
        c1 = self._df['low'][i] < self._df['low'][i - 1]
        c2 = self._df['low'][i] < self._df['low'][i + 1]
        c3 = self._df['low'][i + 1] < self._df['low'][i + 2]
        c4 = self._df['low'][i - 1] < self._df['low'][i - 2]
        support = c1 and c2 and c3 and c4
        return support

    def _is_resistance(self, i: float):
        c1 = self._df['high'][i] > self._df['high'][i - 1]
        c2 = self._df['high'][i] > self._df['high'][i + 1]
        c3 = self._df['high'][i + 1] > self._df['high'][i + 2]
        c4 = self._df['high'][i - 1] > self._df['high'][i - 2]
        resistance = c1 and c2 and c3 and c4
        return resistance

    def _is_far_from_level(self, l: float, levels):
        s = np.mean(self._df['high'] - self._df['low'])
        return np.sum([abs(l - x) < s for x in levels]) == 0

    def _show(self, levels: List[Tuple]):
        plt.subplot(111)
        plt.plot(self._df['close'])
        plt.ylabel('Price')
        plt.xlabel('Days')

        for level in levels:
            plt.hlines(
                y=level[1],
                xmin=self._df['timestamp'][level[0]],
                xmax=max(self._df['timestamp']),
                colors='blue'
            )

        plt.show()

    def _get(self, name, index):
        if len(self._df) >= abs(index):
            res = self._df[name].iloc[index]
            return None if pandas.isnull(res) else res
