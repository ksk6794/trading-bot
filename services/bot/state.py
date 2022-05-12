import asyncio
import itertools
import logging
from typing import Dict, List

from modules.exchanges.base import BaseExchangeClient
from modules.models import TradeUpdateModel, ContractModel, BookUpdateModel
from modules.models.types import Timeframe, Symbol, TickType

from services.bot import Candles
# from services.bot.depth import Depth


class ExchangeState:
    TIMEFRAMES = ['1m', '5m', '15m', '30m', '1h', '4h', '6h', '1d']

    def __init__(self, exchange: BaseExchangeClient, symbols: List[Symbol], candles_limit: int):
        self.exchange = exchange
        self.candles_limit = candles_limit

        self.contracts: Dict[Symbol, ContractModel] = {}
        self.book: Dict[Symbol, BookUpdateModel] = {}
        self.candles: Dict[Symbol, Dict[Timeframe, Candles]] = {}
        # self.depth: Dict[Symbol, Depth] = {}

        for symbol, timeframe in itertools.product(symbols, self.TIMEFRAMES):
            self.candles.setdefault(symbol, {})[timeframe] = Candles(timeframe, candles_limit)

    async def preload(self):
        self.contracts = await self.exchange.get_contracts()
        self.book = await self.exchange.get_book()
        await self._preload_candles()

        # if self.settings.depth_limit:
        #     await self._preload_depth()

    def update_candles(self, symbol: Symbol, model: TradeUpdateModel) -> Dict[Timeframe, TickType]:
        output = {}

        for timeframe, candles in self.candles[symbol].items():
            tick_type = candles.update(model)
            output[timeframe] = tick_type

        return output

    def get_candles(self, symbol: Symbol, timeframe: Timeframe) -> Candles:
        return self.candles[symbol][timeframe]

    def update_book(self, symbol: Symbol, model: BookUpdateModel):
        self.book[symbol] = model

    def get_book(self, symbol: Symbol) -> BookUpdateModel:
        return self.book[symbol]

    def get_contract(self, symbol: Symbol) -> ContractModel:
        return self.contracts[symbol]

    async def _preload_candles(self):
        for symbol, timeframes in self.candles.items():
            tasks = []

            for timeframe in timeframes:
                task = self.exchange.get_historical_candles(
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=self.candles_limit,
                )
                tasks.append(task)

            logging.info(f'Preloading candlesticks for {symbol}...')
            result = await asyncio.gather(*tasks)

            for snapshot, timeframe in list(zip(result, timeframes)):
                timeframes[timeframe].set_snapshot(snapshot)

    # async def _preload_depth(self):
    #     for symbol, timeframes in self.candles.items():
    #         depth = await self.exchange.get_depth(
    #             symbol=symbol,
    #             limit=self.depth_limit,
    #         )
    #         self.depth.set_depth_snapshot(depth)
