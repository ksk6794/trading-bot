import asyncio
import logging
from time import time
from typing import Dict, List

from modules.mongo import MongoClient
from modules.exchanges import BinanceClient
from modules.line_client import BulkLineClient

from modules.models import TradeUpdateModel, BookUpdateModel
from modules.models.types import StreamEntity, Symbol, TickType, Timestamp
from modules.models.indexes import INDEXES

from services.bot.state import ExchangeState
from services.bot.settings import Settings, StrategyRules
from services.bot.strategies import Strategy


class StrategiesOrchestrator:
    def __init__(self, settings: Settings):
        self.settings = settings

        self.db = MongoClient(
            mongo_uri=settings.mongo_uri,
            indexes=INDEXES,
        )
        self.line = BulkLineClient(
            symbols=settings.symbols,
            uri=settings.broker_amqp_uri,
            entities=[StreamEntity.BOOK, StreamEntity.TRADE],
        )
        self.exchange = BinanceClient(
            testnet=settings.binance_testnet,
        )
        self.state = ExchangeState(self.exchange, self.settings.symbols, self.settings.candles_limit)

        self._strategies: Dict[Symbol: List[Strategy]] = []
        self._loop = asyncio.get_event_loop()

        self.line.add_update_callback(StreamEntity.BOOK, self._on_book_update)
        self.line.add_update_callback(StreamEntity.TRADE, self._on_trade_update)
        # self.line.add_update_callback(StreamEntity.DEPTH, self._on_depth_update)

    async def start(self):
        await self.state.preload()
        await self.line.connect()

    async def stop(self):
        await self.line.disconnect()

    async def add_strategy(self, rules: StrategyRules):
        strategy = Strategy(rules, self.db, self.state)
        self._strategies.append(strategy)
        await strategy.start()

    async def _on_book_update(self, symbol: Symbol, model: BookUpdateModel):
        self.state.update_book(symbol, model)

        for strategy in self._strategies:
            await strategy.on_book_update(symbol)

    async def _on_trade_update(self, symbol: Symbol, model: TradeUpdateModel):
        tick_types = self.state.update_candles(symbol, model)
        skip = self._check_delay(model.timestamp)

        if skip:
            return

        if any([tick_type is TickType.NEW_CANDLE for tick_type in tick_types.values()]):
            logging.info(f'{symbol}: Updating strategies...')

            for strategy in self._strategies:
                await strategy.on_candles_update(symbol)

    def _check_delay(self, timestamp: Timestamp) -> bool:
        skip = False
        diff = time() * 1000 - timestamp
        exchange_name = self.exchange.__class__.__name__

        if diff >= 2000:
            logging.warning(f'{exchange_name}: Messages processing delay of {diff / 1000:.2f}s!')
            skip = True

        return skip
