import asyncio
import itertools
import logging
from time import time
from typing import Dict, List, Tuple

from modules.mongo import MongoClient
from modules.exchanges import BinanceClient, BinanceUserClient, BinanceUserStreamClient
from modules.line_client import LineClient

from modules.models import TradeUpdateModel, BookUpdateModel
from modules.models.types import StreamEntity, Symbol, TickType, Timestamp
from modules.models.strategy import StrategyRules
from modules.models.indexes import INDEXES

from services.bot.state import ExchangeState
from services.bot.settings import Settings
from services.bot.strategies import Strategy


class StrategiesOrchestrator:
    def __init__(self, settings: Settings):
        self.settings = settings

        self.db = MongoClient(
            mongo_uri=settings.mongo_uri,
            indexes=INDEXES,
        )
        self.line = LineClient(
            symbols=settings.symbols,
            uri=settings.broker_amqp_uri,
            entities=[StreamEntity.BOOK, StreamEntity.TRADE],
        )
        self.exchange = BinanceClient(
            testnet=settings.binance_testnet,
        )
        self.state = ExchangeState(
            exchange=self.exchange,
            symbols=self.settings.symbols,
            candles_limit=self.settings.candles_limit
        )

        self.user_clients: Dict[Tuple[str, str], Tuple[BinanceUserClient, BinanceUserStreamClient]] = {}

        self._strategies: Dict[Symbol: List[Strategy]] = []
        self._loop = asyncio.get_event_loop()
        self._event = asyncio.Event()

        # if settings.depth_limit:
        #     self.depth = Depth(
        #         limit=settings.depth_limit,
        #     )
        #     self.depth.add_gap_callback(self._set_depth_snapshot)

        self.line.add_reset_callback(self._on_line_reset)
        self.line.add_update_callback(StreamEntity.BOOK, self._on_book_update)
        self.line.add_update_callback(StreamEntity.TRADE, self._on_trade_update)
        # self.line.add_update_callback(StreamEntity.DEPTH, self._on_depth_update)

    async def start(self):
        await self.db.connect()
        await self.state.preload()
        await self.line.connect()
        self._event.set()

    async def stop(self):
        await self.line.disconnect()

    async def run_strategy(self, rules: StrategyRules):
        key = (rules.binance_public_key, rules.binance_private_key)

        if key in self.user_clients:
            client, stream = self.user_clients[key]

        else:
            client = BinanceUserClient(
                public_key=rules.binance_public_key,
                private_key=rules.binance_private_key,
                testnet=rules.binance_testnet,
            )
            stream = BinanceUserStreamClient(
                exchange=client,
                testnet=rules.binance_testnet,
            )
            self.user_clients[key] = (client, stream)

        strategy = Strategy(rules, self.db, self.state, client, stream)
        await self._event.wait()
        await strategy.start()
        self._strategies.append(strategy)

    async def _on_line_reset(self):
        await self.state.preload()

    async def _on_book_update(self, symbol: Symbol, model: BookUpdateModel):
        self.state.update_book(symbol, model)
        has_commands: List[Strategy] = []

        for strategy in self._strategies:
            if strategy.command_handler.has_outgoing_commands(symbol):
                has_commands.append(strategy)
            else:
                await strategy.on_book_update(symbol)

        await self._execute(symbol, has_commands)

    async def _on_trade_update(self, symbol: Symbol, model: TradeUpdateModel):
        tick_types = self.state.update_candles(symbol, model)
        skip = self._check_delay(model.timestamp)

        if skip:
            return

        if any([tick_type is TickType.NEW_CANDLE for tick_type in tick_types.values()]):
            has_commands = []

            for strategy in self._strategies:
                if strategy.command_handler.has_outgoing_commands(symbol):
                    has_commands.append(strategy)
                else:
                    await strategy.on_candles_update(symbol)

            await self._execute(symbol, has_commands)

    async def _execute(self, symbol: Symbol, strategies: List[Strategy]):
        for strategies in self.chunked(strategies, 10):
            await asyncio.gather(*[s.command_handler.execute(symbol) for s in strategies])
            await asyncio.sleep(0.5)

    @staticmethod
    def chunked(iterable, n):
        it = iter(iterable)
        while True:
            chunk = tuple(itertools.islice(it, n))
            if not chunk:
                return
            yield chunk

    # def _on_depth_update(self, model: DepthUpdateModel):
    #     if not self._ready:
    #         return
    #
    #     self.state.depth_update(model)

    def _check_delay(self, timestamp: Timestamp) -> bool:
        skip = False
        diff = time() * 1000 - timestamp
        exchange_name = self.exchange.__class__.__name__

        if diff >= 5000:
            logging.warning(f'{exchange_name}: Messages processing delay of {diff / 1000:.2f}s!')
            skip = True

        return skip
