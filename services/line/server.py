import asyncio
import logging
from typing import Dict

from modules.exchanges import BinanceStreamClient
from modules.models.indexes import INDEXES
from modules.mongo.client import MongoClient
from modules.models.line import TradeUpdateModel, BookUpdateModel, DepthUpdateModel
from modules.models.types import Symbol, StreamEntity

from .settings import Settings
from .publisher import LinePublisher


class LineServer:
    def __init__(
            self,
            settings: Settings,
    ):
        self.symbols = settings.symbols
        self.db = MongoClient(settings.mongo_uri, INDEXES)

        self.stream = BinanceStreamClient(testnet=settings.binance_testnet)
        self.publisher = LinePublisher(settings.broker_amqp_uri)
        self.prices: Dict[Symbol, BookUpdateModel] = {}
        self._loop = asyncio.get_event_loop()
        self._started = False

        self.stream.add_connect_callback(self._on_connect)
        self.stream.add_update_callback(StreamEntity.TRADE, self._on_trade_update)
        self.stream.add_update_callback(StreamEntity.BOOK, self._on_book_update)
        self.stream.add_update_callback(StreamEntity.DEPTH, self._on_depth_update)

    async def start(self):
        await self.publisher.connect()
        await self.stream.connect()

        self._started = True
        self._loop.create_task(self._alive_task())

    async def stop(self):
        self._started = False
        self._loop.stop()

    async def _on_connect(self):
        await self.stream.subscribe(self.symbols)

    async def _on_trade_update(self, symbol: Symbol, model: TradeUpdateModel):
        await self.publisher.publish(
            action='update',
            payload={
                'entity': StreamEntity.TRADE,
                'symbol': symbol,
                'data': model.dict(),
            },
            routing_key=f'{symbol}.{StreamEntity.TRADE}',
        )

    async def _on_book_update(self, symbol: Symbol, model: BookUpdateModel):
        prices = self.prices.get(symbol)
        cur_bid = prices.bid if prices else None
        cur_ask = prices.ask if prices else None

        if cur_bid != model.bid or cur_ask != model.ask:
            self.prices[symbol] = model

            await self.publisher.publish(
                action='update',
                payload={
                    'entity': StreamEntity.BOOK,
                    'symbol': symbol,
                    'data': model.dict(),
                },
                routing_key=f'{symbol}.{StreamEntity.BOOK}',
            )

    async def _on_depth_update(self, symbol: Symbol, model: DepthUpdateModel):
        await self.publisher.publish(
            action='update',
            payload={
                'entity': StreamEntity.DEPTH,
                'symbol': symbol,
                'data': model.dict(),
            },
            routing_key=f'{symbol}.{StreamEntity.DEPTH}',
        )

    async def _alive_task(self):
        while self._started:
            try:
                await self.publisher.publish(
                    action='alive',
                    routing_key='alive',
                    payload=None,
                )

            except Exception as err:
                logging.exception(err)

            finally:
                await asyncio.sleep(10)
