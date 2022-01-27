import asyncio
import logging
import time

from pymongo.operations import InsertOne

from modules.line_client import BulkLineClient
from modules.models.indexes import INDEXES
from modules.models.line import BookUpdateModel, TradeUpdateModel, DepthUpdateModel, UpdateLogModel
from modules.models.types import StreamEntity, Symbol
from modules.mongo import MongoClient

from .settings import Settings


class LineLogger:
    def __init__(self, settings: Settings):
        self.db = MongoClient(
            mongo_uri=settings.mongo_uri,
            indexes=INDEXES,
        )
        self.line = BulkLineClient(
            symbols=settings.symbols,
            uri=settings.broker_amqp_uri,
            entities=settings.entities,
        )
        self._settings = settings
        self._loop = asyncio.get_event_loop()
        self._queue = asyncio.Queue()
        self._started = False

        self.line.add_update_callback(StreamEntity.BOOK, self._on_book_update)
        self.line.add_update_callback(StreamEntity.TRADE, self._on_trade_update)
        self.line.add_update_callback(StreamEntity.DEPTH, self._on_depth_update)

    async def start(self):
        await self.db.connect()
        await self.line.start()
        self._started = True
        self._loop.create_task(self._writer())

    async def stop(self):
        self._started = False
        await self.line.stop()
        self._loop.stop()

    async def _writer(self):
        while self._started:
            try:
                operations = []

                while not self._queue.empty():
                    operation = await self._queue.get()
                    operations.append(operation)

                if operations:
                    await self.db.bulk_write(UpdateLogModel, operations)
                    logging.info(f'Wrote {len(operations)} updates')

            except Exception as err:
                logging.error('"_writer" task failed: %r', err)

            finally:
                await asyncio.sleep(self._settings.bulk_interval)

    async def _on_book_update(self, symbol: Symbol, model: BookUpdateModel):
        log_model = UpdateLogModel(
            symbol=symbol,
            entity=StreamEntity.BOOK,
            data=model.encode(),
            timestamp=int(time.time() * 1000),
        )
        data = log_model.dict(by_alias=True)
        operation = InsertOne(data)
        await self._queue.put(operation)

    async def _on_trade_update(self, symbol: Symbol, model: TradeUpdateModel):
        log_model = UpdateLogModel(
            symbol=symbol,
            entity=StreamEntity.TRADE,
            data=model.encode(),
            timestamp=int(time.time() * 1000),
        )
        data = log_model.dict(by_alias=True)
        operation = InsertOne(data)
        await self._queue.put(operation)

    async def _on_depth_update(self, symbol: Symbol, model: DepthUpdateModel):
        log_model = UpdateLogModel(
            symbol=symbol,
            entity=StreamEntity.DEPTH,
            data=model.encode(),
            timestamp=int(time.time() * 1000),
        )
        data = log_model.dict(by_alias=True)
        operation = InsertOne(data)
        await self._queue.put(operation)
