import asyncio
import inspect
import logging
from datetime import datetime, timedelta
from typing import Callable, Set, Dict, Optional, List

from pydantic import BaseModel
from pymongo import ASCENDING

from modules.models.line import UpdateLogModel
from modules.models.types import Symbol, StreamEntity, Timestamp
from modules.mongo import MongoClient

from .types import BulkLineCallback

__all__ = (
    'ReplayClient',
)


class ReplayClient:
    def __init__(
            self,
            db: MongoClient,
            symbols: List[Symbol],
            replay_speed: int = 1,
            replay_from: Optional[Timestamp] = None,
            replay_to: Optional[Timestamp] = None,
    ):
        self.db = db
        self.symbols = symbols
        self.replay_speed = replay_speed
        self.replay_from = replay_from
        self.replay_to = replay_to

        self._connected = False
        self._loop = asyncio.get_event_loop()
        self._callbacks: Dict[str, Set] = {}
        self._update_callbacks: Dict[StreamEntity, Set[Callable]] = {}

    async def connect(self):
        if not self._connected:
            self._loop.create_task(self._reader())
            self._connected = True

    async def disconnect(self):
        if self._connected:
            self._connected = False
            self._loop.stop()

    def add_update_callback(self, entity: StreamEntity, cb: BulkLineCallback):
        assert callable(cb)
        self._update_callbacks.setdefault(entity, set()).add(cb)

    def add_done_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('done', set()).add(cb)

    async def _reader(self):
        query: Dict = {'s': {'$in': self.symbols}}

        if self.replay_from:
            query.setdefault('t', {})['$gte'] = self.replay_from

        if self.replay_to:
            query.setdefault('t', {})['$lte'] = self.replay_to

        prev_dt = None
        prev_log: Optional[UpdateLogModel] = None
        processed_cnt = 0

        total_cnt = await self.db.count(UpdateLogModel, query)
        delta = int(total_cnt / 100)

        async for log in self.db.find_iter(UpdateLogModel, query, sort=[('t', ASCENDING)]):
            if prev_log and self.replay_speed:
                diff = log.timestamp - prev_log.timestamp
                delay = diff / 1000 / self.replay_speed

                if delay >= 0.01:
                    await asyncio.sleep(delay)

            dt = self._get_datetime(log.timestamp)

            if prev_dt and dt != prev_dt:
                s = dt.strftime('%d.%m.%Y %H:%M')
                logging.info(f'Current replay period: {s}')

            await self._trigger_update_callbacks(log.symbol, log.entity, log.get_entity_model())
            prev_dt = dt
            prev_log = log
            processed_cnt += 1

            if processed_cnt % delta == 0:
                pct = processed_cnt * 100 / total_cnt
                logging.info(f'Processed {pct:.{2}f}%')

        await self._trigger_callbacks('done')

    async def _trigger_callbacks(self, action: str, *args, **kwargs):
        callbacks = self._callbacks.get(action, set())

        for callback in callbacks:
            result = callback(*args, **kwargs)

            if inspect.isawaitable(result):
                await result

    async def _trigger_update_callbacks(self, symbol: Symbol, entity: StreamEntity, model: BaseModel):
        callbacks = self._update_callbacks.get(entity, set())

        for callback in callbacks:
            result = callback(symbol, model)

            if inspect.isawaitable(result):
                await result

    @staticmethod
    def _get_datetime(timestamp: Timestamp):
        t = datetime.fromtimestamp(timestamp / 1000)
        return t.replace(second=0, microsecond=0, minute=0, hour=t.hour) + timedelta(hours=t.minute // 30)
