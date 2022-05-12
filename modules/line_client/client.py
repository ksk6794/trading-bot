import asyncio
import inspect
import logging
from itertools import product
from time import time
from typing import Callable, Set, Dict, List

from modules.models.line import TradeUpdateModel, BookUpdateModel, DepthUpdateModel
from modules.models.types import Symbol, StreamEntity

from .types import BulkLineCallback
from .subscriber import LineSubscriber

__all__ = (
    'LineClient',
)


class LineClient:
    def __init__(
            self,
            uri: str,
            symbols: List[Symbol],
            entities: List[StreamEntity],
    ):
        self.subscriber = LineSubscriber(uri)
        self._symbols = symbols
        self._entities = entities

        self._last_alive = None
        self._connected = False
        self._callbacks: Dict[str, Set] = {}
        self._update_callbacks: Dict[StreamEntity, Set] = {}
        self._models = {
            StreamEntity.TRADE: TradeUpdateModel,
            StreamEntity.BOOK: BookUpdateModel,
            StreamEntity.DEPTH: DepthUpdateModel,
        }
        self._loop = asyncio.get_event_loop()

        self.subscriber.add_reconnect_callback(self._on_reconnect)
        self.subscriber.add_alive_callback(self._on_alive)
        self.subscriber.add_reset_callback(self._on_reset)
        self.subscriber.add_update_callback(self._on_update)

    async def connect(self):
        if not self._connected:
            await self.subscriber.connect()
            await self._subscribe()
            self._connected = True

    async def disconnect(self):
        if self._connected:
            await self.subscriber.disconnect()
            self._connected = False
            self._loop.stop()

    def add_reset_callback(self, cb: Callable):
        self._callbacks.setdefault('reset', set()).add(cb)

    def add_update_callback(self, entity: StreamEntity, cb: BulkLineCallback):
        assert StreamEntity.has_value(entity)
        assert callable(cb)

        self._update_callbacks.setdefault(entity, set()).add(cb)

    @property
    def is_alive(self):
        return self._last_alive and time() - self._last_alive < 10

    async def _subscribe(self):
        routing_keys = ['alive', 'reset',
                        *[f'{symbol}.{entity}' for symbol, entity in product(self._symbols, self._entities)]]
        await self.subscriber.subscribe(routing_keys)

    async def _on_reconnect(self):
        callbacks = self._callbacks.get('reset', set())
        await self._trigger_callbacks(callbacks)

    def _on_alive(self):
        self._last_alive = time()
        logging.info(f'{self.__class__.__name__}: alive received')

    async def _on_reset(self):
        logging.info(f'{self.__class__.__name__}: reset received')
        callbacks = self._callbacks.get('reset', set())
        await self._trigger_callbacks(callbacks)

    async def _on_update(self, entity: StreamEntity, symbol: Symbol, data: Dict):
        assert StreamEntity.has_value(entity)
        assert symbol in self._symbols
        assert isinstance(data, dict)

        model_class = self._models[entity]
        model = model_class(**data)
        callbacks = self._update_callbacks.get(entity, set())

        await self._trigger_callbacks(callbacks, symbol, model)

    @staticmethod
    async def _trigger_callbacks(callbacks, *args, **kwargs):
        for callback in callbacks:
            result = callback(*args, **kwargs)

            if inspect.isawaitable(result):
                await result
