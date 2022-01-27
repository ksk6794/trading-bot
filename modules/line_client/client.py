import abc
import asyncio
import inspect
import logging
from itertools import product
from time import time
from typing import Callable, Set, Dict, List

from modules.models.line import TradeUpdateModel, BookUpdateModel, DepthUpdateModel
from modules.models.types import Symbol, StreamEntity

from .types import LineCallback, BulkLineCallback
from .subscriber import LineSubscriber

__all__ = (
    'LineClient',
    'BulkLineClient',
)


class BaseLineClient:
    def __init__(
            self,
            uri: str,
            entities: List[StreamEntity],
    ):
        self.subscriber = LineSubscriber(uri)
        self._entities = entities
        self._last_alive = None
        self._started = False
        self._callbacks: Dict[str, Set] = {}
        self._models = {
            StreamEntity.TRADE: TradeUpdateModel,
            StreamEntity.BOOK: BookUpdateModel,
            StreamEntity.DEPTH: DepthUpdateModel,
        }
        self._loop = asyncio.get_event_loop()

        self.subscriber.add_reconnect_callback(self._on_reconnect)
        self.subscriber.add_alive_callback(self._on_alive)
        self.subscriber.add_update_callback(self._on_update)

    async def start(self):
        if not self._started:
            await self.subscriber.connect()
            await self._subscribe()
            self._started = True

    async def stop(self):
        if self._started:
            await self.subscriber.disconnect()
            self._started = False
            self._loop.stop()

    def add_update_callback(self, entity: StreamEntity, cb: Callable):
        assert StreamEntity.has_value(entity)
        assert callable(cb)

        self._callbacks.setdefault(entity, set()).add(cb)

    @property
    def is_alive(self):
        return self._last_alive and time() - self._last_alive < 10

    @abc.abstractmethod
    async def _subscribe(self):
        ...

    @abc.abstractmethod
    async def _on_update(self, entity: StreamEntity, symbol: Symbol, data: Dict):
        ...

    async def _on_reconnect(self):
        logging.info(f'{self.__class__.__name__}: reconnected')
        await self._subscribe()

    def _on_alive(self):
        self._last_alive = time()
        logging.info(f'{self.__class__.__name__}: alive received')

    async def _trigger_callbacks(self, entity: StreamEntity, *args, **kwargs):
        callbacks = self._callbacks.get(entity, set())

        for callback in callbacks:
            result = callback(*args, **kwargs)

            if inspect.isawaitable(result):
                await result


class LineClient(BaseLineClient):
    def __init__(
            self,
            symbol: Symbol,
            uri: str,
            entities: List[StreamEntity],
    ):
        self.symbol = symbol
        super().__init__(uri, entities)

    def add_update_callback(self, entity: StreamEntity, cb: LineCallback):
        super().add_update_callback(entity, cb)

    async def _subscribe(self):
        binding_keys = ['alive', *[f'{self.symbol}.{entity}' for entity in self._entities]]
        await self.subscriber.subscribe(binding_keys)

    async def _on_update(self, entity: StreamEntity, symbol: Symbol, data: Dict):
        assert StreamEntity.has_value(entity)
        assert symbol == self.symbol
        assert isinstance(data, dict)

        model_class = self._models[entity]
        model = model_class(**data)
        await self._trigger_callbacks(entity, model)


class BulkLineClient(BaseLineClient):
    def __init__(
            self,
            symbols: List[Symbol],
            uri: str,
            entities: List[StreamEntity],
    ):
        self.symbols = symbols
        super().__init__(uri, entities)

    def add_update_callback(self, entity: StreamEntity, cb: BulkLineCallback):
        super().add_update_callback(entity, cb)

    async def _subscribe(self):
        binding_keys = ['alive', *[f'{symbol}.{entity}' for symbol, entity in product(self.symbols, self._entities)]]
        await self.subscriber.subscribe(binding_keys)

    async def _on_update(self, entity: StreamEntity, symbol: Symbol, data: Dict):
        assert StreamEntity.has_value(entity)
        assert symbol in self.symbols
        assert isinstance(data, dict)

        model_class = self._models[entity]
        model = model_class(**data)
        await self._trigger_callbacks(entity, symbol, model)
