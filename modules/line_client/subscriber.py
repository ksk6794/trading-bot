import inspect
from typing import Callable, List, Dict, Optional

from modules.amqp import AMQPConsumer


class LineSubscriber:
    def __init__(self, uri):
        self._client = AMQPConsumer(
            uri=uri,
            exchange_name='pubsub_line',
        )

        self._client.add_reconnect_callback(self._on_reconnect)
        self._client.add_reset_callback(self._on_reset)
        self._client.add_update_callback(self._on_update)

        self._callbacks = {}

    async def connect(self):
        await self._client.connect()

    async def disconnect(self):
        await self._client.disconnect()

    async def subscribe(self, binding_keys: List[str]):
        await self._client.subscribe(binding_keys)

    def add_reconnect_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('reconnect', set()).add(cb)

    def add_alive_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('alive', set()).add(cb)

    def add_reset_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('reset', set()).add(cb)

    def add_update_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('update', set()).add(cb)

    def add_done_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('done', set()).add(cb)

    # noinspection PyUnusedLocal,PyProtectedMember
    async def _on_update(self, action: str, payload: Optional[Dict]):
        """
        Accepts updates from LineServer
        """
        callbacks = self._callbacks.get(action, set())
        await self._trigger_callbacks(callbacks, **(payload or {}))

    # noinspection PyUnusedLocal,PyProtectedMember
    async def _on_reconnect(self):
        callbacks = self._callbacks.get('reconnect')
        await self._trigger_callbacks(callbacks)

    async def _on_reset(self):
        callbacks = self._callbacks.get('reset')
        await self._trigger_callbacks(callbacks)

    @staticmethod
    async def _trigger_callbacks(callbacks, *args, **kwargs):
        for callback in callbacks:
            result = callback(*args, **kwargs)

            if inspect.isawaitable(result):
                await result
