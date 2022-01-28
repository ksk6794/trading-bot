from typing import Callable, List, Dict, Optional

from modules.amqp import AMQPConsumer


class LineSubscriber:
    def __init__(self, uri):
        self._client = AMQPConsumer(
            uri=uri,
            exchange_name='pubsub_line',
        )
        self._client.add_message_callback(self._on_message)
        self._callbacks = {}

    async def connect(self):
        await self._client.connect()

    async def disconnect(self):
        await self._client.disconnect()

    async def subscribe(self, binding_keys: List[str]):
        await self._client.subscribe(binding_keys)

    def add_alive_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('alive', set()).add(cb)

    def add_update_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('update', set()).add(cb)

    def add_done_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('done', set()).add(cb)

    # noinspection PyUnusedLocal,PyProtectedMember
    async def _on_message(self, action: str, payload: Optional[Dict]):
        """
        Accepts messages from LineServer
        """
        callbacks = self._callbacks.get(action, set())
        await self._client._trigger_callbacks(callbacks, **(payload or {}))
