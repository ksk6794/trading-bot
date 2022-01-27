import decimal
import logging
from typing import Callable, List, Dict

import orjson

from modules.amqp.pubsub import AMQPSubscriber as BaseAMQPSubscriber


def _default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)


class AMQPSubscriber(BaseAMQPSubscriber):
    def __init__(self, *args, **kwargs):
        self._callback_queue = None
        super().__init__(*args, **kwargs)

    async def subscribe(self, binding_keys):
        logging.info('Binding keys: %s', binding_keys)

        for binding_key in binding_keys:
            await self._channel.queue_bind(
                exchange_name=self._exchange_name,
                queue_name=self._callback_queue,
                routing_key=binding_key
            )

    def _serialize(self, data: dict) -> bytes:
        return orjson.dumps(data, default=_default)

    def _deserialize(self, data: bytes) -> Dict:
        return orjson.loads(data)


class LineSubscriber:
    EXCHANGE_NAME = 'pubsub_line'

    def __init__(self, uri):
        self._client = AMQPSubscriber(
            amqp_uri=uri,
            exchange_name=self.EXCHANGE_NAME,
        )
        self._client.add_message_callback(self._on_message)
        self._client.add_reconnect_callback(self._on_reconnect)

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

    def add_update_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('update', set()).add(cb)

    def add_done_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('done', set()).add(cb)

    # noinspection PyProtectedMember
    def _on_reconnect(self):
        callbacks = self._callbacks.get('reconnect', set())
        self._client._trigger_callbacks(callbacks)

    # noinspection PyUnusedLocal,PyProtectedMember
    def _on_message(self, body, reply_to):
        """
        Accepts messages from LineServer
        """
        action = body.get('action')
        payload = body.get('payload', {})
        callbacks = self._callbacks.get(action, set())
        self._client._trigger_callbacks(callbacks, **payload)
