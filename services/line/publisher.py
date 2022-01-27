import decimal
from typing import Dict, Callable

import orjson

from modules.amqp.pubsub import AMQPPublisher as BaseAMQPPublisher


def _default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)


class AMQPPublisher(BaseAMQPPublisher):
    async def publish(self, routing_key, body):
        if not self.is_closed:
            await self._channel.basic_publish(
                payload=self._serialize(body),
                exchange_name=self._exchange_name,
                routing_key=routing_key,
            )

    def _serialize(self, data: dict) -> bytes:
        return orjson.dumps(data, default=_default)

    def _deserialize(self, data: bytes) -> Dict:
        return orjson.loads(data)


class LinePublisher:
    EXCHANGE_NAME = 'pubsub_line'

    def __init__(self, uri):
        self._client = AMQPPublisher(
            amqp_uri=uri,
            exchange_name=self.EXCHANGE_NAME,
        )
        self._callbacks = {}

    async def connect(self):
        await self._client.connect()

    async def disconnect(self):
        await self._client.disconnect()

    def add_callback(self, action: str, callback: Callable):
        assert isinstance(action, str)
        assert callable(callback)
        self._callbacks.setdefault(action, set()).add(callback)

    async def publish(self, routing_key: str, body: Dict):
        await self._client.publish(
            routing_key=routing_key,
            body=body
        )
