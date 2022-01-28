from typing import Dict, Optional

from modules.amqp import AMQPPublisher


class LinePublisher:
    def __init__(self, uri):
        self._client = AMQPPublisher(
            uri=uri,
            exchange_name='pubsub_line',
        )

    async def connect(self):
        await self._client.connect()

    async def disconnect(self):
        await self._client.disconnect()

    async def publish(self, action: str, payload: Optional[Dict], routing_key: str):
        await self._client.publish(
            action=action,
            payload=payload,
            routing_key=routing_key,
        )
