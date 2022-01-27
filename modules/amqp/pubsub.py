import orjson

from .base import BaseAMQPClient

__all__ = (
    'AMQPPublisher',
    'AMQPSubscriber',
)


class AMQPConnection(BaseAMQPClient):  # pragma: no cover
    def __init__(
            self,
            exchange_name: str,
            amqp_uri: str,
            queue_name: str = None,
            **kwargs
    ):
        super().__init__(url=amqp_uri, **kwargs)

        self._exchange_name = exchange_name
        self._queue_name = queue_name
        self._message_callbacks = set()

    def _serialize(self, data: dict) -> bytes:
        return orjson.dumps(data)

    def _deserialize(self, data: bytes) -> dict:
        return orjson.loads(data)

    async def setup_channel(self, channel):
        raise NotImplemented

    async def _message_callback(self, channel, body, envelope, properties):
        msg = self._deserialize(body)
        await self._trigger_callbacks(self._message_callbacks, msg, properties.reply_to)
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

    def add_message_callback(self, callback):
        assert callable(callback)
        self._message_callbacks.add(callback)

    def del_message_callback(self, callback):
        assert callable(callback)
        self._message_callbacks.discard(callback)


class AMQPPublisher(AMQPConnection):  # pragma: no cover
    async def setup_channel(self, channel):
        await channel.exchange(
            exchange_name=self._exchange_name,
            type_name='topic',
            durable=True
        )

        if self._queue_name:
            result = await channel.queue_declare(self._queue_name, durable=True)
        else:
            result = await self.declare_queue_random(channel, 'pub')

        queue = result['queue']
        assert queue is not None, ('AMQPPublisher.setup_channel: invalid queue: %r' % result)

        await channel.queue_bind(
            exchange_name=self._exchange_name,
            queue_name=queue,
            routing_key='requests'
        )

        await channel.basic_consume(
            callback=self._message_callback,
            queue_name=queue,
        )

    async def reply(self, body, reply_to):
        if not self.is_closed:
            await self._channel.basic_publish(
                payload=self._serialize(body),
                exchange_name='',
                routing_key=reply_to,
            )


class AMQPSubscriber(AMQPConnection):  # pragma: no cover
    async def setup_channel(self, channel):
        await channel.exchange(
            exchange_name=self._exchange_name,
            type_name='topic',
            durable=True
        )

        if self._queue_name:
            result = await channel.queue_declare(self._queue_name, durable=True)
        else:
            result = await self.declare_queue_random(channel, 'sub')

        queue = result['queue']
        assert queue is not None, ('AMQPSubscriber.setup_channel: invalid queue: %r' % result)

        self._callback_queue = queue

        await channel.basic_consume(
            callback=self._message_callback,
            queue_name=self._callback_queue,
        )
