import asyncio
import inspect
import logging
import uuid
from typing import List, Optional, Set, Callable, Dict

import orjson
import aio_pika
from aio_pika import Exchange, Queue, Channel
from aio_pika.connection import Connection
from aio_pika.exceptions import IncompatibleProtocolError

from .base import RobustConnection


class AMQPConsumer:
    def __init__(self, uri: str, exchange_name: str):
        self._uri = uri
        self._exchange_name = exchange_name

        self._connection: Optional[Connection] = None
        self._channel: Optional[Channel] = None
        self._exchange: Optional[Exchange] = None
        self._queue: Optional[Queue] = None

        self._loop = asyncio.get_event_loop()
        self._callbacks: Set[Callable] = set()
        self._consumer_task: Optional[asyncio.Task] = None

    async def connect(self):
        while not self._connection:
            try:
                self._connection = await aio_pika.connect_robust(
                    url=self._uri,
                    loop=self._loop,
                    connection_class=RobustConnection,
                )

            except ConnectionError as err:
                logging.error('AMQPConsumer: %r', err)

            except IncompatibleProtocolError:
                pass

            finally:
                await asyncio.sleep(1)

        await self.setting_up()

    async def disconnect(self):
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
        await self._connection.close()

        if self._consumer_task:
            self._consumer_task.cancel()

    async def setting_up(self):
        self._channel = await self._connection.channel()
        self._exchange = await self._channel.declare_exchange(
            name=self._exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            durable=False,
        )
        self._queue = await self._channel.declare_queue(auto_delete=True)

    async def subscribe(self, routing_keys: List[str]):
        for routing_key in routing_keys:
            await self._queue.bind(
                exchange=self._exchange,
                routing_key=routing_key
            )

        self._consumer_task = asyncio.create_task(self._consume())

    def add_message_callback(self, cb: Callable):
        self._callbacks.add(cb)

    async def _consume(self):
        async with self._queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        body = orjson.loads(message.body)
                        action = body['action']
                        payload = body.get('payload')
                        await self._trigger_callbacks(self._callbacks, action, payload)

                    except Exception as err:
                        logging.exception('Exception during message processing: %r', err)

    @staticmethod
    async def _trigger_callbacks(callbacks, *args, **kwargs):
        for callback in callbacks:
            result = callback(*args, **kwargs)

            if inspect.isawaitable(result):
                await result


async def main():
    consumer = AMQPConsumer(
        uri='amqp://guest:guest@127.0.0.1/',
        exchange_name='line',
    )
    await consumer.connect()
    await consumer.subscribe(routing_keys=['test.*'])


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
