import asyncio
import decimal
import logging
from typing import Optional, Dict

import orjson
import aio_pika
from aio_pika import Connection, Exchange, Channel
from aio_pika.exceptions import IncompatibleProtocolError


def _default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)


class AMQPPublisher:
    def __init__(self, uri: str, exchange_name: str):
        self._uri = uri
        self._exchange_name = exchange_name

        self._connection: Optional[Connection] = None
        self._channel: Optional[Channel] = None
        self._exchange: Optional[Exchange] = None

        self._loop = asyncio.get_event_loop()

    async def connect(self):
        while not self._connection:
            try:
                self._connection = await aio_pika.connect_robust(
                    url=self._uri,
                    loop=self._loop
                )

            except ConnectionError as err:
                logging.error('AMQPPublisher: %r', err)

            except IncompatibleProtocolError:
                pass

            finally:
                await asyncio.sleep(1)

        await self.setting_up()

    async def disconnect(self):
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
            await self._connection.close()

    async def setting_up(self):
        self._channel = await self._connection.channel()
        self._exchange = await self._channel.declare_exchange(
            name=self._exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            durable=False,
        )

    async def publish(self, action: str, payload: Optional[Dict], routing_key: str):
        if self._channel and not self._channel.is_closed and self._exchange:
            body = orjson.dumps({
                'action': action,
                'payload': payload
            }, default=_default)
            await self._exchange.publish(
                message=aio_pika.Message(body),
                routing_key=routing_key,
            )


async def main():
    publisher = AMQPPublisher(
        uri='amqp://guest:guest@127.0.0.1/',
        exchange_name='line'
    )
    await publisher.connect()
    await publisher.publish(
        action='test',
        payload={'test': 'TEST'},
        routing_key='test.1'
    )
    await publisher.disconnect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
