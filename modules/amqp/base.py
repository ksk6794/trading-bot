import asyncio
import inspect
import logging
import os
import random
import socket
import uuid
import ssl as ssl_module
from urllib.parse import urlparse

import psutil
import aioamqp
from aioamqp.channel import Channel as BaseChannel
from aioamqp.protocol import AmqpProtocol as BaseAMQPProtocol

__all__ = (
    'BaseAMQPClient',
)


def make_consumer_id():  # pragma: no cover
    pid = os.getpid()
    p = psutil.Process(pid)
    hostname = socket.gethostname()
    cmdline = ' '.join(filter(None, p.cmdline()))
    consumer_id = '%s|%s|%s|%s' % (
        hostname[:10], pid, cmdline[:170],
        uuid.uuid4().hex[:8])
    return consumer_id


class Channel(BaseChannel):  # pragma: no cover
    async def basic_consume(self, *args, consumer_tag='', **kwargs):
        if not consumer_tag:
            consumer_tag = make_consumer_id()
        return await super().basic_consume(
            *args, consumer_tag=consumer_tag, **kwargs)


class AmqpProtocol(BaseAMQPProtocol):
    CHANNEL_FACTORY = Channel


class BaseAMQPClient(object):  # pragma: no cover
    def __init__(self, *args, loop=None, disconnect_timeout=5, **kwargs):
        kwargs.setdefault('heartbeat', 30)
        self._loop = loop or asyncio.get_event_loop()
        self._disconnect_timeout = disconnect_timeout
        self._connector = self.get_connector(*args, **kwargs)
        self._connect_callbacks = set()
        self._disconnect_callbacks = set()
        self._reconnect_callbacks = set()
        self._reconnected = False
        self._running = False
        self._channel = None
        self._protocol = None
        self._transport = None
        self._connect_task = None
        self._connect_lock = asyncio.Lock()
        self._wait = asyncio.Future()

    @staticmethod
    async def _trigger_callbacks(callbacks, *args, **kwargs):
        for callback in callbacks:
            result = callback(*args, **kwargs)

            if inspect.isawaitable(result):
                await result

    @staticmethod
    def get_connector(*args, **kwargs):
        kwargs.setdefault('protocol_factory', AmqpProtocol)
        from_url = 'url' in kwargs

        if 'ssl' not in kwargs:
            if from_url and urlparse(kwargs['url']).scheme == 'amqps':
                ssl_context = ssl_module.create_default_context()
                kwargs['ssl'] = ssl_context

        return lambda: (
            aioamqp.from_url(*args, **kwargs)
            if from_url else
            aioamqp.connect(*args, **kwargs))

    def add_connect_callback(self, callback):
        """
        Add callback to call after connection established
        """
        assert callable(callback)
        self._connect_callbacks.add(callback)

    def add_disconnect_callback(self, callback):
        """
        Add callback to call after disconnection
        """
        assert callable(callback)
        self._disconnect_callbacks.add(callback)

    def add_reconnect_callback(self, callback):
        """
        Add callback to call after reconnection
        """
        assert callable(callback)
        self._reconnect_callbacks.add(callback)

    def del_connect_callback(self, callback):
        """
        Delete callback passed to add_connect_callback
        """
        assert callable(callback)
        self._connect_callbacks.discard(callback)

    def del_disconnect_callback(self, callback):
        """
        Delete callback passed to add_disconnect_callback
        """
        assert callable(callback)
        self._disconnect_callbacks.discard(callback)

    def del_reconnect_callback(self, callback):
        """
        Delete callback passed to add_reconnect_callback
        """
        assert callable(callback)
        self._reconnect_callbacks.discard(callback)

    @property
    def is_closed(self):
        return not (self._channel and self._channel.is_open and
                    self._protocol and self._protocol.state == aioamqp.protocol.OPEN)

    @property
    def running(self):
        return self._running

    async def _connect_loop(self):
        while self._running:
            try:
                if self._wait.done():
                    self._wait = asyncio.Future()

                logging.info('%s.connecting', self.__class__.__name__)

                transport, protocol = await self._connector()

                channel = await protocol.channel()
                channel.add_cancellation_callback(
                    lambda *a, **k: protocol.close())

                await self.setup_channel(channel)

                self._transport = transport
                self._protocol = protocol
                self._channel = channel

                logging.info('%s.connected', self.__class__.__name__)

                self._wait.set_result(channel)
                callbacks = self._reconnect_callbacks if self._reconnected else self._connect_callbacks
                await self._trigger_callbacks(callbacks)
                self._reconnected = True

                while not self.is_closed:
                    await asyncio.sleep(1)

                self._wait = asyncio.Future()

                logging.info(f'{self.__class__.__name__}.disconnected')

                await self._trigger_callbacks(self._disconnect_callbacks)

            except (ConnectionResetError, ConnectionRefusedError, socket.gaierror) as e:
                logging.warning(f'{self.__class__.__name__}.connect: {e}')

            except Exception as e:
                logging.error(f'{self.__class__.__name__}.connect: {e}')

            finally:
                await self._disconnect()
                await asyncio.sleep(self._disconnect_timeout + self._disconnect_timeout * random.random())

    async def connect(self):
        async with self._connect_lock:
            if not self._running:
                if self._connect_task:
                    await self._connect_task
                self._running = True
                self._connect_task = self._loop.create_task(
                    self._connect_loop())
        return await asyncio.shield(self._wait)

    async def _disconnect(self):
        if self._channel is not None:
            try:
                await self._channel.close()
            except Exception:  # nosec
                pass
            finally:
                self._channel = None
        if self._protocol is not None:
            try:
                await self._protocol.close()
            except Exception:  # nosec
                pass
            finally:
                self._protocol = None
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception:  # nosec
                pass
            finally:
                self._transport = None

    async def disconnect(self):
        if not self._running:
            return
        self._running = False
        await self._disconnect()

    async def declare_queue_random(self, channel, prefix):
        consumer_id = make_consumer_id()
        queue_name = f'{prefix}.gen.{consumer_id}'
        return await channel.queue_declare(
            queue_name=queue_name,
            auto_delete=True,
            exclusive=True
        )

    async def setup_channel(self, channel):
        raise NotImplemented
