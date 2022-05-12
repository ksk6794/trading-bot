import asyncio
import inspect
import logging
from typing import Callable, Dict, Set, Optional

import orjson
import aiohttp
from aiohttp import ClientSession
from yarl import URL


class WebSocketClient:
    def __init__(
            self,
            session: Optional[ClientSession] = None,
            receive_timeout: Optional[int] = 30,
            reconnect_timeout: int = 5
    ):
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ready = asyncio.Event()
        self.session = session or aiohttp.ClientSession()
        self.callbacks: Dict[str: Set[Callable]] = {}
        self._receive_timeout = receive_timeout
        self._reconnect_timeout = reconnect_timeout
        self._task: Optional[asyncio.Task] = None
        self._loop = asyncio.get_event_loop()

    def add_connect_callback(self, cb: Callable):
        assert callable(cb)
        self.callbacks.setdefault('connect', set()).add(cb)

    def add_disconnect_callback(self, cb: Callable):
        assert callable(cb)
        self.callbacks.setdefault('disconnect', set()).add(cb)

    def add_message_callback(self, cb: Callable):
        assert callable(cb)
        self.callbacks.setdefault('message', set()).add(cb)

    def add_error_callback(self, cb: Callable):
        assert callable(cb)
        self.callbacks.setdefault('error', set()).add(cb)

    async def send_json(self, data):
        if not self._ready.is_set():
            raise RuntimeError('Websocket is not ready!')
        await self._ws.send_json(data, dumps=lambda x: orjson.dumps(x).decode())

    async def wait_ready(self):
        await self._ready.wait()

    @property
    def is_ready(self):
        return self._ready.is_set()

    async def connect(self, url: URL):
        logging.info('WebSocket: Connection establishing...')
        self._task = self._loop.create_task(self._fetch(url))
        await self.wait_ready()

    async def reconnect(self):
        await self._ws.close()

    async def _fetch(self, url: URL):
        while True:
            async with self.session.ws_connect(url, receive_timeout=self._receive_timeout) as ws:
                logging.info('Websocket: Connection established!')
                self._ws = ws
                self._ready.set()
                await self._trigger_callbacks('connect')

                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = orjson.loads(msg.data)
                            await self._trigger_callbacks('message', payload=payload)

                        elif msg.type == aiohttp.WSMsgType.PING:
                            await ws.pong()

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logging.error('WebSocket: Exception occurred: %r', ws.exception())
                            await self._trigger_callbacks('error')
                            await ws.close()
                            break

                except asyncio.TimeoutError:
                    logging.error('WebSocket: TimeoutError occurred!')
                    await self._trigger_callbacks('error')

                except aiohttp.ClientError as err:
                    logging.error('WebSocket: ClientError occurred: %r', err)
                    await self._trigger_callbacks('error')

                except Exception as err:
                    logging.exception('WebSocket: %r', err)
                    await self._trigger_callbacks('error')

                finally:
                    self._ready.clear()
                    await self._trigger_callbacks('disconnect')
                    logging.info('WebSocket: Connection lost!')
                    await asyncio.sleep(self._reconnect_timeout)

    async def _trigger_callbacks(self, action, *args, **kwargs):
        callbacks = self.callbacks.get(action, set())

        for callback in callbacks:
            result = callback(*args, **kwargs)

            if inspect.isawaitable(result):
                await result
