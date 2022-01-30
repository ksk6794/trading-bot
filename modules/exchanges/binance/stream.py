import inspect
import logging
from typing import List, Dict, Set, Callable

import orjson
from aiohttp import ClientSession
from yarl import URL

from modules.websocket import WebSocketClient
from modules.exchanges.base import BaseExchangeStreamClient
from modules.models import AccountModel, OrderModel
from modules.models.line import BookUpdateModel, TradeUpdateModel, DepthUpdateModel
from modules.models.types import Symbol, StreamEntity, UserStreamEntity

from .client import BinanceClient


class BinanceStreamClient(BaseExchangeStreamClient):
    """
    API Doc:
    https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams
    """
    WS_CHANNELS = ('aggTrade', 'bookTicker', 'depth')

    def __init__(self, testnet: bool = False):
        super().__init__()

        self._ws_url = URL('wss://stream.binancefuture.com/' if testnet else 'wss://fstream.binance.com/')
        self.ws = WebSocketClient(
            session=ClientSession(
                json_serialize=lambda x: orjson.dumps(x).decode()
            )
        )
        self.ws.add_connect_callback(self._on_connect)
        self.ws.add_message_callback(self._on_message)

        self.ws_id = 0
        self._ws_connected = False

    async def connect(self):
        if not self._ws_connected:
            url = self._ws_url.with_path('/ws')
            await self.ws.connect(url)

    async def subscribe(self, symbols: List[Symbol]):
        if not self._ws_connected:
            await self.connect()

        for symbol in symbols:
            self.ws_id += 1
            logging.info(f'Subscribing to "{symbol}" updates with ID {self.ws_id}...')
            await self.ws.send_json({
                'method': 'SUBSCRIBE',
                'params': [f'{symbol.lower()}@{channel}' for channel in self.WS_CHANNELS],
                'id': self.ws_id
            })

    async def _on_connect(self):
        self._ws_connected = True

    async def _on_message(self, payload: Dict):
        if 'id' in payload:
            _id = payload['id']
            logging.info(f'ID {_id} successfully subscribed!')

        elif 'e' in payload:
            entity = payload['e']

            # Stream Name: <symbol>@aggTrade
            if entity == 'aggTrade':
                symbol = Symbol(payload['s'])
                model = TradeUpdateModel.from_stream(payload)
                await self._trigger_callbacks(StreamEntity.TRADE, symbol, model)

            # Stream Name: <symbol>@depth
            elif entity == 'depthUpdate':
                symbol = Symbol(payload['s'])
                model = DepthUpdateModel.from_stream(payload)
                await self._trigger_callbacks(StreamEntity.DEPTH, symbol, model)

            # Stream Name: <symbol>@bookTicker
            elif entity == 'bookTicker':
                symbol: Symbol = payload['s']
                model = BookUpdateModel.from_stream(payload)
                await self._trigger_callbacks(StreamEntity.BOOK, symbol, model)


class BinanceUserStreamClient:
    """
    API Doc:
    https://binance-docs.github.io/apidocs/futures/en/#user-data-streams
    """
    def __init__(
            self,
            exchange: BinanceClient,
            testnet: bool = False,
    ):
        self._ws_url = URL('wss://stream.binancefuture.com/' if testnet else 'wss://fstream.binance.com/')
        self._exchange = exchange
        self.ws = WebSocketClient(
            session=ClientSession(
                json_serialize=lambda x: orjson.dumps(x).decode()
            )
        )
        self.ws.add_connect_callback(self._on_connect)
        self.ws.add_message_callback(self._on_message)

        self._ws_connected = False
        self._callbacks: Dict[UserStreamEntity, Set] = {}

    async def connect(self):
        if not self._ws_connected:
            listen_key = await self._exchange.create_listen_key()
            url = self._ws_url.with_path(f'/ws/{listen_key}')
            await self.ws.connect(url)

    def add_update_callback(self, event_type: UserStreamEntity, cb: Callable):
        UserStreamEntity.has_value(event_type)
        self._callbacks.setdefault(event_type, set()).add(cb)

    async def _on_connect(self):
        self._ws_connected = True

    async def _on_message(self, payload: Dict):
        if 'e' in payload:
            raw_entity = payload['e']

            if raw_entity == 'ACCOUNT_UPDATE':
                entity = UserStreamEntity.ACCOUNT_UPDATE
                model = AccountModel.from_user_stream(payload)
                await self._trigger_callbacks(entity, model)

            elif raw_entity == 'ORDER_TRADE_UPDATE':
                entity = UserStreamEntity.ORDER_TRADE_UPDATE
                model = OrderModel.from_user_stream(payload)
                await self._trigger_callbacks(entity, model)

    async def _trigger_callbacks(self, entity: UserStreamEntity, model):
        callbacks = self._callbacks.get(entity, set())

        for callback in callbacks:
            result = callback(model)

            if inspect.isawaitable(result):
                await result
