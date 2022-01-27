import logging
from typing import List

import orjson
from aiohttp import ClientSession
from yarl import URL

from modules.websocket import WebSocketClient
from modules.exchanges.base import BaseExchangeStreamClient
from modules.models.line import BookUpdateModel, TradeUpdateModel, DepthUpdateModel
from modules.models.types import Symbol, StreamEntity


class BinanceStreamClient(BaseExchangeStreamClient):
    """
    API Doc:
    https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams
    """
    WS_CHANNELS = ('aggTrade', 'bookTicker', 'depth')

    def __init__(self, testnet: bool = False):
        super().__init__()
        ws_url = 'wss://stream.binancefuture.com/ws' if testnet else 'wss://fstream.binance.com/ws'

        self._session = ClientSession(
            json_serialize=lambda x: orjson.dumps(x).decode()
        )

        self.ws = WebSocketClient(
            url=URL(ws_url),
            session=self._session
        )
        self.ws.add_connect_callback(self._on_connect)
        self.ws.add_message_callback(self._on_message)
        self.ws_id = 0
        self.ws_connected = False

    async def connect(self):
        if not self.ws_connected:
            await self.ws.connect()

    async def subscribe(self, symbols: List[Symbol]):
        if not self.ws_connected:
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
        self.ws_connected = True
        await self._trigger_callbacks('connect')

    async def _on_message(self, payload):
        if 'id' in payload:
            _id = payload['id']
            logging.info(f'ID {_id} successfully subscribed!')

        # Stream Name: <symbol>@aggTrade
        elif 'e' in payload and payload['e'] == 'aggTrade':
            symbol = Symbol(payload['s'])
            model = TradeUpdateModel(
                price=payload['p'],
                quantity=payload['q'],
                timestamp=int(payload['T']),
                is_buyer_maker=payload['m'],
            )
            await self._trigger_callbacks(StreamEntity.TRADE, symbol, model)

        # Stream Name: <symbol>@depth
        elif 'e' in payload and payload['e'] == 'depthUpdate':
            symbol = Symbol(payload['s'])
            model = DepthUpdateModel(
                symbol=payload['s'],
                first_update_id=payload['U'],
                last_update_id=payload['u'],
                bids=payload['b'],
                asks=payload['a'],
                timestamp=payload['E'],
            )
            await self._trigger_callbacks(StreamEntity.DEPTH, symbol, model)

        # Stream Name: <symbol>@bookTicker
        elif 'e' in payload and payload['e'] == 'bookTicker':
            symbol: Symbol = payload['s']
            model = BookUpdateModel(
                bid=payload['b'],
                ask=payload['a']
            )
            await self._trigger_callbacks(StreamEntity.BOOK, symbol, model)
