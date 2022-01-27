import time
import hmac
import hashlib
import logging
from urllib.parse import urlencode
from decimal import Decimal
from typing import Dict, Optional, List

import orjson
from yarl import URL
from aiohttp import ClientSession, ClientResponseError

from modules.exchanges.base import BaseExchangeClient
from modules.exchanges.exceptions import OperationFailed
from modules.models.types import OrderSide, OrderType, TimeInForce, Symbol, Timeframe, MarginType, PositionSide
from modules.models import AccountModel, FundingRateModel, CandleModel, ContractModel, OrderModel, DepthModel
from helpers import remove_exponent, date_to_milliseconds


class BinanceClient(BaseExchangeClient):
    """
    API Doc:
    https://binance-docs.github.io/apidocs/futures/en
    """
    def __init__(
            self,
            public_key: str,
            private_key: str,
            testnet: bool = False,
    ):
        super().__init__()
        api_url = 'https://testnet.binancefuture.com' if testnet else 'https://fapi.binance.com'

        self._session = ClientSession(
            json_serialize=lambda x: orjson.dumps(x).decode()
        )
        self._base_url = URL(api_url)
        self._public_key = public_key
        self._private_key = private_key

    async def get_account_info(self) -> AccountModel:
        params = {'timestamp': int(time.time() * 1000)}
        body = await self._request('GET', '/fapi/v2/account', params, signed=True)
        return AccountModel.from_binance(body)

    async def get_contracts(self) -> Dict[Symbol, ContractModel]:
        """
        Current exchange trading rules and symbol information
        """
        body = await self._request('GET', '/fapi/v1/exchangeInfo')
        return {contract['symbol']: ContractModel.from_binance(contract) for contract in body['symbols']}

    async def get_funding_rate(self, contract: ContractModel) -> Dict[Symbol, FundingRateModel]:
        """
        Mark Price and Funding Rate
        """
        params = {'symbol': contract.symbol}
        body = await self._request('GET', '/fapi/v1/premiumIndex', params)
        return FundingRateModel.from_binance(body)

    async def change_leverage(self, contract: ContractModel, leverage: int):
        """
        Change user's initial leverage of specific symbol market.
        """
        assert 10 >= leverage > 0
        params = {
            'symbol': contract.symbol,
            'leverage': leverage,
            'timestamp': int(time.time() * 1000)
        }
        res = await self._request('POST', '/fapi/v1/leverage', params, signed=True)

        if not res:
            raise OperationFailed()

    async def is_hedge_mode(self):
        params = {'timestamp': int(time.time() * 1000)}
        res = await self._request('GET', '/fapi/v1/positionSide/dual', params, signed=True)
        return res['dualSidePosition']

    async def change_position_mode(self, hedge_mode: bool):
        params = {
            'dualSidePosition': 'true' if hedge_mode else 'false',
            'timestamp': int(time.time() * 1000)
        }
        res = await self._request('POST', '/fapi/v1/positionSide/dual', params, signed=True)

        if not res:
            raise OperationFailed()

    async def change_margin_type(self, contract: ContractModel, margin_type: MarginType):
        params = {
            'symbol': contract.symbol,
            'marginType': margin_type,
            'timestamp': int(time.time() * 1000)
        }
        await self._request('POST', '/fapi/v1/marginType', params)

    async def get_historical_candles(
            self,
            symbol: Symbol,
            timeframe: Timeframe,
            limit: int = 1000,
            start_time: Optional[str] = None,
    ) -> List[CandleModel]:
        """
        Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        """
        params = {
            'symbol': symbol,
            'interval': timeframe,
            'limit': limit,
        }

        if start_time:
            params['startTime'] = date_to_milliseconds(start_time)

        body = await self._request('GET', '/fapi/v1/klines', params) or []
        return [CandleModel.from_binance(item) for item in body]

    async def get_depth(
            self,
            symbol: Symbol,
            limit: int = 1000,
    ) -> DepthModel:
        params = {
            'symbol': symbol,
            'limit': limit,
        }
        body = await self._request('GET', '/fapi/v1/depth', params)
        return DepthModel.from_binance(body)

    async def place_order(
            self,
            contract: ContractModel,
            order_type: OrderType,
            quantity: Decimal,
            order_side: OrderSide,
            position_side: PositionSide = PositionSide.BOTH,
            price: Decimal = None,
            tif: Optional[TimeInForce] = None,
    ) -> Optional[OrderModel]:
        """
        Place new order.

        In one way mode:
            Open position:
                Long: position_side: PositionSide.BOTH, order_side=OrderSide.BUY
                Short: position_side: PositionSide.BOTH, order_side=OrderSide.SELL
            Close position:
                Long: position_side: PositionSide.BOTH, order_side=OrderSide.SELL
                Short: position_side: PositionSide.BOTH, order_side=OrderSide.BUY

        In hedge mode:
            Open position:
                Long: position_side=PositionSide.LONG, order_side=OrderSide.BUY
                Short: position_side=PositionSide.SHORT, order_side=OrderSide.SELL
            Close position:
                Long: position_side=PositionSide.LONG, order_side=OrderSide.SELL
                Short: position_side=PositionSide.SHORT, order_side=OrderSide.BUY
        """
        quantity = str(remove_exponent(round(quantity / contract.lot_size) * contract.lot_size))
        params = {
            'timestamp': int(time.time() * 1000),
            'symbol': contract.symbol,
            'side': order_side.upper(),
            'positionSide': position_side.upper(),
            'quantity': quantity,
            'type': order_type.upper(),
        }

        if price:
            params['price'] = str(remove_exponent(round(price / contract.tick_size) * contract.tick_size))

        if tif:
            params['timeInForce'] = tif.upper()

        body = await self._request('POST', '/fapi/v1/order', params, signed=True)

        if body:
            return OrderModel.from_binance(body)

    async def cancel_order(self, contract: ContractModel, order_id: int):
        """
        Cancel an active order.
        """
        params = {
            'timestamp': int(time.time() * 1000),
            'symbol': contract.symbol,
            'orderId': order_id,
        }
        body = await self._request('DELETE', '/fapi/v1/order', params, signed=True) or {}
        return OrderModel.from_binance(body)

    async def get_order(self, contract: ContractModel, order_id: int) -> OrderModel:
        """
        Check an order's status.
        """
        params = {
            'timestamp': int(time.time() * 1000),
            'symbol': contract.symbol,
            'orderId': order_id,
        }
        body = await self._request('GET', '/fapi/v1/order', params, signed=True) or {}
        return OrderModel.from_binance(body)

    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None, signed: bool = False, **kwargs):
        if signed and params:
            params['signature'] = self._sign(params)

        try:
            async with self._session.request(
                    method=method,
                    url=self._base_url.with_path(endpoint),
                    headers={'X-MBX-APIKEY': self._public_key},
                    params=params,
                    **kwargs
            ) as resp:
                if resp.status == 200:
                    body = await resp.json()
                    return body

                elif resp.status in (400, 401):
                    body = await resp.json()
                    logging.error(
                        'Bad Request: endpoint="%s"; code=%d; message="%s"',
                        endpoint, body['code'], body['msg']
                    )

                else:
                    logging.error('Server respond with status %d', resp.status)

        except ClientResponseError as err:
            logging.error(err)

    def _sign(self, data: Dict):
        return hmac.new(self._private_key.encode(), urlencode(data).encode(), hashlib.sha256).hexdigest()
