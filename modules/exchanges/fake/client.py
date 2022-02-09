import inspect
import logging
import os
import random
import time
from abc import ABC
from decimal import Decimal
from itertools import product
from typing import Dict, Optional, List, Set, Callable

import orjson

from modules.models import ContractModel, CandleModel, AccountModel, OrderModel, BookUpdateModel, DepthModel
from modules.models.exchange import AccountBalanceModel, AccountPositionModel, FundingRateModel
from modules.models.types import (
    Symbol, Timeframe, Asset, OrderType, OrderSide, TimeInForce,
    OrderId, OrderStatus, PositionSide, UserStreamEntity, MarginType
)
from modules.exchanges.base import BaseExchangeClient

__all__ = ('FakeExchangeClient',)


class FakeUserStream:
    def __init__(self):
        self._callbacks: Dict[UserStreamEntity, Set] = {}

    async def connect(self):
        pass

    def add_update_callback(self, entity: UserStreamEntity, cb: Callable):
        self._callbacks.setdefault(entity, set()).add(cb)

    async def trigger_callbacks(self, entity: UserStreamEntity, model):
        callbacks = self._callbacks.get(entity, set())

        for callback in callbacks:
            result = callback(model)

            if inspect.isawaitable(result):
                await result


class FakeExchangeClient(BaseExchangeClient, ABC):
    # https://www.binance.com/en/fee/futureFee
    MAKER_FEE = Decimal('0.0002')  # 0.02% for ≥ 0 BNB
    TAKER_FEE = Decimal('0.0004')  # 0.04% for ≥ 0 BNB

    ASSETS: Dict[Asset, Decimal] = {
        Asset('BTC'): Decimal('0.1'),
        Asset('ETH'): Decimal('1'),
        Asset('DOT'): Decimal('100'),
        Asset('USDT'): Decimal('1000'),
    }
    SYMBOLS: List[Symbol] = [
        Symbol('BTCUSDT'),
        Symbol('ETHUSDT'),
        Symbol('DOTUSDT'),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.user_stream = FakeUserStream()

        # Initial state
        self._book: Optional[BookUpdateModel] = None
        self._orders: Dict[OrderId, OrderModel] = {}
        self._leverage: Dict[Symbol: int] = {symbol: 1 for symbol in self.SYMBOLS}
        self._margin_type: Dict[Symbol, MarginType] = {symbol: MarginType.ISOLATED for symbol in self.SYMBOLS}
        self._hedge_mode = True

        self._assets: Dict[Asset, Decimal] = {**self.ASSETS}
        self._positions: Dict[Symbol, AccountPositionModel] = {
            symbol: AccountPositionModel(
                symbol=symbol,
                side=side,
                quantity=Decimal('0'),
                entry_price=Decimal('0'),
                leverage=1,
                isolated=True,
            ) for symbol, side in product(self.SYMBOLS, PositionSide)
        }

    def set_price(self, price: BookUpdateModel):
        self._book = price

    async def get_account_info(self) -> AccountModel:
        return AccountModel(
            assets={
                asset: AccountBalanceModel(
                    asset=asset,
                    wallet_balance=balance,
                ) for asset, balance in self._assets.items()
            },
            positions=list(self._positions.values()),
        )

    async def get_contracts(self) -> Dict[Symbol, ContractModel]:
        symbols = self._read('contracts.json')
        return {contract['symbol']: ContractModel(**contract) for contract in symbols}

    async def get_funding_rate(self, contract: ContractModel) -> Dict[Symbol, FundingRateModel]:
        pass

    async def change_leverage(self, symbol: Symbol, leverage: int):
        self._leverage[symbol] = leverage

    async def is_hedge_mode(self) -> bool:
        return self._hedge_mode

    async def change_position_mode(self, hedge_mode: bool):
        self._hedge_mode = hedge_mode

    async def change_margin_type(self, symbol: Symbol, margin_type: MarginType):
        self._margin_type[symbol] = margin_type

    async def get_historical_candles(
            self,
            symbol: Symbol,
            timeframe: Timeframe,
            limit: int = 1000,
            start_time: Optional[str] = None
    ) -> List[CandleModel]:
        return []

    async def get_depth(self, symbol: Symbol, limit: int = 1000) -> Optional[DepthModel]:
        return DepthModel(
            last_update_id=0,
        )

    async def place_order(
            self,
            client_order_id: str,
            contract: ContractModel,
            order_type: OrderType,
            quantity: Decimal,
            order_side: OrderSide,
            position_side: PositionSide = PositionSide.BOTH,
            price: Decimal = None,
            tif: Optional[TimeInForce] = None,
    ) -> OrderModel:
        assert contract.symbol in self._positions

        # TODO: Implement insufficient balance exception
        # TODO: Implement positions updates
        # TODO: Emulate LIMIT orders placing
        order_id = OrderId(random.randint(10000000, 1000000000))
        book_price = self._book.get_price(order_side)
        amount = book_price * quantity
        fee_stake = self._get_fee_stake(order_type)
        commission = amount * fee_stake

        if order_side is OrderSide.BUY:
            # quote asset decreasing
            self._assets[contract.quote_asset] -= (amount + commission)

            # base asset increasing
            self._assets[contract.base_asset] += quantity

        elif order_side is OrderSide.SELL:
            # quote asset increasing
            self._assets[contract.quote_asset] += (amount - commission)

            # base asset decreasing
            self._assets[contract.base_asset] -= quantity

        model = AccountModel(
            assets={
                asset: AccountBalanceModel(
                    asset=asset,
                    wallet_balance=balance,
                )
                for asset, balance in self._assets.items()
            },
            positions=list(self._positions.values())
        )
        await self.user_stream.trigger_callbacks(UserStreamEntity.ACCOUNT_UPDATE, model)

        order = OrderModel(
            id=order_id,
            client_order_id=client_order_id,
            symbol=contract.symbol,
            quantity=quantity,
            entry_price=book_price,
            status=OrderStatus.FILLED,
            type=order_type,
            side=order_side,
            position_side=position_side,
            timestamp=int(time.time() * 1000),
        )
        self._orders[order_id] = order

        return order

    async def cancel_order(self, symbol: Symbol, order_id: OrderId):
        pass

    async def get_order(self, contract: ContractModel, order_id: OrderId) -> OrderModel:
        return self._orders[order_id]

    def _get_fee_stake(self, order_type: OrderType):
        if order_type == OrderType.LIMIT:
            fee = self.MAKER_FEE

        elif order_type == OrderType.MARKET:
            fee = self.TAKER_FEE

        else:
            logging.warning(f'Undefined fee for order_type {order_type}')
            fee = Decimal('0')

        return fee

    @staticmethod
    def _read(file_name):
        path = os.path.join(os.path.dirname(__file__), 'mocks', file_name)

        with open(path, encoding='utf-8') as f:
            return orjson.loads(f.read())
