import inspect
import logging
import os
import random
import time
from abc import ABC
from decimal import Decimal
from typing import Dict, Optional, List, Set, Callable
from itertools import chain

import orjson

from helpers import to_decimal_places
from modules.line_client import ReplayClient
from modules.models import ContractModel, CandleModel, AccountModel, OrderModel, BookUpdateModel, DepthModel
from modules.models.exchange import AccountBalanceModel, AccountPositionModel, FundingRateModel
from modules.models.types import (
    Symbol, Timeframe, Asset, OrderType, OrderSide, TimeInForce,
    OrderId, OrderStatus, PositionSide, UserStreamEntity, MarginType, StreamEntity
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
        Asset('BNB'): Decimal('1'),
        Asset('DOT'): Decimal('100'),
        Asset('USDT'): Decimal('1000'),
    }
    SYMBOLS: List[Symbol] = [
        Symbol('BTCUSDT'),
        Symbol('ETHUSDT'),
        Symbol('BNBUSDT'),
        Symbol('DOTUSDT'),
    ]

    def __init__(self, line: ReplayClient, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.line = line
        self.line.add_update_callback(StreamEntity.BOOK, self._on_book_update)

        self.user_stream = FakeUserStream()

        # Initial state
        self._book: Optional[BookUpdateModel] = None
        self._leverage: Dict[Symbol: int] = {symbol: 1 for symbol in self.SYMBOLS}
        self._margin_type: Dict[Symbol, MarginType] = {symbol: MarginType.ISOLATED for symbol in self.SYMBOLS}
        self._hedge_mode = False

        self._orders: Dict[OrderId, OrderModel] = {}
        self._position_orders: Dict[Symbol, Dict[PositionSide, Set[OrderId]]] = {}

        self._assets: Dict[Asset, Decimal] = {**self.ASSETS}
        self._positions: Dict[Symbol, Dict[PositionSide, AccountPositionModel]] = {
            symbol: {
                side: AccountPositionModel(
                    symbol=symbol,
                    side=side,
                    quantity=Decimal('0'),
                    entry_price=Decimal('0'),
                    leverage=1,
                    isolated=True,
                    margin=Decimal('0'),
                ) for side in PositionSide
            } for symbol in self.SYMBOLS
        }
        # TODO: calc size(dynamic)?/margin/liq.price

    async def get_account_info(self) -> AccountModel:
        return AccountModel(
            assets={
                asset: AccountBalanceModel(
                    asset=asset,
                    wallet_balance=balance,
                ) for asset, balance in self._assets.items()
            },
            positions=list(chain.from_iterable([
                symbols.values()
                for symbols in self._positions.values()
            ])),
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

        if not self._hedge_mode:
            raise RuntimeError('Hedge mode must be activated!')

        order_id = OrderId(random.randint(100000000, 10000000000))
        price = self._book.get_price(order_side)
        amount = price * quantity
        fee_stake = self._get_fee_stake(order_type)
        commission = amount * fee_stake
        leverage = self._leverage[contract.symbol]
        position = self._positions[contract.symbol][position_side]

        # In hedge mode
        is_enter = ((position_side is PositionSide.LONG and order_side is OrderSide.BUY) or
                    (position_side is PositionSide.SHORT and order_side is OrderSide.SELL))
        is_exit = ((position_side is PositionSide.LONG and order_side is OrderSide.SELL) or
                   (position_side is PositionSide.SHORT and order_side is OrderSide.BUY))

        if is_enter:
            if self._assets[contract.quote_asset] == 0:
                raise RuntimeError('Insufficient balance!')

            # quote asset decreasing
            self._assets[contract.quote_asset] -= amount / leverage + commission

            # base asset increasing
            self._assets[contract.base_asset] += quantity

        elif is_exit:
            if quantity > position.quantity:
                raise RuntimeError('Invalid quantity!')

            # quote asset increasing
            pnl = position.calc_pnl(self._book, quantity)
            # TODO: calc with avg_leverage!
            self._assets[contract.quote_asset] += position.margin * quantity / position.quantity + pnl - commission

            # base asset decreasing
            self._assets[contract.base_asset] -= quantity

        order = OrderModel(
            id=order_id,
            client_order_id=client_order_id,
            symbol=contract.symbol,
            quantity=quantity,
            entry_price=price,
            status=OrderStatus.FILLED,
            type=order_type,
            side=order_side,
            position_side=position_side,
            timestamp=int(time.time() * 1000),
        )

        self._orders[order_id] = order
        self._position_orders \
            .setdefault(contract.symbol, {}) \
            .setdefault(position_side, set()) \
            .add(order_id)

        if is_enter:
            entry_orders = self._get_entry_orders(position.symbol, position_side)
            total_price = sum([order.quantity * order.entry_price for order in entry_orders])
            total_quantity = sum([order.quantity for order in entry_orders])

            position.quantity += quantity
            position.entry_price = to_decimal_places(total_price / total_quantity, contract.lot_size)
            position.margin += quantity * order.entry_price / leverage

        elif is_exit:
            position.quantity -= quantity
            position.margin -= quantity * position.entry_price / leverage

            if not position.quantity:
                position.margin = 0

            # Cleanup state
            if position.quantity == 0:
                orders_ids = self._position_orders.get(contract.symbol, {}).get(position_side, set())
                self._orders = {
                    order.id: order
                    for order in self._orders.values()
                    if order.id not in orders_ids
                }
                orders_ids.clear()

        account = await self.get_account_info()
        await self.user_stream.trigger_callbacks(UserStreamEntity.ACCOUNT_UPDATE, account)
        await self.user_stream.trigger_callbacks(UserStreamEntity.ORDER_TRADE_UPDATE, order)

        return order

    async def cancel_order(self, symbol: Symbol, order_id: OrderId):
        raise NotImplemented

    async def get_order(self, contract: ContractModel, order_id: OrderId) -> OrderModel:
        return self._orders[order_id]

    def _get_entry_orders(self, symbol: Symbol, position_side: PositionSide):
        entry_side = OrderSide.BUY if position_side is PositionSide.LONG else OrderSide.SELL
        return self._get_orders(symbol, position_side, entry_side)

    def _get_exit_orders(self, symbol: Symbol, position_side: PositionSide):
        exit_side = OrderSide.SELL if position_side is PositionSide.LONG else OrderSide.BUY
        return self._get_orders(symbol, position_side, exit_side)

    def _get_orders(self, symbol: Symbol, position_side: PositionSide, order_side: OrderSide):
        orders_ids = self._position_orders.get(symbol, {}).get(position_side, set())
        return [self._orders[order_id] for order_id in orders_ids if self._orders[order_id].side is order_side]

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

    def _on_book_update(self, model: BookUpdateModel):
        self._book = model
