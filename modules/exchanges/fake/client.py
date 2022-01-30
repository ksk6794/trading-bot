import logging
import os
import random
import time
from decimal import Decimal
from typing import Dict, Optional, List

import orjson

from modules.models import ContractModel, CandleModel, AccountModel, OrderModel, BookUpdateModel, DepthModel
from modules.models.exchange import AccountBalanceModel, AccountPositionModel
from modules.models.types import (
    Symbol, Timeframe, Asset, OrderType, OrderSide, TimeInForce,
    OrderId, OrderStatus, PositionSide
)
from modules.exchanges.base import BaseExchangeClient


class FakeExchangeClient(BaseExchangeClient):
    # https://www.binance.com/en/fee/futureFee
    MAKER_FEE = Decimal('0.00016')  # 0.0160% for ≥ 50 BNB
    TAKER_FEE = Decimal('0.0004')  # 0.0400% for ≥ 50 BNB

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Initial state
        self._book: Optional[BookUpdateModel] = None
        self._orders: Dict[OrderId, OrderModel] = {}

        self._balances = {
            Asset('USDT'): AccountBalanceModel(
                asset=Asset('USDT'),
                wallet_balance=Decimal('1000'),
            )
        }
        self._positions = {
            Symbol('BTCUSDT'): AccountPositionModel(
                symbol=Symbol('BTCUSDT'),
                side=PositionSide.BOTH,
                quantity=Decimal('0'),
                entry_price=Decimal('0'),
                leverage=2,
                isolated=True,
            ),
            Symbol('ETHUSDT'): AccountPositionModel(
                symbol=Symbol('ETHUSDT'),
            )
        }

    async def get_account_info(self) -> AccountModel:
        return AccountModel(
            assets={
                asset: AccountBalanceModel(
                    asset=asset,
                    wallet_balance=free,
                )
                for asset, free in self._balances.items()
            },
            positions=self._positions
        )

    async def get_contracts(self) -> Dict[Symbol, ContractModel]:
        symbols = self._read('contracts.json')
        return {contract['symbol']: ContractModel(**contract) for contract in symbols}

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
            price: Decimal = None,
            tif: Optional[TimeInForce] = None
    ) -> OrderModel:
        assert contract.symbol in self._positions

        # TODO: Implement insufficient balance exception
        # TODO: Emulate LIMIT orders placing
        order_id = OrderId(random.randint(10000000, 1000000000))
        book_price = self._book.get_price(order_side)
        amount = book_price * quantity
        fee_stake = self._get_fee_stake(order_type)
        commission = amount * fee_stake

        # set base asset
        self._balances.setdefault(contract.base_asset, Decimal('0'))

        if order_side is OrderSide.BUY:
            # quote asset decreasing
            self._balances[contract.quote_asset] -= (amount + commission)

            # base asset increasing
            self._balances[contract.base_asset] += quantity

        elif order_side is OrderSide.SELL:
            # quote asset increasing
            self._balances[contract.quote_asset] += (amount - commission)

            # base asset decreasing
            self._balances[contract.base_asset] -= quantity

        order = OrderModel(
            id=order_id,
            symbol=contract.symbol,
            quantity=quantity,
            entry_price=amount,
            status=OrderStatus.FILLED,
            type=order_type,
            side=order_side,
            timestamp=int(time.time() * 1000),
        )
        self._orders[order_id] = order

        return order

    async def cancel_order(self, contract: ContractModel, order_id: OrderId):
        pass

    async def get_order(self, contract: ContractModel, order_id: OrderId) -> OrderModel:
        return self._orders[order_id]

    def _get_fee_stake(self, order_type: OrderType):
        if order_type in (OrderType.LIMIT,):
            fee = self.MAKER_FEE

        elif order_type in (OrderType.MARKET,):
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
